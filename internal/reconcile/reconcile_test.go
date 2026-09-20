package reconcile

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"http-relay-gateway/internal/config"
	"http-relay-gateway/internal/deploy"
	"http-relay-gateway/internal/deploy/workers"
	"http-relay-gateway/internal/readiness"

	"github.com/rs/zerolog"
)

// --- first bring-up, reuse and drift ---

func TestPassFirstBringUpDeploysAndAdmits(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	rig.sim.setVersion(deploy.RelayVersion)
	client := rig.newPrimaryClient(false) // no deployment on the platform yet

	rig.factory.next = client
	rig.worker.pass()

	if got := rig.factory.deployCount(); got != 1 {
		t.Fatalf("deploys = %d, want 1", got)
	}
	spec := rig.factory.lastDeploy()
	key := deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}
	if spec.Project != deploy.ProjectName("edge-a") {
		t.Errorf("spec.Project = %q, want %q", spec.Project, deploy.ProjectName("edge-a"))
	}
	if spec.Version != deploy.RelayVersion {
		t.Errorf("spec.Version = %q, want %q", spec.Version, deploy.RelayVersion)
	}
	if spec.Token != testRelayKey {
		t.Errorf("spec.Token = %q, want the relay key", spec.Token)
	}
	if spec.Source != workers.MustAssemble(deploy.PlatformVercel) {
		t.Error("spec.Source is not the assembled embedded worker")
	}
	if !rig.reg.IsReady(key) {
		t.Fatal("relay not admitted after a verified first deploy")
	}
	rec, ok := rig.reg.StateOf(key)
	if !ok || rec.State != readiness.StateReady {
		t.Fatalf("state = %+v ok=%t, want ready", rec, ok)
	}
	serving := rig.reg.Serving()
	if len(serving) != 1 || serving[0].URL != client.url || serving[0].Token != testRelayKey {
		t.Fatalf("serving = %+v, want the verified URL+key pair", serving)
	}
	if serving[0].Version != rec.Generation {
		t.Errorf("serving generation %d, want %d", serving[0].Version, rec.Generation)
	}
}

func TestPassReusesVerifiedDeploymentWithoutRedeploy(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	rig.sim.setVersion(deploy.RelayVersion)
	rig.newPrimaryClient(true)

	rig.worker.pass()
	if got := rig.factory.deployCount(); got != 0 {
		t.Fatalf("deploys = %d, want 0 (verified deployment must be reused)", got)
	}
	key := deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}
	if !rig.reg.IsReady(key) {
		t.Fatal("relay not admitted on reuse")
	}

	// A restart equivalent: a fresh registry over the same desired state and
	// sim — re-verify, still no deploy.
	fresh := readiness.New(readiness.Config{Now: rig.clock.Now})
	rig.reg = fresh
	rig.drainer.reg = fresh
	rig.worker = rig.newWorker()
	rig.worker.pass()
	if got := rig.factory.deployCount(); got != 0 {
		t.Fatalf("deploys after restart-equivalent pass = %d, want 0", got)
	}
	if !fresh.IsReady(key) {
		t.Fatal("fresh registry did not re-admit the healthy relay")
	}
}

func TestPassRedeploysOnVersionDrift(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	rig.sim.setVersion("0.0.1-stale")
	rig.newPrimaryClient(true)

	rig.worker.pass()

	if got := rig.factory.deployCount(); got != 1 {
		t.Fatalf("deploys = %d, want 1 (version drift queues a replacement)", got)
	}
	key := deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}
	if !rig.reg.IsReady(key) {
		t.Fatal("relay not re-admitted after the redeploy verified")
	}
}

func TestPassRedeploysOnRelayKeyDriftAndAuthDriftOnly(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformCloudflare, testToken))
	rig.sim.setVersion(deploy.RelayVersion)
	rig.sim.mu.Lock()
	rig.sim.acceptKey = testRelayKey
	rig.sim.mu.Unlock()
	rig.newPrimaryClient(true)

	rig.worker.pass()
	key := deploy.RelayKey{Provider: deploy.PlatformCloudflare, Name: "edge-a"}
	if !rig.reg.IsReady(key) {
		t.Fatal("relay not ready on the first pass")
	}

	// The relay key rotates. The DEPLOYED worker still expects the old key
	// (it was deployed with it), so the forwarded probe now comes back 404 —
	// the fix is a redeploy that injects the new key.
	rig.worker.cfg.RelayKey = func() (string, error) { return "relay-key-2", nil }

	rig.worker.pass()

	if got := rig.factory.deployCount(); got != 1 {
		t.Fatalf("deploys = %d, want 1 (relay key rotation redeploys)", got)
	}
	if spec := rig.factory.lastDeploy(); spec.Token != "relay-key-2" {
		t.Fatalf("redeploy token = %q, want the rotated relay key", spec.Token)
	}
	if got := rig.sim.lastToken(); got != "relay-key-2" {
		t.Fatalf("verification presented relay key %q, want the rotated one", got)
	}
	if !rig.reg.IsReady(key) {
		t.Fatal("relay not re-admitted after the key-rotation redeploy")
	}
}

func TestPassCredentialRotationReusesDeployment(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	rig.sim.setVersion(deploy.RelayVersion)
	rig.newPrimaryClient(true)

	rig.worker.pass()
	if !rig.reg.IsReady(deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}) {
		t.Fatal("relay not ready on the first pass")
	}
	before := rig.factory.forCount()

	// The provider credential rotates; discovery and verification succeed
	// under the new token, so the deployment must be reused, not replaced.
	rig.desired.Relays = []config.Relay{relay("edge-a", deploy.PlatformVercel, "provider-token-2")}
	rig.worker.pass()

	if got := rig.factory.deployCount(); got != 0 {
		t.Fatalf("deploys = %d, want 0 (credential rotation must not redeploy)", got)
	}
	if got := rig.factory.forCount(); got != before+1 {
		t.Fatalf("factory calls = %d, want %d", got, before+1)
	}
	if cred := rig.factory.lastCred(); cred.Token != "provider-token-2" {
		t.Fatalf("discovery credential = %q, want the rotated provider token", cred.Token)
	}
	if !rig.reg.IsReady(deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}) {
		t.Fatal("relay dropped from the pool across a credential rotation")
	}
}

// --- probe classification: what must never deploy ---

func TestPassUnreachableDeploymentNeverDeploys(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	client := rig.newPrimaryClient(true)
	client.url = rig.server.URL + "-closed" // a dead origin: transport errors

	rig.worker.pass()

	if got := rig.factory.deployCount(); got != 0 {
		t.Fatalf("deploys = %d, want 0 (transport failure is never deploy evidence)", got)
	}
	key := deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}
	rec, ok := rig.reg.StateOf(key)
	if !ok || rec.State != readiness.StateFailed {
		t.Fatalf("state = %+v ok=%t, want failed", rec, ok)
	}
	if rec.Reason != readiness.ReasonUnreachable {
		t.Fatalf("reason = %q, want %q", rec.Reason, readiness.ReasonUnreachable)
	}
	if rig.reg.IsReady(key) {
		t.Error("unreachable relay admitted")
	}
}

func TestPassBrokenRoundTripDoesNotRedeploy(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	rig.sim.setVersion(deploy.RelayVersion)
	rig.sim.mu.Lock()
	rig.sim.forward = 502 // the worker forwards; its own upstream fetch fails
	rig.sim.mu.Unlock()
	rig.newPrimaryClient(true)

	rig.worker.pass()

	if got := rig.factory.deployCount(); got != 0 {
		t.Fatalf("deploys = %d, want 0 (the worker is current; a broken round trip is not)", got)
	}
	key := deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}
	rec, _ := rig.reg.StateOf(key)
	if rec.State != readiness.StateFailed || rec.Reason != readiness.ReasonProbeFailed {
		t.Fatalf("state/reason = %s/%s, want failed/probe_failed", rec.State, rec.Reason)
	}
}

func TestPassSuspensionPausesThenRevives(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	rig.newPrimaryClient(true)
	rig.sim.mu.Lock()
	rig.sim.suspend = true // platform answers instead of the worker
	rig.sim.mu.Unlock()

	rig.worker.pass()

	key := deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}
	rec, _ := rig.reg.StateOf(key)
	if rec.State != readiness.StatePaused || rec.Reason != readiness.ReasonPaused {
		t.Fatalf("state/reason = %s/%s, want paused/paused", rec.State, rec.Reason)
	}
	if rig.reg.IsReady(key) {
		t.Fatal("suspended relay admitted")
	}
	if got := rig.factory.deployCount(); got != 0 {
		t.Fatalf("deploys = %d, want 0 (no deploy lifts a suspension)", got)
	}

	// Before the revival cadence elapses the pass must not even probe.
	rig.sim.mu.Lock()
	rig.sim.suspend = false
	rig.sim.version = deploy.RelayVersion
	rig.sim.mu.Unlock()
	rig.worker.pass()
	if got := rig.factory.deployCount(); got != 0 {
		t.Fatalf("deploys = %d, want 0", got)
	}
	if rec, _ := rig.reg.StateOf(key); rec.State != readiness.StatePaused {
		t.Fatalf("state = %s, want still paused before the revival cadence", rec.State)
	}

	// Past the cadence the relay re-probes and rejoins on its own.
	rig.clock.Advance(10 * time.Minute)
	rig.worker.pass()
	if !rig.reg.IsReady(key) {
		t.Fatal("revived relay not re-admitted")
	}
	if got := rig.factory.deployCount(); got != 0 {
		t.Fatalf("deploys across revival = %d, want 0", got)
	}
}

// --- credential problems ---

func TestPassCredentialRejectionNeverDeploys(t *testing.T) {
	for name, setupErr := range map[string]struct {
		forErr      error
		discoverErr error
	}{
		"factory":  {forErr: deploy.ErrCredentials},
		"discover": {discoverErr: fmt.Errorf("vercel: %w", deploy.ErrCredentials)},
		"scope":    {discoverErr: fmt.Errorf("vercel: %w", deploy.ErrAmbiguousScope)},
	} {
		t.Run(name, func(t *testing.T) {
			rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
			client := rig.newPrimaryClient(true)
			client.forErrOverride(setupErr.forErr, setupErr.discoverErr)

			rig.worker.pass()

			if got := rig.factory.deployCount(); got != 0 {
				t.Fatalf("deploys = %d, want 0 (operator data, not a deploy)", got)
			}
			key := deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}
			rec, _ := rig.reg.StateOf(key)
			if rec.State != readiness.StateFailed {
				t.Fatalf("state = %s, want failed", rec.State)
			}
			if rec.Reason != readiness.ReasonCredentials {
				t.Fatalf("reason = %q, want %q", rec.Reason, readiness.ReasonCredentials)
			}
		})
	}
}

// forErrOverride configures the single handed-out client with factory-time
// and discovery-time errors.
func (c *recordedClient) forErrOverride(forErr, discoverErr error) {
	c.f.mu.Lock()
	c.f.forErr = forErr
	c.f.mu.Unlock()
	c.discoverErr = discoverErr
}

// --- no desired state / no relay key ---

func TestPassWithoutDesiredStateDoesNothing(t *testing.T) {
	rig := newTestRig(t)
	rig.desired = nil
	rig.worker.pass()
	if rig.factory.forCount() != 0 {
		t.Fatal("pass touched the platform without desired state")
	}
	if len(rig.reg.Snapshot()) != 0 {
		t.Fatal("pass mutated the registry without desired state")
	}
}

func TestPassKeepsFleetServingWhenRelayKeyUnresolvable(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	rig.sim.setVersion(deploy.RelayVersion)
	rig.newPrimaryClient(true)

	rig.worker.pass()
	key := deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}
	if !rig.reg.IsReady(key) {
		t.Fatal("relay not ready on the first pass")
	}
	forCalls := rig.factory.forCount()

	rig.worker.cfg.RelayKey = func() (string, error) { return "", errors.New("key file missing") }
	rig.worker.pass()

	if !rig.reg.IsReady(key) {
		t.Fatal("the last verified fleet must keep serving when the key cannot be resolved")
	}
	if got := rig.factory.forCount(); got != forCalls {
		t.Fatalf("factory calls = %d, want %d (a key failure must not touch the platform)", got, forCalls)
	}
}

// --- Strategy A: replacement of a serving relay ---

func TestStrategyAOrderDemoteSettleDrainThenDeploy(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	rig.sim.setVersion(deploy.RelayVersion)
	rig.newPrimaryClient(true)
	rig.worker.pass()
	key := deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}
	if !rig.reg.IsReady(key) {
		t.Fatal("relay not ready before the replacement")
	}

	// Version drift on a serving relay: Strategy A — demote, settle the pool
	// swap, drain in-flight, and only then deploy.
	rig.sim.setVersion("0.0.1-stale")
	rig.drainer.setIdle(true) // in-flight requests drain promptly
	rig.worker.pass()

	events := rig.log.all()
	deployAt, drainAt, settleAt := -1, -1, -1
	for i, e := range events {
		switch {
		case e == "deploy" && deployAt < 0:
			deployAt = i
		case len(e) > 5 && e[:5] == "drain" && drainAt < 0:
			drainAt = i
		case e == "settle" && settleAt < 0:
			settleAt = i
		}
	}
	if settleAt < 0 || drainAt < 0 || deployAt < 0 {
		t.Fatalf("missing steps: settle@%d drain@%d deploy@%d in %v", settleAt, drainAt, deployAt, events)
	}
	if settleAt > drainAt {
		t.Errorf("settle must precede the drain: %v", events)
	}
	if drainAt > deployAt {
		t.Errorf("drain must precede the deploy: %v", events)
	}
	if !strings.Contains(events[drainAt], "serving=false") {
		t.Fatalf("drain ran while the old admission still stood: %v", events)
	}
	if rig.drainer.calls[0] != "vercel/edge-a" {
		t.Fatalf("drain identity = %q", rig.drainer.calls[0])
	}
	if rig.drainer.timeouts[0] != 5*time.Second {
		t.Fatalf("drain timeout = %s, want the configured quiesce timeout", rig.drainer.timeouts[0])
	}
	if !rig.reg.IsReady(key) {
		t.Fatal("replacement not re-admitted after verifying")
	}
	if rig.settles < 1 {
		t.Error("settle barrier never ran")
	}
}

func TestStrategyADrainTimeoutDefersReplacement(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	rig.sim.setVersion(deploy.RelayVersion)
	rig.newPrimaryClient(true)
	rig.worker.pass()
	key := deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}

	rig.sim.setVersion("0.0.1-stale")
	rig.drainer.setIdle(false) // a straggler request keeps streaming
	rig.worker.pass()

	if got := rig.factory.deployCount(); got != 0 {
		t.Fatalf("deploys = %d, want 0 (deploying under a live stream is the race this prevents)", got)
	}
	rec, _ := rig.reg.StateOf(key)
	if rec.State != readiness.StateUnready || rec.Reason != readiness.ReasonReplacing {
		t.Fatalf("state/reason = %s/%s, want unready/replacing", rec.State, rec.Reason)
	}
	if rig.reg.IsReady(key) {
		t.Fatal("a demoted relay with a pending replacement must not serve")
	}

	// The stream ends; the backoff gate must not hot-loop, but the next pass
	// after it elapses completes the replacement.
	rig.clock.Advance(2 * time.Minute)
	rig.drainer.setIdle(true)
	rig.worker.pass()

	if got := rig.factory.deployCount(); got != 1 {
		t.Fatalf("deploys = %d, want 1 on the retried pass", got)
	}
	if !rig.reg.IsReady(key) {
		t.Fatal("deferred replacement not completed")
	}
}

func TestStrategyAReplacementVerifyFailureKeepsServingOff(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	rig.sim.setVersion(deploy.RelayVersion)
	client := rig.newPrimaryClient(true)
	rig.worker.pass()
	key := deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}

	// Drift triggers a replacement, but the deploy does NOT heal the sim:
	// the new worker still fails verification. The old worker is gone from
	// that URL — admission must stay revoked.
	rig.sim.setVersion("0.0.1-stale")
	rig.drainer.setIdle(true)
	rig.factory.mu.Lock()
	client.fixSim = false
	rig.factory.mu.Unlock()
	rig.worker.pass()

	if got := rig.factory.deployCount(); got != 1 {
		t.Fatalf("deploys = %d, want 1", got)
	}
	if rig.reg.IsReady(key) {
		t.Fatal("a replacement that failed verification must not keep serving")
	}
	rec, _ := rig.reg.StateOf(key)
	// The lifecycle label after this path is failed (Enter(deploying)
	// replaced the unready label mid-rollout) — what matters is that the
	// relay is out of the pool with the verification failure as the reason.
	if rec.State != readiness.StateFailed && rec.State != readiness.StateUnready {
		t.Fatalf("state = %s, want failed or unready", rec.State)
	}
	if rec.Reason != readiness.ReasonVersionFailed {
		t.Fatalf("reason = %q, want %q", rec.Reason, readiness.ReasonVersionFailed)
	}
	if got := len(rig.reg.Serving()); got != 0 {
		t.Fatalf("serving pool = %d entries, want 0", got)
	}
}

// --- removals and deletes ---

func TestRemoveDeletesRemoteAndPurges(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	rig.sim.setVersion(deploy.RelayVersion)
	rig.newPrimaryClient(true)
	rig.worker.pass()
	key := deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}
	if !rig.reg.IsReady(key) {
		t.Fatal("relay not ready before removal")
	}

	rig.desired.Relays = nil
	rig.worker.pass()

	if got := rig.factory.deleteCount(); got != 1 {
		t.Fatalf("deletes = %d, want 1", got)
	}
	if project := rig.factory.deletes[0]; project != deploy.ProjectName("edge-a") {
		t.Fatalf("deleted project = %q, want %q", project, deploy.ProjectName("edge-a"))
	}
	if _, ok := rig.reg.StateOf(key); ok {
		t.Fatal("completed delete must purge the entry")
	}
	if got := rig.reg.ReadyCount(); got != 0 {
		t.Fatalf("readyCount = %d, want 0 after removal", got)
	}
	if got := len(rig.reg.Serving()); got != 0 {
		t.Fatalf("serving pool = %d, want 0 after removal", got)
	}
}

func TestRemoveDeleteFailureRetriesUnderBackoff(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	rig.sim.setVersion(deploy.RelayVersion)
	client := rig.newPrimaryClient(true)
	client.deleteErr = errors.New("platform 500")
	rig.worker.pass()
	key := deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}

	rig.desired.Relays = nil
	rig.worker.pass()

	if got := rig.factory.deleteCount(); got != 1 {
		t.Fatalf("deletes = %d, want 1", got)
	}
	rec, ok := rig.reg.StateOf(key)
	if !ok || rec.State != readiness.StateRemoving {
		t.Fatalf("state = %+v ok=%t, want removing", rec, ok)
	}
	if rec.Reason == "" {
		t.Error("failed delete left no operator-facing reason")
	}

	// The backoff gate holds the retry; past it the delete retries and
	// succeeds.
	rig.worker.pass()
	if got := rig.factory.deleteCount(); got != 1 {
		t.Fatalf("deletes = %d, want 1 (retry must be backoff-gated)", got)
	}
	rig.clock.Advance(2 * time.Minute)
	client.deleteErr = nil
	rig.worker.pass()
	if got := rig.factory.deleteCount(); got != 2 {
		t.Fatalf("deletes = %d, want 2 (the retry must happen after backoff)", got)
	}
	if _, ok := rig.reg.StateOf(key); ok {
		t.Fatal("successful retry must purge the entry")
	}
}

func TestRemoveDrainTimeoutDefersDelete(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	rig.sim.setVersion(deploy.RelayVersion)
	rig.newPrimaryClient(true)
	rig.worker.pass()
	key := deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}

	rig.desired.Relays = nil
	rig.drainer.setIdle(false)
	rig.worker.pass()

	if got := rig.factory.deleteCount(); got != 0 {
		t.Fatalf("deletes = %d, want 0 (no remote removal under live traffic)", got)
	}
	if rec, ok := rig.reg.StateOf(key); !ok || rec.State != readiness.StateRemoving {
		t.Fatalf("state = %+v ok=%t, want still removing", rec, ok)
	}
	rig.clock.Advance(2 * time.Minute)
	rig.drainer.setIdle(true)
	rig.worker.pass()
	if got := rig.factory.deleteCount(); got != 1 {
		t.Fatalf("deletes = %d, want 1 after the drain succeeds", got)
	}
	if _, ok := rig.reg.StateOf(key); ok {
		t.Fatal("entry must purge after the delete completes")
	}
}

func TestRemoveWithoutMemoizedCredentialPurgesAnyway(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	// No prior pass: no credential on record. Sync the identity, then take
	// it away before any ensure ran.
	rig.reg.Sync(rig.desired.Keys())
	rig.desired.Relays = nil
	rig.worker.pass()

	key := deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}
	if _, ok := rig.reg.StateOf(key); ok {
		t.Fatal("a relay without a memoized credential must still leave the registry")
	}
	if got := rig.factory.forCount(); got != 0 {
		t.Fatalf("factory calls = %d, want 0 (nothing to authenticate with)", got)
	}
	if got := rig.factory.deleteCount(); got != 0 {
		t.Fatalf("deletes = %d, want 0", got)
	}
}

// --- stale delete vs re-add: the incarnation guarantee ---

func TestStaleDeleteBlocksReaddedIdentityUntilOldProjectGone(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	rig.sim.setVersion(deploy.RelayVersion)
	client := rig.newPrimaryClient(true)
	rig.worker.pass()
	key := deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}

	// Take the relay out of the config and run only the delete half of a
	// pass, with the platform delete blocked mid-flight.
	started := make(chan struct{})
	release := make(chan struct{})
	deleteDone := make(chan struct{})
	rig.desired.Relays = nil
	rig.reg.Sync(nil)         // the removal half of a pass: label removing, revoke admission
	rig.drainer.setIdle(true) // the drain succeeds; the platform delete is what blocks
	rig.factory.mu.Lock()
	client.deleteHook = func() {
		close(started)
		<-release
	}
	rig.factory.mu.Unlock()
	go func() {
		defer close(deleteDone)
		rig.worker.runDeletes()
	}()
	<-started

	// While the delete holds the single-flight, the identity cannot begin
	// any deploy — even after being re-added.
	rig.reg.Sync([]deploy.RelayKey{key})
	rec, ok := rig.reg.StateOf(key)
	if !ok || rec.State != readiness.StateConfigured {
		t.Fatalf("re-added state = %+v ok=%t, want configured", rec, ok)
	}
	if _, _, ok := rig.reg.Begin(key); ok {
		t.Fatal("a re-added identity must not deploy while its stale delete is in flight")
	}
	if _, _, ok := rig.reg.BeginDelete(key); ok {
		t.Fatal("a second delete must not start while the first holds the relay")
	}

	// The stale delete completes against the OLD incarnation's generation.
	close(release)
	<-deleteDone

	rec, ok = rig.reg.StateOf(key)
	if !ok {
		t.Fatal("the re-added identity was purged by the stale delete's completion")
	}
	if rec.State != readiness.StateConfigured {
		t.Fatalf("state after stale delete completion = %s, want configured", rec.State)
	}
	if rec.Generation != 3 {
		t.Fatalf("generation = %d, want 3 (initial, removal, re-add)", rec.Generation)
	}

	// Now a normal pass brings the re-added identity up: the OLD project was
	// deleted by the stale delete, so this is a fresh deploy.
	rig.desired.Relays = []config.Relay{relay("edge-a", deploy.PlatformVercel, testToken)}
	rig.factory.mu.Lock()
	client.exists = false
	rig.factory.mu.Unlock()
	rig.worker.pass()

	if got := rig.factory.deployCount(); got < 1 {
		t.Fatalf("deploys = %d, want at least 1 for the re-added identity", got)
	}
	if !rig.reg.IsReady(key) {
		t.Fatal("re-added identity not admitted after its fresh deploy")
	}
	if got := rig.reg.Serving(); len(got) != 1 || got[0].URL == "" {
		t.Fatalf("serving = %+v, want one admitted relay with a verified URL", got)
	}
}

// --- concurrency ---

func TestConcurrentPassesNeverDoubleDeploy(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	rig.sim.setVersion(deploy.RelayVersion)
	rig.newPrimaryClient(false) // nothing deployed yet: exactly one pass may deploy

	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rig.worker.pass()
		}()
	}
	wg.Wait()

	if got := rig.factory.deployCount(); got != 1 {
		t.Fatalf("deploys = %d, want exactly 1 across concurrent passes", got)
	}
	key := deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}
	if !rig.reg.IsReady(key) {
		t.Fatal("relay not admitted after concurrent bring-up")
	}
}

// --- single relay among several: per-relay isolation ---

func TestPassFailsOneRelayWithoutTouchingOthers(t *testing.T) {
	rig := newTestRig(t,
		relay("good-a", deploy.PlatformVercel, testToken),
		relay("bad-b", deploy.PlatformCloudflare, testToken),
	)
	rig.sim.setVersion(deploy.RelayVersion)
	good := rig.newPrimaryClient(true)
	rig.factory.mu.Lock()
	rig.factory.next = nil // per-platform split below
	rig.factory.mu.Unlock()
	bad := &recordedClient{f: rig.factory, platform: deploy.PlatformCloudflare,
		discoverErr: errors.New("cloudflare down")}
	rig.factory.split = map[string]*recordedClient{
		deploy.PlatformVercel:     good,
		deploy.PlatformCloudflare: bad,
	}

	rig.worker.pass()

	if !rig.reg.IsReady(deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "good-a"}) {
		t.Error("healthy relay not admitted")
	}
	rec, _ := rig.reg.StateOf(deploy.RelayKey{Provider: deploy.PlatformCloudflare, Name: "bad-b"})
	if rec.State != readiness.StateFailed {
		t.Fatalf("failing relay state = %s, want failed", rec.State)
	}
	if rig.reg.ReadyCount() != 1 {
		t.Fatalf("readyCount = %d, want 1", rig.reg.ReadyCount())
	}
}

// --- verify classification table (exercised through the probe paths) ---

func TestVerifyClassifiesProbeAnswers(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))

	t.Run("missing worker", func(t *testing.T) {
		rig.sim.mu.Lock()
		rig.sim.version = ""
		rig.sim.suspend = false
		rig.sim.mu.Unlock()
		v := rig.worker.verify(rig.server.URL, testRelayKey)
		if v.kind != verifyMissing {
			t.Fatalf("kind = %d, want verifyMissing", v.kind)
		}
	})
	t.Run("suspended", func(t *testing.T) {
		rig.sim.mu.Lock()
		rig.sim.suspend = true
		rig.sim.mu.Unlock()
		v := rig.worker.verify(rig.server.URL, testRelayKey)
		if v.kind != verifySuspended {
			t.Fatalf("kind = %d, want verifySuspended", v.kind)
		}
	})
	t.Run("version mismatch", func(t *testing.T) {
		rig.sim.mu.Lock()
		rig.sim.suspend = false
		rig.sim.version = "other"
		rig.sim.mu.Unlock()
		v := rig.worker.verify(rig.server.URL, testRelayKey)
		if v.kind != verifyVersionMismatch {
			t.Fatalf("kind = %d, want verifyVersionMismatch", v.kind)
		}
	})
	t.Run("auth failed", func(t *testing.T) {
		rig.sim.mu.Lock()
		rig.sim.version = deploy.RelayVersion
		rig.sim.acceptKey = "someone-elses-key"
		rig.sim.mu.Unlock()
		v := rig.worker.verify(rig.server.URL, testRelayKey)
		if v.kind != verifyAuthFailed {
			t.Fatalf("kind = %d, want verifyAuthFailed", v.kind)
		}
	})
	t.Run("ok", func(t *testing.T) {
		rig.sim.mu.Lock()
		rig.sim.acceptKey = testRelayKey
		rig.sim.mu.Unlock()
		v := rig.worker.verify(rig.server.URL, testRelayKey)
		if v.kind != verifyOK {
			t.Fatalf("kind = %d, want verifyOK", v.kind)
		}
	})
	t.Run("unreachable", func(t *testing.T) {
		v := rig.worker.verify(rig.server.URL+"-dead", testRelayKey)
		if v.kind != verifyUnreachable {
			t.Fatalf("kind = %d, want verifyUnreachable", v.kind)
		}
	})
}

// --- shutdown at the settle barrier ---

// A process that receives its shutdown signal mid-rollout cannot wait on the
// settle barrier: the applier loop has left its select and the barrier will
// never be satisfied. The rollout must abandon instead of hanging Stop(), and
// the demotion it already made must stand — the replacement belongs to the
// next process, whose first pass completes it.
func TestShutdownSettleBarrierAbandonsTheRollout(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	rig.sim.setVersion(deploy.RelayVersion)
	rig.newPrimaryClient(true)
	rig.worker.pass()
	key := deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}

	rig.sim.setVersion("0.0.1-stale")
	rig.drainer.setIdle(true)
	captured := rig.worker.settle
	rig.worker.settle = func() bool { return false } // shutdown: barrier unsatisfiable
	rig.worker.pass()

	if got := rig.factory.deployCount(); got != 0 {
		t.Fatalf("deploys = %d, want 0 (a shutting-down process never deploys)", got)
	}
	rec, _ := rig.reg.StateOf(key)
	if rec.State != readiness.StateUnready || rec.Reason != readiness.ReasonReplacing {
		t.Fatalf("state/reason = %s/%s, want unready/replacing", rec.State, rec.Reason)
	}
	if rig.reg.IsReady(key) {
		t.Fatal("the demotion made before the barrier must stand")
	}

	rig.worker.settle = func() bool { captured(); return true } // restore the live process's barrier
	rig.clock.Advance(2 * time.Minute)                          // past the backoff gate the demotion armed
	rig.worker.pass()

	if got := rig.factory.deployCount(); got != 1 {
		t.Fatalf("deploys = %d, want 1 on the next process's first pass", got)
	}
	if !rig.reg.IsReady(key) {
		t.Fatal("deferred replacement not completed")
	}
}

// The same shutdown discipline for deletes: a relay left labeled removing
// when the barrier goes unsatisfiable keeps its admission revoked, the delete
// is retried from scratch by the next pass, and the entry purges on success.
func TestShutdownSettleBarrierAbandonsTheDelete(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	rig.sim.setVersion(deploy.RelayVersion)
	rig.newPrimaryClient(true)
	rig.worker.pass()
	key := deploy.RelayKey{Provider: deploy.PlatformVercel, Name: "edge-a"}

	rig.desired.Relays = nil
	captured := rig.worker.settle
	rig.worker.settle = func() bool { return false } // shutdown: barrier unsatisfiable
	rig.worker.pass()

	if got := rig.factory.deleteCount(); got != 0 {
		t.Fatalf("deletes = %d, want 0 (a shutting-down process never deletes)", got)
	}
	rec, ok := rig.reg.StateOf(key)
	if !ok || rec.State != readiness.StateRemoving {
		t.Fatalf("state = %q (ok=%t), want removing", rec.State, ok)
	}
	if rig.reg.IsReady(key) {
		t.Fatal("a removing relay must not serve")
	}

	rig.worker.settle = func() bool { captured(); return true } // restore the live process's barrier
	rig.worker.pass()

	if got := rig.factory.deleteCount(); got != 1 {
		t.Fatalf("deletes = %d, want 1 on the retried pass", got)
	}
	if _, ok := rig.reg.StateOf(key); ok {
		t.Fatal("completed delete must purge the entry")
	}
}

// A scope pin (team/account) change under a stable relay identity moves the
// identity to a different platform scope: the pass deploys fresh there and
// the old scope's deployment is unreachable from the desired state forever.
// The operator gets a warning — once per actual change, not per pass.
func TestScopePinChangeWarnsAboutTheOrphanedDeployment(t *testing.T) {
	rig := newTestRig(t, relay("edge-a", deploy.PlatformVercel, testToken))
	rig.sim.setVersion(deploy.RelayVersion)
	rig.newPrimaryClient(true)
	var buf bytes.Buffer
	rig.worker.log = zerolog.New(&buf)
	rig.worker.pass()

	if strings.Contains(buf.String(), "orphaned") {
		t.Fatalf("first memo must not warn: %s", buf.String())
	}
	changed := relay("edge-a", deploy.PlatformVercel, testToken)
	changed.Team = "team-b"
	*rig.desired = config.Config{Relays: []config.Relay{changed}, Settings: config.DefaultSettings()}
	rig.worker.pass()
	if !strings.Contains(buf.String(), "orphaned") {
		t.Fatalf("scope change without an orphan warning: %s", buf.String())
	}

	buf.Reset()
	rig.worker.pass()
	if buf.Len() != 0 {
		t.Fatalf("an unchanged scope must stay silent, got: %s", buf.String())
	}
}
