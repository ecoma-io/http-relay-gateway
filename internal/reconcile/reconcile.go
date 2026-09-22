// Package reconcile is the desired-state worker: it keeps the remote relay
// fleet in step with the configuration file and keeps the readiness registry
// honest. One pass does the whole job —
//
//	sync the registry with the desired relay set,
//	delete the remotes of relays that left the configuration,
//	for every desired relay: discover its deployment, probe it, and
//	reuse, redeploy or pause accordingly.
//
// Readiness is the admission gate. A relay enters the serving pool only
// through registry.Ready — reached only after a positive end-to-end
// verification: the deployment answers with this binary's worker version,
// accepts the relay key, and a relay-spec request forwarded back through the
// relay's own origin round-trips. Everything else — unreachable, wrong
// version, rejected key, platform suspension — keeps the relay out (or
// removes it, past the demote streak).
//
// A replacement of a serving relay follows Strategy A, in this exact order:
// demote the old admission, wait for the pool swap (the settle barrier),
// wait for the old incarnation's in-flight requests to drain, and only then
// deploy. The order is forced by the platforms: a project has one production
// URL, and deploy switches what answers on it — so an old admission left
// standing would serve whatever lands on that URL next, verified or not.
//
// Probes never trigger deploys on transport failures: an unreachable relay
// may be a cold start or a network blip, and one missed answer is never
// evidence the worker is outdated. An HTTP answer that is not the worker at
// all splits by shape: a platform suspension page pauses the relay (no
// deploy lifts a suspension — the revival cadence re-probes until the worker
// answers again), while a missing worker queues a redeploy.
package reconcile

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"http-relay-gateway/internal/config"
	"http-relay-gateway/internal/deploy"
	"http-relay-gateway/internal/deploy/workers"
	"http-relay-gateway/internal/readiness"
	"http-relay-gateway/internal/sanitize"
)

// Concurrency bounds. Probes are cheap and fan out wide; deploys serialize
// per platform (an account's API rate limits are the constraint) so a large
// fleet still heals quickly without tripping those limits; deletes are rare
// and bounded like deploys.
const (
	verifyParallelism = 4
	deleteParallelism = 2
	probeTimeout      = 10 * time.Second
)

// DefaultQuiesceTimeout bounds how long a rollout waits for an old
// incarnation's in-flight requests before giving up on this pass. A request
// that outlives it (a stalled stream with no response-header timeout) holds
// the replacement off until it finishes; the relay stays demoted and the
// next pass retries the drain.
const DefaultQuiesceTimeout = 30 * time.Second

// Drainer is the gateway's per-identity in-flight view. A rollout or delete
// must wait for an old incarnation's requests before touching its remote.
// ctx ends the wait early: it carries the whole-process shutdown budget, so
// a drain can never push shutdown past the grace.
type Drainer interface {
	AwaitIdle(ctx context.Context, provider, name string, timeout time.Duration) bool
}

// Config wires the worker to its collaborators. Every function is re-read
// per pass so configuration reloads take effect without a restart.
type Config struct {
	// Desired returns the current desired state; nil before the first
	// successful load (the file may not exist yet). The watcher wakes the
	// worker when it changes.
	Desired func() *config.Config
	// RelayKey resolves the global relay API key for this pass (environment
	// or key file — the file is re-read, so rotation needs no restart).
	RelayKey func() (string, error)
	// Factory builds platform clients from credentials.
	Factory deploy.Factory
	// Registry is the admission gate this worker drives.
	Registry *readiness.Registry
	// Drainer awaits the gateway's in-flight requests per relay identity.
	Drainer Drainer
	// Settle blocks until every registry notification emitted before the
	// call has been applied to the serving pool. It is the barrier between
	// demote and drain: without it a rollout could deploy under traffic the
	// pool swap had not yet stopped routing. It returns false when the
	// process is shutting down — the applier has left its loop, the barrier
	// will never be satisfied, and the caller must abandon the operation
	// instead of waiting forever.
	Settle func() bool
	// Log receives the worker's operational events.
	Log zerolog.Logger
	// QuiesceTimeout bounds the drain wait; zero takes the default.
	QuiesceTimeout time.Duration
}

// Worker runs the fleet loop: a pass at start, a pass whenever the
// configuration changes, and a pass on the verify tick.
type Worker struct {
	cfg           Config
	reg           *readiness.Registry
	drainer       Drainer
	settle        func() bool
	log           zerolog.Logger
	probeHTTP     *http.Client
	platformLocks map[string]*sync.Mutex
	credMu        sync.Mutex
	// credentials remembers each desired relay's provider credential so a
	// relay that later leaves the configuration can still have its remote
	// deleted. In-memory by design: after a restart the registry starts
	// empty and holds only config-listed relays, so a removing entry never
	// lacks its memo unless the configuration vanished mid-run — and then
	// the remote is left behind deliberately rather than guessed at.
	credentials map[deploy.RelayKey]deploy.Credential

	wake   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	// The shutdown budget and the drain tree. Stop publishes the budget so
	// drains that start from then on clamp their wait to what remains of
	// it, and cancels the drain tree at the budget's deadline so a wait
	// already parked inside AwaitIdle — which cannot re-read the published
	// deadline — still ends inside the budget.
	budgetMu    sync.Mutex
	budget      context.Context
	drainCtx    context.Context
	drainCancel context.CancelFunc
}

// Start launches the worker. It runs one pass immediately and returns.
func Start(cfg Config) *Worker {
	if cfg.QuiesceTimeout <= 0 {
		cfg.QuiesceTimeout = DefaultQuiesceTimeout
	}
	settle := cfg.Settle
	if settle == nil {
		settle = func() bool { return true }
	}
	ctx, cancel := context.WithCancel(context.Background())
	drainCtx, drainCancel := context.WithCancel(context.Background())
	locks := map[string]*sync.Mutex{}
	for _, platform := range deploy.Platforms() {
		locks[platform] = &sync.Mutex{}
	}
	w := &Worker{
		cfg:           cfg,
		reg:           cfg.Registry,
		drainer:       cfg.Drainer,
		settle:        settle,
		log:           cfg.Log,
		probeHTTP:     deploy.ProbeClient(probeTimeout),
		platformLocks: locks,
		credentials:   map[deploy.RelayKey]deploy.Credential{},
		wake:          make(chan struct{}, 1),
		ctx:           ctx,
		cancel:        cancel,
		done:          make(chan struct{}),
		budget:        context.Background(),
		drainCtx:      drainCtx,
		drainCancel:   drainCancel,
	}
	go w.loop()
	return w
}

// Wake triggers an immediate pass — the configuration watcher calls it when
// the desired state changed.
func (w *Worker) Wake() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Stop cancels the worker and waits for the in-flight pass to unwind,
// bounded by ctx — the whole-process shutdown budget. A deploy caught
// mid-flight is abandoned; every deploy is idempotent, so the next start
// simply retries from discovery. A drain parked in AwaitIdle keeps waiting
// inside the budget and is cancelled at its deadline, so the reconciler can
// never push shutdown past the grace the orchestrator honors; if the pass
// still has not unwound by then, the process exit closes whatever remains.
func (w *Worker) Stop(ctx context.Context) {
	w.budgetMu.Lock()
	w.budget = ctx
	w.budgetMu.Unlock()
	w.cancel()
	if dl, ok := ctx.Deadline(); ok {
		time.AfterFunc(time.Until(dl), w.drainCancel)
	}
	select {
	case <-w.done:
	case <-ctx.Done():
	}
}

// awaitIdle bounds one drain wait by the quiesce timeout and by whatever
// remains of the shutdown budget Stop published — the whole drain, the
// reconciler's share included, must fit inside the process's grace. The
// wait itself runs on the drain tree, which Stop cancels at the budget's
// deadline.
func (w *Worker) awaitIdle(provider, name string) bool {
	w.budgetMu.Lock()
	budget := w.budget
	w.budgetMu.Unlock()
	timeout := w.cfg.QuiesceTimeout
	if dl, ok := budget.Deadline(); ok {
		if remain := time.Until(dl); remain < timeout {
			timeout = remain
		}
	}
	return w.drainer.AwaitIdle(w.drainCtx, provider, name, timeout)
}

// loop runs a pass at start, on every configuration change, and on the
// verify tick. The tick interval is re-read per iteration so a settings
// reload takes effect without a restart.
func (w *Worker) loop() {
	defer close(w.done)
	w.pass()
	for {
		interval := config.DefaultVerifyInterval
		if cfg := w.cfg.Desired(); cfg != nil && cfg.Settings.VerifyInterval > 0 {
			interval = cfg.Settings.VerifyInterval
		}
		timer := time.NewTimer(interval)
		select {
		case <-w.wake:
			timer.Stop()
			w.pass()
		case <-timer.C:
			w.pass()
		case <-w.ctx.Done():
			timer.Stop()
			return
		}
	}
}

// pass is one full reconciliation sweep: sync, deletes, then the per-relay
// discovery/verify/fix fan-out. Every per-relay attempt is backoff-gated by
// the registry (Allow), so a failing relay is retried on the backoff
// schedule while healthy ones re-verify every tick.
func (w *Worker) pass() {
	cfg := w.cfg.Desired()
	if cfg == nil {
		return // no desired state yet; the watcher wakes us when it appears
	}
	relayKey, err := w.cfg.RelayKey()
	if err != nil {
		// Without the relay key nothing can be verified or deployed. The
		// last verified fleet keeps serving untouched; this pass only logs.
		w.logError(err, "pass: resolve relay key; keeping the last verified fleet")
		return
	}
	w.memoCredentials(cfg)
	w.reg.Sync(cfg.Keys())
	w.runDeletes()

	keys := cfg.Keys()
	run(verifyParallelism, len(keys), func(i int) {
		rel, ok := cfg.Relay(keys[i].Provider, keys[i].Name)
		if !ok {
			return // removed from config mid-pass; Sync already handles it
		}
		w.ensureRelay(rel, relayKey)
	})
}

// memoCredentials records the current credentials for later deletes.
func (w *Worker) memoCredentials(cfg *config.Config) {
	w.credMu.Lock()
	defer w.credMu.Unlock()
	for _, rel := range cfg.Relays {
		token, err := rel.ResolveToken()
		if err != nil {
			continue // ensureRelay reports the credential problem for this relay
		}
		key := deploy.RelayKey{Provider: rel.Provider, Name: rel.Name}
		cred := deploy.Credential{Token: token, Team: rel.Team, Account: rel.Account,
			Organization: rel.Organization}
		if prev, ok := w.credentials[key]; ok && !prev.SameScope(cred) {
			// The scope pin moved under a stable identity: this pass
			// discovers and deploys in the NEW scope, and the deployment the
			// old scope hosted is orphaned there — no later pass can ever
			// reach or delete it again. The operator removes it by hand.
			w.log.Warn().Str("provider", rel.Provider).Str("relay", rel.Name).
				Msg("relay scope changed; the deployment in the previous scope is orphaned and must be removed manually")
		}
		w.credentials[key] = cred
	}
}

// ensureRelay drives one relay toward verified readiness: discover its
// deployment, probe it, and classify — reuse it as-is, replace it (version
// or key drift, missing worker), pause it (platform suspension), or mark it
// failing (unreachable, broken round trip). Single-flighted per relay.
func (w *Worker) ensureRelay(rel config.Relay, relayKey string) {
	key := deploy.RelayKey{Provider: rel.Provider, Name: rel.Name}
	if !w.reg.Allow(key) {
		return // backoff-gated: keep the pass off a failing relay
	}
	if rec, ok := w.reg.StateOf(key); !ok || rec.State == readiness.StateRemoving {
		return // the delete path owns this identity
	}
	release, gen, ok := w.reg.Begin(key)
	if !ok {
		return // another pass is already deploying, verifying or deleting it
	}
	defer release()
	if rec, ok := w.reg.StateOf(key); !ok || rec.State == readiness.StateRemoving {
		return // removed between Allow and Begin; Sync's delete path owns it
	}
	token, err := rel.ResolveToken()
	if err != nil {
		w.reg.Failing(key, gen, readiness.ReasonCredentials, 0)
		w.logError(err, "ensure: resolve provider credential")
		return
	}
	client, err := w.cfg.Factory.For(rel.Provider,
		deploy.Credential{Token: token, Team: rel.Team, Account: rel.Account,
			Organization: rel.Organization})
	if err != nil {
		w.reg.Failing(key, gen, readiness.ReasonCredentials, 0)
		w.logError(err, "ensure: build platform client")
		return
	}
	project := deploy.ProjectName(rel.Name)
	wasReady := w.reg.IsReady(key)
	// A replacement an earlier pass deferred at the drain timeout keeps an
	// incarnation-scoped marker; a pass that resumes it must settle and drain
	// again before deploying. The marker — unlike the (state, reason) pair,
	// which Failing and Pause rewrite between passes — survives that churn
	// until the barrier completes or the incarnation changes.
	drainPending := false
	if rec, ok := w.reg.StateOf(key); ok {
		drainPending = rec.DrainPending
	}

	w.reg.Enter(key, gen, readiness.StateDiscovering, "")
	disc, err := client.Discover(w.ctx, project)
	if err != nil {
		switch {
		case errors.Is(err, deploy.ErrCredentials), errors.Is(err, deploy.ErrAmbiguousScope):
			// The credential is rejected or its scope is ambiguous: operator
			// data, not a gateway fault — and never healed by a deploy.
			w.reg.Failing(key, gen, readiness.ReasonCredentials, 0)
		default:
			w.reg.Failing(key, gen, readiness.ReasonUnreachable, 0)
		}
		w.logError(err, "ensure: discover deployment")
		return
	}
	if !disc.Exists {
		// No deployment for this identity: a first bring-up, or the remote
		// was deleted out-of-band. Either way the fix is a deploy.
		w.reg.Enter(key, gen, readiness.StateDiscovered, readiness.ReasonMissing)
		w.rollout(rel, key, gen, relayKey, client, project, wasReady, drainPending)
		return
	}

	// The deployment exists: probe it. When it verifies as-is it is reused —
	// this is the path a rotated provider credential takes (discovery and
	// the probe both succeed under the new token, no redeploy), and the path
	// every healthy tick takes.
	v := w.verify(disc.URL, relayKey)
	switch v.kind {
	case verifyOK:
		w.reg.Ready(key, gen, disc.URL, relayKey, v.dur)
	case verifyVersionMismatch, verifyAuthFailed, verifyMissing:
		// Drift a deploy fixes: stale worker version, rejected relay key
		// (rotate the key → every relay redeploys), or no worker behind the
		// URL at all.
		w.rollout(rel, key, gen, relayKey, client, project, wasReady, drainPending)
	case verifySuspended:
		// The platform answers instead of the worker — a suspension page.
		// No deploy lifts it: pause, and let the revival cadence re-probe.
		w.reg.Pause(key, gen, readiness.ReasonPaused)
		w.logWarn("relay paused: the platform answers instead of the worker", key)
	case verifyUnreachable:
		w.reg.Failing(key, gen, readiness.ReasonUnreachable, v.dur)
	case verifyProbeFailed:
		w.reg.Failing(key, gen, readiness.ReasonProbeFailed, v.dur)
	}
}

// rollout deploys the embedded worker for one relay and admits it only
// after the new worker verifies end to end. When the relay was serving, the
// rollout is Strategy A: demote, settle the pool swap, drain in-flight
// requests — and only then deploy, because the platform switches the
// production URL mid-deploy and an old admission left standing would serve
// whatever lands on that URL next. The settle+drain gate also covers a
// replacement an earlier pass deferred at the drain timeout (the incarnation-
// scoped drainPending marker): the retry pass re-runs both before deploying —
// the same discipline the delete path applies on every retry.
func (w *Worker) rollout(rel config.Relay, key deploy.RelayKey, gen uint64, relayKey string, client deploy.Client, project string, wasReady, drainPending bool) {
	// Gating the drain on wasReady alone skips it on the retry pass — the
	// first pass already demoted the relay, so IsReady is false from there
	// on — and the deploy would fire under the very straggler the ordering
	// exists to protect.
	if wasReady {
		w.reg.Demote(key, gen, readiness.ReasonReplacing)
	}
	if wasReady || drainPending {
		if !w.settle() {
			// The process is shutting down and the applier will never
			// satisfy the barrier. The demotion stands; the replacement is
			// the next process's first pass. Return so Stop() can finish.
			w.logWarn("rollout: shutting down at the settle barrier; replacement deferred", key)
			return
		}
		if !w.awaitIdle(key.Provider, key.Name) {
			// A straggler request still streams through the old incarnation;
			// deploying now would cut its upstream mid-flight. The relay
			// stays demoted (its URL is about to change — serving it would
			// be the exact race this design removes) and the next pass
			// retries the drain. The marker carries the deferral across
			// any Failing/Pause relabeling in between.
			w.reg.Enter(key, gen, readiness.StateUnready, readiness.ReasonReplacing)
			w.reg.DeferDrain(key, gen)
			w.logWarn("rollout: in-flight drain timed out; replacement deferred to the next pass", key)
			return
		}
		// The barrier passed and the old incarnation is quiet. Deploy
		// retries from here no longer re-drain: admission stays revoked, so
		// in-flight traffic can only shrink.
		w.reg.DrainCompleted(key, gen)
	}
	w.reg.Enter(key, gen, readiness.StateDeploying, "")
	mu := w.platformLocks[rel.Provider]
	mu.Lock()
	result, err := client.Deploy(w.ctx, deploy.Spec{
		Project: project,
		Source:  workers.MustAssemble(rel.Provider),
		Version: deploy.RelayVersion,
		Token:   relayKey,
	})
	mu.Unlock()
	if err != nil {
		// Platform-side failure. A replaced relay was demoted up front (the
		// deploy would have switched its production URL); a first deploy
		// never had admission. Failing arms the backoff gate for the retry —
		// and a previously serving relay keeps its unready label.
		w.reg.Failing(key, gen, readiness.ReasonDeployFailed, 0)
		w.logError(err, "rollout: deploy failed")
		return
	}
	v := w.verify(result.URL, relayKey)
	if v.kind == verifySuspended {
		// The deploy switched the production URL and the platform suspended
		// the project behind it (a re-suspension during a catch-up deploy of
		// a previously paused relay). No deploy lifts a suspension — pause on
		// the revival cadence; a demotion would label it failed with the
		// ordinary backoff armed, re-probing a suspended platform every few
		// seconds instead.
		w.reg.Pause(key, gen, readiness.ReasonPaused)
		w.logWarn("rollout: new worker answered with a suspension page; paused on the revival cadence", key)
		return
	}
	if v.kind != verifyOK {
		// The platform switched the production URL and the new worker cannot
		// be trusted. The old worker no longer exists behind that URL, so
		// admission must stay revoked — Demote directly, never the blip
		// streak: a failing verification may not keep serving.
		w.reg.Demote(key, gen, reasonFor(v))
		w.logError(errVerdict(v), "rollout: new worker failed verification; kept out of the pool")
		return
	}
	w.reg.Ready(key, gen, result.URL, relayKey, v.dur)
	w.log.Info().Str("provider", key.Provider).Str("relay", key.Name).
		Str("duration", v.dur.Round(time.Millisecond).String()).
		Msg("rollout: relay verified and live")
}

// runDeletes deletes the remote deployments of relays that left the desired
// configuration. Each delete is generation-protected: BeginDelete only
// succeeds while the identity is still labeled removing, the completion is
// discarded when the identity was re-added meanwhile, and the single-flight
// hold spans the whole platform call so a re-added identity cannot deploy
// until the stale delete has finished deleting the OLD project. A 404 is
// success: the desired end state is "the remote does not exist".
func (w *Worker) runDeletes() {
	var removing []readiness.Key
	for _, rec := range w.reg.Snapshot() {
		if rec.State == readiness.StateRemoving {
			removing = append(removing, rec.Key)
		}
	}
	run(deleteParallelism, len(removing), func(i int) {
		key := removing[i]
		if !w.reg.Allow(key) {
			return // a failed delete retries on the backoff schedule
		}
		release, gen, ok := w.reg.BeginDelete(key)
		if !ok {
			return // re-added (no longer removing) or already being deleted
		}
		defer release()
		// The removal already revoked admission; wait for the swap and the
		// drain, so no in-flight request is cut when the remote disappears.
		if !w.settle() {
			// Shutting down: leave the relay labeled removing — the delete
			// retries from scratch in the next process (a fresh registry
			// never carries pending deletes).
			w.log.Warn().Str("provider", key.Provider).Str("relay", key.Name).
				Msg("delete: shutting down at the settle barrier; retry belongs to the next process")
			return
		}
		if !w.awaitIdle(key.Provider, key.Name) {
			w.reg.DeleteFailed(key, gen, "in-flight requests still draining")
			return
		}
		cred, ok := w.credentialFor(key)
		if !ok {
			w.log.Warn().Str("provider", key.Provider).Str("relay", key.Name).
				Msg("delete: no credential on record; removing from the fleet, remote left behind")
			w.reg.DeleteDone(key, gen)
			return
		}
		client, err := w.cfg.Factory.For(key.Provider, cred)
		if err != nil {
			w.reg.DeleteFailed(key, gen, sanitize.ErrorString(err))
			return
		}
		if err := client.Delete(w.ctx, deploy.ProjectName(key.Name)); err != nil {
			// A 404 already surfaced as success inside the client; anything
			// else keeps the relay labeled removing for a backoff-gated retry.
			w.reg.DeleteFailed(key, gen, sanitize.ErrorString(err))
			w.logError(err, "delete: remote delete failed; will retry")
			return
		}
		w.reg.DeleteDone(key, gen)
		w.log.Info().Str("provider", key.Provider).Str("relay", key.Name).
			Msg("delete: remote deployment removed")
	})
}

func (w *Worker) credentialFor(key deploy.RelayKey) (deploy.Credential, bool) {
	w.credMu.Lock()
	defer w.credMu.Unlock()
	cred, ok := w.credentials[key]
	return cred, ok
}

// verdictKind classifies one readiness verification of a deployment URL.
type verdictKind uint8

const (
	verifyOK              verdictKind = iota // end-to-end round trip passed
	verifyUnreachable                        // no HTTP answer at all
	verifySuspended                          // the platform answered instead of the worker
	verifyMissing                            // answered, but not our worker
	verifyVersionMismatch                    // our worker, stale version
	verifyAuthFailed                         // our worker, relay key rejected
	verifyProbeFailed                        // our worker, round trip did not complete
)

// verdict is the outcome of one verification: the classification, how long
// it took (the registry feeds it to its bookkeeping), and a sanitized,
// operator-facing detail string. No relay URL or key ever reaches detail.
type verdict struct {
	kind   verdictKind
	dur    time.Duration
	detail string
}

// reasonFor maps a verdict to the readiness failure reason.
func reasonFor(v verdict) string {
	switch v.kind {
	case verifyUnreachable:
		return readiness.ReasonUnreachable
	case verifySuspended:
		return readiness.ReasonPaused
	case verifyMissing:
		return readiness.ReasonMissing
	case verifyVersionMismatch:
		return readiness.ReasonVersionFailed
	case verifyAuthFailed:
		return readiness.ReasonAuthFailed
	default:
		return readiness.ReasonProbeFailed
	}
}

// verify proves one deployment end to end, on the no-proxy transport the
// data plane itself uses: a version answer from the worker, then a
// relay-spec request forwarded back through the relay's own origin with the
// relay key accepted. The relay's origin is a controlled upstream — the
// worker this binary ships — which is what makes a self-origin probe an
// honest end-to-end proof (documented limitation: it does not exercise the
// caller's real target).
func (w *Worker) verify(url, relayKey string) verdict {
	start := time.Now()
	answer, err := deploy.Probe(w.ctx, w.probeHTTP, url)
	if err != nil {
		return verdict{verifyUnreachable, time.Since(start), sanitize.ErrorString(err)}
	}
	if answer.Suspended() {
		return verdict{verifySuspended, time.Since(start), sanitize.ErrorString(answer.NotWorkerErr())}
	}
	if answer.Version == "" {
		return verdict{verifyMissing, time.Since(start), sanitize.ErrorString(answer.NotWorkerErr())}
	}
	if answer.Version != deploy.RelayVersion {
		return verdict{verifyVersionMismatch, time.Since(start),
			"gateway worker version " + deploy.RelayVersion + ", relay reports " + answer.Version}
	}
	fwd, err := deploy.ForwardProbe(w.ctx, w.probeHTTP, url, relayKey)
	if err != nil {
		return verdict{verifyUnreachable, time.Since(start), sanitize.ErrorString(err)}
	}
	switch {
	case fwd.Status == http.StatusNotFound:
		// The worker answered 404 — its relay key check rejected the key.
		// Deploying the current key heals this, so it queues a replacement.
		return verdict{verifyAuthFailed, time.Since(start), "relay key rejected"}
	case fwd.Status < 500 && fwd.JSON:
		return verdict{verifyOK, time.Since(start), ""}
	default:
		// The worker forwarded but the round trip failed on its side (its
		// own fetch of the probe target answered 5xx). No redeploy: the
		// worker is current; retry on the next tick.
		return verdict{verifyProbeFailed, time.Since(start),
			"end-to-end probe did not round-trip (relay answered HTTP " +
				http.StatusText(fwd.Status) + ")"}
	}
}

// run executes fn(0)…fn(n-1) with at most width calls in flight.
func run(width, n int, fn func(i int)) {
	if n == 0 {
		return
	}
	sem := make(chan struct{}, width)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			fn(i)
		}()
	}
	wg.Wait()
}

func (w *Worker) logError(err error, msg string) {
	w.log.Error().Str("error", sanitize.ErrorString(err)).Msg(msg)
}

func (w *Worker) logWarn(msg string, key deploy.RelayKey) {
	w.log.Warn().Str("provider", key.Provider).Str("relay", key.Name).Msg(msg)
}

// errVerdict renders a failed verification as an error for the structured
// log — the detail string is already sanitized.
func errVerdict(v verdict) error {
	return errors.New(v.detail)
}
