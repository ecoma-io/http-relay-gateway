// Package reconcile keeps the managed relay fleet in step with the gateway:
// it proves every relay's readiness end to end, marks drift, and redeploys
// the embedded worker when a relay's deployment no longer matches.
//
// Readiness is the admission gate: a relay enters the serving pool only after
// a positive verification — its deployment answers with the gateway's worker
// version, the relay key is accepted, and a relay-spec request forwarded back
// through the relay's own origin round-trips. The gate lives in
// internal/readiness (in-memory, derived runtime state); the database stays
// the single source of configuration truth. A relay that fails verification
// stays out (or leaves the pool after a bounded streak), and the always-on
// verify tick re-proves every relay so recovery never needs a restart.
//
// One worker goroutine owns all fleet work. Mutations land on a coalesced
// queue — a flood of requests for the same action collapses into one — and
// the worker drains whole batches, probing in parallel but deploying with a
// small, per-platform-serialized pool. Probes never trigger deploys on
// transport failures: an unreachable relay may be a cold start or a network
// blip, and one missed answer is never evidence the worker is outdated.
//
// An HTTP answer that is not the worker at all is the platform speaking —
// typically a suspension page after free-quota exhaustion. No deploy can
// lift that, so such relays are marked paused and pulled from the pool, and
// an always-on revival scan re-probes them on its own cadence until the
// worker answers again; only then do they rejoin (or queue a catch-up
// redeploy if the fleet version moved on while they were dark).
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"http-relay-gateway/internal/deploy"
	"http-relay-gateway/internal/deploy/workers"
	"http-relay-gateway/internal/readiness"
	"http-relay-gateway/internal/sanitize"
	"http-relay-gateway/internal/store"
)

// probeParallelism bounds concurrent verification probes; redeploys run two
// at a time so a large fleet heals quickly without hammering one platform's
// API. reviveScanInterval is how often the revival scan re-probes paused
// relays — independent of the reconcile interval, which defaults to off —
// and verifyScanInterval is how often every relay's readiness is re-proven,
// the always-on assurance that demotions recover without a restart.
const (
	probeParallelism   = 4
	deployParallelism  = 2
	reviveScanInterval = 10 * time.Minute
	verifyScanInterval = 60 * time.Second
)

// VerifyScanIntervalEnv overrides the readiness re-probe cadence. It is a
// test-only knob like ReviveScanIntervalEnv — the black-box suite shrinks the
// minute-default so a relay's admission flips are observable in seconds — and
// it never appears in the API or the database.
const VerifyScanIntervalEnv = "RELAY_VERIFY_INTERVAL"

// jobKind names one unit of fleet work on the queue.
type jobKind string

const (
	jobCheck     jobKind = "check"     // probe everything, update statuses
	jobStartup   jobKind = "startup"   // verify, then queue redeploys for drift
	jobVerify    jobKind = "verify"    // re-prove every relay's readiness
	jobReconcile jobKind = "reconcile" // verify, then redeploy drift in-batch
	jobRevive    jobKind = "revive"    // re-probe paused relays, rejoin or redeploy
	jobRedeploy  jobKind = "redeploy"  // one relay
	jobAdopt     jobKind = "adopt"     // one relay onto one account
)

// job is one queued unit of work; its key decides coalescing.
type job struct {
	kind      jobKind
	relayID   int64
	accountID int64
}

func (j job) key() string {
	switch j.kind {
	case jobRedeploy:
		return string(j.kind) + ":" + strconv.FormatInt(j.relayID, 10)
	case jobAdopt:
		return string(j.kind) + ":" + strconv.FormatInt(j.relayID, 10) + ":" + strconv.FormatInt(j.accountID, 10)
	default:
		return string(j.kind)
	}
}

// queue is a coalescing job set: pushing a job whose key is already pending
// replaces it, and the worker is woken once, not once per push.
type queue struct {
	mu      sync.Mutex
	pending map[string]job
	wake    chan struct{}
}

func newQueue() *queue {
	return &queue{pending: map[string]job{}, wake: make(chan struct{}, 1)}
}

func (q *queue) push(j job) {
	q.mu.Lock()
	q.pending[j.key()] = j
	q.mu.Unlock()
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// drain takes every pending job. Priority order matters: a fleet-wide pass
// supersedes the per-relay requests queued behind it, so those run after and
// stay idempotent rather than racing it.
func (q *queue) drain() []job {
	q.mu.Lock()
	defer q.mu.Unlock()
	jobs := make([]job, 0, len(q.pending))
	for _, kind := range []jobKind{jobStartup, jobVerify, jobCheck, jobReconcile, jobRevive} {
		if j, ok := q.pending[string(kind)]; ok {
			jobs = append(jobs, j)
			delete(q.pending, string(kind))
		}
	}
	for key, j := range q.pending {
		jobs = append(jobs, j)
		delete(q.pending, key)
	}
	return jobs
}

// Worker owns the fleet loop. Start runs it; Stop cancels it.
type Worker struct {
	db          *store.Store
	factory     deploy.Factory
	probeHTTP   *http.Client
	log         zerolog.Logger
	queue       *queue
	interval    func() int64  // seconds until the next automatic reconcile; 0 = off
	reviveEvery time.Duration // cadence of the paused-relay revival scan; 0 = off
	verifyEvery time.Duration // cadence of the always-on readiness pass; 0 = off
	reg         *readiness.Registry

	platformLocks map[string]*sync.Mutex
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
}

// intervalFn returns the reconcile interval in seconds, read fresh each
// cycle so a settings change takes effect without a restart.
type intervalFn func() int64

// ReviveScanIntervalEnv overrides the revival scan cadence. Like the
// platform base-override variables in internal/deploy it is a test-only
// knob — the black-box suite shrinks the ten-minute default so a paused
// relay's revival is observable in seconds — and it never appears in the
// API or the database.
const ReviveScanIntervalEnv = "RELAY_REVIVE_SCAN_INTERVAL"

// Start launches the fleet worker. reg is the readiness registry the worker
// drives — every admission and demotion funnels through it, and the gateway
// rebuilds its pool from it. It does no work on the hot path of serving —
// the pool is already live from the database — and returns immediately.
func Start(db *store.Store, factory deploy.Factory, log zerolog.Logger, interval intervalFn, reg *readiness.Registry) *Worker {
	reviveEvery := reviveScanInterval
	if v := os.Getenv(ReviveScanIntervalEnv); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			reviveEvery = d
		}
	}
	verifyEvery := verifyScanInterval
	if v := os.Getenv(VerifyScanIntervalEnv); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			verifyEvery = d
		}
	}
	return startWorker(db, factory, log, interval, reg, reviveEvery, verifyEvery)
}

// startWorker is Start with the revival and verify cadences as parameters —
// tests run both at test-scale intervals by calling it directly.
func startWorker(db *store.Store, factory deploy.Factory, log zerolog.Logger, interval intervalFn, reg *readiness.Registry, reviveEvery, verifyEvery time.Duration) *Worker {
	ctx, cancel := context.WithCancel(context.Background())
	w := &Worker{
		db:          db,
		factory:     factory,
		probeHTTP:   &http.Client{Timeout: 10 * time.Second},
		log:         log,
		queue:       newQueue(),
		interval:    interval,
		reviveEvery: reviveEvery,
		verifyEvery: verifyEvery,
		reg:         reg,
		platformLocks: map[string]*sync.Mutex{
			deploy.PlatformVercel:     {},
			deploy.PlatformCloudflare: {},
			deploy.PlatformDeno:       {},
		},
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	// The startup pass finds fleet drift from a gateway upgrade or an
	// offline period without blocking serving — the pool is already live
	// from the database.
	w.queue.push(job{kind: jobStartup})
	go w.loop()
	return w
}

// Fleet is the admin plane's view of the reconciler: every method enqueues
// and returns immediately.
type Fleet interface {
	CheckAll()
	Reconcile()
	Redeploy(relayID int64)
	Adopt(relayID, accountID int64)
}

// CheckAll queues a passive probe pass: statuses update, nothing redeploys.
func (w *Worker) CheckAll() { w.queue.push(job{kind: jobCheck}) }

// Reconcile queues a full pass: probe, then redeploy every drifted relay.
func (w *Worker) Reconcile() { w.queue.push(job{kind: jobReconcile}) }

// Redeploy queues one relay's redeploy — also how a managed relay gets its
// first deployment.
func (w *Worker) Redeploy(relayID int64) {
	w.queue.push(job{kind: jobRedeploy, relayID: relayID})
}

// Adopt queues moving one legacy relay onto one platform account.
func (w *Worker) Adopt(relayID, accountID int64) {
	w.queue.push(job{kind: jobAdopt, relayID: relayID, accountID: accountID})
}

// Stop cancels the worker and waits for the in-flight batch to unwind.
// Deploys caught mid-flight are abandoned; every deploy is idempotent, so
// the next start simply retries, and the row keeps whichever status truth
// it had.
func (w *Worker) Stop() {
	w.cancel()
	<-w.done
}

// loop drains the queue until stopped, inserting an automatic reconcile
// between batches when the interval setting is non-zero, a paused-relay
// revival scan on its own always-on cadence, and the readiness verify tick
// that re-proves every relay.
func (w *Worker) loop() {
	defer close(w.done)
	var revive, verify *time.Ticker
	if w.reviveEvery > 0 {
		revive = time.NewTicker(w.reviveEvery)
		defer revive.Stop()
	}
	if w.verifyEvery > 0 {
		verify = time.NewTicker(w.verifyEvery)
		defer verify.Stop()
	}
	for {
		var timer *time.Timer
		var timeout <-chan time.Time
		if seconds := w.interval(); seconds > 0 {
			timer = time.NewTimer(time.Duration(seconds) * time.Second)
			timeout = timer.C
		}
		var reviveC, verifyC <-chan time.Time
		if revive != nil {
			reviveC = revive.C
		}
		if verify != nil {
			verifyC = verify.C
		}
		select {
		case <-w.queue.wake:
			if timer != nil {
				timer.Stop()
			}
			w.run(w.queue.drain())
		case <-timeout:
			w.queue.push(job{kind: jobReconcile})
			w.run(w.queue.drain())
		case <-reviveC:
			w.queue.push(job{kind: jobRevive})
			w.run(w.queue.drain())
		case <-verifyC:
			w.queue.push(job{kind: jobVerify})
			w.run(w.queue.drain())
		case <-w.ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		}
	}
}

// run processes one batch: fleet passes first, then the per-relay jobs they
// did not supersede.
func (w *Worker) run(jobs []job) {
	if len(jobs) == 0 {
		return
	}
	var redeploys []int64
	for _, j := range jobs {
		switch j.kind {
		case jobStartup:
			for _, id := range w.verifyFleet(true) {
				w.queue.push(job{kind: jobRedeploy, relayID: id})
			}
		case jobVerify:
			redeploys = append(redeploys, w.verifyFleet(true)...)
		case jobCheck:
			w.verifyFleet(false)
		case jobReconcile:
			redeploys = append(redeploys, w.verifyFleet(true)...)
		case jobRevive:
			redeploys = append(redeploys, w.revivePaused()...)
		case jobRedeploy:
			redeploys = append(redeploys, j.relayID)
		case jobAdopt:
			w.adoptRelay(j.relayID, j.accountID)
		}
	}
	if len(redeploys) > 0 {
		w.runRedeploys(redeploys)
	}
}

// verifyVerdict classifies one readiness verification.
type verifyVerdict uint8

const (
	verifiedOK verifyVerdict = iota
	verifyUnreachable
	verifyNotWorker // the platform answered, our worker did not
	verifyVersionMismatch
	verifyAuthFailed  // the worker answered and rejected the relay key
	verifyProbeFailed // the worker answered but the forward round trip failed
)

// statusFor maps a verification verdict to the deployment row's status —
// Active-with-reason for verdicts where the worker itself answers but is not
// usable, Paused/Unreachable/Stale for verdicts where the URL is not the
// worker or not reachable at all.
func statusFor(v verifyVerdict) string {
	switch v {
	case verifiedOK:
		return store.DeployActive
	case verifyUnreachable:
		return store.DeployUnreachable
	case verifyNotWorker:
		return store.DeployPaused
	case verifyVersionMismatch:
		return store.DeployStale
	default: // verifyAuthFailed, verifyProbeFailed
		return store.DeployActive
	}
}

// reasonFor maps a verification verdict to the readiness failure reason.
func reasonFor(v verifyVerdict) string {
	switch v {
	case verifyUnreachable:
		return readiness.ReasonUnreachable
	case verifyNotWorker:
		return readiness.ReasonPaused
	case verifyVersionMismatch:
		return readiness.ReasonVersionFailed
	case verifyAuthFailed:
		return readiness.ReasonAuthFailed
	default:
		return readiness.ReasonProbeFailed
	}
}

// relayKey is the registry identity of one relay row.
func relayKey(relay *store.RelayRow) readiness.Key {
	return readiness.Key{Provider: relay.Provider, Name: relay.Name}
}

// verifyDeployment proves one deployment ready end to end: the URL answers
// with the gateway's worker version, then a relay-spec request forwarded back
// through the relay's own origin round-trips with the relay key accepted. The
// returned duration is the probe time fed to the registry's backoff; the
// detail string is a sanitized reason for the database status row.
func (w *Worker) verifyDeployment(url, token string) (verifyVerdict, time.Duration, string) {
	start := time.Now()
	answer, err := deploy.Probe(w.ctx, w.probeHTTP, url)
	if err != nil {
		return verifyUnreachable, time.Since(start), sanitize.ErrorString(err)
	}
	if answer.Version == "" {
		return verifyNotWorker, time.Since(start), sanitize.ErrorString(answer.NotWorkerErr())
	}
	if answer.Version != deploy.RelayVersion {
		return verifyVersionMismatch, time.Since(start),
			"gateway worker version " + deploy.RelayVersion + ", relay reports " + answer.Version
	}
	fwd, err := deploy.ForwardProbe(w.ctx, w.probeHTTP, url, token)
	if err != nil {
		return verifyUnreachable, time.Since(start), sanitize.ErrorString(err)
	}
	switch {
	case fwd.Status == http.StatusNotFound:
		// The worker answered 404 — its relay key check rejected the token.
		// A fresh token heals this, so it queues a redeploy.
		return verifyAuthFailed, time.Since(start), "relay key rejected"
	case fwd.Status < 500 && fwd.JSON:
		return verifiedOK, time.Since(start), ""
	default:
		// The worker forwarded but the round trip failed on its side (for
		// example its own fetch of the target answered 502). No redeploy:
		// the worker is current; whatever it fetches is its own business.
		return verifyProbeFailed, time.Since(start),
			fmt.Sprintf("end-to-end probe did not round-trip (relay answered HTTP %d)", fwd.Status)
	}
}

// verifyRelay probes one managed relay and records the verdict in both
// layers: the deployment row keeps the current URL truth, the registry keeps
// admission. Returns the relay id when the deployment demonstrably failed —
// a rejected key or drifted version — and a redeploy is requested.
func (w *Worker) verifyRelay(relay *store.RelayRow, dep *store.DeploymentRow, requeue bool) int64 {
	key := relayKey(relay)
	if !w.reg.Allow(key) {
		return 0 // backoff-gated: keep the fleet pass off a failing relay
	}
	w.reg.Enter(key, readiness.StateVerifying, "")
	verdict, dur, detail := w.verifyDeployment(dep.URL, dep.AuthToken)
	now := time.Now().Unix()
	switch verdict {
	case verifiedOK:
		_ = w.db.SetDeploymentStatus(relay.ID, store.DeployActive, "", now)
		w.reg.Ready(key, dur)
	case verifyUnreachable:
		_ = w.db.SetDeploymentStatus(relay.ID, store.DeployUnreachable, detail, now)
		w.reg.Failing(key, readiness.ReasonUnreachable, dur)
	case verifyNotWorker:
		// The platform answered, our worker did not: a suspension page
		// (quota exhausted), a deleted deployment, a stranger's app. A
		// redeploy cannot lift a platform suspension, so the relay waits for
		// the revival scan instead of burning deploy attempts.
		_ = w.db.SetDeploymentStatus(relay.ID, store.DeployPaused, detail, now)
		w.reg.Failing(key, readiness.ReasonPaused, dur)
	case verifyVersionMismatch:
		_ = w.db.SetDeploymentStatus(relay.ID, store.DeployStale, detail, now)
		w.reg.Failing(key, readiness.ReasonVersionFailed, dur)
		if requeue {
			return relay.ID
		}
	case verifyAuthFailed:
		// The worker rejects its token: the deployment is current (the URL
		// answers) but unusable — a fresh token heals it.
		_ = w.db.SetDeploymentStatus(relay.ID, store.DeployActive, detail, now)
		w.reg.Failing(key, readiness.ReasonAuthFailed, dur)
		if requeue {
			return relay.ID
		}
	case verifyProbeFailed:
		_ = w.db.SetDeploymentStatus(relay.ID, store.DeployActive, detail, now)
		w.reg.Failing(key, readiness.ReasonProbeFailed, dur)
	}
	return 0
}

// verifyLegacy proves one legacy (unmanaged, tokenless) relay's readiness:
// it owns its URL, so the only gate that means anything is that a relay-spec
// request forwarded back through that URL made the whole loop. Any answer
// below 500 proves the forward path carried a request end-to-end; 5xx means
// the round trip failed on the relay. No database writes — a legacy relay has
// no deployment row to update.
func (w *Worker) verifyLegacy(relay *store.RelayRow) {
	key := relayKey(relay)
	if !w.reg.Allow(key) {
		return
	}
	w.reg.Enter(key, readiness.StateDiscovered, "")
	start := time.Now()
	fwd, err := deploy.ForwardProbe(w.ctx, w.probeHTTP, relay.URL, "")
	if err != nil {
		w.reg.Failing(key, readiness.ReasonUnreachable, time.Since(start))
		w.log.Warn().Str("name", relay.Name).Msg("verify: legacy relay unreachable")
		return
	}
	if fwd.Status >= 500 {
		w.reg.Failing(key, readiness.ReasonProbeFailed, time.Since(start))
		w.log.Warn().Str("name", relay.Name).Int("status", fwd.Status).
			Msg("verify: legacy relay failed its forward probe")
		return
	}
	w.reg.Ready(key, time.Since(start))
}

// verifyFleet re-proves every relay's readiness in parallel: staging and a
// version answer for managed deployments, then an end-to-end forward probe
// back through the relay's own origin. requeue true (startup, reconcile and
// the always-on verify tick) returns the ids of relays whose deployment
// demonstrably failed — a rejected key or drifted version — for redeployment;
// a passive check only records verdicts. Transport failures never redeploy:
// an unreachable relay may be a cold start, and one missed answer is not
// evidence the worker is broken. Paused relays are the revival scan's job,
// and a relay with a replacement mid-flight verifies itself rather than
// chancing a demote of the old worker it still serves.
func (w *Worker) verifyFleet(requeue bool) []int64 {
	relays, err := w.db.Relays()
	if err != nil {
		w.log.Error().Err(err).Msg("reconcile: list relays")
		return nil
	}
	stale := make([]int64, len(relays)) // indexed by slot: the closures below write disjoint cells
	run(probeParallelism, len(relays), func(i int) {
		relay := relays[i]
		if relay.Origin != store.OriginManaged {
			w.verifyLegacy(&relay)
			return
		}
		if relay.AccountID == nil {
			// Managed but its account is gone: nothing can deploy for it, so
			// it is announced as configured and stays out until it is
			// adopted back onto an account.
			w.reg.Enter(relayKey(&relay), readiness.StateConfigured, "")
			return
		}
		dep, err := w.db.Deployment(relay.ID)
		if errors.Is(err, store.ErrNoDeployment) {
			w.reg.Enter(relayKey(&relay), readiness.StateDiscovered, "")
			if requeue {
				stale[i] = relay.ID // first deploy is queued by the verify pass
			}
			return
		}
		if err != nil {
			w.log.Error().Err(err).Int64("relay", relay.ID).Msg("reconcile: read deployment")
			return
		}
		if dep.URL == "" {
			if requeue {
				stale[i] = relay.ID // a failed first deploy retries on the next pass
			}
			return
		}
		if dep.Status == store.DeployPaused {
			return // waiting on the platform, not on the fleet: revival scan's job
		}
		if rec, ok := w.reg.StateOf(relayKey(&relay)); ok && rec.State == readiness.StateDeploying {
			return // a replacement is mid-flight; the redeploy path verifies itself
		}
		stale[i] = w.verifyRelay(&relay, &dep, requeue)
	})
	nonZero := stale[:0]
	for _, id := range stale {
		if id != 0 {
			nonZero = append(nonZero, id)
		}
	}
	return nonZero
}

// revivePaused re-probes every paused deployment — the always-on background
// wait for platforms to lift a suspension. A transport failure leaves the
// pause standing (it is not evidence either way); a non-worker answer
// refreshes the recorded reason and keeps the registry failure streak
// climbing so a suspended relay loses admission without operator action; a
// worker answer revives the relay — back to active on the current verified
// worker, or stale with a queued redeploy when the fleet moved on while the
// relay was dark. Returns the ids that need that redeploy.
func (w *Worker) revivePaused() []int64 {
	ids, err := w.db.DeploymentRelayIDsByStatus(store.DeployPaused)
	if err != nil {
		w.log.Error().Err(err).Msg("revive: list paused")
		return nil
	}
	if len(ids) == 0 {
		return nil
	}
	stale := make([]int64, len(ids)) // indexed by slot: the closures below write disjoint cells
	run(probeParallelism, len(ids), func(i int) {
		relayID := ids[i]
		dep, err := w.db.Deployment(relayID)
		if err != nil {
			return // deleted mid-scan; the next tick retries the rest
		}
		relay, err := w.db.Relay(relayID)
		if err != nil {
			return // deleted mid-scan
		}
		key := relayKey(&relay)
		verdict, dur, detail := w.verifyDeployment(dep.URL, dep.AuthToken)
		now := time.Now().Unix()
		switch verdict {
		case verifyUnreachable:
			return // still dark; the pause stands
		case verifyNotWorker:
			_ = w.db.SetDeploymentStatus(relayID, store.DeployPaused, detail, now)
			w.reg.Failing(key, readiness.ReasonPaused, dur)
		case verifyVersionMismatch:
			_ = w.db.SetDeploymentStatus(relayID, store.DeployStale, detail, now)
			w.reg.Failing(key, readiness.ReasonVersionFailed, dur)
			stale[i] = relayID
		case verifyAuthFailed:
			_ = w.db.SetDeploymentStatus(relayID, store.DeployActive, detail, now)
			w.reg.Failing(key, readiness.ReasonAuthFailed, dur)
			stale[i] = relayID // a fresh token will clear the rejection
		case verifyProbeFailed:
			_ = w.db.SetDeploymentStatus(relayID, store.DeployActive, detail, now)
			w.reg.Failing(key, readiness.ReasonProbeFailed, dur)
			w.log.Warn().Int64("relay", relayID).Msg("revive: paused relay answers but fails its forward probe")
		case verifiedOK:
			_ = w.db.SetDeploymentStatus(relayID, store.DeployActive, "", now)
			w.reg.Ready(key, dur)
			w.log.Info().Int64("relay", relayID).Msg("revive: paused relay answers again")
		}
	})
	nonZero := stale[:0]
	for _, id := range stale {
		if id != 0 {
			nonZero = append(nonZero, id)
		}
	}
	return nonZero
}

// runRedeploys deploys the given relays deployParallelism at a time; two
// redeploys of the same platform still serialize on the platform lock.
func (w *Worker) runRedeploys(ids []int64) {
	run(deployParallelism, len(ids), func(i int) {
		w.redeployRelay(ids[i])
	})
}

// redeployRelay replaces one relay's worker: fresh token, current embedded
// source, the same project and account as before (or a brand-new deployment
// when the relay has none). The replacement only gains admission after the
// full end-to-end verification — version answer plus accepted key plus a
// forward round trip. A platform-side deploy failure never pulls a live relay
// out of rotation (the old worker keeps serving under its previous
// verification); a deploy that succeeded but fails verification records the
// new URL as the current truth but keeps it out of the pool until it
// verifies.
func (w *Worker) redeployRelay(relayID int64) {
	relay, err := w.db.Relay(relayID)
	if errors.Is(err, store.ErrNoRelay) {
		return // deleted while queued
	}
	if err != nil {
		w.log.Error().Err(err).Int64("relay", relayID).Msg("redeploy: read relay")
		return
	}
	if relay.Origin != store.OriginManaged {
		w.log.Warn().Int64("relay", relayID).Msg("redeploy: relay is not managed; adopt it first")
		return
	}
	if relay.AccountID == nil {
		w.log.Warn().Int64("relay", relayID).Msg("redeploy: relay has no platform account")
		return
	}
	key := relayKey(&relay)
	release, ok := w.reg.Begin(key)
	if !ok {
		w.log.Debug().Int64("relay", relayID).Msg("redeploy: already in flight")
		return
	}
	defer release()
	// The deploying phase marks this a genuinely fresh attempt: it clears
	// the failure streak and reopens the backoff gate, so a relay that was
	// gated for failing gets its replacement attempt immediately.
	w.reg.Enter(key, readiness.StateDeploying, "")

	account, err := w.db.Account(*relay.AccountID)
	if err != nil {
		w.log.Error().Err(err).Int64("relay", relayID).Msg("redeploy: read account")
		return
	}
	client, err := w.clientFor(&account)
	if err != nil {
		w.log.Error().Err(err).Int64("relay", relayID).Msg("redeploy: build client")
		return
	}

	prev, prevErr := w.db.Deployment(relayID)
	project := ""
	hadActive := false
	if prevErr == nil {
		project = prev.Project
		hadActive = prev.Status == store.DeployActive
	} else if !errors.Is(prevErr, store.ErrNoDeployment) {
		w.log.Error().Err(prevErr).Int64("relay", relayID).Msg("redeploy: read deployment")
		return
	}
	if project == "" {
		project = deploy.ProjectName(relay.Name)
	}

	result, err := w.deploy(w.ctx, client, account.Platform, project)
	if err != nil {
		// Platform-side failure: the old worker still exists and keeps
		// serving. The Failing streak accumulates anyway — a redeploy was
		// queued because the served worker demonstrably failed, so it is
		// broken in a way a fresh token or version can fix; the backoff gate
		// it pushes out bounds the retry cadence. Past DemoteAfter failures
		// the relay loses admission; the passive pool layer covers real
		// traffic before that.
		w.recordDeployFailure(relayID, &account, project, hadActive, prevErr == nil, err)
		w.reg.Failing(key, readiness.ReasonDeployFailed, 0)
		return
	}

	verdict, dur, detail := w.verifyDeployment(result.URL, result.token)
	now := time.Now().Unix()
	if verdict != verifiedOK {
		// The platform succeeded and the new worker is live, but it cannot
		// be trusted to serve: record the new URL as the current truth —
		// with a status that carries why — and keep it out of the pool.
		// Registry first, row second: any pool rebuild triggered by the row
		// change already sees the admission revoked.
		w.reg.Failing(key, reasonFor(verdict), dur)
		if err := w.db.UpsertDeployment(store.DeploymentRow{
			RelayID: relayID, AccountID: account.ID, Platform: account.Platform,
			Project: result.Project, ExternalID: result.ExternalID, URL: result.URL,
			Version: deploy.RelayVersion, AuthToken: result.token,
			Status: statusFor(verdict), LastError: detail, LastCheckedAt: now, DeployedAt: now,
		}); err != nil {
			w.log.Error().Err(err).Int64("relay", relayID).Msg("redeploy: record deployment")
		}
		w.log.Error().Str("error", detail).Int64("relay", relayID).
			Msg("redeploy: new worker failed verification; kept out of the pool")
		return
	}

	w.reg.Ready(key, dur)
	if err := w.db.UpsertDeployment(store.DeploymentRow{
		RelayID: relayID, AccountID: account.ID, Platform: account.Platform,
		Project: result.Project, ExternalID: result.ExternalID, URL: result.URL,
		Version: deploy.RelayVersion, AuthToken: result.token, Status: store.DeployActive,
		LastCheckedAt: now, DeployedAt: now,
	}); err != nil {
		w.log.Error().Err(err).Int64("relay", relayID).Msg("redeploy: record deployment")
		return
	}
	w.log.Info().Str("project", result.Project).Str("platform", account.Platform).
		Msg("redeploy: relay verified and live")
}

// recordDeployFailure stores a sanitized failure without disturbing a relay
// that is still serving its previous deployment.
func (w *Worker) recordDeployFailure(relayID int64, account *store.AccountRow, project string, wasActive, hadRow bool, cause error) {
	message := sanitize.ErrorString(cause)
	now := time.Now().Unix()
	var err error
	switch {
	case hadRow && wasActive:
		// The old worker is untouched and still answers; keep it serving.
		err = w.db.SetDeploymentStatus(relayID, store.DeployActive, message, now)
	case hadRow:
		err = w.db.SetDeploymentStatus(relayID, store.DeployError, message, now)
	default:
		err = w.db.UpsertDeployment(store.DeploymentRow{
			RelayID: relayID, AccountID: account.ID, Platform: account.Platform,
			Project: project, Status: store.DeployError,
			LastError: message, LastCheckedAt: now,
		})
	}
	if err != nil {
		w.log.Error().Err(err).Int64("relay", relayID).Msg("redeploy: record failure")
	}
	w.log.Error().Str("error", message).Int64("relay", relayID).Msg("redeploy: failed")
}

// adoptRelay moves a legacy relay onto an account: deploy the embedded
// worker, verify it end to end, then flip origin and land the deployment row
// in one transaction. Every failure leaves the relay exactly as it was —
// legacy, serving its own URL, its readiness untouched — and the log carries
// the reason.
func (w *Worker) adoptRelay(relayID, accountID int64) {
	relay, err := w.db.Relay(relayID)
	if errors.Is(err, store.ErrNoRelay) {
		return // deleted while queued
	}
	if err != nil {
		w.log.Error().Err(err).Int64("relay", relayID).Msg("adopt: read relay")
		return
	}
	// Managed with an account is genuinely managed; managed without one is a
	// relay whose account was force-deleted — adoption is how it recovers.
	if relay.Origin == store.OriginManaged && relay.AccountID != nil {
		w.log.Warn().Int64("relay", relayID).Msg("adopt: relay is already managed")
		return
	}
	key := relayKey(&relay)
	release, ok := w.reg.Begin(key)
	if !ok {
		w.log.Debug().Int64("relay", relayID).Msg("adopt: already in flight")
		return
	}
	defer release()
	w.reg.Enter(key, readiness.StateDeploying, "")

	account, err := w.db.Account(accountID)
	if err != nil {
		w.log.Error().Err(err).Int64("relay", relayID).Msg("adopt: read account")
		return
	}
	if relay.Provider != account.Platform {
		w.log.Warn().
			Str("provider", relay.Provider).Str("platform", account.Platform).
			Int64("relay", relayID).Msg("adopt: relay provider does not match account platform")
		return
	}
	client, err := w.clientFor(&account)
	if err != nil {
		w.log.Error().Err(err).Int64("relay", relayID).Msg("adopt: build client")
		return
	}

	result, err := w.deploy(w.ctx, client, account.Platform, deploy.ProjectName(relay.Name))
	if err != nil {
		w.log.Error().Str("error", sanitize.ErrorString(err)).Int64("relay", relayID).
			Msg("adopt: failed; relay left unmanaged")
		return
	}
	verdict, dur, detail := w.verifyDeployment(result.URL, result.token)
	if verdict != verifiedOK {
		// The remote worker is live but cannot be trusted; the relay stays
		// legacy and serving its own URL. The failed adoption never gains
		// admission and the old URL's readiness is untouched, so the
		// registry is left exactly as it was.
		w.log.Error().Str("error", detail).Int64("relay", relayID).
			Msg("adopt: new worker failed verification; relay left unmanaged")
		return
	}

	now := time.Now().Unix()
	deployment := store.DeploymentRow{
		RelayID: relayID, AccountID: accountID, Platform: account.Platform,
		Project: result.Project, ExternalID: result.ExternalID, URL: result.URL,
		Version: deploy.RelayVersion, AuthToken: result.token, Status: store.DeployActive,
		LastCheckedAt: now, DeployedAt: now,
	}
	if err := w.db.AdoptRelay(relayID, accountID, deployment); err != nil {
		// The remote worker is live but unrecorded; a redeploy re-adopts it.
		w.log.Error().Err(err).Int64("relay", relayID).Msg("adopt: record deployment")
		return
	}
	w.reg.Ready(key, dur)
	w.log.Info().Str("project", result.Project).Str("platform", account.Platform).
		Msg("adopt: relay managed")
}

// deployResult pairs the platform's answer with the freshly generated relay
// token — the only place the token exists outside the deployment write.
type deployResult struct {
	deploy.Result
	token string
}

// deploy generates a token and pushes the assembled worker; deploys to the
// same platform serialize so an account's API rate limits are respected.
func (w *Worker) deploy(ctx context.Context, client deploy.Client, platform, project string) (deployResult, error) {
	token, err := deploy.Token()
	if err != nil {
		return deployResult{}, err
	}
	mu := w.platformLocks[platform]
	mu.Lock()
	result, err := client.Deploy(ctx, deploy.Spec{
		Project: project,
		Source:  workers.MustAssemble(platform),
		Version: deploy.RelayVersion,
		Token:   token,
	})
	mu.Unlock()
	if err != nil {
		return deployResult{}, err
	}
	return deployResult{Result: result, token: token}, nil
}

// clientFor builds a platform client from the account's stored credential.
func (w *Worker) clientFor(account *store.AccountRow) (deploy.Client, error) {
	token, err := w.db.Tokens().PlatformToken(account.ID)
	if err != nil {
		return nil, err
	}
	return w.factory.For(account.Platform, token, account.AccountRef)
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
