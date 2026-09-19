// Package reconcile keeps the managed relay fleet in step with the gateway:
// it probes every managed relay's version endpoint, marks drift, and
// redeploys the embedded worker when a relay's deployment no longer matches.
//
// One worker goroutine owns all fleet work. Mutations land on a coalesced
// queue — a flood of requests for the same action collapses into one — and
// the worker drains whole batches, probing in parallel but deploying with a
// small, per-platform-serialized pool. Probes never trigger deploys on
// transport failures: an unreachable relay may be a cold start or a network
// blip, and one missed answer is never evidence the worker is outdated.
package reconcile

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"http-relay-gateway/internal/deploy"
	"http-relay-gateway/internal/deploy/workers"
	"http-relay-gateway/internal/sanitize"
	"http-relay-gateway/internal/store"
)

// probeParallelism bounds concurrent version probes; redeploys run two at a
// time so a large fleet heals quickly without hammering one platform's API.
const (
	probeParallelism  = 4
	deployParallelism = 2
)

// jobKind names one unit of fleet work on the queue.
type jobKind string

const (
	jobCheck     jobKind = "check"     // probe everything, update statuses
	jobStartup   jobKind = "startup"   // probe, then queue redeploys for drift
	jobReconcile jobKind = "reconcile" // probe, then redeploy drift in-batch
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
	for _, kind := range []jobKind{jobStartup, jobCheck, jobReconcile} {
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
	db        *store.Store
	factory   deploy.Factory
	probeHTTP *http.Client
	log       zerolog.Logger
	queue     *queue
	interval  func() int64 // seconds until the next automatic reconcile; 0 = off

	platformLocks map[string]*sync.Mutex
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
}

// intervalFn returns the reconcile interval in seconds, read fresh each
// cycle so a settings change takes effect without a restart.
type intervalFn func() int64

// Start launches the fleet worker. It does no work on the hot path of
// serving — the pool is already live from the database — and returns
// immediately.
func Start(db *store.Store, factory deploy.Factory, log zerolog.Logger, interval intervalFn) *Worker {
	ctx, cancel := context.WithCancel(context.Background())
	w := &Worker{
		db:        db,
		factory:   factory,
		probeHTTP: &http.Client{Timeout: 10 * time.Second},
		log:       log,
		queue:     newQueue(),
		interval:  interval,
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
// between batches when the interval setting is non-zero.
func (w *Worker) loop() {
	defer close(w.done)
	for {
		var timer *time.Timer
		var timeout <-chan time.Time
		if seconds := w.interval(); seconds > 0 {
			timer = time.NewTimer(time.Duration(seconds) * time.Second)
			timeout = timer.C
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
			for _, id := range w.probeFleet(true) {
				w.queue.push(job{kind: jobRedeploy, relayID: id})
			}
		case jobCheck:
			w.probeFleet(false)
		case jobReconcile:
			redeploys = append(redeploys, w.probeFleet(true)...)
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

// probeFleet versions every managed deployment in parallel. When requeue is
// true (startup and reconcile) the ids of relays found stale come back for
// redeployment; a passive check just records the verdicts. Probe transport
// failures mark unreachable and never redeploy.
func (w *Worker) probeFleet(requeue bool) []int64 {
	relays, err := w.db.Relays()
	if err != nil {
		w.log.Error().Err(err).Msg("reconcile: list relays")
		return nil
	}
	now := time.Now().Unix()
	stale := make([]int64, 0)
	run(probeParallelism, len(relays), func(i int) {
		relay := relays[i]
		if relay.Origin != store.OriginManaged || relay.AccountID == nil {
			return // legacy relays own their URLs; nothing to probe
		}
		dep, err := w.db.Deployment(relay.ID)
		if errors.Is(err, store.ErrNoDeployment) {
			return // managed but never deployed: the row URL serves tokenless
		}
		if err != nil {
			w.log.Error().Err(err).Int64("relay", relay.ID).Msg("reconcile: read deployment")
			return
		}
		if dep.URL == "" {
			return // a failed first deploy has no URL to probe
		}
		reported, err := deploy.ProbeVersion(w.ctx, w.probeHTTP, dep.URL)
		switch {
		case err != nil:
			_ = w.db.SetDeploymentStatus(relay.ID, store.DeployUnreachable, sanitize.ErrorString(err), now)
		case reported != deploy.RelayVersion:
			_ = w.db.SetDeploymentStatus(relay.ID, store.DeployStale,
				"gateway worker version "+deploy.RelayVersion+", relay reports "+reported, now)
			if requeue {
				stale = append(stale, relay.ID)
			}
		default:
			_ = w.db.SetDeploymentStatus(relay.ID, store.DeployActive, "", now)
		}
	})
	return stale
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
// when the relay has none). Until the new worker answers its version
// endpoint the old one keeps serving — a failed redeploy must never pull a
// live relay out of rotation, so an active relay that fails to redeploy
// stays active with the failure recorded.
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
	if err == nil {
		err = w.verifyLive(result.URL)
	}
	if err != nil {
		w.recordDeployFailure(relayID, &account, project, hadActive, prevErr == nil, err)
		return
	}

	now := time.Now().Unix()
	token := result.token
	if err := w.db.UpsertDeployment(store.DeploymentRow{
		RelayID: relayID, AccountID: account.ID, Platform: account.Platform,
		Project: result.Project, ExternalID: result.ExternalID, URL: result.URL,
		Version: deploy.RelayVersion, AuthToken: token, Status: store.DeployActive,
		LastCheckedAt: now, DeployedAt: now,
	}); err != nil {
		w.log.Error().Err(err).Int64("relay", relayID).Msg("redeploy: record deployment")
		return
	}
	w.log.Info().Str("project", result.Project).Str("platform", account.Platform).
		Msg("redeploy: relay live")
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
// worker, verify it, then flip origin and land the deployment row in one
// transaction. Every failure leaves the relay exactly as it was — legacy,
// serving its own URL — and the log carries the reason.
func (w *Worker) adoptRelay(relayID, accountID int64) {
	relay, err := w.db.Relay(relayID)
	if errors.Is(err, store.ErrNoRelay) {
		return // deleted while queued
	}
	if err != nil {
		w.log.Error().Err(err).Int64("relay", relayID).Msg("adopt: read relay")
		return
	}
	if relay.Origin == store.OriginManaged {
		w.log.Warn().Int64("relay", relayID).Msg("adopt: relay is already managed")
		return
	}
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
	if err == nil {
		err = w.verifyLive(result.URL)
	}
	if err != nil {
		w.log.Error().Str("error", sanitize.ErrorString(err)).Int64("relay", relayID).
			Msg("adopt: failed; relay left unmanaged")
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

// verifyLive confirms the just-deployed URL answers with the gateway's
// version — the client already waited for it, but the record only becomes
// serving truth once the gateway has seen the answer itself.
func (w *Worker) verifyLive(url string) error {
	reported, err := deploy.ProbeVersion(w.ctx, w.probeHTTP, url)
	if err != nil {
		return err
	}
	if reported != deploy.RelayVersion {
		return errors.New("relay not live: reports version " + reported)
	}
	return nil
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
