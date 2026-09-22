package reconcile

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"http-relay-gateway/internal/config"
	"http-relay-gateway/internal/deploy"
	"http-relay-gateway/internal/readiness"
)

// fakeClock is the test clock the registry runs on: tests advance it
// explicitly instead of sleeping.
type fakeClock struct {
	nanos int64
	mu    sync.Mutex
}

func newFakeClock() *fakeClock {
	return &fakeClock{nanos: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Unix(0, c.nanos)
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nanos += int64(d)
}

// eventLog records the order of observable rollout/delete steps across the
// collaborators (settle barrier, drainer, platform client).
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(event string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, event)
}

func (l *eventLog) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

// workerSim is a fake edge relay: a real HTTP server answering the two
// routes the verification probes hit. version=="" models a stranger's app
// (200 with a non-JSON body); suspend models a platform suspension page;
// acceptKey pins the relay key the ingress accepts (empty accepts any).
type workerSim struct {
	mu        sync.Mutex
	version   string
	suspend   bool
	forward   int // status the relay-spec ingress answers; 0 → 200 JSON
	acceptKey string
	tokens    []string
}

func (s *workerSim) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/__relay/version", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		version, suspend := s.version, s.suspend
		s.mu.Unlock()
		if suspend {
			w.Header().Set("X-Vercel-Error", "DEPLOYMENT_DISABLED")
			w.WriteHeader(http.StatusPaymentRequired)
			return
		}
		if version == "" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("<html>not a relay worker</html>"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"version":%q}`, version)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.tokens = append(s.tokens, r.Header.Get("X-Relay-Token"))
		acceptKey, forward := s.acceptKey, s.forward
		s.mu.Unlock()
		if acceptKey != "" && r.Header.Get("X-Relay-Token") != acceptKey {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if forward != 0 {
			w.WriteHeader(forward)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	return mux
}

func (s *workerSim) setVersion(v string) { s.mu.Lock(); s.version = v; s.mu.Unlock() }

func (s *workerSim) lastToken() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.tokens) == 0 {
		return ""
	}
	return s.tokens[len(s.tokens)-1]
}

// recordedFactory hands out recorded clients and remembers every
// credential it was asked for.
type recordedFactory struct {
	mu        sync.Mutex
	sim       *workerSim
	log       *eventLog
	forCalls  []string
	creds     []deploy.Credential
	forErr    error
	deploys   []deploy.Spec
	deletes   []string
	discovers []string
	// next, when set, is handed out for every platform; split overrides it
	// per platform.
	next  *recordedClient
	split map[string]*recordedClient
}

func (f *recordedFactory) For(platform string, cred deploy.Credential) (deploy.Client, error) {
	f.mu.Lock()
	if f.forErr != nil {
		err := f.forErr
		f.mu.Unlock()
		return nil, err
	}
	f.forCalls = append(f.forCalls, platform)
	f.creds = append(f.creds, cred)
	next, split := f.next, f.split[platform]
	f.mu.Unlock()
	if split != nil {
		return split, nil
	}
	if next != nil {
		return next, nil
	}
	return &recordedClient{f: f, platform: platform}, nil
}

// newPrimaryClient builds the client most tests use: it discovers the sim
// server (when exists), deploys with sim-healing semantics and reports the
// sim URL. It is installed as the factory's default client.
func (r *testRig) newPrimaryClient(exists bool) *recordedClient {
	c := &recordedClient{
		f:      r.factory,
		exists: exists,
		url:    r.server.URL,
		fixSim: true,
	}
	r.factory.mu.Lock()
	r.factory.next = c
	r.factory.mu.Unlock()
	return c
}

func (f *recordedFactory) forCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.forCalls)
}

func (f *recordedFactory) lastCred() deploy.Credential {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.creds) == 0 {
		return deploy.Credential{}
	}
	return f.creds[len(f.creds)-1]
}

func (f *recordedFactory) deployCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.deploys)
}

func (f *recordedFactory) lastDeploy() deploy.Spec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deploys[len(f.deploys)-1]
}

func (f *recordedFactory) deleteCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.deletes)
}

// recordedClient is one platform client: discovery, deploy and delete all
// recorded, behavior per test via its fields. Deploy with fixSim installs
// the spec's version and key on the sim — what a real deploy does.
type recordedClient struct {
	f           *recordedFactory
	platform    string
	exists      bool
	url         string
	discoverErr error
	deployErr   error
	deleteErr   error
	fixSim      bool
	deleteHook  func() // blocks the delete while non-nil
}

func (c *recordedClient) Platform() string { return c.platform }

func (c *recordedClient) Discover(_ context.Context, project string) (deploy.Discovery, error) {
	c.f.mu.Lock()
	c.f.discovers = append(c.f.discovers, project)
	discoverErr, exists, url := c.discoverErr, c.exists, c.url
	c.f.mu.Unlock()
	if discoverErr != nil {
		return deploy.Discovery{}, discoverErr
	}
	return deploy.Discovery{Exists: exists, URL: url}, nil
}

func (c *recordedClient) Deploy(_ context.Context, spec deploy.Spec) (deploy.Result, error) {
	c.f.mu.Lock()
	c.f.deploys = append(c.f.deploys, spec)
	c.f.log.add("deploy")
	deployErr, fixSim := c.deployErr, c.fixSim
	c.exists = true // a real deploy creates the project; later discovery finds it
	c.f.mu.Unlock()
	if deployErr != nil {
		return deploy.Result{}, deployErr
	}
	if fixSim {
		c.f.sim.mu.Lock()
		c.f.sim.version = spec.Version
		c.f.sim.acceptKey = spec.Token
		c.f.sim.suspend = false
		c.f.sim.forward = 0
		c.f.sim.mu.Unlock()
	}
	return deploy.Result{Project: spec.Project, URL: c.url}, nil
}

func (c *recordedClient) Delete(_ context.Context, project string) error {
	c.f.mu.Lock()
	c.f.deletes = append(c.f.deletes, project)
	c.f.log.add("delete:" + project)
	hook, deleteErr := c.deleteHook, c.deleteErr
	c.f.mu.Unlock()
	if hook != nil {
		hook()
	}
	return deleteErr
}

// fakeDrainer records drain calls and answers with a controllable verdict.
// At call time it logs whether the identity was still serving — the proof
// that Strategy A demotes before draining. The context is ignored: the
// verdict flips when the test says so, not on time.
type fakeDrainer struct {
	mu       sync.Mutex
	log      *eventLog
	reg      *readiness.Registry
	idle     bool
	calls    []string
	timeouts []time.Duration
}

func (d *fakeDrainer) setIdle(idle bool) { d.mu.Lock(); d.idle = idle; d.mu.Unlock() }

func (d *fakeDrainer) AwaitIdle(_ context.Context, provider, name string, timeout time.Duration) bool {
	d.mu.Lock()
	serving := d.reg.IsReady(deploy.RelayKey{Provider: provider, Name: name})
	d.calls = append(d.calls, provider+"/"+name)
	d.timeouts = append(d.timeouts, timeout)
	idle := d.idle
	d.mu.Unlock()
	d.log.add(fmt.Sprintf("drain:%s:serving=%t:idle=%t", provider+"/"+name, serving, idle))
	return idle
}

// budgetDrainer models the real gateway's shutdown behavior: the wait ends
// when its context does, reporting the drain incomplete. It records the
// context and timeout of its first (the test's only) call.
type budgetDrainer struct {
	entered chan struct{}
	once    sync.Once
	ctx     context.Context
	timeout time.Duration
}

func (d *budgetDrainer) AwaitIdle(ctx context.Context, _, _ string, timeout time.Duration) bool {
	d.once.Do(func() {
		d.ctx, d.timeout = ctx, timeout
		close(d.entered)
	})
	<-ctx.Done()
	return false
}

// testRig assembles one synchronous worker plus everything needed to
// observe it. desired and relayKey stay mutable so later passes model
// configuration and secret rotation.
type testRig struct {
	clock   *fakeClock
	sim     *workerSim
	server  *httptest.Server
	factory *recordedFactory
	reg     *readiness.Registry
	drainer *fakeDrainer
	settles int
	log     *eventLog
	desired *config.Config
	worker  *Worker
	t       *testing.T
}

func newTestRig(t *testing.T, relays ...config.Relay) *testRig {
	t.Helper()
	clock := newFakeClock()
	sim := &workerSim{}
	server := httptest.NewServer(sim.handler())
	t.Cleanup(server.Close)
	log := &eventLog{}
	factory := &recordedFactory{sim: sim, log: log}
	registry := readiness.New(readiness.Config{
		BackoffBase: time.Second,
		BackoffMax:  8 * time.Second,
		RecoverMax:  15 * time.Second,
		PauseRetry:  10 * time.Minute,
		DemoteAfter: 3,
		Now:         clock.Now,
	})
	drainer := &fakeDrainer{log: log, reg: registry, idle: true}
	desired := &config.Config{Relays: relays, Settings: config.DefaultSettings()}
	rig := &testRig{
		clock: clock, sim: sim, server: server, factory: factory,
		reg: registry, drainer: drainer, log: log, desired: desired, t: t,
	}
	rig.worker = rig.newWorker()
	return rig
}

// newWorker builds a Worker literal directly — no Start, no background
// loop — so tests drive w.pass() synchronously.
func (r *testRig) newWorker() *Worker {
	locks := map[string]*sync.Mutex{}
	for _, platform := range deploy.Platforms() {
		locks[platform] = &sync.Mutex{}
	}
	settle := func() bool {
		r.log.add("settle")
		r.settles++
		return true
	}
	relayKey := func() (string, error) { return testRelayKey, nil }
	drainCtx, drainCancel := context.WithCancel(context.Background())
	r.t.Cleanup(drainCancel)
	w := &Worker{
		cfg: Config{
			Desired:        func() *config.Config { return r.desired },
			RelayKey:       relayKey,
			Factory:        r.factory,
			Registry:       r.reg,
			Drainer:        r.drainer,
			Settle:         settle,
			Log:            zerolog.Nop(),
			QuiesceTimeout: 5 * time.Second,
		},
		reg:           r.reg,
		drainer:       r.drainer,
		settle:        settle,
		log:           zerolog.Nop(),
		probeHTTP:     deploy.ProbeClient(2 * time.Second),
		platformLocks: locks,
		credentials:   map[deploy.RelayKey]deploy.Credential{},
		wake:          make(chan struct{}, 1),
		ctx:           context.Background(),
		cancel:        func() {},
		done:          make(chan struct{}),
		budget:        context.Background(),
		drainCtx:      drainCtx,
		drainCancel:   drainCancel,
	}
	return w
}

func relay(name, provider, token string) config.Relay {
	return config.Relay{Name: name, Provider: provider, Token: token}
}

const (
	testRelayKey = "relay-key-1"
	testToken    = "provider-token-1"
)
