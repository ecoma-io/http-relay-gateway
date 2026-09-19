// Package admin serves the management plane: a JSON REST API under /api/v1
// plus the embedded SPA, bound to the loopback-only admin listener. The data
// plane never appears here — this package only mutates the database, and the
// applier loop rebuilds the serving pool from store.Changes().
//
// Auth model: first-run setup stores a bcrypt password hash and a random
// session-secret; logins mint HS256 session JWTs in an HttpOnly SameSite=Strict
// cookie. Every management mutation requires that cookie. Credentials never
// appear in responses (tokens are last-4 digests) or logs.
package admin

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"http-relay-gateway/internal/admin/auth"
	"http-relay-gateway/internal/pool"
	"http-relay-gateway/internal/sanitize"
	"http-relay-gateway/internal/store"

	"github.com/rs/zerolog"
)

// apiPrefix guards the SPA fallback: unmatched /api paths are 404s, never the
// SPA entry.
const apiPrefix = "/api/"

// maxBodyBytes bounds every API request body; nothing management-related is
// anywhere near this.
const maxBodyBytes = 1 << 16

// setupPassword bounds: bcrypt reads at most 72 input bytes, and a short
// password defeats the point of hashing.
const (
	minPasswordLen = 8
	maxPasswordLen = 72
)

// Server is the admin-plane root handler.
type Server struct {
	db      *store.Store
	version string
	log     zerolog.Logger
	limiter *auth.LoginLimiter
	spa     http.Handler
	mux     *http.ServeMux
	// secureCookie sets the Secure attribute on session cookies (opt-in for
	// HTTPS-fronted deployments; loopback HTTP defaults off).
	secureCookie bool
}

// Option customizes the server.
type Option func(*Server)

// WithSecureCookie marks session cookies Secure (for HTTPS-fronted admin).
func WithSecureCookie() Option { return func(s *Server) { s.secureCookie = true } }

// New builds the admin server. spa serves the management UI for every
// non-API path (history-mode routing).
func New(db *store.Store, version string, log zerolog.Logger, spa http.Handler, opts ...Option) *Server {
	s := &Server{
		db:      db,
		version: version,
		log:     log,
		limiter: auth.NewLoginLimiter(),
		spa:     spa,
	}
	for _, opt := range opts {
		opt(s)
	}
	mux := http.NewServeMux()
	public := func(pattern string, h http.HandlerFunc) { mux.HandleFunc(pattern, s.public(h)) }
	only := func(pattern string, h http.HandlerFunc) { mux.HandleFunc(pattern, s.authed(h)) }

	public("GET /api/v1/status", s.handleStatus)
	public("GET /api/v1/ping", s.handlePing)
	public("POST /api/v1/setup", s.handleSetup)
	public("POST /api/v1/login", s.handleLogin)
	only("POST /api/v1/logout", s.handleLogout)

	only("GET /api/v1/relays", s.handleListRelays)
	only("POST /api/v1/relays", s.handleCreateRelay)
	only("GET /api/v1/relays/{id}", s.handleGetRelay)
	only("PATCH /api/v1/relays/{id}", s.handlePatchRelay)
	only("DELETE /api/v1/relays/{id}", s.handleDeleteRelay)

	only("GET /api/v1/providers", s.handleListProviders)
	only("PUT /api/v1/providers", s.handlePutProviders)

	only("GET /api/v1/settings", s.handleGetSettings)
	only("PATCH /api/v1/settings", s.handlePatchSettings)

	// Anything that is not the API is the SPA — except unmatched /api paths,
	// which must never fall through to index.html.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, apiPrefix) {
			writeError(w, http.StatusNotFound, "no such API endpoint")
			return
		}
		s.spa.ServeHTTP(w, r)
	})
	s.mux = mux
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	s.mux.ServeHTTP(w, r)
}

// public wraps an unauthenticated handler with the request-body cap.
func (s *Server) public(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		next(w, r)
	}
}

// authed enforces the session cookie on every management call. Credentials
// are read per request: the admin plane is low-traffic and the store
// serializes on one connection, so caching buys nothing worth its
// invalidation complexity.
func (s *Server) authed(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie(auth.CookieName)
		if err != nil || cookie.Value == "" {
			writeError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		_, secret, found, err := s.db.AdminCredentials()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "read admin credentials: "+sanitize.ErrorString(err))
			return
		}
		if !found {
			writeError(w, http.StatusUnauthorized, "setup required")
			return
		}
		issuer, err := auth.NewIssuer(secret)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "load session secret: "+sanitize.ErrorString(err))
			return
		}
		if issuer.Verify(cookie.Value, time.Now()) != nil {
			writeError(w, http.StatusUnauthorized, "session expired")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
		next(w, r)
	}
}

// clientKey is the rate-limit key: the remote address only. The admin plane
// is loopback-bound and sits behind no trusted proxy, so forwarding headers
// are untrusted input, not identity.
func clientKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// limited gates a handler behind the login rate limiter, answering 429 with
// Retry-After when the key (or the global counter) is exhausted.
func (s *Server) limited(w http.ResponseWriter, r *http.Request) bool {
	key := clientKey(r)
	if ok, retryAfter := s.limiter.Allow(key, time.Now()); !ok {
		seconds := int(retryAfter/time.Second) + 1
		w.Header().Set("Retry-After", strconv.Itoa(seconds))
		writeError(w, http.StatusTooManyRequests, "too many attempts; try again later")
		return false
	}
	return true
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	_, _, found, err := s.db.AdminCredentials()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read admin credentials: "+sanitize.ErrorString(err))
		return
	}
	setupAt, hasSetupAt, err := s.db.AdminSetupAt()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read setup time: "+sanitize.ErrorString(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"version":       s.version,
		"setupRequired": !found,
		"setupAt":       setupAt,
		"hasSetupAt":    hasSetupAt,
	})
}

func (s *Server) handlePing(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"ok": "true"})
}

type setupRequest struct {
	Password string `json:"password"`
	Confirm  string `json:"confirm"`
}

// handleSetup performs the one-time first-run setup and logs the new admin
// in. Rate limited like login: setup is reachable pre-auth and must not be
// a brute-force surface either.
func (s *Server) handleSetup(w http.ResponseWriter, r *http.Request) {
	if !s.limited(w, r) {
		return
	}
	var req setupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "decode JSON body: "+sanitize.ErrorString(err))
		return
	}
	if len(req.Password) < minPasswordLen || len(req.Password) > maxPasswordLen {
		writeFieldError(w, http.StatusUnprocessableEntity, "password",
			fmt.Sprintf("must be %d-%d characters", minPasswordLen, maxPasswordLen))
		return
	}
	if subtle.ConstantTimeCompare([]byte(req.Password), []byte(req.Confirm)) != 1 {
		writeFieldError(w, http.StatusUnprocessableEntity, "confirm", "passwords do not match")
		return
	}
	hash, err := auth.HashPassword(req.Password)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "hash password: "+sanitize.ErrorString(err))
		return
	}
	secret, err := auth.NewSecret()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "generate session secret: "+sanitize.ErrorString(err))
		return
	}
	if err := s.db.AdminSetup(hash, secret); err != nil {
		if errors.Is(err, store.ErrSetupDone) {
			writeError(w, http.StatusConflict, "setup already completed")
			return
		}
		writeError(w, http.StatusInternalServerError, "save admin credentials: "+sanitize.ErrorString(err))
		return
	}
	s.log.Info().Str("ip", clientKey(r)).Msg("admin setup completed")
	s.login(w, r)
}

type loginRequest struct {
	Password string `json:"password"`
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if !s.limited(w, r) {
		return
	}
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "decode JSON body: "+sanitize.ErrorString(err))
		return
	}
	hash, _, found, err := s.db.AdminCredentials()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read admin credentials: "+sanitize.ErrorString(err))
		return
	}
	if !found {
		writeError(w, http.StatusConflict, "setup required")
		return
	}
	// The hash comparison happens even for an empty password: the constant
	// work keeps timing from distinguishing "user exists" states, though with
	// a single admin account there is nothing to enumerate anyway.
	if !auth.ComparePassword(hash, req.Password) {
		s.limiter.RecordFailure(clientKey(r), time.Now())
		s.log.Info().Str("ip", clientKey(r)).Msg("admin login failed")
		writeError(w, http.StatusUnauthorized, "invalid password")
		return
	}
	s.limiter.Reset(clientKey(r))
	s.log.Info().Str("ip", clientKey(r)).Msg("admin login ok")
	s.login(w, r)
}

// login mints a fresh session and sets the cookie. Shared by setup (which
// logs the new admin in immediately) and login.
func (s *Server) login(w http.ResponseWriter, _ *http.Request) {
	_, secret, found, err := s.db.AdminCredentials()
	if err != nil || !found {
		writeError(w, http.StatusInternalServerError, "load session secret: "+sanitize.ErrorString(err))
		return
	}
	issuer, err := auth.NewIssuer(secret)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "load session secret: "+sanitize.ErrorString(err))
		return
	}
	token, expires, err := issuer.Issue(time.Now())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "sign session: "+sanitize.ErrorString(err))
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     auth.CookieName,
		Value:    token,
		Path:     "/",
		Expires:  expires,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   s.secureCookie,
	})
	writeJSON(w, http.StatusOK, map[string]string{"ok": "true"})
}

func (s *Server) handleLogout(w http.ResponseWriter, _ *http.Request) {
	// Stateful revocation (secret rotation) is a documented future endpoint;
	// logout clears the cookie client-side.
	http.SetCookie(w, &http.Cookie{
		Name:     auth.CookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   s.secureCookie,
	})
	writeJSON(w, http.StatusOK, map[string]string{"ok": "true"})
}

// --- relays ---

type deploymentJSON struct {
	Platform   string `json:"platform"`
	Project    string `json:"project"`
	URL        string `json:"url"`
	Version    string `json:"version"`
	Status     string `json:"status"`
	TokenLast4 string `json:"tokenLast4"`
	LastError  string `json:"lastError,omitempty"`
	DeployedAt int64  `json:"deployedAt"`
}

type relayJSON struct {
	ID           int64           `json:"id"`
	Name         string          `json:"name"`
	Provider     string          `json:"provider"`
	URL          string          `json:"url"`
	Active       bool            `json:"active"`
	Origin       string          `json:"origin"`
	AccountID    *int64          `json:"accountId"`
	HeaderPolicy *string         `json:"headerPolicy"`
	Deployment   *deploymentJSON `json:"deployment,omitempty"`
	CreatedAt    int64           `json:"createdAt"`
	UpdatedAt    int64           `json:"updatedAt"`
}

func relayView(row store.RelayRow) relayJSON {
	view := relayJSON{
		ID:           row.ID,
		Name:         row.Name,
		Provider:     row.Provider,
		URL:          row.URL,
		Active:       row.Active,
		Origin:       string(row.Origin),
		AccountID:    row.AccountID,
		HeaderPolicy: row.HeaderPolicy,
		CreatedAt:    row.CreatedAt,
		UpdatedAt:    row.UpdatedAt,
	}
	if row.Deployment != nil {
		d := row.Deployment
		view.Deployment = &deploymentJSON{
			Platform:   d.Platform,
			Project:    d.Project,
			URL:        d.URL,
			Version:    d.Version,
			Status:     d.Status,
			TokenLast4: last4(d.AuthToken),
			LastError:  d.LastError,
			DeployedAt: d.DeployedAt,
		}
	}
	return view
}

// last4 is the most a credential ever surfaces outside the store.
func last4(secret string) string {
	if len(secret) < 4 {
		return ""
	}
	return secret[len(secret)-4:]
}

func (s *Server) handleListRelays(w http.ResponseWriter, _ *http.Request) {
	rows, err := s.db.Relays()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read relays: "+sanitize.ErrorString(err))
		return
	}
	views := make([]relayJSON, 0, len(rows))
	for _, row := range rows {
		views = append(views, relayView(row))
	}
	writeJSON(w, http.StatusOK, views)
}

func (s *Server) handleGetRelay(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	row, err := s.db.Relay(id)
	if err != nil {
		s.storeError(w, err, "read relay")
		return
	}
	writeJSON(w, http.StatusOK, relayView(row))
}

type relayRequest struct {
	Name         *string `json:"name"`
	Provider     *string `json:"provider"`
	URL          *string `json:"url"`
	Active       *bool   `json:"active"`
	HeaderPolicy *string `json:"headerPolicy"`
}

func (s *Server) handleCreateRelay(w http.ResponseWriter, r *http.Request) {
	var req relayRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "decode JSON body: "+sanitize.ErrorString(err))
		return
	}
	if req.Provider == nil || strings.TrimSpace(*req.Provider) == "" {
		writeFieldError(w, http.StatusUnprocessableEntity, "provider", "is required")
		return
	}
	if req.Name == nil || strings.TrimSpace(*req.Name) == "" {
		writeFieldError(w, http.StatusUnprocessableEntity, "name", "is required")
		return
	}
	if req.URL == nil {
		writeFieldError(w, http.StatusUnprocessableEntity, "url", "is required")
		return
	}
	provider, name := strings.ToLower(strings.TrimSpace(*req.Provider)), strings.TrimSpace(*req.Name)
	if pool.ReservedProviders[provider] {
		writeFieldError(w, http.StatusUnprocessableEntity, "provider", "is reserved")
		return
	}
	if err := validateRelayURL(*req.URL); err != nil {
		writeFieldError(w, http.StatusUnprocessableEntity, "url", err.Error())
		return
	}
	if err := validatePolicy(req.HeaderPolicy); err != nil {
		writeFieldError(w, http.StatusUnprocessableEntity, "headerPolicy", err.Error())
		return
	}
	active := true
	if req.Active != nil {
		active = *req.Active
	}
	id, err := s.db.CreateRelay(name, provider, strings.TrimSpace(*req.URL), active, nil, req.HeaderPolicy)
	if err != nil {
		s.storeError(w, err, "create relay")
		return
	}
	s.log.Info().Int64("id", id).Str("name", name).Str("provider", provider).Msg("relay created")
	row, err := s.db.Relay(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read relay: "+sanitize.ErrorString(err))
		return
	}
	writeJSON(w, http.StatusCreated, relayView(row))
}

func (s *Server) handlePatchRelay(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req relayRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "decode JSON body: "+sanitize.ErrorString(err))
		return
	}
	var patch store.RelayPatch
	if req.Name != nil {
		if strings.TrimSpace(*req.Name) == "" {
			writeFieldError(w, http.StatusUnprocessableEntity, "name", "must not be blank")
			return
		}
		name := strings.TrimSpace(*req.Name)
		patch.Name = &name
	}
	if req.Provider != nil {
		provider := strings.ToLower(strings.TrimSpace(*req.Provider))
		if pool.ReservedProviders[provider] {
			writeFieldError(w, http.StatusUnprocessableEntity, "provider", "is reserved")
			return
		}
		patch.Provider = &provider
	}
	if req.URL != nil {
		if err := validateRelayURL(*req.URL); err != nil {
			writeFieldError(w, http.StatusUnprocessableEntity, "url", err.Error())
			return
		}
		url := strings.TrimSpace(*req.URL)
		patch.URL = &url
	}
	if req.Active != nil {
		patch.Active = req.Active
	}
	if req.HeaderPolicy != nil {
		if err := validatePolicy(req.HeaderPolicy); err != nil {
			writeFieldError(w, http.StatusUnprocessableEntity, "headerPolicy", err.Error())
			return
		}
		// An empty string clears the policy (verbatim forwarding); JSON null
		// is indistinguishable from absent and changes nothing.
		patch.HeaderPolicy = req.HeaderPolicy
	}
	if err := s.db.UpdateRelay(id, patch); err != nil {
		s.storeError(w, err, "update relay")
		return
	}
	s.log.Info().Int64("id", id).Msg("relay updated")
	row, err := s.db.Relay(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read relay: "+sanitize.ErrorString(err))
		return
	}
	writeJSON(w, http.StatusOK, relayView(row))
}

func (s *Server) handleDeleteRelay(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	// Remote deletion (removing the deployed worker from the platform)
	// arrives with the deployers; until then the flag is refused rather than
	// silently ignored, so a caller never believes an orphan was cleaned up.
	switch r.URL.Query().Get("deleteRemote") {
	case "", "0", "false":
	default:
		writeError(w, http.StatusNotImplemented, "remote deletion is not available yet")
		return
	}
	if err := s.db.DeleteRelay(id); err != nil {
		s.storeError(w, err, "delete relay")
		return
	}
	s.log.Info().Int64("id", id).Msg("relay deleted")
	writeJSON(w, http.StatusOK, map[string]string{"ok": "true"})
}

// --- providers ---

type providerJSON struct {
	Name         string  `json:"name"`
	MaxBody      int64   `json:"maxBody"`
	HeaderPolicy *string `json:"headerPolicy"`
}

func (s *Server) handleListProviders(w http.ResponseWriter, _ *http.Request) {
	rows, err := s.db.Providers()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read providers: "+sanitize.ErrorString(err))
		return
	}
	views := make([]providerJSON, 0, len(rows))
	for _, row := range rows {
		views = append(views, providerJSON{Name: row.Name, MaxBody: row.MaxBody, HeaderPolicy: row.HeaderPolicy})
	}
	writeJSON(w, http.StatusOK, views)
}

func (s *Server) handlePutProviders(w http.ResponseWriter, r *http.Request) {
	var req []providerJSON
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "decode JSON body: "+sanitize.ErrorString(err))
		return
	}
	rows := make([]store.ProviderRow, 0, len(req))
	seen := map[string]bool{}
	for i, view := range req {
		name := strings.ToLower(strings.TrimSpace(view.Name))
		if name == "" {
			writeFieldError(w, http.StatusUnprocessableEntity, fmt.Sprintf("providers[%d].name", i), "is required")
			return
		}
		if pool.ReservedProviders[name] {
			writeFieldError(w, http.StatusUnprocessableEntity, fmt.Sprintf("providers[%d].name", i), "is reserved")
			return
		}
		if seen[name] {
			writeFieldError(w, http.StatusUnprocessableEntity, fmt.Sprintf("providers[%d].name", i), "is duplicated")
			return
		}
		seen[name] = true
		if view.MaxBody < 1 {
			writeFieldError(w, http.StatusUnprocessableEntity, fmt.Sprintf("providers[%d].maxBody", i), "must be positive")
			return
		}
		if err := validatePolicy(view.HeaderPolicy); err != nil {
			writeFieldError(w, http.StatusUnprocessableEntity, fmt.Sprintf("providers[%d].headerPolicy", i), err.Error())
			return
		}
		rows = append(rows, store.ProviderRow{Name: name, MaxBody: view.MaxBody, HeaderPolicy: view.HeaderPolicy})
	}
	if err := s.db.ReplaceProviders(rows); err != nil {
		writeError(w, http.StatusInternalServerError, "replace providers: "+sanitize.ErrorString(err))
		return
	}
	s.log.Info().Int("providers", len(rows)).Msg("providers replaced")
	writeJSON(w, http.StatusOK, map[string]string{"ok": "true"})
}

// --- settings ---

type settingsJSON struct {
	LogLevel                 string `json:"logLevel"`
	MaxRetries               int    `json:"maxRetries"`
	FailureThreshold         int    `json:"failureThreshold"`
	CooldownMs               int64  `json:"cooldownMs"`
	StreamThresholdBytes     int64  `json:"streamThresholdBytes"`
	DialTimeoutMs            int64  `json:"dialTimeoutMs"`
	ResponseHeaderTimeoutMs  int64  `json:"responseHeaderTimeoutMs"`
	ReconcileIntervalSeconds int64  `json:"reconcileIntervalSeconds"`
}

func (s *Server) handleGetSettings(w http.ResponseWriter, _ *http.Request) {
	settings, err := s.db.Settings()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read settings: "+sanitize.ErrorString(err))
		return
	}
	writeJSON(w, http.StatusOK, settingsJSON{
		LogLevel:                 settings.LogLevel,
		MaxRetries:               settings.MaxRetries,
		FailureThreshold:         settings.FailureThreshold,
		CooldownMs:               settings.CooldownMs,
		StreamThresholdBytes:     settings.StreamThresholdBytes,
		DialTimeoutMs:            settings.DialTimeoutMs,
		ResponseHeaderTimeoutMs:  settings.ResponseHeaderTimeoutMs,
		ReconcileIntervalSeconds: settings.ReconcileIntervalSeconds,
	})
}

type settingsPatch struct {
	LogLevel                 *string `json:"logLevel"`
	MaxRetries               *int    `json:"maxRetries"`
	FailureThreshold         *int    `json:"failureThreshold"`
	CooldownMs               *int64  `json:"cooldownMs"`
	StreamThresholdBytes     *int64  `json:"streamThresholdBytes"`
	DialTimeoutMs            *int64  `json:"dialTimeoutMs"`
	ResponseHeaderTimeoutMs  *int64  `json:"responseHeaderTimeoutMs"`
	ReconcileIntervalSeconds *int64  `json:"reconcileIntervalSeconds"`
}

func (s *Server) handlePatchSettings(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var req settingsPatch
	if err := decoder.Decode(&req); err != nil {
		// Unknown fields and bad JSON are both client errors; the decoder's
		// message names the offending field.
		writeError(w, http.StatusUnprocessableEntity, "decode JSON body: "+sanitize.ErrorString(err))
		return
	}
	values := map[string]string{}
	if req.LogLevel != nil {
		values["log_level"] = *req.LogLevel
	}
	if req.MaxRetries != nil {
		values["max_retries"] = strconv.Itoa(*req.MaxRetries)
	}
	if req.FailureThreshold != nil {
		values["failure_threshold"] = strconv.Itoa(*req.FailureThreshold)
	}
	if req.CooldownMs != nil {
		values["cooldown_ms"] = strconv.FormatInt(*req.CooldownMs, 10)
	}
	if req.StreamThresholdBytes != nil {
		values["stream_threshold_bytes"] = strconv.FormatInt(*req.StreamThresholdBytes, 10)
	}
	if req.DialTimeoutMs != nil {
		values["dial_timeout_ms"] = strconv.FormatInt(*req.DialTimeoutMs, 10)
	}
	if req.ResponseHeaderTimeoutMs != nil {
		values["response_header_timeout_ms"] = strconv.FormatInt(*req.ResponseHeaderTimeoutMs, 10)
	}
	if req.ReconcileIntervalSeconds != nil {
		values["reconcile_interval_seconds"] = strconv.FormatInt(*req.ReconcileIntervalSeconds, 10)
	}
	if err := s.db.SetSettings(values); err != nil {
		// Validation failures (unknown key, bad value) are 422; SetSettings
		// only returns those or storage errors, and both are client-visible
		// through the sanitized message.
		writeError(w, http.StatusUnprocessableEntity, sanitize.ErrorString(err))
		return
	}
	s.handleGetSettings(w, r)
	s.log.Info().Msg("settings updated")
}

// --- helpers ---

func (s *Server) storeError(w http.ResponseWriter, err error, action string) {
	switch {
	case errors.Is(err, store.ErrNoRelay):
		writeError(w, http.StatusNotFound, "relay not found")
	case errors.Is(err, store.ErrDuplicateName):
		writeFieldError(w, http.StatusConflict, "name", "already in use")
	default:
		writeError(w, http.StatusInternalServerError, action+": "+sanitize.ErrorString(err))
	}
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func writeFieldError(w http.ResponseWriter, code int, field, msg string) {
	writeJSON(w, code, map[string]string{"error": msg, "field": field})
}

// pathID parses the {id} path segment.
func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeError(w, http.StatusNotFound, "relay not found")
		return 0, false
	}
	return id, true
}

// validateRelayURL requires an absolute http(s) URL with a host.
func validateRelayURL(raw string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return errors.New("is required")
	}
	u, err := url.Parse(trimmed)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("%q must be an absolute http(s) URL", trimmed)
	}
	return nil
}

// validatePolicy checks raw header-policy JSON when present; blank means no
// policy and passes.
func validatePolicy(raw *string) error {
	if raw == nil {
		return nil
	}
	if _, err := pool.ParseHeaderPolicy(raw); err != nil {
		return err
	}
	return nil
}
