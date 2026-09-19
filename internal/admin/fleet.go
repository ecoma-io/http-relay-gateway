package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"http-relay-gateway/internal/deploy"
	"http-relay-gateway/internal/sanitize"
	"http-relay-gateway/internal/store"
)

// Fleet is the admin plane's view of the reconciler: every method enqueues
// work and returns immediately (the reconciler answers with 202).
type Fleet interface {
	CheckAll()
	Reconcile()
	Redeploy(relayID int64)
	Adopt(relayID, accountID int64)
}

// WithFleet wires the reconciler and the deployer factory into the admin
// plane. Without it the fleet endpoints answer 501 — the admin plane stays
// usable for relays-only deployments.
func WithFleet(fleet Fleet, factory deploy.Factory) Option {
	return func(s *Server) {
		s.fleet = fleet
		s.factory = factory
	}
}

// verifyTimeout bounds one platform credential check; the platform API
// clients have their own longer budgets, but a management call hanging
// minutes is worse than failing it.
const verifyTimeout = 30 * time.Second

// --- accounts ---

type accountJSON struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	Platform   string `json:"platform"`
	AccountRef string `json:"accountRef"`
	VerifiedAt int64  `json:"verifiedAt"`
	// TokenLast4 is the most the credential ever surfaces.
	TokenLast4 string `json:"tokenLast4"`
	CreatedAt  int64  `json:"createdAt"`
	UpdatedAt  int64  `json:"updatedAt"`
}

func (s *Server) accountView(row store.AccountRow) accountJSON {
	view := accountJSON{
		ID: row.ID, Name: row.Name, Platform: row.Platform,
		AccountRef: row.AccountRef, VerifiedAt: row.VerifiedAt,
		CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt,
	}
	if token, err := s.db.Tokens().PlatformToken(row.ID); err == nil {
		view.TokenLast4 = last4(token)
	}
	return view
}

func (s *Server) handleListAccounts(w http.ResponseWriter, _ *http.Request) {
	rows, err := s.db.Accounts()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read accounts: "+sanitize.ErrorString(err))
		return
	}
	views := make([]accountJSON, 0, len(rows))
	for _, row := range rows {
		views = append(views, s.accountView(row))
	}
	writeJSON(w, http.StatusOK, views)
}

func (s *Server) handleGetAccount(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	row, err := s.db.Account(id)
	if err != nil {
		s.storeError(w, err, "read account")
		return
	}
	writeJSON(w, http.StatusOK, s.accountView(row))
}

type accountRequest struct {
	Name       *string `json:"name"`
	Platform   *string `json:"platform"`
	Token      *string `json:"token"`
	AccountRef *string `json:"accountRef"`
}

// checkCredentials runs the platform verify call shared by account create,
// token update and explicit re-verify. The answer maps to the API's error
// vocabulary: bad credentials are the caller's problem (422), anything else
// is a platform-side failure (502).
func (s *Server) checkCredentials(w http.ResponseWriter, platform, token, accountRef string) (string, bool) {
	client, err := s.factory.For(platform, token, accountRef)
	if err != nil {
		writeFieldError(w, http.StatusUnprocessableEntity, "platform", err.Error())
		return "", false
	}
	ctx, cancel := context.WithTimeout(context.Background(), verifyTimeout)
	defer cancel()
	ref, err := client.Verify(ctx)
	if errors.Is(err, deploy.ErrCredentials) {
		writeFieldError(w, http.StatusUnprocessableEntity, "token", "platform rejected the credentials")
		return "", false
	}
	if err != nil {
		writeError(w, http.StatusBadGateway, "platform unreachable: "+sanitize.ErrorString(err))
		return "", false
	}
	return ref, true
}

func (s *Server) handleCreateAccount(w http.ResponseWriter, r *http.Request) {
	var req accountRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "decode JSON body: "+sanitize.ErrorString(err))
		return
	}
	if req.Name == nil || strings.TrimSpace(*req.Name) == "" {
		writeFieldError(w, http.StatusUnprocessableEntity, "name", "is required")
		return
	}
	if req.Platform == nil || strings.TrimSpace(*req.Platform) == "" {
		writeFieldError(w, http.StatusUnprocessableEntity, "platform", "is required")
		return
	}
	if req.Token == nil || strings.TrimSpace(*req.Token) == "" {
		writeFieldError(w, http.StatusUnprocessableEntity, "token", "is required")
		return
	}
	name := strings.TrimSpace(*req.Name)
	platform := strings.ToLower(strings.TrimSpace(*req.Platform))
	token := strings.TrimSpace(*req.Token)
	accountRef := ""
	if req.AccountRef != nil {
		accountRef = strings.TrimSpace(*req.AccountRef)
	}
	if !store.ValidPlatform(platform) {
		writeFieldError(w, http.StatusUnprocessableEntity, "platform", "is not a supported platform")
		return
	}
	// The token is proven against the platform before it is stored; a
	// mistyped credential never becomes an account.
	ref, ok := s.checkCredentials(w, platform, token, accountRef)
	if !ok {
		return
	}
	if accountRef == "" {
		accountRef = ref
	}
	id, err := s.db.CreateAccount(name, platform, token, accountRef, time.Now().Unix())
	if err != nil {
		s.storeError(w, err, "create account")
		return
	}
	s.log.Info().Int64("id", id).Str("name", name).Str("platform", platform).Msg("account created")
	row, err := s.db.Account(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read account: "+sanitize.ErrorString(err))
		return
	}
	writeJSON(w, http.StatusCreated, s.accountView(row))
}

func (s *Server) handlePatchAccount(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req accountRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "decode JSON body: "+sanitize.ErrorString(err))
		return
	}
	row, err := s.db.Account(id)
	if err != nil {
		s.storeError(w, err, "read account")
		return
	}
	var patch store.AccountPatch
	if req.Name != nil {
		if strings.TrimSpace(*req.Name) == "" {
			writeFieldError(w, http.StatusUnprocessableEntity, "name", "must not be blank")
			return
		}
		name := strings.TrimSpace(*req.Name)
		patch.Name = &name
	}
	if req.AccountRef != nil {
		ref := strings.TrimSpace(*req.AccountRef)
		patch.AccountRef = &ref
	}
	if req.Token != nil {
		token := strings.TrimSpace(*req.Token)
		if token == "" {
			writeFieldError(w, http.StatusUnprocessableEntity, "token", "must not be blank")
			return
		}
		// A rotated token must still work before it replaces the old one.
		if _, ok := s.checkCredentials(w, row.Platform, token, row.AccountRef); !ok {
			return
		}
		patch.Token = &token
	}
	if err := s.db.UpdateAccount(id, patch); err != nil {
		s.storeError(w, err, "update account")
		return
	}
	s.log.Info().Int64("id", id).Msg("account updated")
	updated, err := s.db.Account(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read account: "+sanitize.ErrorString(err))
		return
	}
	writeJSON(w, http.StatusOK, s.accountView(updated))
}

func (s *Server) handleVerifyAccount(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	row, err := s.db.Account(id)
	if err != nil {
		s.storeError(w, err, "read account")
		return
	}
	token, err := s.db.Tokens().PlatformToken(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read credential: "+sanitize.ErrorString(err))
		return
	}
	ref, ok := s.checkCredentials(w, row.Platform, token, row.AccountRef)
	if !ok {
		return
	}
	now := time.Now().Unix()
	if err := s.db.MarkAccountVerified(id, ref, now); err != nil {
		s.storeError(w, err, "record verification")
		return
	}
	s.log.Info().Int64("id", id).Str("platform", row.Platform).Msg("account verified")
	updated, err := s.db.Account(id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read account: "+sanitize.ErrorString(err))
		return
	}
	writeJSON(w, http.StatusOK, s.accountView(updated))
}

func (s *Server) handleDeleteAccount(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	force := false
	switch r.URL.Query().Get("force") {
	case "1", "true":
		force = true
	case "", "0", "false":
	default:
		writeFieldError(w, http.StatusUnprocessableEntity, "force", "must be true or false")
		return
	}
	if err := s.db.DeleteAccount(id, force); err != nil {
		s.storeError(w, err, "delete account")
		return
	}
	s.log.Info().Int64("id", id).Bool("force", force).Msg("account deleted")
	writeJSON(w, http.StatusOK, map[string]string{"ok": "true"})
}

// --- relay fleet actions ---

// actionReply is the 202 body for everything the reconciler will do later.
func actionReply(w http.ResponseWriter, action string) {
	writeJSON(w, http.StatusAccepted, map[string]string{"ok": "true", "queued": action})
}

func (s *Server) handleRedeployRelay(w http.ResponseWriter, r *http.Request) {
	if s.fleet == nil {
		writeError(w, http.StatusNotImplemented, "no reconciler is running")
		return
	}
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if _, err := s.db.Relay(id); err != nil {
		s.storeError(w, err, "read relay")
		return
	}
	s.fleet.Redeploy(id)
	s.log.Info().Int64("id", id).Msg("redeploy queued")
	actionReply(w, "redeploy")
}

func (s *Server) handleAdoptRelay(w http.ResponseWriter, r *http.Request) {
	if s.fleet == nil {
		writeError(w, http.StatusNotImplemented, "no reconciler is running")
		return
	}
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	relay, err := s.db.Relay(id)
	if err != nil {
		s.storeError(w, err, "read relay")
		return
	}
	// Managed with an account is already managed; a managed row whose
	// account was force-deleted is adoptable again — that is its recovery.
	if relay.Origin == store.OriginManaged && relay.AccountID != nil {
		writeError(w, http.StatusConflict, "relay is already managed")
		return
	}
	var req struct {
		AccountID *int64 `json:"accountId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "decode JSON body: "+sanitize.ErrorString(err))
		return
	}
	if req.AccountID == nil || *req.AccountID < 1 {
		writeFieldError(w, http.StatusUnprocessableEntity, "accountId", "is required")
		return
	}
	account, err := s.db.Account(*req.AccountID)
	if err != nil {
		s.storeError(w, err, "read account")
		return
	}
	if relay.Provider != account.Platform {
		writeFieldError(w, http.StatusUnprocessableEntity, "accountId",
			"relay provider "+relay.Provider+" does not match account platform "+account.Platform)
		return
	}
	s.fleet.Adopt(id, account.ID)
	s.log.Info().Int64("relay", id).Int64("account", account.ID).Msg("adopt queued")
	actionReply(w, "adopt")
}

// --- fleet status ---

func (s *Server) handleFleetVersion(w http.ResponseWriter, _ *http.Request) {
	counts, err := s.db.PlatformCounts()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read deployments: "+sanitize.ErrorString(err))
		return
	}
	if counts == nil {
		counts = map[string]int{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"workerVersion": deploy.RelayVersion,
		"deployments":   counts,
	})
}

func (s *Server) handleFleetCheck(w http.ResponseWriter, _ *http.Request) {
	if s.fleet == nil {
		writeError(w, http.StatusNotImplemented, "no reconciler is running")
		return
	}
	s.fleet.CheckAll()
	actionReply(w, "check")
}

func (s *Server) handleFleetReconcile(w http.ResponseWriter, _ *http.Request) {
	if s.fleet == nil {
		writeError(w, http.StatusNotImplemented, "no reconciler is running")
		return
	}
	s.fleet.Reconcile()
	actionReply(w, "reconcile")
}

// deleteRemote removes the deployed worker from its platform, then the relay
// row. A platform-side failure keeps the row so the deletion can be retried
// — a silently orphaned worker is worse than a visible failure.
func (s *Server) deleteRemote(w http.ResponseWriter, r *http.Request, id int64) {
	if s.factory == nil {
		writeError(w, http.StatusNotImplemented, "remote deletion is not available yet")
		return
	}
	dep, err := s.db.Deployment(id)
	if errors.Is(err, store.ErrNoDeployment) {
		// Nothing was ever deployed; the local row is the whole story.
		s.finishRelayDelete(w, id)
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read deployment: "+sanitize.ErrorString(err))
		return
	}
	account, err := s.db.Account(dep.AccountID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read account: "+sanitize.ErrorString(err))
		return
	}
	token, err := s.db.Tokens().PlatformToken(account.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "read credential: "+sanitize.ErrorString(err))
		return
	}
	client, err := s.factory.For(account.Platform, token, account.AccountRef)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "build deployer: "+sanitize.ErrorString(err))
		return
	}
	if err := client.Delete(r.Context(), dep.Project); err != nil {
		writeError(w, http.StatusBadGateway, "remote delete failed: "+sanitize.ErrorString(err))
		return
	}
	s.finishRelayDelete(w, id)
}

func (s *Server) finishRelayDelete(w http.ResponseWriter, id int64) {
	if err := s.db.DeleteRelay(id); err != nil {
		s.storeError(w, err, "delete relay")
		return
	}
	s.log.Info().Int64("id", id).Msg("relay deleted")
	writeJSON(w, http.StatusOK, map[string]string{"ok": "true"})
}
