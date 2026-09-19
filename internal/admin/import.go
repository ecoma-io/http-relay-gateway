package admin

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"http-relay-gateway/internal/pool"
	"http-relay-gateway/internal/sanitize"
	"http-relay-gateway/internal/store"
)

// maxImportItems bounds one batch. It is an abuse guard, not a product
// limit — a hand-managed relay list is dozens of entries, never thousands.
const maxImportItems = 500

// The bulk import creates legacy-origin relays from an existing list —
// the migration path for URLs managed outside the gateway before it.
// Everything adopts the ordinary create semantics (validation, unique
// names), but a bad row rejects alone instead of failing the batch: the
// reply reports what landed and what did not, always with 200.

type importItem struct {
	Name     *string `json:"name"`
	Provider *string `json:"provider"`
	URL      *string `json:"url"`
	Active   *bool   `json:"active"`
}

type importRequest struct {
	Items []importItem `json:"items"`
}

type importRejection struct {
	Name  string `json:"name"`
	Error string `json:"error"`
}

type importReply struct {
	Imported int               `json:"imported"`
	Rejected []importRejection `json:"rejected"`
}

func (s *Server) handleImportRelays(w http.ResponseWriter, r *http.Request) {
	var req importRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "decode JSON body: "+sanitize.ErrorString(err))
		return
	}
	if len(req.Items) == 0 {
		writeFieldError(w, http.StatusUnprocessableEntity, "items", "must not be empty")
		return
	}
	if len(req.Items) > maxImportItems {
		writeFieldError(w, http.StatusUnprocessableEntity, "items",
			"a batch holds at most "+strconv.Itoa(maxImportItems)+" items")
		return
	}

	seen := make(map[string]struct{}, len(req.Items))
	reply := importReply{Rejected: []importRejection{}}
	for _, item := range req.Items {
		name, rerr := s.importOne(item, seen)
		if rerr != nil {
			reply.Rejected = append(reply.Rejected, importRejection{Name: name, Error: rerr.Error()})
			continue
		}
		reply.Imported++
	}
	s.log.Info().Int("imported", reply.Imported).Int("rejected", len(reply.Rejected)).
		Msg("relays imported")
	writeJSON(w, http.StatusOK, reply)
}

// importOne validates and inserts one item. The returned name is best-effort
// ("" when none was given) so a rejection can still point at its row.
func (s *Server) importOne(item importItem, seen map[string]struct{}) (string, error) {
	if item.Name == nil || strings.TrimSpace(*item.Name) == "" {
		return "", errors.New("name is required")
	}
	name := strings.TrimSpace(*item.Name)
	if _, dup := seen[name]; dup {
		return name, errors.New("duplicate name in batch")
	}
	if item.Provider == nil || strings.TrimSpace(*item.Provider) == "" {
		return name, errors.New("provider is required")
	}
	provider := strings.ToLower(strings.TrimSpace(*item.Provider))
	if pool.ReservedProviders[provider] {
		return name, errors.New("provider " + provider + " is reserved")
	}
	if item.URL == nil || strings.TrimSpace(*item.URL) == "" {
		return name, errors.New("url is required")
	}
	if err := validateRelayURL(*item.URL); err != nil {
		return name, err
	}
	active := true
	if item.Active != nil {
		active = *item.Active
	}
	if _, err := s.db.CreateRelay(name, provider, strings.TrimSpace(*item.URL), active, nil, nil); err != nil {
		if errors.Is(err, store.ErrDuplicateName) {
			return name, errors.New("name already in use")
		}
		return name, errors.New(sanitize.ErrorString(err))
	}
	seen[name] = struct{}{}
	return name, nil
}
