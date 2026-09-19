package pool

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// HeaderPolicy adjusts the request-header set on the relay leg: Strip
// removes headers, Set replaces headers with static values (canonical
// names; Add order preserved for multi-value entries). A relay's policy
// inherits its provider's — see InheritHeaderPolicy.
type HeaderPolicy struct {
	Strip []string
	Set   map[string][]string
}

// policyDeniedHeaders may never be stripped or set: hop-by-hop headers are
// the transport's business, Host/Content-Length are fixed by the request,
// and the X-Relay-* headers are the relay spec itself.
var policyDeniedHeaders = func() map[string]bool {
	denied := map[string]bool{
		"Host":           true,
		"Content-Length": true,
		"X-Relay-Target": true,
		"X-Relay-Path":   true,
		"X-Relay-Token":  true,
	}
	for name := range HopByHop {
		denied[name] = true
	}
	return denied
}()

type headerPolicyJSON struct {
	Strip []string            `json:"strip"`
	Set   map[string][]string `json:"set"`
}

// ParseHeaderPolicy parses and validates raw policy JSON; nil (or empty)
// input means no policy — headers forward verbatim. Header names are
// canonicalized, denied names are rejected, and set values must be
// non-empty. A policy that names nothing also means verbatim.
func ParseHeaderPolicy(raw *string) (*HeaderPolicy, error) {
	if raw == nil || strings.TrimSpace(*raw) == "" {
		return nil, nil
	}
	var parsed headerPolicyJSON
	if err := json.Unmarshal([]byte(*raw), &parsed); err != nil {
		return nil, fmt.Errorf("decode header policy: %w", err)
	}
	policy := &HeaderPolicy{Set: map[string][]string{}}
	for _, name := range parsed.Strip {
		cn := http.CanonicalHeaderKey(strings.TrimSpace(name))
		if cn == "" {
			continue
		}
		if err := checkPolicyHeader(cn); err != nil {
			return nil, err
		}
		policy.Strip = append(policy.Strip, cn)
	}
	for name, values := range parsed.Set {
		cn := http.CanonicalHeaderKey(strings.TrimSpace(name))
		if cn == "" {
			continue
		}
		if err := checkPolicyHeader(cn); err != nil {
			return nil, err
		}
		for _, value := range values {
			if strings.TrimSpace(value) == "" {
				return nil, fmt.Errorf("header policy set %q has an empty value", cn)
			}
		}
		if len(values) > 0 {
			policy.Set[cn] = values
		}
	}
	if len(policy.Strip) == 0 && len(policy.Set) == 0 {
		return nil, nil
	}
	return policy, nil
}

func checkPolicyHeader(name string) error {
	if policyDeniedHeaders[name] {
		return fmt.Errorf("header policy may not touch %q", name)
	}
	return nil
}

// InheritHeaderPolicy merges a relay's policy over its provider's: strips
// union, per-name set entries override, and any stripped name is removed
// from the effective set — strip wins over the inherited set, including an
// override that names the same header in its own Set.
func InheritHeaderPolicy(relay, provider *HeaderPolicy) *HeaderPolicy {
	switch {
	case provider == nil:
		return relay
	case relay == nil:
		return provider
	}
	stripped := map[string]bool{}
	merged := &HeaderPolicy{Set: map[string][]string{}}
	for _, source := range [2][]string{provider.Strip, relay.Strip} {
		for _, name := range source {
			if !stripped[name] {
				stripped[name] = true
				merged.Strip = append(merged.Strip, name)
			}
		}
	}
	for name, values := range provider.Set {
		if !stripped[name] {
			merged.Set[name] = values
		}
	}
	for name, values := range relay.Set {
		if !stripped[name] {
			merged.Set[name] = values
		}
	}
	return merged
}
