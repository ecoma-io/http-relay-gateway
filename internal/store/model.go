package store

// RelayOrigin distinguishes relays imported from the legacy YAML file
// (synced by the transitional config bridge) from relays created and
// deployed through the management plane.
type RelayOrigin string

const (
	OriginLegacy  RelayOrigin = "legacy"
	OriginManaged RelayOrigin = "managed"
)

// Deployment lifecycle statuses. active is the only status whose relay
// serves traffic and the only one whose deployment row joins into Relays().
// paused means the platform answered a probe but no relay worker is behind
// the URL (a free-tier suspension above all) — the relay waits for revival
// and is never redeployed, because a redeploy cannot lift the suspension.
const (
	DeployPending     = "pending"
	DeployDeploying   = "deploying"
	DeployActive      = "active"
	DeployStale       = "stale"
	DeployUnreachable = "unreachable"
	DeployError       = "error"
	DeployPaused      = "paused"
)

// Platforms lists the edge platforms a relay can be deployed to.
var Platforms = []string{"vercel", "cloudflare", "deno"}

// ProviderRow is one provider's body-size contract.
type ProviderRow struct {
	Name    string
	MaxBody int64
	// HeaderPolicy is raw policy JSON or nil for verbatim header forwarding.
	HeaderPolicy *string
}

// DefaultProviderMaxBody applies to a relay whose provider has no providers
// row — the resolution the generation builder performs, not a stored key.
const DefaultProviderMaxBody = 8 << 20 // 8 MiB

// AccountRow is one platform account (its credential stays readable only
// through Tokens).
type AccountRow struct {
	ID         int64
	Name       string
	Platform   string
	Token      string
	AccountRef string
	VerifiedAt int64
	CreatedAt  int64
	UpdatedAt  int64
}

// DeploymentRow is the managed deployment behind one relay. There is exactly
// one per managed relay; a redeploy overwrites it.
type DeploymentRow struct {
	ID            int64
	RelayID       int64
	AccountID     int64
	Platform      string
	Project       string
	ExternalID    string
	URL           string
	Version       string
	AuthToken     string
	Status        string
	LastError     string
	LastCheckedAt int64
	DeployedAt    int64
	CreatedAt     int64
	UpdatedAt     int64
}

// RelayRow is one relay with its active deployment, if any, joined in.
type RelayRow struct {
	ID           int64
	Name         string
	Provider     string
	URL          string
	Active       bool
	Origin       RelayOrigin
	AccountID    *int64
	HeaderPolicy *string
	// Deployment is non-nil only while the deployment status is active —
	// a relay that is not verifiably serving contributes nothing to the pool.
	Deployment *DeploymentRow
	// InactiveDeployment reports that a deployment row exists but is not
	// active (stale, unreachable, paused, error, deploying). The deployment
	// owns the managed relay's serving, so the relay row's own URL must not
	// serve either — the generation builder drops such relays entirely.
	InactiveDeployment bool
	CreatedAt          int64
	UpdatedAt          int64
}
