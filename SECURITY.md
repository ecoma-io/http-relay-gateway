# Security Policy

## Reporting a vulnerability

**Do not open a public issue.** A public report of an exposed relay URL or a
leaked credential is itself a disclosure.

- Preferred: [a private security advisory](https://github.com/ecoma-io/http-relay-gateway/security/advisories/new)
  (the repository's _Security_ tab → _Report a vulnerability_).
- Or email **john.itvn@gmail.com** with: a description of the issue, a
  reproduction or proof of concept, and your assessment of the impact.

Please never paste real relay URLs with private hostnames, provider
credentials, or `config.yaml` contents into any report — a PoC that needs
them should hold placeholders.

## What counts as a vulnerability here

http-relay-gateway is an **unauthenticated** network hop whose entire
purpose is hiding the caller. There is no login to bypass and no credential
store to steal — the security boundary _is_ the network placement, and the
invariants below are what keep that boundary meaningful. Two defect classes
count as security vulnerabilities even when the underlying mechanism is an
ordinary bug:

- **The client's identity reaching the edge relay or the provider.** The
  documented contract is that the gateway forwards end-to-end headers
  verbatim but never adds `X-Forwarded-For` (or any substitute) and strips
  the `X-Relay-Provider` pin. A change that leaks the caller's address,
  a proxy-chain header, or the gateway's own identity upstream defeats the
  reason the gateway exists — the report is a disclosure, not a bug.
- **A provider credential or private relay URL reaching logs, `/stats`,
  error text, or a response body.** `Authorization` and friends must flow
  through the pipe, never into it; `/stats` exposes relay names, providers,
  and health — never URLs. A redaction that misses a format fails in the
  quiet direction.

Supply-chain defects in the CI itself (an unpinned action, an unpinned
container image, a workflow interpolating attacker-reachable input into a
shell command) are also in scope; `.github/semgrep/` pins the classes this
repository treats as vulnerabilities in its own automation.

Everything else — a miscounted metric, a wrong status code, a cooldown that
expires a second early — is an ordinary bug, and the
[public tracker](https://github.com/ecoma-io/http-relay-gateway/issues)
is the right place for it.

## Deployment exposure

The gateway authenticates no one, by design: it is an internal-network
sidecar. Exposing port 20130 (or whatever `LISTEN_ADDR` binds) beyond the
compose network — a published host port on a shared machine, a tunnel, an
ingress route — hands arbitrary third parties a relay hop through your edge
deployments and your provider credentials. Treat any such exposure as a
vulnerability in the deployment, and report it the same way.

## Supported versions

This project is pre-1.0. Security fixes are applied to the `main` branch
and released in the next version; there is no long-term support branch and
no backport policy for older tags.

## Service levels

Stated honestly for a single-maintainer project:

- **Acknowledgement** within 48 hours of a report.
- **Fix published** within 14 days of a confirmed report — "published"
  meaning a tagged release, not an unmerged commit.
- The **disclosure date** is agreed with the reporter; credit in the
  release notes unless anonymity is preferred.
