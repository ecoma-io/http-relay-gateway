## Description

Closes #

## Type of change

- [ ] Bug fix — behavior disagrees with the documented contract
- [ ] Contract change — documented behavior moves; README updated in this PR
- [ ] New feature
- [ ] Refactor — no behavior change
- [ ] Documentation only
- [ ] Build / CI / tooling

## Contract impact

- [ ] Trivial — moves no documented behavior (the wire contract, provider pinning, body limits, failover, reload semantics, the logging/`/stats` contract)
- [ ] Contract-bearing — the affected README sections are updated in this same PR

## Could this fail silently?

<!-- The dangerous direction in this repository is the quiet one: the client's
     identity leaking upstream, a failed reload swapping configuration anyway,
     a body bypassing a provider limit, a request retried after the response
     started. Writing "no" is fine when it is true; leaving this blank is not. -->

- [ ] It cannot, and I considered the quiet direction
- [ ] It could, and a test pins the case where it would — test name:

## How this was verified

1.

- [ ] `gofmt -w .` — clean
- [ ] `go vet ./...` — clean
- [ ] `go test -race ./...` — green
- [ ] `go test ./e2e/` — green (or a `-short` run plus one line on why E2E is unaffected)

## Checklist

- [ ] Self-reviewed the diff
- [ ] Docs updated in the same pass (README when behavior moves)
- [ ] No private relay URLs, provider credentials, platform tokens, or database files anywhere in the diff
- [ ] I have the right to contribute this work under the Apache License 2.0

## AI-assisted development

If any commit in this pull request was AI-assisted, the pull request's last
commit carries its disclosure trailer — `Assisted-by: <tool>` or
`Generated-by: <tool>` — one trailer per pull request, not one per commit.

- [ ] No AI-assisted commits in this PR
- [ ] AI-assisted — the disclosure trailer is on the last commit
