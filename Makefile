.PHONY: web web-stub

# Build the management SPA (Vue 3 + Vite) and sync the bundle into the
# directory embedded by internal/web. Run this before `go build`/`docker
# build` when the UI changed; Docker and CI run it automatically.
web:
	pnpm install --frozen-lockfile
	pnpm --filter web build
	rm -rf internal/web/dist
	cp -r web/dist internal/web/dist

# Restore the committed placeholder after a local `make web`, so the working
# tree goes back to `go build`-without-node state (untracked bundle assets
# removed, tracked stub restored).
web-stub:
	rm -rf internal/web/dist
	git checkout -- internal/web/dist/index.html
