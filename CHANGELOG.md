# Changelog

## [0.3.0](https://github.com/ecoma-io/http-relay-gateway/compare/v0.2.2...v0.3.0) (2026-09-23)


### Features

* **deploy:** keep long SSE streams alive with heartbeat comments ([#73](https://github.com/ecoma-io/http-relay-gateway/issues/73)) ([ec377cc](https://github.com/ecoma-io/http-relay-gateway/commit/ec377cc929f4a7430a6b7a8e13b07e2a08f1f623))
* **deploy:** open an SSE response before the origin answers, past a bounded grace ([#74](https://github.com/ecoma-io/http-relay-gateway/issues/74)) ([4bf1084](https://github.com/ecoma-io/http-relay-gateway/commit/4bf1084c52dbfe88fd0cbe0d39d8ebf0c456faed))


### Bug Fixes

* **gateway:** bind each attempt to one serving generation and classify every outcome ([#59](https://github.com/ecoma-io/http-relay-gateway/issues/59)) ([#61](https://github.com/ecoma-io/http-relay-gateway/issues/61)) ([21381e4](https://github.com/ecoma-io/http-relay-gateway/commit/21381e458424cfe10c5712d9b8d7a01761ae9854))
* **gateway:** classify an EOF-flush abort; pin the data-plane invariants under test ([#66](https://github.com/ecoma-io/http-relay-gateway/issues/66)) ([3d47028](https://github.com/ecoma-io/http-relay-gateway/commit/3d470289e38c9690f16507c3366036aecb4faaaf))
* **gateway:** classify EOF flush aborts and pin data-plane invariants ([3d47028](https://github.com/ecoma-io/http-relay-gateway/commit/3d470289e38c9690f16507c3366036aecb4faaaf))
* **pool:** runtime passive health survives serving generation rebuilds ([#15](https://github.com/ecoma-io/http-relay-gateway/issues/15)) ([#57](https://github.com/ecoma-io/http-relay-gateway/issues/57)) ([6f9d676](https://github.com/ecoma-io/http-relay-gateway/commit/6f9d676b9972d1162af1ce16b50d5b548cad5c0f))
* **readiness:** adopt the first resolved scope as baseline and pin the lifecycle contracts ([#65](https://github.com/ecoma-io/http-relay-gateway/issues/65)) ([afc127a](https://github.com/ecoma-io/http-relay-gateway/commit/afc127acaa55ee328924bc1e0e27655a8b7a8f3f))
* **readiness:** scope pins are part of the deployment incarnation ([#14](https://github.com/ecoma-io/http-relay-gateway/issues/14)) ([#56](https://github.com/ecoma-io/http-relay-gateway/issues/56)) ([0d06712](https://github.com/ecoma-io/http-relay-gateway/commit/0d06712fe6f91e47b6c712c76bdfa47211c3b862))


### Documentation

* **readiness:** state generations restart after a purge and recreate ([#70](https://github.com/ecoma-io/http-relay-gateway/issues/70)) ([5af8032](https://github.com/ecoma-io/http-relay-gateway/commit/5af8032d852c0c7a4f446d704fd40b1edf9e2eb0))

## [0.2.2](https://github.com/ecoma-io/http-relay-gateway/compare/v0.2.1...v0.2.2) (2026-09-22)


### Bug Fixes

* **sanitize:** redact net.OpError read/write relay addresses ([#53](https://github.com/ecoma-io/http-relay-gateway/issues/53)) ([2a4a448](https://github.com/ecoma-io/http-relay-gateway/commit/2a4a4480c25c8cbd730ddc53fa92807e61be3c2d))
* **sanitize:** redact the readfrom body-write wrapper; pin surfaces e2e ([#55](https://github.com/ecoma-io/http-relay-gateway/issues/55)) ([0b18d72](https://github.com/ecoma-io/http-relay-gateway/commit/0b18d725a181bd251bbab680ca3cdfb860a64b04))

## [0.2.1](https://github.com/ecoma-io/http-relay-gateway/compare/v0.2.0...v0.2.1) (2026-09-22)


### Bug Fixes

* **ci:** pin renovate-bumped tool downloads to a single version literal ([#50](https://github.com/ecoma-io/http-relay-gateway/issues/50)) ([eed79d1](https://github.com/ecoma-io/http-relay-gateway/commit/eed79d1e2f996ccad95da1d01b51d31cbc9d130e)), closes [#49](https://github.com/ecoma-io/http-relay-gateway/issues/49)

## [0.2.0](https://github.com/ecoma-io/http-relay-gateway/compare/v0.1.0...v0.2.0) (2026-09-22)


### Features

* **ci:** cache Go modules and the build cache across Docker builds ([cf25834](https://github.com/ecoma-io/http-relay-gateway/commit/cf2583465c06c46a7e92d481f29edc2d332e5dd8)), closes [#16](https://github.com/ecoma-io/http-relay-gateway/issues/16)
* **config:** add config.example.yaml reference layout ([#39](https://github.com/ecoma-io/http-relay-gateway/issues/39)) ([d1d4393](https://github.com/ecoma-io/http-relay-gateway/commit/d1d4393051a74d6686f3437e871195cba52d327b)), closes [#38](https://github.com/ecoma-io/http-relay-gateway/issues/38)


### Bug Fixes

* **ci:** prettier-ignore the release-please changelog ([b46cf7f](https://github.com/ecoma-io/http-relay-gateway/commit/b46cf7fa060e8d6c24fafdeb470f04db4dc61965))
* **ci:** prettier-ignore the release-please changelog ([8c4bab4](https://github.com/ecoma-io/http-relay-gateway/commit/8c4bab4b51e6c50e1202991f9f2682d070e3e199))
* **deploy:** rewrite the deno client against the real deploy api ([#41](https://github.com/ecoma-io/http-relay-gateway/issues/41)) ([767da7f](https://github.com/ecoma-io/http-relay-gateway/commit/767da7f21ff1e9ce6c6e97e14f151b8b81fe0374))
* **deploy:** route Vercel root traffic and configure relay env ([#44](https://github.com/ecoma-io/http-relay-gateway/issues/44)) ([03fa9a7](https://github.com/ecoma-io/http-relay-gateway/commit/03fa9a761419fac72cf3c54a0e07b98e11b8add9))
* lifecycle drain — deferred re-drain, shutdown budget, pause cadence ([#43](https://github.com/ecoma-io/http-relay-gateway/issues/43)) ([6e352a2](https://github.com/ecoma-io/http-relay-gateway/commit/6e352a2baafddb683bb78eef3ff65385156d0632))
* preserve relay-leg bytes, cloudflare deletes, and worker multi-value headers ([#40](https://github.com/ecoma-io/http-relay-gateway/issues/40)) ([f0eeb01](https://github.com/ecoma-io/http-relay-gateway/commit/f0eeb010f4ac4fb048e41da805edadbb268be325))
* reload signals only on real change; verification settings apply live ([#46](https://github.com/ecoma-io/http-relay-gateway/issues/46)) ([bb5dd38](https://github.com/ecoma-io/http-relay-gateway/commit/bb5dd38b4bc8b016cef8c4d5d978285d48d92986)), closes [#25](https://github.com/ecoma-io/http-relay-gateway/issues/25)
* **sanitize:** redact relay addresses and close redaction gaps ([#47](https://github.com/ecoma-io/http-relay-gateway/issues/47)) ([28d7f2d](https://github.com/ecoma-io/http-relay-gateway/commit/28d7f2ddfc8c34ced052de8c24b55e720315c23e))
* subcommand arg handling, token hygiene, worker version pin, and deploy wiring ([#42](https://github.com/ecoma-io/http-relay-gateway/issues/42)) ([a62a0eb](https://github.com/ecoma-io/http-relay-gateway/commit/a62a0eb8bedafb2ac230a03c7199c30b2063264f))


### Documentation

* list the deno organization pin among relay.env variables ([#48](https://github.com/ecoma-io/http-relay-gateway/issues/48)) ([4967295](https://github.com/ecoma-io/http-relay-gateway/commit/49672951ed089a465b970734c71ae26f6aa09ab4))

## 0.1.0 (2026-09-20)


### Features

* **admin:** admin plane — setup-once auth, REST API, database-only runtime ([59c624a](https://github.com/ecoma-io/http-relay-gateway/commit/59c624a9c9c16f45019e502621c1161174d12a09))
* **admin:** bulk relay import — and fix the adoptable-lifecycle trap ([90b1bfa](https://github.com/ecoma-io/http-relay-gateway/commit/90b1bfa5d819c312f4d2ad54bbadcff89e50fc31))
* **admin:** surface per-relay readiness in the API ([83a6c8f](https://github.com/ecoma-io/http-relay-gateway/commit/83a6c8f6a562a758d8cf85ff1ad86b8144717bcf))
* **cmd:** wire readiness registry into generation building ([362d9ff](https://github.com/ecoma-io/http-relay-gateway/commit/362d9ff968941e8d8f52196f371b5e11e22a4967))
* **deploy:** add ForwardProbe for end-to-end relay readiness ([a76ea69](https://github.com/ecoma-io/http-relay-gateway/commit/a76ea69788d5fc7d03cbd596aaecf637d7fff89b))
* **deploy:** managed fleet — platform deployers, embedded workers, reconcile ([630d36e](https://github.com/ecoma-io/http-relay-gateway/commit/630d36e7d25b3731d0eb33315d128d77df2f5c31))
* gate pool admission behind verified relay readiness ([6d4a123](https://github.com/ecoma-io/http-relay-gateway/commit/6d4a123ea99733833c90dd08cd35be2a97d21f20))
* **gateway:** accept forward-proxy inbound (absolute-form targets) ([2688891](https://github.com/ecoma-io/http-relay-gateway/commit/26888919ca012ab5b7842a15fde29123335b7ee7))
* **gateway:** engine v2 — header policy, streaming threshold, timeout knobs ([abdc04b](https://github.com/ecoma-io/http-relay-gateway/commit/abdc04b817114c0cadf6c2bf05b073d5d78ab58d))
* **gateway:** readyz probe and readiness in stats ([0189086](https://github.com/ecoma-io/http-relay-gateway/commit/0189086dbd1f2ee28d3797c8ec71ffe406f4cd6c))
* initial release of http-relay-gateway ([a611d09](https://github.com/ecoma-io/http-relay-gateway/commit/a611d09228b17fc022ab147787ba8ddbcef48ec1))
* **pool:** expose ready relay count ([1911f7a](https://github.com/ecoma-io/http-relay-gateway/commit/1911f7a87bd3e203e0387c0eb572f3322b9dcffb))
* **reconcile:** detect platform-paused relays and wait for revival ([f8f70b9](https://github.com/ecoma-io/http-relay-gateway/commit/f8f70b984507e4cafef70ca08e4890ef90259769))
* **reconcile:** gate pool admission behind verified relay readiness ([6dce020](https://github.com/ecoma-io/http-relay-gateway/commit/6dce02053795dcc1ec6e90abe15537534437b007))
* **reconcile:** readiness registry with lifecycle states ([2200c5e](https://github.com/ecoma-io/http-relay-gateway/commit/2200c5ee7b52648a55567227f1d3a050f7e31116))
* **store:** make SQLite the source of truth behind a legacy config bridge ([2df3566](https://github.com/ecoma-io/http-relay-gateway/commit/2df3566703da41d668a88c7bc4542bdd1ca30348))
* **web:** management SPA — setup/login, relays, settings; embed + CI web job ([9cb1993](https://github.com/ecoma-io/http-relay-gateway/commit/9cb1993f55c98e2b4f398ca3dd17d1ef1b1db646))


### Bug Fixes

* **ci:** prettier-reflow docs, errcheck in bench/readiness tests ([446a66f](https://github.com/ecoma-io/http-relay-gateway/commit/446a66fa694b161eb363b65008cc5091199899b0))
* **deploy:** map vercel token rejections to credential errors ([4b23cb3](https://github.com/ecoma-io/http-relay-gateway/commit/4b23cb3418a9e761062cfba5746b4af89955da3c))
* **e2e:** strict-gate harness, DNS-free seeds, readyz asserts ([ea4b9ce](https://github.com/ecoma-io/http-relay-gateway/commit/ea4b9ce7fdd006205031c9542db77a31893ec3f7))
* **gateway:** zero-ready 503 short-circuit and harness seeds ([f16aa99](https://github.com/ecoma-io/http-relay-gateway/commit/f16aa99e4f3735773fef8d48d372e44ba8d09978))
* **reconcile:** replacement failing verification must not keep serving ([c98da0f](https://github.com/ecoma-io/http-relay-gateway/commit/c98da0f76f52ce7a6234a39df259f943ebd988f5))
* **web:** allow esbuild's install script — unlisted builds hard-fail pnpm 12 ([6281d22](https://github.com/ecoma-io/http-relay-gateway/commit/6281d226349dfef144a143b01400800fcf5d90e5))
* **workspace:** ship /app/data in the image so the data volume is writable ([e9b8392](https://github.com/ecoma-io/http-relay-gateway/commit/e9b8392b894519fc1bd2efa9a7bb9c18751d5836))


### Documentation

* readiness gate contract, env knobs, and benchmarks ([8285f92](https://github.com/ecoma-io/http-relay-gateway/commit/8285f925615a1d260d3a9e91815b3a30b7f4f7ae))
* **readiness:** note the Ready-versus-removing self-healing race ([c68c6b8](https://github.com/ecoma-io/http-relay-gateway/commit/c68c6b86ca7b6dd91d4fa413f737f042da573c16))
