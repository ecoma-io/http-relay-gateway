# Changelog

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
