# Public fork CI and real SDK compilation

The normal `pull_request` workflow requires no repository secret. It keeps `contents: read`, disables
checkout credential persistence, never uses `pull_request_target`, and neither pushes an image nor
exports a remote cache. Fork contributors may still need a maintainer's normal GitHub Actions run
approval; this workflow does not bypass that policy. Cloud verification is separate from actual
GitHub job execution.

## What changed and why

The old Debian 9 recipe used a build ARG and wrote a SignalWire token into `/etc/apt/auth.conf`.
That can persist in image/intermediate layers and `mode=max` exported cache. This is a conditional
exposure risk, not evidence that any actual token leaked. This change does not inspect old caches or
extract secrets. An owner should assess and, if applicable, rotate credentials/restrict/remove old
artifacts through their own approved process; no account changes are performed by this patch.

The new Dockerfile has no credential parameter, private APT repository or auth.conf step. It uses
public Debian packages and fixed official Git source commits. `.dockerignore` is a source allowlist;
.git, .env, keys, local reports and compiled outputs are excluded. Do not add credentials to COPY,
ARG, ENV, URLs, logs or archives. If a future approved private SDK path is added, it must use a
BuildKit `RUN --mount=type=secret` temporary file in the same RUN that consumes it, never copy the
secret into a layer, and stay outside untrusted fork CI. No such private path is needed here.

Old checkout/buildx/cache/build-push actions and the temporary registry have been removed. The only
JavaScript action is checkout v6.1.0 pinned to commit
`d23441a48e516b6c34aea4fa41551a30e30af803` (Node24). GitHub-hosted `ubuntu-24.04` supplies the supported
runner; a self-hosted replacement would need at least runner 2.327.1. Docker build/test commands run
on the host rather than relying on an old Node runtime inside a Debian container. Reference:
https://github.com/actions/checkout/tree/d23441a48e516b6c34aea4fa41551a30e30af803
https://docs.docker.com/build/building/secrets/

## Locked inputs and honest version scope

- Debian bookworm slim is pinned by manifest digest in the Dockerfile.
- `core-packages.lock` / `fs-packages.lock` pin explicit top-level APT package versions, including
  GCC toolchain package, CMake, Python, SQLite development package and librdkafka 2.0.2.
- APT verifies Debian signed indexes and package hashes. Transitive packages follow the signed
  Debian repository and are recorded in `/opt/build-manifest/*-packages.tsv`; this is auditable, not
  a claim of bit-for-bit hermetic builds across future transitive security updates. Missing exact
  package versions fail the build; there is no silent latest-version fallback. Refresh pins/digest
  deliberately and revalidate when Debian removes superseded versions.
- `ci/fs-sources.lock` pins official FreeSWITCH 1.10.12, Sofia-SIP 1.13.18 and SpanDSP 3.1.1 commits.
  `prepare-fs-sources.sh` fetches/checks those hashes on the runner, archives only Git-tracked files
  (no .git/config/credential state), and records archive checksums. The container verifies the
  manifest and checksums before building. It does not disable HTTPS verification or change CAs.
- CMake and the module Makefile require librdkafka >=2.0.2. Debian 9 / old 0.x clients are not claimed
  supported. This replaces the obsolete build environment instead of masking missing APIs.

## Two required jobs, distinct evidence

1. **Core ASan-UBSan and Python:** build the C++17 core with sanitizers and every test binary; execute
   `ctest --output-on-failure --no-tests=error` in a container with external networking disabled.
   Tests are outbox, ordering, Python discovery (including migration/capacity/interruptions), and
   ops_interop, which actually opens Python-tool-generated databases through the C++ core. No empty
   test suite, compiler failure or test failure is allowed to pass. A second local-cache build runs
   the same tests again; Docker build cache is never treated as a test result.
2. **Real public FreeSWITCH SDK compile:** independently prepare pinned public sources, compile the
   genuine FS core/SDK/minimal modules/tools, and compile/link `mod_event_kafka.so` against it.
   Check actual pkg-config versions and unresolved dynamic symbols. This is not a stub and does
   not alter FS core business logic. It does NOT run the FS upstream test suite, all optional media
   modules, load our module, place calls or qualify the deployment's ABI/media/storage. Those remain
   explicit runtime/integration gates, not hidden behind a green core job.

Both jobs run for ordinary forks without SignalWire credentials. Failure of either job is visible;
there is no `continue-on-error`, conditional secret-based skip or manual success substitute.

## Reproduction

Run from the repository root with Docker/BuildKit:

```bash
docker build --no-cache --target core-ci -f .devcontainer/Dockerfile -t event-kafka-core-ci .
docker run --rm --network none event-kafka-core-ci
docker build --target core-ci -f .devcontainer/Dockerfile -t event-kafka-core-ci .
docker run --rm --network none event-kafka-core-ci
bash ci/prepare-fs-sources.sh
docker build --target fs-module -f .devcontainer/Dockerfile -t event-kafka-fs-module .
docker run --rm --network none event-kafka-fs-module
```

The devcontainer uses `core-deps`, with repository-root context, for public core development. It no
longer silently promises an authenticated SDK. Use the explicit fs-module target for SDK compile.
Prepared source archives live in ignored `.ci-sources/`; they are not committed or credentials.
The SDK build mounts these public inputs read-only and removes build-only source trees in the same
RUN. Core and module builds mount the allowlisted source context read-only and copy only required
source files in the same build RUN; prepared archives and private state are not persisted. This
also limits full-tree layer duplication on vfs-backed executors.

No registry or external cache is used. Local BuildKit cache contains only public source/dependencies
and compiled outputs. Cold/hot builds and negative failure-propagation tests must be recorded on the
candidate head; old successful results are not new-head validation. Inspect only newly generated
images/cache used for the experiment, never historical potentially secret-bearing layers.

## Deliverable-age TTL and one retry budget

Public CI stays the secretless Debian bookworm workflow: checkout v6 only, no `debian:9`, no
`actions/cache` v2, no repository secret, and the same ctest targets (`outbox`, `ordering`,
`drill_tools`, `ops_interop`) plus the separate real public FreeSWITCH SDK compile job.

Schema user_version 5 adds `eligible_at_ms`. Age expiry removes only a pending row that is already
deliverable: ungrouped, or the ordered row whose `call_seq` equals that call's `next_seq`. Time spent
waiting behind an in-flight, retrying, or dead head does not count. An in-flight head is not expired
in place. A dead head stays a barrier and does not advance the cursor. `outbox-ttl-ms` 0 disables age
expiry only. Synchronous produce failures and delivery-report failures share one policy: a permanent
broker error, or a durable attempt count that has reached `max_attempts_before_dead`, marks that
in-flight row dead; other failures retry with the same backoff. Retrying a TTL-dead row does not reset
how long it has been deliverable. These checks do not load FreeSWITCH or establish a production incident
cause.
