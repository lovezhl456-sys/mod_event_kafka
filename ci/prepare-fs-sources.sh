#!/bin/bash
# Public Git only; credentials are neither accepted as arguments nor archived.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .ci-sources
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
while read -r name url commit; do
    git init -q "$work/$name"
    git -C "$work/$name" fetch --depth=1 "$url" "$commit"
    git -C "$work/$name" checkout --detach FETCH_HEAD
    test "$(git -C "$work/$name" rev-parse HEAD)" = "$commit"
    git -C "$work/$name" archive --format=tar --prefix="$name/" HEAD > ".ci-sources/$name.tar"
done < ci/fs-sources.lock
cp ci/fs-sources.lock .ci-sources/source-commits.txt
(cd .ci-sources && sha256sum freeswitch.tar sofia-sip.tar spandsp.tar > SHA256SUMS)
