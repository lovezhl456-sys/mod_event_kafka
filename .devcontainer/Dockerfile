# Public dependencies only. No ARG/ENV for credentials, private repositories or auth.conf.
FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251 AS core-deps
COPY .devcontainer/core-packages.lock /tmp/core-packages.lock
RUN apt-get update \
 && xargs -r apt-get install -y --no-install-recommends < /tmp/core-packages.lock \
 && mkdir -p /opt/build-manifest \
 && dpkg-query -W > /opt/build-manifest/core-packages.tsv \
 && rm -rf /var/lib/apt/lists/* /tmp/core-packages.lock
WORKDIR /src

FROM core-deps AS core-ci
COPY CMakeLists.txt ./
COPY include ./include
COPY src ./src
COPY tests ./tests
COPY scripts ./scripts
COPY lab/*.py ./lab/
COPY ci ./ci
RUN cmake -S . -B /opt/core-build -G Ninja -DCMAKE_BUILD_TYPE=Debug -DENABLE_SANITIZERS=ON \
 && cmake --build /opt/core-build --parallel 2
# Tests run on EVERY container invocation, even when all build layers are cached.
CMD ["ctest", "--test-dir", "/opt/core-build", "--output-on-failure", "--no-tests=error"]

FROM core-deps AS fs-sdk
COPY .devcontainer/fs-packages.lock /tmp/fs-packages.lock
RUN apt-get update \
 && xargs -r apt-get install -y --no-install-recommends < /tmp/fs-packages.lock \
 && dpkg-query -W > /opt/build-manifest/fs-packages.tsv \
 && rm -rf /var/lib/apt/lists/* /tmp/fs-packages.lock
COPY ci/build-public-fs.sh /tmp/build-public-fs.sh
COPY ci/fs-sources.lock /tmp/expected-fs-sources.lock
COPY .ci-sources/*.tar .ci-sources/SHA256SUMS .ci-sources/source-commits.txt /tmp/fs-sources/
RUN bash /tmp/build-public-fs.sh
ENV PKG_CONFIG_PATH=/opt/freeswitch/lib/pkgconfig
ENV LD_LIBRARY_PATH=/opt/freeswitch/lib

FROM fs-sdk AS fs-module
COPY Makefile mod_event_kafka.cpp mod_event_kafka.hpp ./
COPY include ./include
COPY src ./src
RUN make -j2 \
 && test -s mod_event_kafka.so \
 && ldd -r mod_event_kafka.so > /opt/build-manifest/module-linkage.txt \
 && ! grep -E 'not found|undefined symbol' /opt/build-manifest/module-linkage.txt
# Genuine module linked against the pinned public FS SDK. Not runtime/call qualification.
CMD ["bash", "-c", "pkg-config --modversion freeswitch rdkafka && cat /opt/build-manifest/module-linkage.txt"]
