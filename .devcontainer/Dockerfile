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
# Read-only context; copy only sources needed at test runtime in one layer.
# No prepared SDK archives, .git or local credentials are persisted here.
RUN --mount=type=bind,target=/context \
    cp -a /context/CMakeLists.txt /context/include /context/src /context/tests /context/scripts /context/lab /src/ \
 && cmake -S . -B /opt/core-build -G Ninja -DCMAKE_BUILD_TYPE=Debug -DENABLE_SANITIZERS=ON \
 && cmake --build /opt/core-build --parallel 2
# Tests run on EVERY container invocation, even when all build layers are cached.
CMD ["ctest", "--test-dir", "/opt/core-build", "--output-on-failure", "--no-tests=error"]

FROM core-deps AS fs-sdk
COPY .devcontainer/fs-packages.lock /tmp/fs-packages.lock
RUN apt-get update \
 && xargs -r apt-get install -y --no-install-recommends < /tmp/fs-packages.lock \
 && dpkg-query -W > /opt/build-manifest/fs-packages.tsv \
 && rm -rf /var/lib/apt/lists/* /tmp/fs-packages.lock
# Public archives are read-only build inputs, not persistent image layers.
RUN --mount=type=bind,source=.ci-sources,target=/tmp/fs-sources \
    --mount=type=bind,source=ci,target=/tmp/ci \
    bash /tmp/ci/build-public-fs.sh && rm -rf /opt/source
ENV PKG_CONFIG_PATH=/opt/freeswitch/lib/pkgconfig
ENV LD_LIBRARY_PATH=/opt/freeswitch/lib

FROM fs-sdk AS fs-module
RUN --mount=type=bind,target=/context \
    cp -a /context/Makefile /context/mod_event_kafka.cpp /context/mod_event_kafka.hpp /context/include /context/src /src/ \
 && make -j2 \
 && test -s mod_event_kafka.so \
 && ldd -r mod_event_kafka.so > /opt/build-manifest/module-linkage.txt \
 && ! grep -E 'not found|undefined symbol' /opt/build-manifest/module-linkage.txt
# Genuine module linked against the pinned public FS SDK. Not runtime/call qualification.
CMD ["bash", "-c", "pkg-config --modversion freeswitch rdkafka && cat /opt/build-manifest/module-linkage.txt"]
