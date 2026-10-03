#!/bin/bash
# Build a real public FreeSWITCH SDK; no auth packages, stubs or source-code changes.
set -euo pipefail
export PKG_CONFIG_PATH=/opt/freeswitch/lib/pkgconfig
export LD_LIBRARY_PATH=/opt/freeswitch/lib
mkdir -p /opt/source /opt/build-manifest
cd /tmp/fs-sources
sha256sum -c SHA256SUMS
cmp source-commits.txt /tmp/ci/fs-sources.lock
for name in sofia-sip spandsp freeswitch; do tar -xf "$name.tar" -C /opt/source; done
cp source-commits.txt SHA256SUMS /opt/build-manifest/
(cd /opt/source/sofia-sip && ./bootstrap.sh && ./configure --prefix=/opt/freeswitch --without-glib --disable-static && make -j2 && make install)
(cd /opt/source/spandsp && ./bootstrap.sh && ./configure --prefix=/opt/freeswitch --disable-static && make -j2 && make install)
cd /opt/source/freeswitch
# Select public core/SDK + minimal tools, not proprietary or optional media modules.
printf '%s\n' applications/mod_commands event_handlers/mod_event_socket loggers/mod_console > modules.conf
./bootstrap.sh -j
./configure --prefix=/opt/freeswitch --disable-libyuv --disable-libvpx --disable-core-odbc-support --disable-core-libedit-support --without-python --without-python3
make -j2 core modules freeswitch fs_cli fs_ivrd tone2wav fs_encode fs_tts
make core-install mod_install install-binPROGRAMS install-library_includeHEADERS install-pkgconfigDATA
pkg-config --modversion freeswitch sofia-sip-ua spandsp >> /opt/build-manifest/source-commits.txt
