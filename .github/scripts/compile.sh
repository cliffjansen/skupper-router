#!/usr/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# https://sipb.mit.edu/doc/safe-shell
set -Eefuxo pipefail

BUILD_FLAGS="$(rpmbuild --undefine _annotated_build --eval '%set_build_flags')"
eval "${BUILD_FLAGS}"

if [ -z "${REMOTE_SOURCES_DIR:-}" ]; then
  # If no REMOTE_SOURCES_DIR present in env, we use $(pwd) as working dir
  WORKING_DIR="$(pwd)"
  SKUPPER_DIR="$WORKING_DIR"
  PROTON_DIR="$WORKING_DIR/proton/app"
  LWS_DIR="$WORKING_DIR/libwebsockets/app"
  LIBUNWIND_DIR="$WORKING_DIR/libunwind/app"
  # $REMOTE_SOURCES_DIR was not provided, we will have to download the libwebsockets source from ${LWS_SOURCE_URL}
  # Get the libwebsockets source into a libwebsockets.tar.gz file
  # and untar it into the libwebsockets folder
  wget "${LWS_SOURCE_URL}" -O libwebsockets.tar.gz
  wget "${LIBUNWIND_SOURCE_URL}" -O libunwind.tar.gz
  tar -zxf libwebsockets.tar.gz --one-top-level="${LWS_DIR}"       --strip-components 1
  tar -zxf libunwind.tar.gz     --one-top-level="${LIBUNWIND_DIR}" --strip-components 1

  # No $REMOTE_SOURCES_DIR was provided, we will have to download the proton source tar.gz from ${PROTON_SOURCE_URL}
  wget "${PROTON_SOURCE_URL}" -O qpid-proton.tar.gz
  tar -zxf qpid-proton.tar.gz --one-top-level="${PROTON_DIR}" --strip-components 1
else
  # If the env contains REMOTE_SOURCES_DIR, we will use that as the working dir
  # If REMOTE_SOURCES_DIR is provided, this scripts expects the following -
  # 1. proton sources to be in $REMOTE_SOURCES_DIR/app/proton
  # 2. libwebsockets sources to be in $REMOTE_SOURCES_DIR/app/libwebsockets
  # 3. skupper-router sources to be in $REMOTE_SOURCES_DIR/app/skupper-router
  # 4. libunwind souces will be in $REMOTE_SOURCES_DIR/app/libunwind
  WORKING_DIR="${REMOTE_SOURCES_DIR}"
  SKUPPER_DIR="${WORKING_DIR}/skupper-router/app"
  PROTON_DIR="${WORKING_DIR}/proton/app"
  LWS_DIR="${WORKING_DIR}/libwebsockets/app"
  LIBUNWIND_DIR="${WORKING_DIR}/libunwind/app"
  cd "${WORKING_DIR}"
fi

LWS_BUILD_DIR="${LWS_DIR}/build"
LWS_INSTALL_DIR="${LWS_DIR}/install"
LIBUNWIND_INSTALL_DIR="${LIBUNWIND_DIR}/install"

PROTON_INSTALL_DIR="${PROTON_DIR}/proton_install"
PROTON_BUILD_DIR="${PROTON_DIR}/build"
SKUPPER_BUILD_DIR="${SKUPPER_DIR}/build"

# We are installing libwebsockets and libunwind from source
# First, we will install these libraries in /usr/local/lib
# and the include files in /usr/local/include and when skupper-router is compiled
# in the subsequent step, it can find the libraries and include files in /usr/local/
# Second, we install the library *again* in a custom folder so we can
# tar up the usr folder and untar in the Containerfile so that these libraries
# can be used by skupper-router runtime.

#region libwebsockets
# Build libwebsockets library.
# Source folder (cmake -S) is $LWS_DIR
# Build dir (cmake -B) is $LWS_BUILD_DIR
cmake -S "${LWS_DIR}" -B "${LWS_BUILD_DIR}" \
  -DLWS_LINK_TESTAPPS_DYNAMIC=ON \
  -DLWS_WITH_LIBUV=OFF \
  -DLWS_WITHOUT_BUILTIN_GETIFADDRS=ON \
  -DLWS_USE_BUNDLED_ZLIB=OFF \
  -DLWS_WITHOUT_BUILTIN_SHA1=ON \
  -DLWS_WITH_STATIC=OFF \
  -DLWS_IPV6=ON \
  -DLWS_WITH_HTTP2=OFF \
  -DLWS_WITHOUT_CLIENT=OFF \
  -DLWS_WITHOUT_SERVER=OFF \
  -DLWS_WITHOUT_TESTAPPS=ON \
  -DLWS_WITHOUT_TEST_SERVER=ON \
  -DLWS_WITHOUT_TEST_SERVER_EXTPOLL=ON \
  -DLWS_WITHOUT_TEST_PING=ON \
  -DLWS_WITHOUT_TEST_CLIENT=ON
cmake --build "${LWS_BUILD_DIR}" --parallel "$(nproc)" --verbose
cmake --install "${LWS_BUILD_DIR}"

# Read about DESTDIR here - https://www.gnu.org/prep/standards/html_node/DESTDIR.html
DESTDIR="${LWS_INSTALL_DIR}" cmake --install "${LWS_BUILD_DIR}"
tar -z -C "${LWS_INSTALL_DIR}" -cf /libwebsockets-image.tar.gz usr
#endregion libwebsockets

#region libunwind
pushd "${LIBUNWIND_DIR}"
autoreconf -i
./configure
make install
DESTDIR="${LIBUNWIND_INSTALL_DIR}" make install
tar -z -C "${LIBUNWIND_INSTALL_DIR}" -cf /libunwind-image.tar.gz usr
popd
#endregion libunwind

do_patch () {
    PATCH_DIR=$1
    PATCH_SRC=$2
    if [ -d "${PATCH_DIR}" ]
    then
        for patch in $(find "${PATCH_DIR}" -type f -name "*.patch"); do
            echo Applying patch "${patch}"
            patch -f -d "${PATCH_SRC}" -p1 < "$patch"
        done;
    fi
}

do_patch "patches/proton" "${PROTON_DIR}"

do_build () {
  local suffix=${1}
  local runtime_check=${2}

  if [ "$runtime_check" == "OFF" ]; then
     # Turn on PGO only when we are not doing asan or tsan
     BUILD_OPTS="-DENABLE_PROFILE_GUIDED_OPTIMIZATION=ON"
  else
     BUILD_OPTS="-DBUILD_TESTING=OFF"
  fi

  cmake -S "${PROTON_DIR}" -B "${PROTON_BUILD_DIR}${suffix}" \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DRUNTIME_CHECK="${runtime_check}" \
    -DENABLE_LINKTIME_OPTIMIZATION=ON \
    -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON \
    -DBUILD_TLS=ON -DSSL_IMPL=openssl -DBUILD_STATIC_LIBS=ON -DBUILD_BINDINGS=python \
    -DBUILD_EXAMPLES=OFF -DBUILD_TESTING=OFF \
    -DCMAKE_INSTALL_PREFIX=$PROTON_BUILD_DIR${suffix}/install
  cmake --build "${PROTON_BUILD_DIR}${suffix}" --verbose

  # `cmake --install` Proton for the build image only as the router links it statically
  # Proton Python for the run image is installed later
  cmake --install "$PROTON_BUILD_DIR${suffix}"

   if [ "$runtime_check" == "OFF" ]; then
     # This will install the proton python libraries in sys.path so the tests using
     # proton can be run successfully.
     python3 -m pip install "$(find "$PROTON_BUILD_DIR/python/" -name 'python-qpid-proton-*.tar.gz')"
   fi

  cmake -S "${SKUPPER_DIR}" -B "${SKUPPER_BUILD_DIR}${suffix}" \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DRUNTIME_CHECK="${runtime_check}" \
    -DProton_USE_STATIC_LIBS=ON \
    -DProton_DIR="$PROTON_BUILD_DIR${suffix}/install/lib64/cmake/Proton" \
    ${BUILD_OPTS} \
    -DVERSION="${VERSION}" \
    -DCMAKE_INSTALL_PREFIX=/usr
  cmake --build "${SKUPPER_BUILD_DIR}${suffix}" --verbose
}

# This is required to install the python packages that the system tests use.
python3 -m pip install -r "${SKUPPER_DIR}"/requirements-dev.txt

# Do a regular build without asan or tsan.
do_build "" OFF

# Install Proton Python
python3 -m pip install --ignore-installed --prefix="$PROTON_INSTALL_DIR/usr" "$(find "$PROTON_BUILD_DIR/python/" -name 'python-qpid-proton-*.tar.gz')"

# Then perform sanitized builds of Proton and the Router.
# talking to annobin is not straightforward, https://bugzilla.redhat.com/show_bug.cgi?id=1536569
common_sanitizer_flags="-Wp,-U_FORTIFY_SOURCE -fplugin=annobin -fplugin-arg-annobin-no-active-checks"
export CFLAGS="${CFLAGS} ${common_sanitizer_flags}"
export CXXFLAGS="${CXXFLAGS} ${common_sanitizer_flags}"
do_build "_asan" asan
do_build "_tsan" tsan

tar -z -C "${PROTON_INSTALL_DIR}" -cf /qpid-proton-image.tar.gz usr

DESTDIR="${SKUPPER_DIR}/staging/" cmake --install "${SKUPPER_BUILD_DIR}"
# Remove router tests (enabled for PGO) since *.pem files trigger security warnings
rm -rf ${SKUPPER_DIR}/staging/usr/lib/skupper-router/tests
# Add sanitized router binaries
cp "${SKUPPER_BUILD_DIR}_asan/router/skrouterd" "${SKUPPER_DIR}/staging/usr/sbin/skrouterd_asan"
cp "${SKUPPER_BUILD_DIR}_tsan/router/skrouterd" "${SKUPPER_DIR}/staging/usr/sbin/skrouterd_tsan"
cp --target-directory="${SKUPPER_DIR}/staging/" "${SKUPPER_DIR}/tests/tsan.supp" "${SKUPPER_BUILD_DIR}_asan/tests/lsan.supp"

tar -z -C "${SKUPPER_DIR}/staging/" -cf /skupper-router-image.tar.gz usr etc lsan.supp tsan.supp
#endregion qpid-proton and skupper-router
