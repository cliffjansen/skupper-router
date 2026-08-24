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

# Find liburing include dirs and libraries.
#
# Sets the following variables:
#
#   liburing_FOUND            - True if headers and requested libraries were found
#   liburing_VERSION          - The library version number

include(FindPackageHandleStandardArgs)
find_package(PkgConfig REQUIRED)

pkg_check_modules(liburing REQUIRED IMPORTED_TARGET liburing)

find_package_handle_standard_args(liburing
    REQUIRED_VARS liburing_FOUND
    VERSION_VAR liburing_VERSION
    HANDLE_COMPONENTS)

set_package_properties(liburing PROPERTIES
    TYPE REQUIRED
    PURPOSE "Fast IO"
    DESCRIPTION "Provides native async IO for the Linux kernel, in a fast and efficient manner"
    URL "https://git.kernel.dk/cgit/liburing/")
