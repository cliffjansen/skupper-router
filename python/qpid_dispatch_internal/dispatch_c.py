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
# under the License
#

"""Access to functions in libqpid-dispatch.so"""

import ctypes, os
from ctypes import c_char_p, c_long, py_object
try:
    from dispatch import QPID_DISPATCH_LIB
except:
    QPID_DISPATCH_LIB = None

class CError(Exception):
    """Exception raised if there is an error in a C call"""
    pass

class QdDll(ctypes.PyDLL):
    """
    Load the library, set up function prototypes.

    NOTE: We use the python calling convention because the C library
    internally makes python calls.
    """
    _instance = None
    @classmethod
    def instance(cls):
        if not cls._instance:
            cls._instance = QdDll()
        return cls._instance

    def __init__(self):
        lib = QPID_DISPATCH_LIB or os.environ.get('QPID_DISPATCH_LIB')
        assert lib
        super(QdDll, self).__init__(lib)

        # Types
        # TODO aconway 2014-06-27: can we use typed pointers instead of void*?
        self.qd_dispatch_p = ctypes.c_void_p

        # No check on qd_error_* functions, it would be recursive
        self._prototype(self.qd_error_code, c_long, [], check=False)
        self._prototype(self.qd_error_message, c_char_p, [], check=False)

        self._prototype(self.qd_log_entity, c_long, [py_object])
        self._prototype(self.qd_dispatch_configure_container, None, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_dispatch_configure_router, None, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_dispatch_prepare, None, [self.qd_dispatch_p])
        self._prototype(self.qd_dispatch_configure_listener, None, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_dispatch_configure_connector, None, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_dispatch_configure_address, None, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_dispatch_configure_waypoint, None, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_dispatch_set_agent, None, [self.qd_dispatch_p, py_object])
        self._prototype(self.qd_connection_manager_start, None, [self.qd_dispatch_p])
        self._prototype(self.qd_waypoint_activate_all, None, [self.qd_dispatch_p])

    def _errcheck(self, result, func, args):
        if self.qd_error_code():
            raise CError(self.qd_error_message())

    def _prototype(self, f, restype, argtypes, check=True):
        """Set up the return and argument types and the error checker for a ctypes function"""
        f.restype = restype
        f.argtypes = argtypes
        if check: f.errcheck = self._errcheck

def instance():
    return QdDll.instance()
