# Copyright (c) 2015 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Tests for swiftonfile.swift.obj.server subclass """

from tempfile import mkdtemp
from shutil import rmtree
import mock
import os

from swift.common.swob import Request

from swiftonfile.swift.obj import server as object_server
from contextlib import contextmanager
from swift.common.exceptions import ConnectionTimeout
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.exceptions import DiskFileNotExist

import unittest
from test.unit import debug_logger


@contextmanager
def get_controller():
    devices = mkdtemp()
    conf = {"devices": devices, "mount_check": "false"}
    try:
        controller = object_server.ObjectController(conf, logger=debug_logger())
        controller.bytes_per_sync = 1
        yield controller
    finally:
        rmtree(devices)


class TestSwiftOnFileDiskFileRouter(unittest.TestCase):
    """Test swiftonfile.swift.obj.server.SwiftOnFileDiskFileRouter"""

    def test_getitem(self):
        with mock.patch("swiftonfile.swift.obj.server.DiskFileManager"):
            router = object_server.SwiftOnFileDiskFileRouter()
            assert router["policy"] == router.manager_cls


class TestObjectController(unittest.TestCase):
    """Test swiftonfile.swift.obj.server.ObjectController"""

    def test_app_factory(self):
        with mock.patch.object(object_server, "ObjectController") as mocked:
            object_server.app_factory(mock.MagicMock())
            mocked.assert_called()

    def test_container_list(self):
        with get_controller() as controller:
            rv = 10
            v = mock.MagicMock(**{"getresponse": lambda: rv})

            with mock.patch.object(object_server, "http_connect", return_value=v):
                result = controller.watcher_container_list(
                    "account/container", "localhost:9999", "0", "device"
                )
                assert result == rv

                try:
                    result = controller.watcher_container_list(
                        "account/container", "localhost:9999", "", "device"
                    )
                except Exception:
                    pass
                else:
                    self.fail("Exception waited")

                try:
                    result = controller.watcher_container_list(
                        "account/container", "", "0", "device"
                    )
                except Exception:
                    pass
                else:
                    self.fail("Exception waited")

                try:
                    result = controller.watcher_container_list(
                        "account/container", "localhost:9999", "0", ""
                    )
                except Exception:
                    pass
                else:
                    self.fail("Exception waited")

            # Check connection timeout
            def mock_http_connect(
                ip,
                port,
                contdevice,
                partition,
                op,
                container_path,
                headers=None,
                query_string=None,
            ):
                raise ConnectionTimeout()

            controller.conn_timeout = 1
            with mock.patch.object(object_server, "http_connect", mock_http_connect):
                try:
                    controller.watcher_container_list(
                        "account/container/subfolder/sub2",
                        "localhost:9999",
                        "0",
                        "device",
                    )
                    mock_http_connect.assert_called_with(
                        "localhost",
                        "9999",
                        "device",
                        "0",
                        "GET",
                        "account/container",
                        headers={"user-agent": "object-server %s" % os.getpid()},
                        query_string="prefix=subfolder/sub2",
                    )
                except ConnectionTimeout:
                    pass
                else:
                    self.fail("ConnectionTimeout waited")

    def test_container_update(self):
        with get_controller() as controller:
            rv = 10
            v = mock.MagicMock(**{"getresponse": lambda: rv})

            with mock.patch.object(object_server, "http_connect", return_value=v):
                result = controller.watcher_container_update(
                    "PUT",
                    "account/container",
                    "obj",
                    "localhost:9999",
                    "0",
                    "device",
                    {},
                )
                assert result == rv

                try:
                    result = controller.watcher_container_update(
                        "PUT",
                        "account/container",
                        "obj",
                        "localhost:9999",
                        "",
                        "device",
                        {},
                    )
                except Exception:
                    pass
                else:
                    self.fail("Exception waited")

                try:
                    result = controller.watcher_container_update(
                        "PUT", "account/container", "obj", "", "0", "device", {}
                    )
                except Exception:
                    pass
                else:
                    self.fail("Exception waited")

                try:
                    result = controller.watcher_container_update(
                        "PUT", "account/container", "obj", "localhost:9999", "0", "", {}
                    )
                except Exception:
                    pass
                else:
                    self.fail("Exception waited")

            # Check connection timeout
            def mock_http_connect(
                ip, port, contdevice, partition, op, container_path, headers_out
            ):
                raise ConnectionTimeout()

            controller.conn_timeout = 1
            with mock.patch.object(object_server, "http_connect", mock_http_connect):
                try:
                    controller.watcher_container_update(
                        "PUT",
                        "account/container",
                        "obj",
                        "localhost:9999",
                        "0",
                        "device",
                        {},
                    )
                except ConnectionTimeout:
                    pass
                else:
                    self.fail("ConnectionTimeout waited")

    def test_REPLICATE(self):
        with get_controller() as controller:
            req = Request.blank(
                "/sda1/p/suff", environ={"REQUEST_METHOD": "REPLICATE"}, headers={}
            )
            resp = req.get_response(controller)
            self.assertEqual(resp.status_int, 501)

    def test_REPLICATION(self):
        with get_controller() as controller:
            req = Request.blank(
                "/sda1/p/suff", environ={"REQUEST_METHOD": "REPLICATION"}, headers={}
            )
            resp = req.get_response(controller)
            self.assertEqual(resp.status_int, 501)

    def test_PUT(self):
        with get_controller() as controller:
            with mock.patch.object(
                object_server, "check_object_creation", return_value=None
            ):
                with mock.patch.object(
                    object_server.server.ObjectController, "PUT"
                ) as mock_PUT:
                    req = Request.blank(
                        "/device/partition/account/container/obj",
                        environ={"REQUEST_METHOD": "PUT"},
                        headers={},
                    )
                    req.get_response(controller)
                    mock_PUT.assert_called()

    def test_PUT_error(self):
        with get_controller() as controller:
            with mock.patch.object(
                object_server, "check_object_creation", return_value=None
            ):
                with mock.patch.object(object_server, "split_and_validate_path"):

                    def put_mock(a, b):
                        raise object_server.AlreadyExistsAsFile()

                    with mock.patch.object(
                        object_server.server.ObjectController, "PUT", put_mock
                    ):
                        req = Request.blank(
                            "/device/partition/account/container/obj",
                            environ={"REQUEST_METHOD": "PUT"},
                            headers={},
                        )
                        resp = req.get_response(controller)
                        self.assertEqual(resp.status_int, 409)

    def test_PUT_bad_url(self):
        with get_controller() as controller:
            req = Request.blank(
                "/device/partition/account/container",
                environ={"REQUEST_METHOD": "PUT"},
                headers={},
            )
            resp = req.get_response(controller)
            self.assertEqual(resp.status_int, 400)

    def test_PUT_bad_constraint(self):
        with get_controller() as controller:
            rv = mock.MagicMock(**{"status_int": 400})
            with mock.patch.object(
                object_server, "check_object_creation", return_value=rv
            ) as _:
                req = Request.blank(
                    "/device/partition/account/container/obj",
                    environ={"REQUEST_METHOD": "PUT"},
                    headers={"X-Timestamp": "0"},
                )
                result = controller.PUT(req)
                self.assertEqual(result, rv)

    def test_PATCH(self):
        with get_controller() as controller:
            mock_ring = mock.MagicMock()

            def get_nodes(account, container):
                return (0, [{"device": "device", "ip": "127.0.0.1", "port": 7777}])

            mock_ring.get_nodes = get_nodes

            def ring_init(swift_dir, ring_name):
                return mock_ring

            with mock.patch.object(object_server, "Ring", ring_init):
                mock_diskfile = mock.MagicMock()
                mock_diskfile.read_metadata = lambda: {
                    "Content-Length": "10",
                    "Content-Type": "text",
                    "X-Timestamp": "0",
                    "ETag": "tag",
                }
                mock_diskfile.has_metadata = lambda: False
                with mock.patch.object(
                    controller, "get_diskfile", return_value=mock_diskfile
                ):
                    # Bad method
                    req = Request.blank(
                        "/device/account/container/obj",
                        environ={"REQUEST_METHOD": "PATCH"},
                        headers={"X-Timestamp": "0", "x-patch-method": "FAKE"},
                    )
                    result = controller.PATCH(req)
                    assert result.status_int == 400

                    rv = mock.MagicMock(**{"status": 201})
                    with mock.patch.object(
                        controller, "watcher_container_update", return_value=rv
                    ) as mock_update:
                        # PUT
                        req = Request.blank(
                            "/device/account/container/obj",
                            environ={"REQUEST_METHOD": "PATCH"},
                            headers={"X-Timestamp": "0", "x-patch-method": "PUT"},
                        )
                        result = controller.PATCH(req)
                        assert result.status_int == 200
                        update_headers = HeaderKeyDict(
                            {
                                "x-size": "10",
                                "x-content-type": "text",
                                "x-timestamp": "0",
                                "x-etag": "tag",
                                "x-trans-id": "-",
                                "referer": "PATCH http://localhost/device/account/container/obj",  # noqa
                                "X-Backend-Storage-Policy-Index": 0,
                            }
                        )
                        mock_update.assert_called_once_with(
                            "PUT",
                            "account/container",
                            "obj",
                            "127.0.0.1:7777",
                            "0",
                            "device",
                            update_headers,
                        )

                    # With error 400
                    rv = mock.MagicMock(**{"status": 400})
                    with mock.patch.object(
                        controller, "watcher_container_update", return_value=rv
                    ) as mock_update:
                        # PUT
                        req = Request.blank(
                            "/device/account/container/obj",
                            environ={"REQUEST_METHOD": "PATCH"},
                            headers={"X-Timestamp": "0", "x-patch-method": "PUT"},
                        )
                        result = controller.PATCH(req)
                        assert result.status_int == 404

                    # With error 500
                    rv = mock.MagicMock(**{"status": 500})
                    with mock.patch.object(
                        controller, "watcher_container_update", return_value=rv
                    ) as mock_update:
                        # PUT
                        req = Request.blank(
                            "/device/account/container/obj",
                            environ={"REQUEST_METHOD": "PATCH"},
                            headers={"X-Timestamp": "0", "x-patch-method": "PUT"},
                        )
                        result = controller.PATCH(req)
                        assert result.status_int == 500

                mock_diskfile = mock.MagicMock()

                def read_metadata():
                    raise DiskFileNotExist()

                mock_diskfile.read_metadata = read_metadata
                with mock.patch.object(
                    controller, "get_diskfile", return_value=mock_diskfile
                ):
                    rv = mock.MagicMock(**{"status": 201})
                    with mock.patch.object(
                        controller, "watcher_container_update", return_value=rv
                    ) as mock_update:
                        # PUT
                        req = Request.blank(
                            "/device/account/container/obj",
                            environ={"REQUEST_METHOD": "PATCH"},
                            headers={"X-Timestamp": "0", "x-patch-method": "PUT"},
                        )
                        result = controller.PATCH(req)
                        assert result.status_int == 400

                        rv_t = mock.MagicMock()
                        rv_t.internal = 0
                        with mock.patch.object(
                            object_server.Timestamp, "now", return_value=rv_t
                        ):
                            # DELETE
                            req = Request.blank(
                                "/device/account/container/obj",
                                environ={"REQUEST_METHOD": "PATCH"},
                                headers={
                                    "X-Timestamp": "0",
                                    "x-patch-method": "DELETE",
                                },
                            )
                            result = controller.PATCH(req)
                            assert result.status_int == 200
                            update_headers = HeaderKeyDict(
                                {
                                    "x-timestamp": "0",
                                    "x-trans-id": "-",
                                    "referer": "PATCH http://localhost/device/account/container/obj",  # noqa
                                    "X-Backend-Storage-Policy-Index": 0,
                                }
                            )
                            mock_update.assert_called_once_with(
                                "DELETE",
                                "account/container",
                                "obj",
                                "127.0.0.1:7777",
                                "0",
                                "device",
                                update_headers,
                            )

    def test_LIST(self):
        with get_controller() as controller:
            mock_ring = mock.MagicMock()

            def get_nodes(account, container):
                return (0, [{"device": "device", "ip": "127.0.0.1", "port": 7777}])

            mock_ring.get_nodes = get_nodes

            def ring_init(swift_dir, ring_name):
                return mock_ring

            with mock.patch.object(object_server, "Ring", ring_init):
                with mock.patch.object(
                    controller,
                    "watcher_container_list",
                    return_value=mock.MagicMock(**{"status": 200}),
                ):
                    req = Request.blank(
                        "/device/account/container",
                        environ={"REQUEST_METHOD": "LIST"},
                        headers={"X-Timestamp": "0"},
                    )
                    result = controller.LIST(req)
                    assert result.status_int == 200

                with mock.patch.object(
                    controller,
                    "watcher_container_list",
                    return_value=mock.MagicMock(**{"status": 404}),
                ):
                    req = Request.blank(
                        "/device/account/container",
                        environ={"REQUEST_METHOD": "LIST"},
                        headers={"X-Timestamp": "0"},
                    )
                    result = controller.LIST(req)
                    assert result.status_int == 404

                with mock.patch.object(
                    controller,
                    "watcher_container_list",
                    return_value=mock.MagicMock(**{"status": 500}),
                ):
                    req = Request.blank(
                        "/device/account/container",
                        environ={"REQUEST_METHOD": "LIST"},
                        headers={"X-Timestamp": "0"},
                    )
                    result = controller.LIST(req)
                    assert result.status_int == 500
