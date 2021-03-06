import aiohttp
import aiounittest
from aioresponses import aioresponses
import tempfile
import shutil
import random
import uuid
import os
from os import path
import mock
from contextlib import contextmanager

from swiftonfile.watcher.generic import (
    walk,
    GenericWatcher,
    WatcherError,
    WATCHER_INTERVAL_DEFAULT,
)
from test.utils import check_equals_list_dict, touch_file


@contextmanager
def get_watcher():
    bind_port = random.randint(100, 10000)
    devices = tempfile.mkdtemp()
    watcher_interval = random.randint(1, 60)
    fs_name = "test" + str(random.randint(1, 9999))
    os.mkdir(path.join(devices, fs_name))

    conf = {
        "bind_port": bind_port,
        "devices": devices,
        "watcher_interval": watcher_interval,
        "watcher_nb_threads": 1,
    }

    try:
        yield GenericWatcher(conf)
    finally:
        shutil.rmtree(devices)


class TestGenericWatcher(aiounittest.AsyncTestCase):
    """Tests for swiftonfile.watcher.generic"""

    async def test_walk(self):
        path = "path"
        with mock.patch("swiftonfile.watcher.generic.os.walk") as mock_walk:
            await walk(path)
            mock_walk.assert_called_with(path)

    def test_constructor_no_wacher_interval(self):
        devices = tempfile.mkdtemp()
        fs_name = "lustre_test"
        os.mkdir(path.join(devices, fs_name))

        conf = {"bind_port": 1000, "devices": devices}
        watcher = GenericWatcher(conf)
        shutil.rmtree(devices)
        assert watcher.watcher_interval == WATCHER_INTERVAL_DEFAULT

    def test_constructor_no_fsname(self):
        devices = tempfile.mkdtemp()
        conf = {"bind_port": 1000, "devices": devices}
        try:
            GenericWatcher(conf)
        except WatcherError:
            pass
        else:
            self.fail("Expecting WatcherError")

    async def test_send_patch_bad_method(self):
        with get_watcher() as watcher:
            try:
                await watcher.send_patch("FAKE", "", "")
            except WatcherError:
                pass
            else:
                self.fail("Expecting WatcherError")

    async def test_send_patch(self):
        with get_watcher() as watcher:
            cpath = "account1/container1"
            filepath = "file1.txt"
            url = "http://localhost:{port}/{fs_name}/{fullpath}".format(
                port=watcher.swift_port,
                fs_name=watcher.fs_name,
                fullpath=path.join(cpath, filepath),
            )
            methods = ["PUT", "DELETE"]
            for method in methods:
                with aioresponses() as m:
                    m.add(
                        url,
                        "patch",
                        headers={
                            "X-Patch-Method": method,
                            "X-Patch-Force": "True",
                        },
                    )
                    await watcher.send_patch(method, cpath, filepath)

    async def test_list_container_files_error(self):
        with get_watcher() as watcher:
            rv = mock.MagicMock(**{"status": 500})
            with mock.patch.object(aiohttp, "request", return_value=rv):
                try:
                    await watcher.list_container_files("xxx")
                except WatcherError:
                    pass
                else:
                    self.fail("Expecting WatcherError")

    async def test_list_container_files(self):
        with get_watcher() as watcher:
            waited_files = ["file1.txt", "file2.txt", "file3.txt"]

            with aioresponses() as m:
                url = f"http://localhost:{watcher.swift_port}/{watcher.fs_name}/xxx"
                m.add(url, "list", body="\n".join(waited_files))
                result_files = await watcher.list_container_files("xxx")

            assert waited_files == result_files

    async def test_get_all_local_containers_path(self):
        with get_watcher() as watcher:
            fs_path = path.join(watcher.node_folder, watcher.fs_name)

            # Create container folders and generate fake files
            cpaths = []
            apaths = []
            for x in range(10):
                account_folder = "account" + str(x)
                apaths.append(path.join(fs_path, account_folder))
                for y in range(10):
                    container_folder = "container" + str(y)
                    container_path = path.join(account_folder, container_folder)
                    cpath = path.join(fs_path, container_path)
                    touch_file(path.join(cpath, "file1.txt"))
                    touch_file(path.join(cpath, "file2.txt"))
                    cpaths.append(container_path)

            cpaths_result = await watcher.get_all_local_containers_path()
            assert cpaths_result.sort() == cpaths.sort()

            # Create a blacklisted folder
            bad_cpaths = []
            for x in range(10):
                bad_account_folder = "bad_account" + str(x)
                apaths.append(path.join(fs_path, bad_account_folder))
                for y in range(10):
                    bad_container_folder = "bad_container" + str(y)
                    bad_container_path = path.join(
                        bad_account_folder, bad_container_folder
                    )
                    cpath = path.join(fs_path, bad_container_path)
                    touch_file(path.join(cpath, "file1.txt"))
                    touch_file(path.join(cpath, "file2.txt"))
                    bad_cpaths.append(bad_container_path)

            watcher.container_path_blacklist += bad_cpaths
            cpaths_result = await watcher.get_all_local_containers_path()
            assert cpaths_result.sort() == cpaths.sort()

            for apath in apaths:
                shutil.rmtree(apath)

    def test_check_request_response(self):
        with get_watcher() as watcher:
            # Check success
            cpath = "account/container"
            watcher.check_request_response(200, cpath)
            assert cpath not in watcher.container_path_blacklist

            # Check error 500
            cpath = "account3/container3"
            try:
                watcher.check_request_response(500, cpath)
            except WatcherError:
                pass
            else:
                self.fail("WatcherError should have been called")

            assert cpath not in watcher.container_path_blacklist

            # Check error 400
            cpath = "account2/container2"
            try:
                watcher.check_request_response(404, cpath)
            except WatcherError:
                pass
            else:
                self.fail("WatcherError should have been called")

            assert cpath in watcher.container_path_blacklist

    def test_check_container_path(self):
        with get_watcher() as watcher:
            cpath = "account/container"

            # no exception should be raised
            watcher.check_container_path(cpath)

            # exception should be raised
            watcher.container_path_blacklist.append(cpath)
            try:
                watcher.check_container_path(cpath)
            except WatcherError:
                pass
            else:
                self.fail("WatcherError should be raised")

    async def test_get_local_files_in_container(self):
        with get_watcher() as watcher:
            fs_path = path.join(watcher.node_folder, watcher.fs_name)

            # Add a container
            container_path = "account/container"
            cpath = path.join(fs_path, container_path)

            expected_files = ["file1.txt", "subdir/file2.txt"]
            for f in expected_files:
                touch_file(path.join(cpath, f))

            result_files = await watcher.get_local_files_in_container(container_path)

            assert expected_files == result_files

    async def test_get_data_from_fs(self):
        with get_watcher() as watcher:
            fs_path = path.join(watcher.node_folder, watcher.fs_name)

            # Add a container
            container_path = "account/container"
            cpath = path.join(fs_path, container_path)

            files_to_add = ["file" + str(x) for x in range(10)]
            for f in files_to_add:
                touch_file(path.join(cpath, f))

            expected = [
                {"method": "PUT", "container_path": container_path, "filepath": x}
                for x in files_to_add
            ]

            url = "http://localhost:{port}/{fs_name}/{container_path}".format(
                port=watcher.swift_port,
                fs_name=watcher.fs_name,
                container_path=container_path,
            )

            # Check data returned are good
            with aioresponses() as m:
                m.add(url, "list")
                await watcher.get_data(0)
                results = []
                for _ in range(watcher.queue.qsize()):
                    results.append(await watcher.queue.get())
                check_equals_list_dict(expected, results)

            # No data returned when request error
            with aioresponses() as m:
                m.add(url, "list", status=500)
                await watcher.get_data(0)
                assert watcher.queue.qsize() == 0

            # Check with one file in container
            with aioresponses() as m:
                m.add(url, "list", body=files_to_add[0])
                await watcher.get_data(0)
                results = []
                for _ in range(watcher.queue.qsize()):
                    results.append(await watcher.queue.get())
                check_equals_list_dict(expected[1:], results)

    async def test_get_data_from_container(self):
        with get_watcher() as watcher:
            fs_path = path.join(watcher.node_folder, watcher.fs_name)

            # Add a container
            container_path = "account/container"
            cpath = path.join(fs_path, container_path)
            os.makedirs(cpath)

            files_to_delete = ["file" + str(x) for x in range(10)]
            expected = [
                {"method": "DELETE", "container_path": container_path, "filepath": x}
                for x in files_to_delete
            ]

            url = "http://localhost:{port}/{fs_name}/{container_path}".format(
                port=watcher.swift_port,
                fs_name=watcher.fs_name,
                container_path=container_path,
            )

            # Check data returned are good
            with aioresponses() as m:
                m.add(url, "list", body="\n".join(files_to_delete))
                await watcher.get_data(0)
                results = []
                for _ in range(watcher.queue.qsize()):
                    results.append(await watcher.queue.get())
                check_equals_list_dict(expected, results)

            # Check with one file in fs
            touch_file(path.join(cpath, files_to_delete[0]))
            with aioresponses() as m:
                m.add(url, "list", body="\n".join(files_to_delete))
                await watcher.get_data(0)
                results = []
                for _ in range(watcher.queue.qsize()):
                    results.append(await watcher.queue.get())
                check_equals_list_dict(expected[1:], results)

    async def test_send_data(self):
        with get_watcher() as watcher:
            data = {
                "method": "PUT",
                "container_path": "account/container",
                "filepath": "file1",
            }

            with mock.patch.object(watcher, "send_patch") as mocked_send_patch:
                await watcher.send_data(data)
                mocked_send_patch.assert_called_once_with(
                    data["method"], data["container_path"], data["filepath"], False
                )

            # Pass through the WatcherError path
            data["method"] = "FAKE"
            await watcher.send_data(data)

    async def test_send_patch_raise(self):
        with get_watcher() as watcher:
            with mock.patch.object(
                aiohttp, "request", return_value=mock.MagicMock(**{"status": 404})
            ):
                try:
                    await watcher.send_patch("PUT", "account/container", "filename")
                except WatcherError:
                    pass
                else:
                    self.fail("WatcherError waited")

    async def test_send_no_raise(self):
        with get_watcher() as watcher:
            filename = ".object." + uuid.uuid4().hex
            assert (
                await watcher.send_patch("PUT", "account/container", filename) is None
            )

            container_path = "account/container"
            filepath = "filename"
            url = "http://localhost:{port}/{fs_name}/{fullpath}".format(
                port=watcher.swift_port,
                fs_name=watcher.fs_name,
                fullpath=path.join(container_path, filepath),
            )
            # error 400 doesn't raise
            with aioresponses() as m:
                m.add(url, "patch", status=400)
                try:
                    await watcher.send_patch("PUT", container_path, filepath)
                except WatcherError:
                    self.fail("send_patch shouldn't fail")

    async def test_producer(self):
        with get_watcher() as watcher:
            watcher.stop_watch = True
            with mock.patch.object(
                watcher, "get_data", return_value=mock.AsyncMock()
            ) as mocked_get_data:
                await watcher.producer()
                mocked_get_data.assert_called_once_with(0)

    async def test_consumer(self):
        with get_watcher() as watcher:
            watcher.stop_watch = True
            data = mock.AsyncMock()
            with mock.patch.object(
                watcher.queue, "get", return_value=data
            ) as mocked_get:
                with mock.patch.object(watcher, "send_data") as mocked_send_data:
                    await watcher.consumer()
                    mocked_get.assert_called()
                    mocked_send_data.assert_called_once_with(data)

    async def test_watch(self):
        with get_watcher() as watcher:
            watcher.stop_watch = True
            with mock.patch.object(
                watcher, "consumer", mock.AsyncMock()
            ) as mocked_consumer:
                with mock.patch.object(
                    watcher, "producer", mock.AsyncMock()
                ) as mocked_producer:
                    await watcher.watch()
                    mocked_consumer.assert_awaited()
                    mocked_producer.assert_awaited()
