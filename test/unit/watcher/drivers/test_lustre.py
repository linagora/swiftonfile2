import asyncio
import tempfile
import shutil
import random
import aiounittest
import os
from os import path
import mock
from contextlib import contextmanager
from test.utils import touch_file

from swiftonfile.watcher.drivers.lustre import (
    extract_fid,
    LustreWatcher,
    LustreError,
    FidNoSuchFileError,
    subprocess,
    NoContainerPathError,
)


def init_conf():
    bind_port = random.randint(100, 10000)
    devices = tempfile.mkdtemp()
    watcher_interval = random.randint(1, 60)
    fs_name = "test" + str(random.randint(1, 9999))
    os.mkdir(path.join(devices, fs_name))
    conf = {
        "bind_port": bind_port,
        "devices": devices,
        "watcher_interval": watcher_interval,
        "force_sync_at_boot": True,
    }
    return conf


@contextmanager
def get_watcher():
    conf = init_conf()
    mdt = "lustre-MDT0000"
    lustre_client = "cl1"

    try:
        with mock.patch.object(LustreWatcher, "get_mdt", return_value=mdt):
            with mock.patch.object(
                LustreWatcher, "get_lustre_client", return_value=lustre_client
            ):
                yield LustreWatcher(conf)
    finally:
        shutil.rmtree(conf["devices"])


class TestLustreWatcher(aiounittest.AsyncTestCase):
    """Tests for swiftonfile.watcher.drivers.lustre"""

    def test_extract_fid(self):
        fid_result = "result"
        fid = "t=" + fid_result
        assert fid_result == extract_fid(fid)

    def test_get_mdt(self):
        conf = init_conf()
        mdt = "lustre-MDT0000"
        lustre_client = "cl1"
        rmdt = mock.MagicMock(
            **{
                "returncode": 0,
                "stdout": "MDTS:\n0: {}_UUID ACTIVE".format(mdt).encode(),
            }
        )
        with mock.patch.object(subprocess, "run", return_value=rmdt) as mock_subprocess:
            with mock.patch.object(
                LustreWatcher, "get_lustre_client", return_value=lustre_client
            ):
                watcher = LustreWatcher(conf)
                assert watcher.mdt == mdt

                mnt_point = path.join(watcher.node_folder, watcher.fs_name)
                mock_subprocess.assert_called_once_with(
                    ["lfs", "mdts", mnt_point],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )

        shutil.rmtree(conf["devices"])

    def test_get_mdt_error(self):
        conf = init_conf()

        # returncode error
        rmdt = mock.MagicMock(**{"returncode": 10, "stdout": b""})
        with mock.patch.object(subprocess, "run", return_value=rmdt) as _:
            with mock.patch.object(LustreWatcher, "get_lustre_client"):
                try:
                    LustreWatcher(conf)
                except LustreError:
                    pass
                else:
                    self.fail("LustreError exception sould be raised")

        # stdout empty
        rmdt = mock.MagicMock(**{"returncode": 0, "stdout": b""})
        with mock.patch.object(subprocess, "run", return_value=rmdt) as _:
            with mock.patch.object(LustreWatcher, "get_lustre_client"):
                try:
                    LustreWatcher(conf)
                except LustreError:
                    pass
                else:
                    self.fail("LustreError exception sould be raised")

        shutil.rmtree(conf["devices"])

    def test_get_lustre_client(self):
        conf = init_conf()
        expected_lustre_client = "cl4"

        def mock_test_client(self, client):
            if client != expected_lustre_client:
                return 2
            return 0

        with mock.patch.object(LustreWatcher, "test_client", mock_test_client):
            with mock.patch.object(LustreWatcher, "get_mdt"):
                watcher = LustreWatcher(conf)
                assert watcher.lustre_cl == expected_lustre_client

        shutil.rmtree(conf["devices"])

    def test_get_lustre_client_error_several(self):
        conf = init_conf()
        expected_lustre_clients = ["cl4", "cl2"]

        def mock_changelog_clear(endrec=1, client=None):
            if client not in expected_lustre_clients:
                return 2
            return 0

        with mock.patch.object(LustreWatcher, "changelog_clear", mock_changelog_clear):
            with mock.patch.object(LustreWatcher, "get_mdt"):
                try:
                    LustreWatcher(conf)
                except LustreError:
                    pass
                else:
                    self.fail("LustreWatcher should have been raised")

        shutil.rmtree(conf["devices"])

    def test_get_lustre_client_error_none(self):
        conf = init_conf()

        def mock_changelog_clear(endrec=1, client=None):
            return 2

        with mock.patch.object(LustreWatcher, "changelog_clear", mock_changelog_clear):
            with mock.patch.object(LustreWatcher, "get_mdt"):
                try:
                    LustreWatcher(conf)
                except LustreError:
                    pass
                else:
                    self.fail("LustreWatcher should have been raised")

        shutil.rmtree(conf["devices"])

    def test_get_lustre_client_error_permission_denied(self):
        conf = init_conf()

        def mock_changelog_clear(endrec=1, client=None):
            return 13

        with mock.patch.object(LustreWatcher, "changelog_clear", mock_changelog_clear):
            with mock.patch.object(LustreWatcher, "get_mdt"):
                try:
                    LustreWatcher(conf)
                except LustreError:
                    pass
                else:
                    self.fail("LustreWatcher should have been raised")

        shutil.rmtree(conf["devices"])

    async def test_fid_to_path(self):
        path = "account/container"

        async def communicate():
            return path.encode(), b"stderr"

        with get_watcher() as watcher:
            fid = "fid"
            rv = mock.AsyncMock(**{"returncode": 0, "communicate": communicate})
            with mock.patch.object(
                asyncio, "create_subprocess_shell", return_value=rv
            ) as mock_subprocess:
                result = await watcher.fid_to_path(fid)
                mock_subprocess.assert_called_once_with(
                    f"lfs fid2path {watcher.mdt} {fid}",
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                assert result == path

    async def test_fid_to_path_error(self):
        async def communicate():
            return b"stdout", b"stderr"

        with get_watcher() as watcher:
            rv = mock.AsyncMock(**{"returncode": 10, "communicate": communicate})
            with mock.patch.object(asyncio, "create_subprocess_shell", return_value=rv):
                try:
                    await watcher.fid_to_path("fid")
                except LustreError:
                    pass
                else:
                    self.fail("Should have raised LustreError")

    async def test_fid_to_path_error_no_such_file(self):
        async def communicate():
            return b"stdout", b"stderr"

        with get_watcher() as watcher:
            rv = mock.AsyncMock(**{"returncode": 2, "communicate": communicate})
            with mock.patch.object(asyncio, "create_subprocess_shell", return_value=rv):
                try:
                    await watcher.fid_to_path("fid")
                except FidNoSuchFileError:
                    pass
                else:
                    self.fail("Should have raised LustreError")

    def test_parent_path_to_container_path(self):
        with get_watcher() as watcher:
            r, _ = watcher.parent_path_to_container_path("account/container")
            assert r == "account/container"

            r, subfolder = watcher.parent_path_to_container_path(
                "account/container/subfolder1/"
            )
            assert r == "account/container"
            assert subfolder == "subfolder1"

            r, subfolder = watcher.parent_path_to_container_path(
                "/account/container/subfolder1/sub2"
            )
            assert r == "account/container"
            assert subfolder == "subfolder1/sub2"

            try:
                watcher.parent_path_to_container_path("account")
            except LustreError:
                pass
            else:
                self.fail("LustreError waited")

    async def test_changelog_clear(self):
        with get_watcher() as watcher:
            rv = mock.AsyncMock(**{"returncode": 0})
            with mock.patch.object(
                asyncio, "create_subprocess_shell", return_value=rv
            ) as mock_subprocess:
                endrec = 345
                cl = "cl1"

                # With client
                await watcher.changelog_clear(endrec, cl)
                mock_subprocess.assert_called_with(
                    f"lfs changelog_clear {watcher.mdt} {cl} {str(endrec)}",
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )

                # Without client
                await watcher.changelog_clear(endrec)
                mock_subprocess.assert_called_with(
                    "lfs changelog_clear {mdt} {cl} {endrec}".format(
                        mdt=watcher.mdt, cl=watcher.lustre_cl, endrec=str(endrec)
                    ),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )

    async def test_get_changelog_data(self):
        with get_watcher() as watcher:
            expected_filename = "test.txt"
            old_expected_filename = "test2.txt"
            expected_rename_dir = "new_dir"
            old_expected_rename_dir = "old_dir"
            out = [
                "231768 01CREAT 12:58:52.470582516 2021.04.18 0x0 t=[0x200000403:0x1d886:0x0] ef=0xf u=160:160 nid=10.0.0.1@tcp p=[0x200000403:0x27c:0x0] "  # noqa
                + expected_filename,
                "231769 06UNLNK 12:58:52.473262387 2021.04.18 0x1 t=[0x200000403:0x1d885:0x0] ef=0xf u=160:160 nid=10.0.0.1@tcp p=[0x200000403:0x27c:0x0] "  # noqa
                + expected_filename,
                "231770 08RENME 13:31:35.825896252 2021.04.18 0x0 t=[0:0x0:0x0] ef=0xf u=160:160 nid==10.0.0.1@tcp p=[0x200000403:0x1a00a:0x0] "  # noqa
                + expected_filename
                + " s=[0x200000403:0x1a00d:0x0] sp=[0x200000403:0x1a00a:0x0] "
                + old_expected_filename,
                "231771 08RENME 13:31:35.825896252 2021.04.18 0x0 t=[0:0x0:0x0] ef=0xf u=160:160 nid==10.0.0.1@tcp p=[0x200000403:0x1a00a:0x0] "  # noqa
                + expected_rename_dir
                + " s=[0x200000403:0x1a00d:0x0] sp=[0x200000403:0x1a00a:0x0] "
                + old_expected_rename_dir,
            ]

            async def communicate():
                return "\n".join(out).encode(), b"stderr"

            v1 = mock.AsyncMock(**{"returncode": 0, "communicate": communicate})

            # Add a container
            fs_path = path.join(watcher.node_folder, watcher.fs_name)
            container_path = "account/container"
            full_cpath = path.join(fs_path, container_path)
            os.makedirs(full_cpath)
            touch_file(path.join(full_cpath, expected_rename_dir, expected_filename))
            v2 = container_path

            with mock.patch.object(
                asyncio, "create_subprocess_shell", mock.AsyncMock(return_value=v1)
            ):
                with mock.patch.object(
                    watcher, "fid_to_path", mock.AsyncMock(return_value=v2)
                ):
                    await watcher.get_changelog_data()

                    assert watcher.queue.qsize() == 6
                    assert watcher.last_id == 231771

                    self.assertDictEqual(
                        await watcher.queue.get(),
                        {
                            "method": "PUT",
                            "container_path": container_path,
                            "filepath": expected_filename,
                            "force": False,
                        },
                    )
                    self.assertDictEqual(
                        await watcher.queue.get(),
                        {
                            "method": "DELETE",
                            "container_path": container_path,
                            "filepath": expected_filename,
                            "force": False,
                        },
                    )
                    self.assertDictEqual(
                        await watcher.queue.get(),
                        {
                            "method": "DELETE",
                            "container_path": container_path,
                            "filepath": old_expected_filename,
                            "force": False,
                        },
                    )
                    self.assertDictEqual(
                        await watcher.queue.get(),
                        {
                            "method": "PUT",
                            "container_path": container_path,
                            "filepath": expected_filename,
                            "force": True,
                        },
                    )
                    self.assertDictEqual(
                        await watcher.queue.get(),
                        {
                            "method": "DELETE",
                            "container_path": container_path,
                            "filepath": path.join(
                                old_expected_rename_dir, expected_filename
                            ),
                            "force": False,
                        },
                    )
                    self.assertDictEqual(
                        await watcher.queue.get(),
                        {
                            "method": "PUT",
                            "container_path": container_path,
                            "filepath": path.join(
                                expected_rename_dir, expected_filename
                            ),
                            "force": True,
                        },
                    )

    async def test_get_changelog_data_rmdir(self):
        with get_watcher() as watcher:
            expected_filenames = ["testrm1.txt", "testrm2.txt"]
            folder_to_delete = "folder_to_delete"
            subfolder = "sub1/sub2/" + folder_to_delete
            out = (
                "345671 07RMDIR 17:32:36.947640226 2022.11.28 0x1 t=[0x200000424:0x1a2:0x0] ef=0xf u=0:0 nid=152.228.214.30@tcp p=[0x200000424:0x138:0x0] "  # noqa
                + folder_to_delete
            )

            async def communicate():
                return out.encode(), b"stderr"

            v1 = mock.AsyncMock(**{"returncode": 0, "communicate": communicate})

            # Add a container
            fs_path = path.join(watcher.node_folder, watcher.fs_name)
            container_path = "account/container"
            os.makedirs(path.join(fs_path, container_path))
            v2 = path.join(container_path, subfolder)

            v3 = [path.join(subfolder, filename) for filename in expected_filenames]

            with mock.patch.object(asyncio, "create_subprocess_shell", return_value=v1):
                with mock.patch.object(
                    watcher, "fid_to_path", mock.AsyncMock(return_value=v2)
                ):
                    with mock.patch.object(
                        watcher, "list_container_files", mock.AsyncMock(return_value=v3)
                    ):
                        await watcher.get_changelog_data()
                        assert watcher.queue.qsize() == 2
                        self.assertDictEqual(
                            await watcher.queue.get(),
                            {
                                "method": "DELETE",
                                "container_path": container_path,
                                "filepath": path.join(subfolder, expected_filenames[0]),
                                "force": False,
                            },
                        )
                        self.assertDictEqual(
                            await watcher.queue.get(),
                            {
                                "method": "DELETE",
                                "container_path": container_path,
                                "filepath": path.join(subfolder, expected_filenames[1]),
                                "force": False,
                            },
                        )

    # def test_get_changelog_data_hardlink(self):
    #    with get_watcher() as watcher:
    #        expected_filenames = ["testrm1.txt", "testrm2.txt"]
    #        folder_to_delete = "folder_to_delete"
    #        subfolder = "sub1/sub2/" + folder_to_delete
    #        out = (
    #            "9345804 03HLINK 14:36:54.579562827 2022.11.30 0x0 t=[0x200000424:0x184:0x0] ef=0xf u=0:0 nid=152.228.214.30@tcp p=[0x200000424:0x138:0x0] "  # noqa
    #            + new_file
    #        )

    async def test_get_changelog_data_subfolder(self):
        with get_watcher() as watcher:
            expected_filename = "test.txt"
            subfolder = "sub1/sub2/"
            out = (
                "231768 01CREAT 12:58:52.470582516 2021.04.18 0x0 t=[0x200000403:0x1d886:0x0] ef=0xf u=160:160 nid=10.0.0.1@tcp p=[0x200000403:0x27c:0x0] "  # noqa
                + expected_filename
            )

            async def communicate():
                return out.encode(), b"stderr"

            v1 = mock.AsyncMock(**{"returncode": 0, "communicate": communicate})

            # Add a container
            fs_path = path.join(watcher.node_folder, watcher.fs_name)
            container_path = "account/container"
            os.makedirs(path.join(fs_path, container_path))
            v2 = path.join(container_path, subfolder)

            with mock.patch.object(asyncio, "create_subprocess_shell", return_value=v1):
                with mock.patch.object(
                    watcher, "fid_to_path", mock.AsyncMock(return_value=v2)
                ):
                    await watcher.get_changelog_data()
                    watcher.queue.qsize() == 1
                    self.assertDictEqual(
                        await watcher.queue.get(),
                        {
                            "method": "PUT",
                            "container_path": container_path,
                            "filepath": path.join(subfolder, expected_filename),
                            "force": False,
                        },
                    )

    async def test_get_changelog_data_bad_lustre_type(self):
        with get_watcher() as watcher:
            expected_filename = "test.txt"
            out = (
                "231768 09FAKE 12:58:52.470582516 2021.04.18 0x0 t=[0x200000403:0x1d886:0x0] ef=0xf u=160:160 nid=10.0.0.1@tcp p=[0x200000403:0x27c:0x0] "  # noqa
                + expected_filename
            )

            async def communicate():
                return out.encode(), b"stderr"

            v1 = mock.AsyncMock(**{"returncode": 0, "communicate": communicate})

            with mock.patch.object(asyncio, "create_subprocess_shell", return_value=v1):
                await watcher.get_changelog_data()
                watcher.queue.qsize() == 0

    async def test_get_changelog_data_no_container_path(self):
        with get_watcher() as watcher:
            expected_filename = "test.txt"
            out = (
                "231768 01RENME 12:58:52.470582516 2021.04.18 0x0 t=[0x200000403:0x1d886:0x0] ef=0xf u=160:160 nid=10.0.0.1@tcp p=[0x200000403:0x27c:0x0] "  # noqa
                + expected_filename
            )

            async def communicate():
                return out.encode(), b"stderr"

            v1 = mock.AsyncMock(**{"returncode": 0, "communicate": communicate})

            with mock.patch.object(asyncio, "create_subprocess_shell", return_value=v1):
                # test the globale fid_to_container_path
                with mock.patch.object(
                    watcher, "fid_to_container_path", side_effect=NoContainerPathError
                ):
                    await watcher.get_changelog_data()
                    watcher.queue.qsize() == 0

                # test the rename fid_to_container_path
                with mock.patch.object(
                    watcher,
                    "fid_to_container_path",
                    side_effect=[None, NoContainerPathError],
                ):
                    await watcher.get_changelog_data()
                    watcher.queue.qsize() == 0

    async def test_get_changelog_data_error(self):
        with get_watcher() as watcher:
            # Add a container
            fs_path = path.join(watcher.node_folder, watcher.fs_name)
            container_path = "account/container"
            os.makedirs(path.join(fs_path, container_path))
            v2 = container_path

            with mock.patch.object(watcher, "fid_to_path", return_value=v2):
                # Check last id
                v = mock.MagicMock(
                    **{
                        "returncode": 0,
                        "stdout": b"1000 01CREAT 12:58:52.470582516 2021.04.18 0x0 t=[0x200000403:0x1d886:0x0] ef=0xf u=160:160 nid=10.0.0.1@tcp p=[0x200000403:0x27c:0x0] test.txt",  # noqa
                    }
                )
                with mock.patch.object(subprocess, "run", return_value=v):
                    last_id = 2000
                    watcher.last_id = last_id
                    await watcher.get_changelog_data()
                    assert watcher.last_id == last_id

                # Check exception
                v = mock.MagicMock(
                    **{
                        "returncode": 0,
                        "stdout": b"1000 09XATTR 12:58:52.470582516 2021.04.18 0x0 t=[0x200000403:0x1d886:0x0] ef=0xf u=160:160 nid=10.0.0.1@tcp p=[0x200000403:0x27c:0x0] test.txt",  # noqa
                    }
                )
                with mock.patch.object(subprocess, "run", return_value=v):
                    results = await watcher.get_changelog_data()
                    assert not results

            with mock.patch.object(
                watcher, "fid_to_path", return_value="bad_account/bad_container"
            ):
                # Check rename not in container
                v = mock.MagicMock(
                    **{
                        "returncode": 0,
                        "stdout": b"1000 08RENME 13:31:35.825896252 2021.04.18 0x0 t=[0:0x0:0x0] ef=0xf u=160:160 nid==10.0.0.1@tcp p=[0x200000403:0x1a00a:0x0] old_test.txt s=[0x200000403:0x1a00d:0x0] sp=[0x200000403:0x1a00a:0x0] new_test.txt",  # noqa
                    }
                )
                with mock.patch.object(subprocess, "run", return_value=v):
                    results = await watcher.get_changelog_data()
                    assert not results

            def fid_to_container_path_mock(fid):
                if fid == "sp=[0x200000403:0x1a00a:0x0]":
                    raise NoContainerPathError()
                return "path", ""

            with mock.patch.object(
                watcher, "fid_to_container_path", fid_to_container_path_mock
            ):
                # Check rename not in container
                v = mock.MagicMock(
                    **{
                        "returncode": 0,
                        "stdout": b"1000 08RENME 13:31:35.825896252 2021.04.18 0x0 t=[0:0x0:0x0] ef=0xf u=160:160 nid==10.0.0.1@tcp p=[0x200000403:0x1a00a:0x0] old_test.txt s=[0x200000403:0x1a00d:0x0] sp=[0x200000403:0x1a00a:0x0] new_test.txt",  # noqa
                    }
                )
                with mock.patch.object(subprocess, "run", return_value=v):
                    results = await watcher.get_changelog_data()
                    assert not results

    async def test_get_data(self):
        with get_watcher() as watcher:
            # Check first iter
            with mock.patch("swiftonfile.watcher.generic.GenericWatcher.get_data"):
                await watcher.get_data(0)
                assert watcher.last_id == 0

            # check others
            with mock.patch.object(watcher, "get_changelog_data") as mock_data:
                await watcher.get_data(1)
                mock_data.assert_called_once_with()
