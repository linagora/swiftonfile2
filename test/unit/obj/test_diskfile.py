# Copyright (c) 2012-2013 Red Hat, Inc.
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

""" Tests for swiftonfile.swift.obj.diskfile """

import os
import stat
import errno
import unittest
import tempfile
import shutil
import mock
import time
from eventlet import tpool
from mock import Mock, patch
from hashlib import md5
from copy import deepcopy
from swiftonfile.swift.common.exceptions import AlreadyExistsAsDir, AlreadyExistsAsFile
from swift.common.exceptions import (
    DiskFileNoSpace,
    DiskFileNotOpen,
    DiskFileNotExist,
    DiskFileExpired,
    InvalidAccountInfo,
)

from swiftonfile.swift.common.exceptions import SwiftOnFileSystemOSError
import swiftonfile.swift.common.utils
from swiftonfile.swift.common.utils import normalize_timestamp
import swiftonfile.swift.obj.diskfile
from swiftonfile.swift.obj import diskfile
from swiftonfile.swift.obj.diskfile import (
    DiskFileWriter,
    DiskFileManager,
    DiskFileReader,
    UserMappingDiskFileBehavior,
    GroupMappingDiskFileBehavior,
)
from swiftonfile.swift.common.utils import (
    DEFAULT_UID,
    DEFAULT_GID,
    X_OBJECT_TYPE,
    DIR_OBJECT,
    X_TIMESTAMP,
    X_CONTENT_TYPE,
)

from test.utils import nested
from test.unit.common.test_utils import _initxattr, _destroyxattr
from test.unit import FakeLogger

_metadata = {}


def _mapit(filename_or_fd):
    if isinstance(filename_or_fd, int):
        statmeth = os.fstat
    else:
        statmeth = os.lstat
    stats = statmeth(filename_or_fd)
    return stats.st_ino


def _mock_read_metadata(filename_or_fd):
    global _metadata
    ino = _mapit(filename_or_fd)
    if ino in _metadata:
        md = _metadata[ino]
    else:
        md = {}
    return md


def _mock_write_metadata(filename_or_fd, metadata):
    global _metadata
    ino = _mapit(filename_or_fd)
    _metadata[ino] = metadata


def _mock_clear_metadata():
    global _metadata
    _metadata = {}


class MockException(Exception):
    pass


def _mock_rmobjdir(p):
    raise MockException("swiftonfile.swift.obj.diskfile.rmobjdir() called")


def _mock_do_fsync(fd):
    return


def _mock_fallocate(fd, size):
    return


class MockRenamerCalled(Exception):
    pass


def _mock_renamer(a, b):
    raise MockRenamerCalled()


class TestDiskFileManager(unittest.TestCase):
    """Tests for swiftonfile.swift.obj.diskfile.DiskFileWriter"""

    def test_get_diskfile(self):
        conf = dict(
            devices="devices",
            mb_per_sync=2,
            keep_cache_size=(1024 * 1024),
            mount_check=False,
        )
        mgr = DiskFileManager(conf, FakeLogger())
        with mock.patch.object(mgr, "get_dev_path", return_value=None):
            try:
                mgr.get_diskfile("", "", "", "", "")
            except diskfile.DiskFileDeviceUnavailable:
                pass
            else:
                self.fail("DiskFileDeviceUnavailable waited")

    def test_get_diskfile_invalid_account(self):
        conf = dict(
            devices="devices",
            mb_per_sync=2,
            keep_cache_size=(1024 * 1024),
            mount_check=False,
            user="root",
            match_fs_user="swiftonfile.swift.obj.diskfile.UserMappingDiskFileBehavior",
        )
        mgr = DiskFileManager(conf, FakeLogger())
        with mock.patch.object(mgr, "get_dev_path"):
            with mock.patch.object(
                mgr.match_fs_user, "get_uid_gid", side_effect=InvalidAccountInfo
            ):
                try:
                    mgr.get_diskfile("", "", "", "", "")
                except InvalidAccountInfo:
                    pass
                else:
                    self.fail("InvalidAccountInfo should have been raised")

    def test_write_pickle(self):
        conf = dict(
            devices="devices",
            mb_per_sync=2,
            keep_cache_size=(1024 * 1024),
            mount_check=False,
        )
        mgr = DiskFileManager(conf, FakeLogger())
        with mock.patch.object(diskfile, "write_pickle") as mock_pickle:
            mgr.pickle_async_update(
                "device", "account", "container", "object", "data", 10, 0
            )
            hashed = "devices/device/async_pending/9ae/b32cfce25ac1560568986a7c616629ae-0000000010.00000"  # noqa
            mock_pickle.assert_called_once_with("data", hashed, "devices/device/tmp")

    def test_init_match_fs_user_invalid_account(self):
        conf = dict(
            devices="devices",
            mb_per_sync=2,
            keep_cache_size=(1024 * 1024),
            mount_check=False,
            swift_user="swift",
            match_fs_user="swiftonfile.swift.obj.diskfile.UserMappingDiskFileBehavior",
        )
        try:
            DiskFileManager(conf, FakeLogger())
        except InvalidAccountInfo:
            pass
        else:
            self.fail("InvalidAccountInfo should have been raised")


class TestDiskFileWriter(unittest.TestCase):
    """Tests for swiftonfile.swift.obj.diskfile.DiskFileWriter"""

    def test_close(self):
        dw = DiskFileWriter(10, None)
        dw._fd = 100
        mock_close = Mock()
        with patch("swiftonfile.swift.obj.diskfile.do_close", mock_close):
            # It should call do_close
            self.assertEqual(100, dw._fd)
            dw.close()
            self.assertEqual(1, mock_close.call_count)
            self.assertEqual(None, dw._fd)

            # It should not call do_close since it should
            # have made fd equal to None
            dw.close()
            self.assertEqual(None, dw._fd)
            self.assertEqual(1, mock_close.call_count)

    def test__write_entire_chunk(self):
        dw = DiskFileWriter(10, None)
        dw._disk_file = mock.MagicMock(
            **{"_mgr": mock.MagicMock(**{"bytes_per_sync": 1})}
        )

        def do_write(fd, chunk):
            return 1

        with mock.patch.object(diskfile, "do_write", do_write):
            with mock.patch.object(diskfile, "do_fdatasync") as fdatasync:
                with mock.patch.object(diskfile, "do_fadvise64") as fadvise64:
                    dw._write_entire_chunk([1, 2, 3, 4])
                    fdatasync.assert_called()
                    fadvise64.assert_called()

    def test_open_bad_path(self):
        dw = DiskFileWriter(10, None)
        dw._disk_file = mock.MagicMock(**{"_container_path": "path"})

        def mock_makedirs(path):
            e = OSError()
            e.errno = errno.EISDIR
            raise e

        with mock.patch.object(diskfile.os, "makedirs", mock_makedirs):
            try:
                dw.open()
            except OSError:
                pass
            else:
                self.fail("OSError waited")

    def test_open_ok(self):
        dw = DiskFileWriter(10, None)
        dw._disk_file = mock.MagicMock(
            **{
                "_container_path": "container_path",
                "_put_datadir": "put_data_dir",
                "_obj": "obj",
                "_uid": diskfile.DEFAULT_UID,
                "_gid": diskfile.DEFAULT_GID,
            }
        )

        size = 10
        fd = 12
        dw._size = size
        with mock.patch.object(diskfile.os, "makedirs"):
            with mock.patch.object(diskfile, "do_open", return_value=fd):
                # fallocate ok
                with mock.patch.object(diskfile, "fallocate") as mock_fallocate:
                    dw.open()
                    mock_fallocate.assert_called_once_with(fd, size)

                # fallocate raise1
                def fallocate_raise(a1, a2):
                    e = OSError()
                    e.errno = errno.ENOSPC
                    raise e

                with mock.patch.object(diskfile, "fallocate", fallocate_raise):
                    try:
                        dw.open()
                    except diskfile.DiskFileNoSpace:
                        pass
                    else:
                        self.fail("DiskFileNoSpace waited")

                # fallocate raise2
                def fallocate_raise(a1, a2):
                    e = OSError()
                    e.errno = errno.ENOSTR
                    raise e

                with mock.patch.object(diskfile, "fallocate", fallocate_raise):
                    try:
                        dw.open()
                    except OSError:
                        pass
                    else:
                        self.fail("OSError waited")

                # Call fchown
                dw._size = None
                dw._disk_file._uid = 10
                dw._disk_file._gid = 20
                with mock.patch.object(diskfile, "do_fchown") as mock_fchown:
                    dw.open()
                    mock_fchown.assert_called_once_with(fd, 10, 20)

    def test_open_errors(self):
        dw = DiskFileWriter(10, None)
        dw._disk_file = mock.MagicMock(
            **{
                "_container_path": "container_path",
                "_put_datadir": "put_data_dir",
                "_obj": "obj",
            }
        )

        with mock.patch.object(diskfile.os, "makedirs"):
            # ENOTDIR
            def mock_open(a1, a2):
                e = SwiftOnFileSystemOSError()
                e.errno = errno.ENOTDIR
                raise e

            with mock.patch.object(diskfile, "do_open", mock_open):
                try:
                    dw.open()
                except AlreadyExistsAsFile:
                    pass
                else:
                    self.fail("AlreadyExistsAsFile waited")

            # OTHER
            def mock_open(a1, a2):
                e = SwiftOnFileSystemOSError()
                e.errno = errno.ENXIO
                raise e

            with mock.patch.object(diskfile, "do_open", mock_open):
                try:
                    dw.open()
                except SwiftOnFileSystemOSError:
                    pass
                else:
                    self.fail("SwiftOnFileSystemOSError waited")

            # Reduce attempts to increase test speed
            diskfile.MAX_OPEN_ATTEMPTS = 2

            # EEXIST
            def mock_open(a1, a2):
                e = SwiftOnFileSystemOSError()
                e.errno = errno.EEXIST
                raise e

            with mock.patch.object(diskfile, "do_open", mock_open):
                try:
                    dw.open()
                except diskfile.DiskFileError:
                    pass
                else:
                    self.fail("DiskFileError waited")

            # EIO
            def mock_open(a1, a2):
                e = SwiftOnFileSystemOSError()
                e.errno = errno.EIO
                raise e

            with mock.patch.object(diskfile, "do_open", mock_open):
                try:
                    dw.open()
                except diskfile.DiskFileError:
                    pass
                else:
                    self.fail("DiskFileError waited")

            # ENOENT
            dw._disk_file._obj_path = None

            def mock_open(a1, a2):
                e = SwiftOnFileSystemOSError()
                e.errno = errno.ENOENT
                raise e

            with mock.patch.object(diskfile, "do_open", mock_open):
                try:
                    dw.open()
                except diskfile.DiskFileError:
                    pass
                else:
                    self.fail("DiskFileError waited")

            # ENOENT with attempts > 2
            diskfile.MAX_OPEN_ATTEMPTS = 3
            dw._container_path = "path"

            def mock_open(a1, a2):
                e = SwiftOnFileSystemOSError()
                e.errno = errno.ENOENT
                raise e

            with mock.patch.object(diskfile, "do_open", mock_open):
                try:
                    dw.open()
                except diskfile.DiskFileContainerDoesNotExist:
                    pass
                else:
                    self.fail("DiskFileContainerDoesNotExist waited")

            # attempts > 1
            dw.open_count = 0

            def mock_open(a1, a2):
                if dw.open_count == 1:
                    dw._disk_file._obj_path = "path"
                e = SwiftOnFileSystemOSError()
                e.errno = errno.ENOENT
                dw.open_count += 1
                raise e

            with mock.patch.object(diskfile, "do_open", mock_open):
                try:
                    dw.open()
                except diskfile.DiskFileError:
                    pass
                else:
                    self.fail("DiskFileError waited")

            # ENOENT with obj_path
            dw._disk_file._obj_path = "path"
            # Needed to avoid infinite loop
            dw.open_count = 0

            def mock_open(a1, a2):
                if dw.open_count == 0:
                    e = SwiftOnFileSystemOSError()
                    e.errno = errno.ENOENT
                    dw.open_count += 1
                    raise e

                e = SwiftOnFileSystemOSError()
                e.errno = errno.EEXIST
                dw.open_count += 1
                raise e

            with mock.patch.object(diskfile, "do_open", mock_open):
                try:
                    dw.open()
                except diskfile.DiskFileError:
                    pass
                else:
                    self.fail("DiskFileError waited")

    def test_chunks_finished(self):
        dw = DiskFileWriter(10, None)
        dw._upload_size = 10
        dw._chunks_etag = mock.MagicMock()
        dw._chunks_etag.hexdigest = lambda: "digest"
        usize, hex = dw.chunks_finished()
        assert usize == 10 and hex == "digest"

    def test__finalize_put(self):
        dw = DiskFileWriter(10, None)
        dw._fd = 10
        dw._last_sync = 11
        dw._upload_size = 12
        dw._tmppath = "tmp"
        dw._disk_file = mock.MagicMock()
        dw._disk_file._data_file = "data"
        dw._disk_file._put_datadir = "put"
        with nested(
            mock.patch.object(diskfile, "write_metadata"),
            mock.patch.object(diskfile, "do_fsync"),
            mock.patch.object(diskfile, "do_fadvise64"),
        ):
            # OK
            with mock.patch.object(diskfile, "do_rename"):
                dw._finalize_put({})

            # Error
            def rename_error(a1, a2):
                e = OSError()
                e.errno = errno.ENOENT
                raise e

            with mock.patch.object(diskfile, "do_rename", rename_error):
                # Stat None
                with mock.patch.object(diskfile, "do_stat", return_value=None):
                    with mock.patch.object(diskfile, "do_fstat", return_value=True):
                        try:
                            dw._finalize_put({})
                        except diskfile.DiskFileError:
                            pass
                        else:
                            self.fail("DiskFileError waited")

                # Stat OK no put data dir
                def do_stat(path):
                    if path == dw._disk_file._put_datadir:
                        return None
                    r = mock.MagicMock(**{"st_ino": 1})
                    return r

                with mock.patch.object(diskfile, "do_stat", do_stat):
                    with mock.patch.object(
                        diskfile,
                        "do_fstat",
                        return_value=mock.MagicMock(**{"st_ino": 1}),
                    ):
                        try:
                            dw._finalize_put({})
                        except diskfile.DiskFileError:
                            pass
                        else:
                            self.fail("DiskFileError waited")

                # Stat OK not dir
                with mock.patch.object(
                    diskfile, "do_stat", return_value=mock.MagicMock(**{"st_ino": 1})
                ):
                    with mock.patch.object(
                        diskfile,
                        "do_fstat",
                        return_value=mock.MagicMock(**{"st_ino": 1}),
                    ):
                        with mock.patch.object(
                            diskfile.stat, "S_ISDIR", return_value=False
                        ):
                            try:
                                dw._finalize_put({})
                            except diskfile.DiskFileError:
                                pass
                            else:
                                self.fail("DiskFileError waited")

    def test_commit(self):
        dw = DiskFileWriter(10, None)
        dw.commit(0)


class TestDiskFileReader(unittest.TestCase):
    """Tests for swiftonfile.swift.obj.diskfile.DiskFileReader"""

    def test_init(self):
        dr = DiskFileReader(10, 10, 5, 10, True)
        assert dr._keep_cache

        dr = DiskFileReader(10, 10, 10, 5, True)
        assert not dr._keep_cache

    def test_iter(self):
        dr = DiskFileReader(10, 10, 5, 10)
        dr._fd = 0
        dr._suppress_file_closing = False
        with mock.patch.object(diskfile, "do_read", return_value=[0, 0, 1]):
            with mock.patch.object(dr, "_drop_cache") as mock_drop_cache:
                with mock.patch.object(dr, "close") as mock_close:
                    iterator = dr.__iter__()
                    next(iterator)
                    dr._fd = -1
                    try:
                        next(iterator)
                    except StopIteration:
                        pass
                    mock_drop_cache.assert_called()
                    mock_close.assert_called()

    # def test_app_iter_range(self):
    #     dr = DiskFileReader(10, 10, 5, 10)
    #     dr._fd = 10
    #     dr._suppress_file_closing = False
    #     dr.app_iter_range(None, None)

    #     def iter(self):
    #         yield 1
    #         yield 2

    #     with mock.patch.object(diskfile, "do_lseek"):
    #         # Mocking not working
    #         with mock.patch.object(dr, "__iter__", iter):
    #             with mock.patch.object(dr, "close") as mock_close:
    #                 iterator = dr.app_iter_range(10, 20)
    #                 try:
    #                     next(iterator)
    #                 except StopIteration:
    #                     pass
    #                 mock_close.assert_called()

    def test_app_iter_ranges(self):
        dr = DiskFileReader(10, 10, 5, 10)
        for i in dr.app_iter_ranges(None, None, None, 0):
            assert i == ""

    def test__drop_cache(self):
        dr = DiskFileReader(10, 10, 5, 10)
        dr._keep_cache = True
        dr._fd = -1
        with mock.patch.object(diskfile, "do_fadvise64") as fadvise:
            dr._drop_cache(0, 0)
            fadvise.assert_not_called()


class TestDiskFile(unittest.TestCase):
    """Tests for swiftonfile.swift.obj.diskfile"""

    def setUp(self):
        self._orig_tpool_exc = tpool.execute
        tpool.execute = lambda f, *args, **kwargs: f(*args, **kwargs)
        self.lg = FakeLogger()
        _initxattr()
        _mock_clear_metadata()
        self._saved_df_wm = swiftonfile.swift.obj.diskfile.write_metadata
        self._saved_df_rm = swiftonfile.swift.obj.diskfile.read_metadata
        swiftonfile.swift.obj.diskfile.write_metadata = _mock_write_metadata
        swiftonfile.swift.obj.diskfile.read_metadata = _mock_read_metadata
        self._saved_ut_wm = swiftonfile.swift.common.utils.write_metadata
        self._saved_ut_rm = swiftonfile.swift.common.utils.read_metadata
        swiftonfile.swift.common.utils.write_metadata = _mock_write_metadata
        swiftonfile.swift.common.utils.read_metadata = _mock_read_metadata
        self._saved_do_fsync = swiftonfile.swift.obj.diskfile.do_fsync
        swiftonfile.swift.obj.diskfile.do_fsync = _mock_do_fsync
        self._saved_fallocate = swiftonfile.swift.obj.diskfile.fallocate
        swiftonfile.swift.obj.diskfile.fallocate = _mock_fallocate
        self.td = tempfile.mkdtemp()
        self.conf = dict(
            devices=self.td,
            mb_per_sync=2,
            keep_cache_size=(1024 * 1024),
            mount_check=False,
        )
        self.mgr = DiskFileManager(self.conf, self.lg)

    def tearDown(self):
        tpool.execute = self._orig_tpool_exc
        self.lg = None
        _destroyxattr()
        swiftonfile.swift.obj.diskfile.write_metadata = self._saved_df_wm
        swiftonfile.swift.obj.diskfile.read_metadata = self._saved_df_rm
        swiftonfile.swift.common.utils.write_metadata = self._saved_ut_wm
        swiftonfile.swift.common.utils.read_metadata = self._saved_ut_rm
        swiftonfile.swift.obj.diskfile.do_fsync = self._saved_do_fsync
        swiftonfile.swift.obj.diskfile.fallocate = self._saved_fallocate
        shutil.rmtree(self.td)

    def _get_diskfile(self, d, p, a, c, o, **kwargs):
        return self.mgr.get_diskfile(d, p, a, c, o, **kwargs)

    def test_constructor_no_slash(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        assert gdf._mgr is self.mgr
        assert gdf._device_path == os.path.join(self.td, "vol0")
        assert gdf._uid == DEFAULT_UID
        assert gdf._gid == DEFAULT_GID
        assert gdf._obj == "z"
        assert gdf._obj_path == ""
        assert gdf._put_datadir == os.path.join(
            self.td, "vol0", "ufo47", "bar"
        ), gdf._put_datadir
        assert gdf._data_file == os.path.join(self.td, "vol0", "ufo47", "bar", "z")
        assert gdf._is_dir is False
        assert gdf._fd is None

    def test_constructor_leadtrail_slash(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "/b/a/z/")
        assert gdf._obj == "z"
        assert gdf._obj_path == "b/a"
        assert gdf._put_datadir == os.path.join(
            self.td, "vol0", "ufo47", "bar", "b", "a"
        ), gdf._put_datadir

    def test_timestamp(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "/b/a/z/")
        gdf._metadata = {X_TIMESTAMP: 10}
        assert int(gdf.timestamp) == 10

    def test_data_timestamp(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "/b/a/z/")
        gdf._metadata = {X_TIMESTAMP: 10}
        assert int(gdf.data_timestamp) == 10

    def test_fragments(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "/b/a/z/")
        assert gdf.fragments is None

    def test_durable_timestamp(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "/b/a/z/")
        gdf._metadata = None
        try:
            gdf.durable_timestamp
        except DiskFileNotOpen:
            pass
        else:
            self.fail("DiskFileNotOpen waited")

        gdf._metadata = {X_TIMESTAMP: 10}
        assert gdf.durable_timestamp == 10

    def test_content_type(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "/b/a/z/")
        gdf._metadata = None
        try:
            gdf.content_type
        except DiskFileNotOpen:
            pass
        else:
            self.fail("DiskFileNotOpen waited")

        gdf._metadata = {X_CONTENT_TYPE: 10}
        assert gdf.content_type == 10

    def test_content_type_timestamp(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "/b/a/z/")
        gdf._metadata = None
        try:
            gdf.content_type_timestamp
        except DiskFileNotOpen:
            pass
        else:
            self.fail("DiskFileNotOpen waited")

        gdf._metadata = {X_TIMESTAMP: 10}
        assert gdf.content_type_timestamp == 10

    def test_get_metafile_metadata(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "/b/a/z/")
        gdf._metadata = None
        try:
            gdf.get_metafile_metadata()
        except DiskFileNotOpen:
            pass
        else:
            self.fail("DiskFileNotOpen waited")

        gdf._metadata = 10
        assert gdf.get_metafile_metadata() == 10

    def test_get_datafile_metadata(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "/b/a/z/")
        gdf._metadata = None
        try:
            gdf.get_datafile_metadata()
        except DiskFileNotOpen:
            pass
        else:
            self.fail("DiskFileNotOpen waited")

        gdf._metadata = 10
        assert gdf.get_datafile_metadata() == 10

    def test_has_metadata(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "/b/a/z/")
        with mock.patch.object(diskfile, "read_metadata", return_value={"x": "y"}):
            assert gdf.has_metadata()
        with mock.patch.object(diskfile, "read_metadata", return_value=None):
            assert not gdf.has_metadata()

        # ENOENT
        def mock_read_metadata(a):
            e = OSError()
            e.errno = errno.ENOENT
            raise e

        with mock.patch.object(diskfile, "read_metadata", mock_read_metadata):
            try:
                gdf.has_metadata()
            except DiskFileNotExist:
                pass
            else:
                self.fail("DiskFileNotExist waited")

        # OTHER
        def mock_read_metadata(a):
            e = OSError()
            e.errno = errno.E2BIG
            raise e

        with mock.patch.object(diskfile, "read_metadata", mock_read_metadata):
            try:
                gdf.has_metadata()
            except OSError:
                pass
            else:
                self.fail("OSError waited")

    def test_open_no_fd(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")

        # ENOENT
        def do_open(a, b):
            e = SwiftOnFileSystemOSError()
            e.errno = errno.E2BIG
            raise e

        with mock.patch.object(diskfile, "do_open", do_open):
            try:
                gdf.open()
            except SwiftOnFileSystemOSError:
                pass
            else:
                self.fail("SwiftOnFileSystemOSError waited")

        # OTHER
        def do_open(a, b):
            e = SwiftOnFileSystemOSError()
            e.errno = errno.ENOENT
            raise e

        with mock.patch.object(diskfile, "do_open", do_open):
            try:
                gdf.open()
            except DiskFileNotExist:
                pass
            else:
                self.fail("DiskFileNotExist waited")

    def test__is_object_expired(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        assert not gdf._is_object_expired({"X-Delete-At": "fake"})
        assert not gdf._is_object_expired({"X-Delete-At": time.time() + 20})

    def test___enter__(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        gdf._disk_file_open = False
        try:
            gdf.__enter__()
        except DiskFileNotOpen:
            pass
        else:
            self.fail("DiskFileNotOpen waited")

    def test_get_metadata(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        gdf._disk_file_open = False
        try:
            gdf.get_metadata()
        except DiskFileNotOpen:
            pass
        else:
            self.fail("DiskFileNotOpen waited")

    def test__filter_metadata(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        expected_metadata = {"fake": "fake"}
        gdf._metadata = {
            diskfile.X_TYPE: "fake",
            diskfile.X_OBJECT_TYPE: "fake",
            "fake": "fake",
        }
        gdf._filter_metadata()
        assert gdf._metadata == expected_metadata
        gdf._filter_metadata()
        assert gdf._metadata == expected_metadata

    def test_open_no_metadata(self):
        the_path = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_file = os.path.join(the_path, "z")
        os.makedirs(the_path)
        with open(the_file, "wb") as fd:
            fd.write(b"1234")
        stats = os.stat(the_file)
        ts = normalize_timestamp(stats.st_ctime)
        etag = md5()
        etag.update(b"1234")
        etag = etag.hexdigest()
        exp_md = {
            "Content-Length": 4,
            "ETag": etag,
            "X-Timestamp": ts,
            "X-Object-PUT-Mtime": normalize_timestamp(stats.st_mtime),
            "Content-Type": "application/octet-stream",
        }
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        assert gdf._obj == "z"
        assert gdf._fd is None
        assert gdf._disk_file_open is False
        assert gdf._metadata is None
        assert not gdf._is_dir
        with gdf.open():
            assert gdf._data_file == the_file
            assert not gdf._is_dir
            assert gdf._fd is not None
            assert gdf._metadata == exp_md
            assert gdf._disk_file_open is True
        assert gdf._disk_file_open is False

    def test_read_metadata_optimize_open_close(self):
        the_path = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_file = os.path.join(the_path, "z")
        os.makedirs(the_path)
        with open(the_file, "wb") as fd:
            fd.write(b"1234")
        init_md = {
            "X-Type": "Object",
            "X-Object-Type": "file",
            "Content-Length": 4,
            "Content-Type-Timestamp": normalize_timestamp(os.stat(the_file).st_ctime),
            "ETag": md5(b"1234").hexdigest(),
            "X-Timestamp": normalize_timestamp(os.stat(the_file).st_ctime),
            "Content-Type": "application/octet-stream",
        }
        _metadata[_mapit(the_file)] = init_md
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        assert gdf._obj == "z"
        assert gdf._fd is None
        assert gdf._disk_file_open is False
        assert gdf._metadata is None
        assert not gdf._is_dir

        # Case 1
        # Ensure that reading metadata for non-GET requests
        # does not incur opening and closing the file when
        # metadata is NOT stale.
        mock_open = Mock()
        mock_close = Mock()
        with mock.patch("swiftonfile.swift.obj.diskfile.do_open", mock_open):
            with mock.patch("swiftonfile.swift.obj.diskfile.do_close", mock_close):
                md = gdf.read_metadata()
                self.assertEqual(md, init_md)
        self.assertFalse(mock_open.called)
        self.assertFalse(mock_close.called)

        # Case 2
        # Ensure that reading metadata for non-GET requests
        # still opens and reads the file when metadata is stale
        with open(the_file, "a") as fd:
            # Append to the existing file to make the stored metadata
            # invalid/stale.
            fd.write("5678")
        md = gdf.read_metadata()
        # Check that the stale metadata is recalculated to account for
        # change in file content
        self.assertNotEqual(md, init_md)
        self.assertEqual(md["Content-Length"], 8)
        self.assertEqual(md["ETag"], md5(b"12345678").hexdigest())

    def test_read_metadata_exception(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")

        # ENOENT
        def read_metadata(a):
            e = OSError()
            e.errno = errno.ENOENT
            raise e

        with mock.patch.object(diskfile, "read_metadata", read_metadata):
            try:
                gdf.read_metadata()
            except DiskFileNotExist:
                pass
            else:
                self.fail("DiskFileNotExist waited")

        # Other
        def read_metadata(a):
            e = OSError()
            e.errno = errno.ENOEXEC
            raise e

        with mock.patch.object(diskfile, "read_metadata", read_metadata):
            try:
                gdf.read_metadata()
            except OSError:
                pass
            else:
                self.fail("OSError waited")

    def test_read_metadata_stat_exception(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")

        with mock.patch.object(diskfile, "read_metadata", return_value=None):
            # ENOENT
            def do_stat(a):
                e = OSError()
                e.errno = errno.ENOENT
                raise e

            with mock.patch.object(diskfile, "do_stat", do_stat):
                try:
                    gdf.read_metadata()
                except DiskFileNotExist:
                    pass
                else:
                    self.fail("DiskFileNotExist waited")

            # Other
            def do_stat(a):
                e = OSError()
                e.errno = errno.ENOPKG
                raise e

            with mock.patch.object(diskfile, "do_stat", do_stat):
                try:
                    gdf.read_metadata()
                except OSError:
                    pass
                else:
                    self.fail("OSError waited")

    def test__unlinkold(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        gdf._is_dir = False
        gdf._container_path = "container"
        gdf._data_file = "/fake/path/2"
        with mock.patch.object(diskfile, "do_unlink"):
            with mock.patch.object(
                diskfile, "rmobjdir", return_value=False
            ) as rmobjdir:
                gdf._unlinkold()
                rmobjdir.assert_called()
            with mock.patch.object(diskfile, "rmobjdir", return_value=True) as rmobjdir:
                gdf._unlinkold()
                rmobjdir.assert_called()

    def test_delete_error(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        gdf._metadata = None

        # ENOENT
        def read_metadata(a):
            e = OSError()
            e.errno = errno.ENOENT
            raise e

        with mock.patch.object(diskfile, "read_metadata", read_metadata):
            with mock.patch.object(gdf, "_unlinkold") as _unlinkold:
                gdf.delete(0)
                _unlinkold.assert_called()

        # OTHER
        def read_metadata(a):
            e = OSError()
            e.errno = errno.E2BIG
            raise e

        with mock.patch.object(diskfile, "read_metadata", read_metadata):
            try:
                gdf.delete(0)
            except OSError:
                pass
            else:
                self.fail("OSError waited")

    def test_open_invalid_existing_metadata(self):
        the_path = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_file = os.path.join(the_path, "z")
        os.makedirs(the_path)
        with open(the_file, "wb") as fd:
            fd.write(b"1234")
        inv_md = {
            "Content-Length": 5,
            "ETag": "etag",
            "X-Timestamp": "ts",
            "Content-Type": "application/loctet-stream",
        }
        _metadata[_mapit(the_file)] = inv_md
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        assert gdf._obj == "z"
        assert not gdf._is_dir
        assert gdf._fd is None
        assert gdf._disk_file_open is False
        with gdf.open():
            assert gdf._data_file == the_file
            assert gdf._metadata != inv_md
            assert gdf._disk_file_open is True
        assert gdf._disk_file_open is False

    def test_open_isdir(self):
        the_path = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_dir = os.path.join(the_path, "d")
        os.makedirs(the_dir)
        ini_md = {
            "X-Type": "Object",
            "X-Object-Type": "dir",
            "Content-Length": 5,
            "ETag": "etag",
            "X-Timestamp": "ts",
            "Content-Type": "application/loctet-stream",
        }
        _metadata[_mapit(the_dir)] = ini_md
        exp_md = ini_md.copy()
        del exp_md["X-Type"]
        del exp_md["X-Object-Type"]
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "d")
        assert gdf._obj == "d"
        assert gdf._is_dir is False
        assert gdf._disk_file_open is False
        with gdf.open():
            assert gdf._is_dir
            assert gdf._data_file == the_dir
            assert gdf._disk_file_open is True
        assert gdf._disk_file_open is False

    def _create_and_get_diskfile(self, dev, par, acc, con, obj, fsize=256):
        # FIXME: assumes account === volume
        the_path = os.path.join(self.td, dev, acc, con)
        the_file = os.path.join(the_path, obj)
        base_obj = os.path.basename(the_file)
        base_dir = os.path.dirname(the_file)
        os.makedirs(base_dir)
        with open(the_file, "wb") as fd:
            fd.write(b"y" * fsize)
        gdf = self._get_diskfile(dev, par, acc, con, obj)
        assert gdf._obj == base_obj
        assert not gdf._is_dir
        assert gdf._fd is None
        return gdf

    def test_reader(self):
        closed = [False]
        fd = [-1]

        def mock_close(*args, **kwargs):
            closed[0] = True
            os.close(fd[0])

        with mock.patch("swiftonfile.swift.obj.diskfile.do_close", mock_close):
            gdf = self._create_and_get_diskfile("vol0", "p57", "ufo47", "bar", "z")
            with gdf.open():
                assert gdf._fd is not None
                assert gdf._data_file == os.path.join(
                    self.td, "vol0", "ufo47", "bar", "z"
                )
                reader = gdf.reader()
            assert reader._fd is not None
            fd[0] = reader._fd
            chunks = [ck for ck in reader]
            assert reader._fd is None
            assert closed[0]
            assert len(chunks) == 1, repr(chunks)

    def test_reader_disk_chunk_size(self):
        conf = dict(disk_chunk_size=64)
        conf.update(self.conf)
        self.mgr = DiskFileManager(conf, self.lg)
        gdf = self._create_and_get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        with gdf.open():
            reader = gdf.reader()
        try:
            assert reader._disk_chunk_size == 64
            chunks = [ck for ck in reader]
        finally:
            reader.close()
        assert len(chunks) == 4, repr(chunks)
        for chunk in chunks:
            assert len(chunk) == 64, repr(chunks)

    def test_reader_larger_file(self):
        closed = [False]
        fd = [-1]

        def mock_close(*args, **kwargs):
            closed[0] = True
            os.close(fd[0])

        with mock.patch("swiftonfile.swift.obj.diskfile.do_close", mock_close):
            gdf = self._create_and_get_diskfile(
                "vol0", "p57", "ufo47", "bar", "z", fsize=1024 * 1024 * 2
            )
            with gdf.open():
                assert gdf._fd is not None
                assert gdf._data_file == os.path.join(
                    self.td, "vol0", "ufo47", "bar", "z"
                )
                reader = gdf.reader()
            assert reader._fd is not None
            fd[0] = reader._fd
            _ = [ck for ck in reader]
            assert reader._fd is None
            assert closed[0]

    def test_reader_dir_object(self):
        called = [False]

        def our_do_close(fd):
            called[0] = True
            os.close(fd)

        the_cont = os.path.join(self.td, "vol0", "ufo47", "bar")
        os.makedirs(os.path.join(the_cont, "dir"))
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "dir")
        with gdf.open():
            reader = gdf.reader()
        try:
            chunks = [ck for ck in reader]
            assert len(chunks) == 0, repr(chunks)
            with mock.patch("swiftonfile.swift.obj.diskfile.do_close", our_do_close):
                reader.close()
            assert not called[0]
        finally:
            reader.close()

    def test_create_dir_object_no_md(self):
        the_cont = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_dir = "dir"
        os.makedirs(the_cont)
        gdf = self._get_diskfile(
            "vol0", "p57", "ufo47", "bar", os.path.join(the_dir, "z")
        )
        # Not created, dir object path is different, just checking
        assert gdf._obj == "z"
        gdf._create_dir_object(the_dir)
        full_dir_path = os.path.join(the_cont, the_dir)
        assert os.path.isdir(full_dir_path)
        assert _mapit(full_dir_path) not in _metadata

    def test_create_dir_object_with_md(self):
        the_cont = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_dir = "dir"
        os.makedirs(the_cont)
        gdf = self._get_diskfile(
            "vol0", "p57", "ufo47", "bar", os.path.join(the_dir, "z")
        )
        # Not created, dir object path is different, just checking
        assert gdf._obj == "z"
        dir_md = {"Content-Type": "application/directory", X_OBJECT_TYPE: DIR_OBJECT}
        gdf._create_dir_object(the_dir, dir_md)
        full_dir_path = os.path.join(the_cont, the_dir)
        assert os.path.isdir(full_dir_path)
        assert _mapit(full_dir_path) in _metadata

    def test_create_dir_object_exists(self):
        the_path = os.path.join(self.td, "vol0", "bar")
        the_dir = os.path.join(the_path, "dir")
        os.makedirs(the_path)
        with open(the_dir, "wb") as fd:
            fd.write(b"1234")
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "dir/z")
        # Not created, dir object path is different, just checking
        assert gdf._obj == "z"

        def _mock_do_chown(p, u, g):
            assert u == DEFAULT_UID
            assert g == DEFAULT_GID

        dc = swiftonfile.swift.obj.diskfile.do_chown
        swiftonfile.swift.obj.diskfile.do_chown = _mock_do_chown
        self.assertRaises(AlreadyExistsAsFile, gdf._create_dir_object, the_dir)
        swiftonfile.swift.obj.diskfile.do_chown = dc
        self.assertFalse(os.path.isdir(the_dir))
        self.assertFalse(_mapit(the_dir) in _metadata)

    def test_create_dir_object_do_stat_failure(self):
        the_path = os.path.join(self.td, "vol0", "bar")
        the_dir = os.path.join(the_path, "dir")
        os.makedirs(the_path)
        with open(the_dir, "wb") as fd:
            fd.write(b"1234")
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "dir/z")
        # Not created, dir object path is different, just checking
        assert gdf._obj == "z"

        def _mock_do_chown(p, u, g):
            assert u == DEFAULT_UID
            assert g == DEFAULT_GID

        dc = swiftonfile.swift.obj.diskfile.do_chown
        swiftonfile.swift.obj.diskfile.do_chown = _mock_do_chown
        self.assertRaises(AlreadyExistsAsFile, gdf._create_dir_object, the_dir)
        swiftonfile.swift.obj.diskfile.do_chown = dc
        self.assertFalse(os.path.isdir(the_dir))
        self.assertFalse(_mapit(the_dir) in _metadata)

    def test_write_metadata(self):
        the_path = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_dir = os.path.join(the_path, "z")
        os.makedirs(the_dir)
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        md = {"Content-Type": "application/octet-stream", "a": "b"}
        gdf.write_metadata(md.copy())
        fmd = _metadata[_mapit(the_dir)]
        md.update({"X-Object-Type": "file", "X-Type": "Object"})
        self.assertTrue(fmd["a"], md["a"])
        self.assertTrue(fmd["Content-Type"], md["Content-Type"])

    def test_add_metadata_to_existing_file(self):
        the_path = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_file = os.path.join(the_path, "z")
        os.makedirs(the_path)
        with open(the_file, "wb") as fd:
            fd.write(b"1234")
        ini_md = {
            "X-Type": "Object",
            "X-Object-Type": "file",
            "Content-Length": 4,
            "ETag": "etag",
            "X-Timestamp": "ts",
            "Content-Type": "application/loctet-stream",
        }
        _metadata[_mapit(the_file)] = ini_md
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        md = {"Content-Type": "application/octet-stream", "a": "b"}
        gdf.write_metadata(md.copy())
        self.assertTrue(_metadata[_mapit(the_file)]["a"], "b")
        newmd = {"X-Object-Meta-test": "1234"}
        gdf.write_metadata(newmd.copy())
        on_disk_md = _metadata[_mapit(the_file)]
        self.assertTrue(on_disk_md["Content-Length"], 4)
        self.assertTrue(on_disk_md["X-Object-Meta-test"], "1234")
        self.assertTrue(on_disk_md["X-Type"], "Object")
        self.assertTrue(on_disk_md["X-Object-Type"], "file")
        self.assertTrue(on_disk_md["ETag"], "etag")
        self.assertFalse("a" in on_disk_md)

    def test_add_md_to_existing_file_with_md_in_gdf(self):
        the_path = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_file = os.path.join(the_path, "z")
        os.makedirs(the_path)
        with open(the_file, "wb") as fd:
            fd.write(b"1234")
        ini_md = {
            "X-Type": "Object",
            "X-Object-Type": "file",
            "Content-Length": 4,
            "name": "z",
            "ETag": "etag",
            "X-Timestamp": "ts",
        }
        _metadata[_mapit(the_file)] = ini_md
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")

        # make sure gdf has the _metadata
        gdf.open()
        md = {"a": "b"}
        gdf.write_metadata(md.copy())
        self.assertTrue(_metadata[_mapit(the_file)]["a"], "b")
        newmd = {"X-Object-Meta-test": "1234"}
        gdf.write_metadata(newmd.copy())
        on_disk_md = _metadata[_mapit(the_file)]
        self.assertTrue(on_disk_md["Content-Length"], 4)
        self.assertTrue(on_disk_md["X-Object-Meta-test"], "1234")
        self.assertFalse("a" in on_disk_md)

    def test_add_metadata_to_existing_dir(self):
        the_cont = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_dir = os.path.join(the_cont, "dir")
        os.makedirs(the_dir)
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "dir")
        self.assertEqual(gdf._metadata, None)
        init_md = {
            "X-Type": "Object",
            "Content-Length": 0,
            "ETag": "etag",
            "X-Timestamp": "ts",
            "X-Object-Meta-test": "test",
            "Content-Type": "application/directory",
        }
        _metadata[_mapit(the_dir)] = init_md

        md = {"X-Object-Meta-test": "test"}
        gdf.write_metadata(md.copy())
        self.assertEqual(_metadata[_mapit(the_dir)]["X-Object-Meta-test"], "test")
        self.assertEqual(
            _metadata[_mapit(the_dir)]["Content-Type"].lower(), "application/directory"
        )

        # set new metadata
        newmd = {"X-Object-Meta-test2": "1234"}
        gdf.write_metadata(newmd.copy())
        self.assertEqual(
            _metadata[_mapit(the_dir)]["Content-Type"].lower(), "application/directory"
        )
        self.assertEqual(_metadata[_mapit(the_dir)]["X-Object-Meta-test2"], "1234")
        self.assertEqual(_metadata[_mapit(the_dir)]["X-Object-Type"], DIR_OBJECT)
        self.assertFalse("X-Object-Meta-test" in _metadata[_mapit(the_dir)])

    def test_write_metadata_w_meta_file(self):
        the_path = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_file = os.path.join(the_path, "z")
        os.makedirs(the_path)
        with open(the_file, "wb") as fd:
            fd.write(b"1234")
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        newmd = deepcopy(gdf.read_metadata())
        newmd["X-Object-Meta-test"] = "1234"
        gdf.write_metadata(newmd)
        assert _metadata[_mapit(the_file)] == newmd

    def test_write_metadata_w_meta_file_no_content_type(self):
        the_path = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_file = os.path.join(the_path, "z")
        os.makedirs(the_path)
        with open(the_file, "wb") as fd:
            fd.write(b"1234")
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        newmd = deepcopy(gdf.read_metadata())
        newmd["Content-Type"] = ""
        newmd["X-Object-Meta-test"] = "1234"
        gdf.write_metadata(newmd)
        assert _metadata[_mapit(the_file)] == newmd

    def test_write_metadata_w_meta_dir(self):
        the_path = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_dir = os.path.join(the_path, "dir")
        os.makedirs(the_dir)
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "dir")
        newmd = deepcopy(gdf.read_metadata())
        newmd["X-Object-Meta-test"] = "1234"
        gdf.write_metadata(newmd)
        assert _metadata[_mapit(the_dir)] == newmd

    def test_write_metadata_w_marker_dir(self):
        the_path = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_dir = os.path.join(the_path, "dir")
        os.makedirs(the_dir)
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "dir")
        newmd = deepcopy(gdf.read_metadata())
        newmd["X-Object-Meta-test"] = "1234"
        gdf.write_metadata(newmd)
        assert _metadata[_mapit(the_dir)] == newmd

    def test_put_w_marker_dir_create(self):
        the_cont = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_dir = os.path.join(the_cont, "dir")
        os.makedirs(the_cont)
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "dir")
        assert gdf._metadata is None
        newmd = {
            "ETag": "etag",
            "X-Timestamp": "ts",
            "Content-Type": "application/directory",
        }
        with gdf.create() as dw:
            dw.put(newmd)
        assert gdf._data_file == the_dir
        for key, val in newmd.items():
            assert _metadata[_mapit(the_dir)][key] == val
        assert _metadata[_mapit(the_dir)][X_OBJECT_TYPE] == DIR_OBJECT

    def test_put_is_dir(self):
        the_path = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_dir = os.path.join(the_path, "dir")
        os.makedirs(the_dir)
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "dir")
        with gdf.open():
            origmd = gdf.get_metadata()
        origfmd = _metadata[_mapit(the_dir)]
        newmd = deepcopy(origmd)
        # FIXME: This is a hack to get to the code-path; it is not clear
        # how this can happen normally.
        newmd["Content-Type"] = ""
        newmd["X-Object-Meta-test"] = "1234"
        with gdf.create() as dw:
            try:
                # FIXME: We should probably be able to detect in .create()
                # when the target file name already exists as a directory to
                # avoid reading the data off the wire only to fail as a
                # directory.
                dw.write(b"12345\n")
                dw.put(newmd)
            except AlreadyExistsAsDir:
                pass
            else:
                self.fail("Expected to encounter" " 'already-exists-as-dir' exception")
        with gdf.open():
            assert gdf.get_metadata() == origmd
        assert _metadata[_mapit(the_dir)] == origfmd, "was: %r, is: %r" % (
            origfmd,
            _metadata[_mapit(the_dir)],
        )

    def test_put(self):
        the_cont = os.path.join(self.td, "vol0", "ufo47", "bar")
        os.makedirs(the_cont)
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        assert gdf._obj == "z"
        assert gdf._obj_path == ""
        assert gdf._container_path == os.path.join(self.td, "vol0", "ufo47", "bar")
        assert gdf._put_datadir == the_cont
        assert gdf._data_file == os.path.join(self.td, "vol0", "ufo47", "bar", "z")

        body = b"1234\n"
        etag = md5()
        etag.update(body)
        etag = etag.hexdigest()
        metadata = {
            "X-Timestamp": "1234",
            "Content-Type": "file",
            "ETag": etag,
            "Content-Length": "5",
        }

        with gdf.create() as dw:
            assert dw._tmppath is not None
            tmppath = dw._tmppath
            dw.write(body)
            dw.put(metadata)

        assert os.path.exists(gdf._data_file)
        assert not os.path.exists(tmppath)

    def test_put_ENOSPC(self):
        the_cont = os.path.join(self.td, "vol0", "ufo47", "bar")
        os.makedirs(the_cont)
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        assert gdf._obj == "z"
        assert gdf._obj_path == ""
        assert gdf._container_path == os.path.join(self.td, "vol0", "ufo47", "bar")
        assert gdf._put_datadir == the_cont
        assert gdf._data_file == os.path.join(self.td, "vol0", "ufo47", "bar", "z")

        body = b"1234\n"
        etag = md5()
        etag.update(body)
        etag = etag.hexdigest()
        metadata = {
            "X-Timestamp": "1234",
            "Content-Type": "file",
            "ETag": etag,
            "Content-Length": "5",
        }

        def mock_open(*args, **kwargs):
            raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))

        with mock.patch("os.open", mock_open):
            try:
                with gdf.create() as dw:
                    assert dw._tmppath is not None
                    dw.write(body)
                    dw.put(metadata)
            except DiskFileNoSpace:
                pass
            else:
                self.fail("Expected exception DiskFileNoSpace")

    def test_put_rename_ENOENT(self):
        the_cont = os.path.join(self.td, "vol0", "ufo47", "bar")
        os.makedirs(the_cont)
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        assert gdf._obj == "z"
        assert gdf._obj_path == ""
        assert gdf._container_path == os.path.join(self.td, "vol0", "ufo47", "bar")
        assert gdf._put_datadir == the_cont
        assert gdf._data_file == os.path.join(self.td, "vol0", "ufo47", "bar", "z")

        body = b"1234\n"
        etag = md5()
        etag.update(body)
        etag = etag.hexdigest()
        metadata = {
            "X-Timestamp": "1234",
            "Content-Type": "file",
            "ETag": etag,
            "Content-Length": "5",
        }

        def mock_sleep(*args, **kwargs):
            # Return without sleep, no need to dely unit tests
            return

        def mock_rename(*args, **kwargs):
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT))

        with mock.patch("swiftonfile.swift.obj.diskfile.sleep", mock_sleep):
            with mock.patch("os.rename", mock_rename):
                try:
                    with gdf.create() as dw:
                        assert dw._tmppath is not None
                        dw.write(body)
                        dw.put(metadata)
                except SwiftOnFileSystemOSError:
                    pass
                else:
                    self.fail("Expected exception DiskFileError")

    def test_put_obj_path(self):
        the_obj_path = os.path.join("b", "a")
        the_file = os.path.join(the_obj_path, "z")
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", the_file)
        assert gdf._obj == "z"
        assert gdf._obj_path == the_obj_path
        assert gdf._container_path == os.path.join(self.td, "vol0", "ufo47", "bar")
        assert gdf._put_datadir == os.path.join(
            self.td, "vol0", "ufo47", "bar", "b", "a"
        )
        assert gdf._data_file == os.path.join(
            self.td, "vol0", "ufo47", "bar", "b", "a", "z"
        )

        body = b"1234\n"
        etag = md5()
        etag.update(body)
        etag = etag.hexdigest()
        metadata = {
            "X-Timestamp": "1234",
            "Content-Type": "file",
            "ETag": etag,
            "Content-Length": "5",
        }

        with gdf.create() as dw:
            assert dw._tmppath is not None
            tmppath = dw._tmppath
            dw.write(body)
            dw.put(metadata)

        assert os.path.exists(gdf._data_file)
        assert not os.path.exists(tmppath)

    def test_delete(self):
        the_path = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_file = os.path.join(the_path, "z")
        os.makedirs(the_path)
        with open(the_file, "wb") as fd:
            fd.write(b"1234")
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        assert gdf._obj == "z"
        assert gdf._data_file == the_file
        assert not gdf._is_dir
        later = float(gdf.read_metadata()["X-Timestamp"]) + 1
        gdf.delete(normalize_timestamp(later))
        assert os.path.isdir(gdf._put_datadir)
        assert not os.path.exists(os.path.join(gdf._put_datadir, gdf._obj))

    def test_delete_same_timestamp(self):
        the_path = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_file = os.path.join(the_path, "z")
        os.makedirs(the_path)
        with open(the_file, "wb") as fd:
            fd.write(b"1234")
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        assert gdf._obj == "z"
        assert gdf._data_file == the_file
        assert not gdf._is_dir
        now = float(gdf.read_metadata()["X-Timestamp"])
        gdf.delete(normalize_timestamp(now))
        assert os.path.isdir(gdf._put_datadir)
        assert os.path.exists(os.path.join(gdf._put_datadir, gdf._obj))

    def test_delete_file_not_found(self):
        the_path = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_file = os.path.join(the_path, "z")
        os.makedirs(the_path)
        with open(the_file, "wb") as fd:
            fd.write(b"1234")
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        assert gdf._obj == "z"
        assert gdf._data_file == the_file
        assert not gdf._is_dir
        later = float(gdf.read_metadata()["X-Timestamp"]) + 1

        # Handle the case the file is not in the directory listing.
        os.unlink(the_file)

        gdf.delete(normalize_timestamp(later))
        assert os.path.isdir(gdf._put_datadir)
        assert not os.path.exists(os.path.join(gdf._put_datadir, gdf._obj))

    def test_delete_file_unlink_error(self):
        the_path = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_file = os.path.join(the_path, "z")
        os.makedirs(the_path)
        with open(the_file, "wb") as fd:
            fd.write(b"1234")
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        assert gdf._obj == "z"
        assert gdf._data_file == the_file
        assert not gdf._is_dir

        later = float(gdf.read_metadata()["X-Timestamp"]) + 1

        def _mock_os_unlink_eacces_err(f):
            raise OSError(errno.EACCES, os.strerror(errno.EACCES))

        stats = os.stat(the_path)
        try:
            os.chmod(the_path, stats.st_mode & (~stat.S_IWUSR))

            # Handle the case os_unlink() raises an OSError
            with patch("os.unlink", _mock_os_unlink_eacces_err):
                try:
                    gdf.delete(normalize_timestamp(later))
                except OSError as e:
                    assert e.errno == errno.EACCES
                else:
                    self.fail("Excepted an OSError when unlinking file")
        finally:
            os.chmod(the_path, stats.st_mode)

        assert os.path.isdir(gdf._put_datadir)
        assert os.path.exists(os.path.join(gdf._put_datadir, gdf._obj))

    def test_delete_is_dir(self):
        the_path = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_dir = os.path.join(the_path, "d")
        os.makedirs(the_dir)
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "d")
        assert gdf._data_file == the_dir
        later = float(gdf.read_metadata()["X-Timestamp"]) + 1
        gdf.delete(normalize_timestamp(later))
        assert os.path.isdir(gdf._put_datadir)
        assert not os.path.exists(os.path.join(gdf._put_datadir, gdf._obj))

    def test_create(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "dir/z")
        saved_tmppath = ""
        saved_fd = None
        with gdf.create() as dw:
            assert gdf._put_datadir == os.path.join(
                self.td, "vol0", "ufo47", "bar", "dir"
            )
            assert os.path.isdir(gdf._put_datadir)
            saved_tmppath = dw._tmppath
            assert os.path.dirname(saved_tmppath) == gdf._put_datadir
            assert os.path.basename(saved_tmppath)[:3] == ".z."
            assert os.path.exists(saved_tmppath)
            dw.write(b"123")
            saved_fd = dw._fd
        # At the end of previous with block a close on fd is called.
        # Calling os.close on the same fd will raise an OSError
        # exception and we must catch it.
        try:
            os.close(saved_fd)
        except OSError:
            pass
        else:
            self.fail("Exception expected")
        assert not os.path.exists(saved_tmppath)

    def test_create_err_on_close(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "dir/z")
        saved_tmppath = ""
        with gdf.create() as dw:
            assert gdf._put_datadir == os.path.join(
                self.td, "vol0", "ufo47", "bar", "dir"
            )
            assert os.path.isdir(gdf._put_datadir)
            saved_tmppath = dw._tmppath
            assert os.path.dirname(saved_tmppath) == gdf._put_datadir
            assert os.path.basename(saved_tmppath)[:3] == ".z."
            assert os.path.exists(saved_tmppath)
            dw.write(b"123")
            # Closing the fd prematurely should not raise any exceptions.
            dw.close()
        assert not os.path.exists(saved_tmppath)

    def test_create_err_on_unlink(self):
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "dir/z")
        saved_tmppath = ""
        with gdf.create() as dw:
            assert gdf._put_datadir == os.path.join(
                self.td, "vol0", "ufo47", "bar", "dir"
            )
            assert os.path.isdir(gdf._put_datadir)
            saved_tmppath = dw._tmppath
            assert os.path.dirname(saved_tmppath) == gdf._put_datadir
            assert os.path.basename(saved_tmppath)[:3] == ".z."
            assert os.path.exists(saved_tmppath)
            dw.write(b"123")
            os.unlink(saved_tmppath)
        assert not os.path.exists(saved_tmppath)

    def test_unlink_not_called_after_rename(self):
        the_obj_path = os.path.join("b", "a")
        the_file = os.path.join(the_obj_path, "z")
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", the_file)

        body = b"1234\n"
        etag = md5(body).hexdigest()
        metadata = {
            "X-Timestamp": "1234",
            "Content-Type": "file",
            "ETag": etag,
            "Content-Length": "5",
        }

        _mock_do_unlink = Mock()  # Shouldn't be called
        with patch("swiftonfile.swift.obj.diskfile.do_unlink", _mock_do_unlink):
            with gdf.create() as dw:
                assert dw._tmppath is not None
                tmppath = dw._tmppath
                dw.write(body)
                dw.put(metadata)
                # do_unlink is not called if dw._tmppath is set to None
                assert dw._tmppath is None
        self.assertFalse(_mock_do_unlink.called)

        assert os.path.exists(gdf._data_file)  # Real file exists
        assert not os.path.exists(tmppath)  # Temp file does not exist

    def test_fd_closed_when_diskfile_open_raises_exception_race(self):
        # do_open() succeeds but read_metadata() fails(GlusterFS)
        _m_do_open = Mock(return_value=999)
        _m_do_fstat = Mock(
            return_value=os.stat_result(
                (
                    33261,
                    2753735,
                    2053,
                    1,
                    1000,
                    1000,
                    6873,
                    1431415969,
                    1376895818,
                    1433139196,
                )
            )
        )
        _m_rmd = Mock(side_effect=IOError(errno.ENOENT, os.strerror(errno.ENOENT)))
        _m_do_close = Mock()
        _m_log = Mock()

        with nested(
            patch("swiftonfile.swift.obj.diskfile.do_open", _m_do_open),
            patch("swiftonfile.swift.obj.diskfile.do_fstat", _m_do_fstat),
            patch("swiftonfile.swift.obj.diskfile.read_metadata", _m_rmd),
            patch("swiftonfile.swift.obj.diskfile.do_close", _m_do_close),
            patch("swiftonfile.swift.obj.diskfile.logging.warn", _m_log),
        ):
            gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
            try:
                with gdf.open():
                    pass
            except DiskFileNotExist:
                pass
            else:
                self.fail("Expecting DiskFileNotExist")
            _m_do_fstat.assert_called_once_with(999)
            _m_rmd.assert_called_once_with(999)
            _m_do_close.assert_called_once_with(999)
            self.assertFalse(gdf._fd)
            # Make sure ENOENT failure is logged
            self.assertTrue("failed with ENOENT" in _m_log.call_args[0][0])

    def test_fd_closed_when_diskfile_open_raises_DiskFileExpired(self):
        # A GET/DELETE on an expired object should close fd
        the_path = os.path.join(self.td, "vol0", "ufo47", "bar")
        the_file = os.path.join(the_path, "z")
        os.makedirs(the_path)
        with open(the_file, "w") as fd:
            fd.write("1234")
        md = {
            "X-Type": "Object",
            "X-Object-Type": "file",
            "Content-Length": str(os.path.getsize(the_file)),
            "ETag": md5(b"1234").hexdigest(),
            "X-Timestamp": os.stat(the_file).st_mtime,
            "X-Delete-At": 0,  # This is in the past
            "Content-Type": "application/octet-stream",
        }
        _metadata[_mapit(the_file)] = md

        _m_do_close = Mock()

        with patch("swiftonfile.swift.obj.diskfile.do_close", _m_do_close):
            gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
            try:
                with gdf.open():
                    pass
            except DiskFileExpired:
                # Confirm that original exception is re-raised
                pass
            else:
                self.fail("Expecting DiskFileExpired")
            self.assertEqual(_m_do_close.call_count, 1)
            self.assertFalse(gdf._fd)
            # Close the actual fd, as we had mocked do_close
            os.close(_m_do_close.call_args[0][0])

    def test_make_directory(self):
        with mock.patch.object(diskfile, "do_mkdir"):
            with mock.patch.object(diskfile, "do_chown") as mock_chown:
                b, m = diskfile.make_directory("path", 34, 78)
                assert b and m is None
                mock_chown.assert_called_once_with("path", 34, 78)

    def test_make_directory_enotdir(self):
        def mock_mkdir(path):
            e = OSError()
            e.errno = errno.ENOTDIR
            raise e

        with mock.patch.object(diskfile, "do_mkdir", mock_mkdir):
            try:
                diskfile.make_directory("path", 1, 1)
            except AlreadyExistsAsFile:
                pass
            else:
                self.fail("AlreadyExistsAsFile waited")

    def test_make_directory_eacces(self):
        def mock_mkdir(path):
            e = OSError()
            e.errno = errno.EACCES
            raise e

        with mock.patch.object(diskfile, "do_mkdir", mock_mkdir):
            try:
                diskfile.make_directory("path", 1, 1)
            except diskfile.DiskFileError:
                pass
            else:
                self.fail("DiskFileError waited")

    def test_make_directory_eexist(self):
        def mock_mkdir(path):
            e = OSError()
            e.errno = errno.EEXIST
            raise e

        with mock.patch.object(diskfile, "do_mkdir", mock_mkdir):
            # raise ENOENT
            def mock_stat(path):
                e = SwiftOnFileSystemOSError()
                e.errno = errno.ENOENT
                raise e

            with mock.patch.object(diskfile, "do_stat", mock_stat):
                try:
                    diskfile.make_directory("path", 1, 1)
                except diskfile.DiskFileError:
                    pass
                else:
                    self.fail("DiskFileError waited")

            # stat not null
            with mock.patch.object(
                diskfile, "do_stat", return_value=mock.MagicMock(**{"st_mode": 1})
            ):
                # Return False
                with mock.patch.object(diskfile.stat, "S_ISDIR", return_value=False):
                    try:
                        diskfile.make_directory("path", 1, 1)
                    except AlreadyExistsAsFile:
                        pass
                    else:
                        self.fail("AlreadyExistsAsFile waited")

                # Return True
                with mock.patch.object(diskfile.stat, "S_ISDIR", return_value=True):
                    expected_metadata = 10
                    b, metadata = diskfile.make_directory(
                        "path", 1, 1, expected_metadata
                    )
                    assert b and expected_metadata == metadata

    def test_make_directory_eio(self):
        def mock_mkdir(path):
            e = OSError()
            e.errno = errno.EIO
            raise e

        with mock.patch.object(diskfile, "do_mkdir", mock_mkdir):
            # raise ENOENT
            def mock_stat(path):
                e = SwiftOnFileSystemOSError()
                e.errno = errno.ENOENT
                raise e

            with mock.patch.object(diskfile, "do_stat", mock_stat):
                try:
                    diskfile.make_directory("path", 1, 1)
                except diskfile.DiskFileError:
                    pass
                else:
                    self.fail("DiskFileError waited")

            # raise EIO
            def mock_stat(path):
                e = SwiftOnFileSystemOSError()
                e.errno = errno.EIO
                raise e

            with mock.patch.object(diskfile, "do_stat", mock_stat):
                try:
                    diskfile.make_directory("path", 1, 1)
                except diskfile.DiskFileError:
                    pass
                else:
                    self.fail("DiskFileError waited")

            # return None
            with mock.patch.object(diskfile, "do_stat", return_value=None):
                try:
                    diskfile.make_directory("path", 1, 1)
                except diskfile.DiskFileError:
                    pass
                else:
                    self.fail("DiskFileError waited")

            # stat not null
            with mock.patch.object(
                diskfile, "do_stat", return_value=mock.MagicMock(**{"st_mode": 1})
            ):
                # Return False
                with mock.patch.object(diskfile.stat, "S_ISDIR", return_value=False):
                    try:
                        diskfile.make_directory("path", 1, 1)
                    except diskfile.DiskFileError:
                        pass
                    else:
                        self.fail("DiskFileError waited")

                # Return True
                with mock.patch.object(diskfile.stat, "S_ISDIR", return_value=True):
                    expected_metadata = 10
                    b, metadata = diskfile.make_directory(
                        "path", 1, 1, expected_metadata
                    )
                    assert b and expected_metadata == metadata

    def test__adjust_metadata_isreg(self):
        mtime = 12
        with mock.patch.object(
            diskfile,
            "do_fstat",
            return_value=mock.MagicMock(**{"st_mode": 1, "st_mtime": mtime}),
        ):
            with mock.patch.object(diskfile.stat, "S_ISREG", return_value=False):
                with mock.patch.object(
                    diskfile, "normalize_timestamp"
                ) as mock_timestamp:
                    diskfile._adjust_metadata("path", {})
                    mock_timestamp.assert_not_called()

    def make_directory_chown_call(self):
        path = os.path.join(self.td, "a/b/c")
        _m_do_chown = Mock()
        with patch("swiftonfile.swift.obj.diskfile.do_chown", _m_do_chown):
            diskfile.make_directory(path, -1, -1)
        self.assertFalse(_m_do_chown.called)
        self.assertTrue(os.path.isdir(path))

        path = os.path.join(self.td, "d/e/f")
        _m_do_chown.reset_mock()
        with patch("swiftonfile.swift.obj.diskfile.do_chown", _m_do_chown):
            diskfile.make_directory(path, -1, 99)
        self.assertEqual(_m_do_chown.call_count, 3)
        self.assertTrue(os.path.isdir(path))

        path = os.path.join(self.td, "g/h/i")
        _m_do_chown.reset_mock()
        with patch("swiftonfile.swift.obj.diskfile.do_chown", _m_do_chown):
            diskfile.make_directory(path, 99, -1)
        self.assertEqual(_m_do_chown.call_count, 3)
        self.assertTrue(os.path.isdir(path))

    def test_fchown_not_called_on_default_uid_gid_values(self):
        the_cont = os.path.join(self.td, "vol0", "ufo47", "bar")
        os.makedirs(the_cont)
        body = b"1234"
        metadata = {
            "X-Timestamp": "1234",
            "Content-Type": "file",
            "ETag": md5(body).hexdigest(),
            "Content-Length": len(body),
        }

        _m_do_fchown = Mock()
        gdf = self._get_diskfile("vol0", "p57", "ufo47", "bar", "z")
        with gdf.create() as dw:
            assert dw._tmppath is not None
            tmppath = dw._tmppath
            dw.write(body)
            with patch("swiftonfile.swift.obj.diskfile.do_fchown", _m_do_fchown):
                dw.put(metadata)
        self.assertFalse(_m_do_fchown.called)
        assert os.path.exists(gdf._data_file)
        assert not os.path.exists(tmppath)


class TestDiskFileBehavior(unittest.TestCase):
    """Tests for swiftonfile.swift.obj.diskfile.BaseMappingDiskFileBehavior"""

    @mock.patch("swiftonfile.swift.obj.diskfile.get_user_uid_gid")
    def test_user_get_uid_gid(self, mock_get_user_uid_gid):
        mock_get_user_uid_gid.return_value = (1000, 1000)
        mapping = UserMappingDiskFileBehavior({})
        uid, gid = mapping.get_uid_gid("AUTH_test_user")
        self.assertEqual(uid, 1000)
        self.assertEqual(gid, 1000)
        mock_get_user_uid_gid.assert_called_with("test_user")

    @mock.patch("swiftonfile.swift.obj.diskfile.get_user_uid_gid")
    def test_user_get_uid_gid_raise(self, mock_get_user_uid_gid):
        mock_get_user_uid_gid.side_effect = KeyError
        mapping = UserMappingDiskFileBehavior({})
        try:
            mapping.get_uid_gid("AUTH_test_user")
        except InvalidAccountInfo:
            pass
        else:
            self.fail("InvalidAccountInfo must be raised")

    def test_user_get_uid_gid_ignore_account(self):
        conf = {"match_fs_ignore_accounts": "test_user"}
        mapping = UserMappingDiskFileBehavior(conf)
        uid, gid = mapping.get_uid_gid("AUTH_test_user")
        self.assertEqual(uid, DEFAULT_UID)
        self.assertEqual(gid, DEFAULT_GID)

    @mock.patch("swiftonfile.swift.obj.diskfile.get_group_gid")
    @mock.patch("swiftonfile.swift.obj.diskfile.get_user_uid_gid")
    def test_group_uid_gid(self, mock_get_user_uid_gid, mock_get_group_gid):
        mock_get_group_gid.return_value = 1000
        mock_get_user_uid_gid.return_value = (1000, 1000)
        mapping = GroupMappingDiskFileBehavior({})
        uid, gid = mapping.get_uid_gid("AUTH_test_user")
        self.assertEqual(uid, 1000)
        self.assertEqual(gid, 1000)
        mock_get_user_uid_gid.assert_called_with("test_user")
        mock_get_group_gid.assert_called_with("test_user")

    @mock.patch("swiftonfile.swift.obj.diskfile.get_user_uid_gid")
    def test_group_init_error(self, mock_get_user_uid_gid):
        mock_get_user_uid_gid.side_effect = KeyError
        try:
            GroupMappingDiskFileBehavior({"match_fs_default_user": "unknown"})
        except InvalidAccountInfo:
            pass
        else:
            self.fail("InvalidAccountInfo should have been raised")

    @mock.patch("swiftonfile.swift.obj.diskfile.get_group_gid")
    @mock.patch("swiftonfile.swift.obj.diskfile.get_user_uid_gid")
    def test_group_uid_gid_error(self, mock_get_user_uid_gid, mock_get_group_gid):
        mock_get_group_gid.side_effect = KeyError
        mock_get_user_uid_gid.return_value = (1000, 1000)
        mapping = GroupMappingDiskFileBehavior({})
        try:
            uid, gid = mapping.get_uid_gid("AUTH_test_user")
        except InvalidAccountInfo:
            pass
        else:
            self.fail("InvalidAccountInfo should have been raised")

    @mock.patch("swiftonfile.swift.obj.diskfile.get_group_gid")
    @mock.patch("swiftonfile.swift.obj.diskfile.get_user_uid_gid")
    def test_group_uid_gid_default(self, mock_get_user_uid_gid, mock_get_group_gid):
        default_uid = 1200
        mock_get_group_gid.return_value = 1000
        mock_get_user_uid_gid.return_value = (default_uid, 1000)
        mapping = GroupMappingDiskFileBehavior({})
        mock_get_user_uid_gid.side_effect = KeyError
        uid, gid = mapping.get_uid_gid("AUTH_test_user")
        self.assertEqual(uid, default_uid)
        self.assertEqual(gid, 1000)
