import glob
import logging
import os
import tempfile
import time
from threading import RLock

from bitc.consts import DATA_HEADER_SIZE
from bitc.logger import CustomAdapter
from bitc.cask_file import (
    CaskDataFile,
    CaskHintFile,
)
from bitc import utils


DATAFILE_START_INDEX = 0


class CaskKeyDirEntry(object):
    def __init__(self, file_obj, value_size, value_pos, tstamp):
        self.value_size = value_size
        self.value_pos = value_pos
        self.tstamp = tstamp
        self.file_obj = file_obj

    def __repr__(self):
        return "size={},position={},timestamp={}".format(
            self.value_size, self.value_pos, self.tstamp
        )


class CaskFilePersistor(object):
    def __init__(self, cask_id, file_path, os_sync=True, max_file_size=10000):
        self._file_path = file_path
        self._data_file = None
        self._hint_file = None
        self._next_id = self._get_next_id()
        self._os_sync = os_sync
        self._max_file_size = max_file_size
        self._read_files = {}
        self._lock = RLock()
        self._merge_running = False
        self.logger = CustomAdapter(
            logging.getLogger(__name__),
            {"logger": "{}:{}".format("CASKSTORAGE", cask_id)},
        )

    def _get_next_id(self):
        data_files = utils.get_datafiles(self._file_path)
        if not data_files:
            return DATAFILE_START_INDEX
        else:
            return utils.get_file_id_from_absolute_path(data_files[-1]) + 1

    def _close_current_write_files(self):
        if self._data_file is not None:
            self._data_file.close()
            self._data_file = None
        if self._hint_file is not None:
            self._hint_file.close()
            self._hint_file = None

    def _create_new_data_file(self, file_id):
        return CaskDataFile(self._file_path, file_id, False, os_sync=self._os_sync)

    def _create_new_hint_file(self, file_id):
        return CaskHintFile(self._file_path, file_id, False, os_sync=self._os_sync)

    def _create_new_files(self):
        self._data_file = self._create_new_data_file(self._next_id)
        self._hint_file = self._create_new_hint_file(self._next_id)
        self._read_files[self._data_file.basename] = self._data_file

    def _rotate_files(self):
        last_write_file = self._data_file
        self._next_id += 1
        self._close_current_write_files()
        self._data_file = self._create_new_data_file(self._next_id)
        self._hint_file = self._create_new_hint_file(self._next_id)
        self._read_files[last_write_file.basename] = CaskDataFile(
            self._file_path, last_write_file.file_id, True
        )
        self._read_files[self._data_file.basename] = self._data_file

    def _check_write(self, data_len):
        with self._lock:
            if self._data_file is None:
                self._create_new_files()
            elif (
                self._data_file.size + DATA_HEADER_SIZE + data_len > self._max_file_size
            ):
                self._rotate_files()

    def merge(self, keydir):

        try:
            with self._lock:
                if self._merge_running:
                    return False
                else:
                    self._merge_running = True
                files_to_merge = utils.get_datafiles(self._file_path)
                if not files_to_merge:
                    return
                last_id = utils.get_file_id_from_absolute_path(files_to_merge[-1])
                merge_id = last_id + 1
                self._next_id = merge_id
                self._rotate_files()
                with tempfile.TemporaryDirectory(dir=self._file_path) as tempdir:
                    pass
        finally:
            self._merge_running = False

    def store(self, key, value):
        self._check_write(len(key) + len(value))
        current_offset = self._data_file.size
        timestamp = round(time.time())
        self._data_file.write(timestamp, key, value)
        entry_size = DATA_HEADER_SIZE + len(key) + len(value)
        self._hint_file.write(key, timestamp, current_offset, entry_size)
        return CaskKeyDirEntry(
            self._data_file.basename, entry_size, current_offset, timestamp
        )

    def retrieve(self, file_name, entry_offset, entry_size):
        data_file = self._read_files[file_name]
        self.logger.debug(
            "Reading size {} from offset {} from file {}".format(
                entry_size, entry_offset, data_file
            )
        )
        return data_file.read(entry_offset, entry_size)
