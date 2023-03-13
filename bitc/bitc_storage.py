import glob
import logging
import os
import tempfile
import time
from threading import RLock

from bitc.consts import DATAFILE_START_INDEX, DATA_HEADER_SIZE, TOMBSTONE_ENTRY
from bitc.logger import CustomAdapter
from bitc.cask_file import (
    CaskDataFile,
    CaskHintFile,
)
from bitc import utils


class CaskKeyDirEntry(object):
    def __init__(self, file_obj, value_size, value_pos, tstamp):
        self.value_size = value_size
        self.value_pos = value_pos
        self.tstamp = tstamp
        self.file_obj = file_obj

    def __repr__(self):
        return "size={},position={},timestamp={}, filename={}".format(
            self.value_size, self.value_pos, self.tstamp, self.file_name
        )


class CaskStorage(object):
    def __init__(self, file_path, key_dir, os_sync=True, max_file_size=100):
        self._file_path = file_path
        self._key_dir = key_dir
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
            {"logger": "{}".format("CASKSTORAGE")},
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
        if self._data_file is None:
            self._create_new_files()
        elif self._data_file.size + DATA_HEADER_SIZE + data_len > self._max_file_size:
            self._rotate_files()

    def store(self, key, value):
        with self._lock:
            self._check_write(len(key) + len(value))
            current_offset = self._data_file.size
            timestamp = round(time.time())
            self._data_file.write(timestamp, key, value)
            entry_size = DATA_HEADER_SIZE + len(key) + len(value)
            self._hint_file.write(key, timestamp, current_offset, entry_size)
            self._key_dir.add(
                key,
                CaskKeyDirEntry(self._data_file, entry_size, current_offset, timestamp),
            )

    def retrieve(self, key):
        with self._lock:
            entry = self._key_dir.get(key)
            if entry is not None:
                data_file = self._read_files[entry.file_obj.basename]
                self.logger.debug(
                    "Reading size {} from offset {} from file {}".format(
                        entry.value_size, entry.value_pos, data_file
                    )
                )
                return data_file.read(entry.value_pos, entry.value_size)
            else:
                return None

    def delete(self, key):
        with self._lock:
            entry = self._key_dir.get(key)
            if entry is not None:
                self.store(key, TOMBSTONE_ENTRY)
                self._key_dir.delete(key)
                return True
            else:
                return False

    def merge(self):
        try:
            with self._lock:
                if self._merge_running:
                    return
                else:
                    self._merge_running = True
                files_to_merge = utils.get_datafiles(self._file_path)
                if not files_to_merge or len(files_to_merge) < 3:
                    return
                # Leave last file where current writes are landing
                files_to_merge = files_to_merge[:-1]
            key_val_map = {}
            merged_index = {}
            last_id = utils.get_file_id_from_absolute_path(files_to_merge[-1])
            for datafile in reversed(files_to_merge):
                datafile_obj = self._read_files[os.path.basename(datafile)]
                for (
                    key,
                    _,
                    _,
                    timestamp,
                    value,
                ) in datafile_obj.read_all_entries():
                    if (
                        key not in key_val_map
                        or key_val_map[key][1] == datafile_obj.basename
                    ):
                        key_val_map[key] = (value, datafile_obj.basename, timestamp)
            with tempfile.TemporaryDirectory(dir=self._file_path) as tempdir:
                new_data_file = CaskDataFile(
                    tempdir, last_id, False, os_sync=self._os_sync
                )
                new_hint_file = CaskHintFile(tempdir, last_id, False)
                for key, metadata in key_val_map.items():
                    current_offset = new_data_file.size
                    new_data_file.write(metadata[2], key, metadata[0])
                    entry_size = DATA_HEADER_SIZE + len(key) + len(metadata[0])
                    new_hint_file.write(key, metadata[2], current_offset, entry_size)
                    merged_index[key] = (
                        entry_size,
                        current_offset,
                        timestamp,
                    )
                with self._lock:
                    new_data_file.close()
                    new_hint_file.close()
                    os.rename(
                        new_data_file.name,
                        os.path.join(self._file_path, new_data_file.basename),
                    )
                    os.rename(
                        new_hint_file.name,
                        os.path.join(self._file_path, new_hint_file.basename),
                    )
                    # Delete except last file, which has been created just now
                    files_to_delete = files_to_merge[:-1]
                    for file_path in files_to_delete:
                        os.remove(file_path)
                        os.remove(utils.get_hint_filename_for_data_file(file_path))
                        del self._read_files[os.path.basename(file_path)]
                    new_data_file = CaskDataFile(
                        self._file_path, last_id, True, os_sync=self._os_sync
                    )
                    self._read_files[new_data_file.basename] = new_data_file
                    self._key_dir.merge_index(merged_index, new_data_file)
        finally:
            self._merge_running = False

    def rebuild_index(self):
        data_files = utils.get_datafiles(self._file_path)
        hint_files = utils.get_hintfiles(self._file_path)
        if not data_files:
            return []
        for index, data_file in enumerate(data_files):
            data_file_id = utils.get_file_id_from_absolute_path(data_file)
            data_file_obj = CaskDataFile(self._file_path, data_file_id, True)
            hint_file_path = utils.get_hint_filename_for_data_file(data_file)
            self._read_files[data_file_obj.basename] = data_file_obj
            if hint_file_path in hint_files:
                hint_file = CaskHintFile(self._file_path, data_file_id, True)
                for (
                    key,
                    entry_size,
                    entry_offset,
                    timestamp,
                ) in hint_file.read_all_entries():
                    self._key_dir.add(
                        key,
                        CaskKeyDirEntry(
                            data_file_obj, entry_size, entry_offset, timestamp
                        ),
                    )
            else:
                for (
                    key,
                    entry_size,
                    entry_offset,
                    timestamp,
                ) in data_file_obj.read_all_entries():
                    self._key_dir.add(
                        key,
                        CaskKeyDirEntry(
                            data_file_obj, entry_size, entry_offset, timestamp
                        ),
                    )
