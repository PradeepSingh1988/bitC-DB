from threading import Lock

from bitc import utils
from bitc.cask_file import CaskDataFile, CaskHintFile
from bitc.storage import CaskKeyDirEntry


class KeyDir(object):
    def __init__(self, file_path):
        self._index = {}
        self._lock = Lock()
        self._file_path = file_path

    def add(self, key, value):
        with self._lock:
            self._index[key] = value

    def delete(self, key):
        with self._lock:
            del self._index[key]

    def get(self, key):
        with self._lock:
            return self._index.get(key)

    def rebuild(self):
        data_files = utils.get_datafiles(self._file_path)
        hint_files = utils.get_hintfiles(self._file_path)
        if not data_files:
            return []
        for index, data_file in enumerate(data_files):
            data_file_id = utils.get_file_id_from_absolute_path(data_file)
            data_file_obj = CaskDataFile(self._file_path, data_file_id, True)
            hint_file_path = utils.get_hint_filename_for_data_file(data_file)
            if hint_file_path in hint_files:
                hint_file = CaskHintFile(self._file_path, data_file_id, True)
                for (
                    key,
                    entry_size,
                    entry_offset,
                    timestamp,
                ) in hint_file.read_all_entries():
                    self._index[key] = CaskKeyDirEntry(
                        data_file_obj, entry_size, entry_offset, timestamp
                    )
            elif index == len(data_files) - 1:
                for (
                    key,
                    entry_size,
                    entry_offset,
                    timestamp,
                ) in data_file_obj.read_all_entries():
                    self._index[key] = CaskKeyDirEntry(
                        data_file_obj, entry_size, entry_offset, timestamp
                    )
            else:
                raise utils.CaskIOException(
                    "Hint file not found, although file is not last data file"
                )
