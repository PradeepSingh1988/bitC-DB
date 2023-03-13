from threading import Lock

from bitc.bitc_storage import CaskKeyDirEntry


class KeyDir(object):
    def __init__(self):
        self._index = {}

    def add(self, key, value):
        self._index[key] = value

    def delete(self, key):
        del self._index[key]

    def get(self, key):
        return self._index.get(key)

    def merge_index(self, new_index, data_file):
        for key, metadata in new_index.items():
            entry = self._index.get(key)
            if (
                entry is not None
                and entry.tstamp == metadata[2]
                and entry.file_name != data_file
            ):
                self._index[key] = CaskKeyDirEntry(
                    data_file, metadata[0], metadata[1], metadata[2]
                )
