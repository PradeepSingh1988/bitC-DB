import logging

from bitc import consts
from bitc.keydir import KeyDir
from bitc.logger import CustomAdapter
from bitc.storage import CaskFilePersistor, CustomAdapter


class BitCdb:
    def __init__(
        self,
        file_path=".",
    ):
        self.logger = CustomAdapter(
            logging.getLogger(__name__),
            {"logger": "{}".format("CASK")},
        )
        self._file_path = file_path
        self._key_dir = KeyDir(file_path)
        self._persistor = CaskFilePersistor(file_path)
        # This can be moved out of init to boost up start process
        self._build_key_dir()

    def _build_key_dir(self):
        self._key_dir.rebuild()

    def put(self, key, value):
        self.logger.debug("Got put request with k={}, v={}".format(key, value))
        entry = self._persistor.store(key, value)
        self.logger.debug("Entry is {}".format(entry))
        self._key_dir.add(key, entry)

    def get(self, key):
        self.logger.debug("Got get request with k={}".format(key))
        if self._key_dir.get(key) is None:
            return ""
        else:
            entry = self._key_dir.get(key)
            value = self._persistor.retrieve(
                entry.file_obj, entry.value_pos, entry.value_size
            )
            return "" if value == consts.TOMBSTONE_ENTRY else value

    def delete(self, key):
        if self._key_dir.get(key) is None:
            return False
        else:
            self._persistor.store(key, consts.TOMBSTONE_ENTRY)
            del self._key_dir.delete(key)
