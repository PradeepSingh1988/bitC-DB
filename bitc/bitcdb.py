import logging
from threading import Thread, Timer

from bitc import bitc_pb2, bitc_pb2_grpc, consts
from bitc.keydir import KeyDir
from bitc.logger import CustomAdapter
from bitc.storage import CaskStorage, CustomAdapter


class BitCdb(bitc_pb2_grpc.BitCdbKeyValueServiceServicer):
    def __init__(self, file_path=".", merge_interval=3600 * 12):
        self.logger = CustomAdapter(
            logging.getLogger(__name__),
            {"logger": "{}".format("CASK")},
        )
        self._key_dir = KeyDir()
        self._file_path = file_path
        self._persistor = CaskStorage(file_path)
        self._merge_interval_seconds = merge_interval
        # This can be moved out of init to boost up start process
        self._build_key_dir()
        self._schedule_merge_timer()

    def _schedule_merge_timer(self):
        self._timer = Timer(self._merge_interval_seconds, self._merge)
        self._timer.daemon = True
        self._timer.start()

    def _build_key_dir(self):
        for key, entry in self._persistor.rebuild_index():
            self._key_dir.add(key, entry)

    def _merge(self):
        try:
            status, merged_index, new_data_file_obj = self._persistor.merge()
            if not status:
                self.logger.debug("Merge process is still running")
            self._key_dir.merge_index(merged_index, new_data_file_obj)
        finally:
            self._schedule_merge_timer()

    def put(self, request, context):
        self.logger.debug(
            "Got put request with k={}, v={}".format(request.key, request.value)
        )
        entry = self._persistor.store(request.key, request.value)
        self._key_dir.add(request.key, entry)
        return bitc_pb2.PutReply()

    def get(self, request, context):
        self.logger.debug("Got get request with k={}".format(request.key))
        if self._key_dir.get(request.key) is None:
            return bitc_pb2.GetReply(value="")
        else:
            entry = self._key_dir.get(request.key)
            value = self._persistor.retrieve(
                entry.file_name, entry.value_pos, entry.value_size
            )
            return bitc_pb2.GetReply(
                value="" if value == consts.TOMBSTONE_ENTRY else value
            )

    def delete(self, request, context):
        if self._key_dir.get(request.key) is None:
            return bitc_pb2.DeleteReply(result=False)
        else:
            self._persistor.store(request.key, consts.TOMBSTONE_ENTRY)
            self._key_dir.delete(request.key)
            return bitc_pb2.DeleteReply(result=True)
