import logging
from threading import Thread, Timer

from bitc import bitc_pb2, bitc_pb2_grpc, consts
from bitc.keydir import KeyDir
from bitc.logger import CustomAdapter
from bitc.bitc_storage import CaskStorage, CustomAdapter


class BitCdb(bitc_pb2_grpc.BitCdbKeyValueServiceServicer):
    def __init__(self, file_path, cask_file_size, merge_interval=3600 * 12):
        self.logger = CustomAdapter(
            logging.getLogger(__name__),
            {"logger": "{}".format("CASK")},
        )

        self._file_path = file_path
        self._persistor = CaskStorage(
            file_path, KeyDir(), os_sync=True, max_file_size=cask_file_size
        )
        self._merge_interval_seconds = merge_interval
        # This can be moved out of init to boost up start process
        self._build_key_dir()
        self._schedule_merge_timer()

    def _schedule_merge_timer(self):
        self._timer = Timer(self._merge_interval_seconds, self._merge)
        self._timer.daemon = True
        self._timer.start()

    def _build_key_dir(self):
        self.logger.debug("Start Building Index")
        self._persistor.rebuild_index()
        self.logger.debug("End Building Index")

    def _merge(self):
        try:
            self.logger.debug("Start merge process")
            self._persistor.merge()
            self.logger.debug("End merge process")
        finally:
            self._schedule_merge_timer()

    def put(self, request, context):
        self.logger.debug(
            "Got put request with k={}, v={}".format(request.key, request.value)
        )
        self._persistor.store(request.key, request.value)
        return bitc_pb2.PutReply()

    def get(self, request, context):
        self.logger.debug("Got get request with k={}".format(request.key))
        value = self._persistor.retrieve(request.key)
        if value is None:
            return bitc_pb2.GetReply(value="")
        else:
            return bitc_pb2.GetReply(
                value="" if value == consts.TOMBSTONE_ENTRY else value
            )

    def delete(self, request, context):
        deleted = self._persistor.delete(request.key)
        return bitc_pb2.DeleteReply(result=deleted)
