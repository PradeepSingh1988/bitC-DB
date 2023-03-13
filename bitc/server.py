import argparse
import logging
import os
from concurrent import futures

import grpc

from bitc.logger import setup_logger
from bitc import bitc_pb2_grpc
from bitc.bitcdb import BitCdb

setup_logger()
LOG = logging.getLogger(__name__)


def serve(port, db_dir, merge_interval, cask_file_size):
    kv_svc = BitCdb(db_dir, cask_file_size, merge_interval)
    port = str(port)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    bitc_pb2_grpc.add_BitCdbKeyValueServiceServicer_to_server(kv_svc, server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


def main():
    parser = argparse.ArgumentParser(
        prog="BitCdbKeyValueStoreService",
        description="bitCDB Key Value Store service based on bitcask",
    )

    parser.add_argument("--db-dir", required=True, help="database file directory")
    parser.add_argument("--port", default=12345, help="Port")
    parser.add_argument(
        "--merge-interval",
        required=False,
        default=3600 * 12,
        help="File merge interval",
    )
    parser.add_argument(
        "--max-cask-file-size",
        required=False,
        default=100 * 1000 * 1000,
        help="Max cask file size in bytes",
    )
    args = parser.parse_args()
    cask_file_size = int(args.max_cask_file_size)
    merge_interval = int(args.merge_interval)
    port = int(args.port)
    serve(port, args.db_dir, merge_interval, cask_file_size)


if __name__ == "__main__":
    main()
