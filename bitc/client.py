from functools import lru_cache
import logging

import grpc

from bitc import bitc_pb2, bitc_pb2_grpc


LOG = logging.getLogger(__name__)


class RPCFailedError(Exception):
    pass


class ConnectionFailedException(Exception):
    pass


class RPCClient(object):
    """
    Top level object to access RPyc API
    """

    def __init__(
        self,
        host="127.0.0.1",
        port=12345,
    ):
        @lru_cache(maxsize=1)
        def c():
            try:
                config = (
                    ("grpc.keepalive_time_ms", 1000),
                    ("grpc.keepalive_timeout_ms", 300),
                )
                channel = grpc.insecure_channel(
                    "{}:{}".format(host, port), options=config
                )
                return channel
            except grpc.RpcError as rpc_error:
                raise ConnectionFailedException(
                    "Connection failed to {}:{} due to: code={}, message={}".format(
                        host, port, rpc_error.code(), rpc_error.details()
                    )
                )

        self._conn = c

    def __del__(self):
        if self._conn.cache_info().hits > 0:
            self._conn().close()


class BitCdbRpcClient(RPCClient):
    def get(self, key):
        try:
            request = bitc_pb2.GetRequest(key=key)
            stub = bitc_pb2_grpc.BitCdbKeyValueServiceStub(self._conn())
            response = stub.get(request)
            return response
        except grpc.RpcError as rpc_error:
            raise RPCFailedError(
                "RPC Call 'Get' failed due to: code={}, message={}".format(
                    rpc_error.code(), rpc_error.details()
                )
            )
        except Exception as ex:
            raise RPCFailedError("RPC Call 'get' failed due to {}".format(ex))

    def put(self, key, value):
        try:
            request = bitc_pb2.PutRequest(
                key=key,
                value=value,
            )
            stub = bitc_pb2_grpc.BitCdbKeyValueServiceStub(self._conn())
            response = stub.put(request)
            return response
        except grpc.RpcError as rpc_error:
            raise RPCFailedError(
                "RPC Call 'Put' failed due to: code={}, message={}".format(
                    rpc_error.code(), rpc_error.details()
                )
            )
        except Exception as ex:
            raise RPCFailedError("RPC Call 'Put' failed due to {}".format(ex))

    def delete(self, key):
        try:
            request = bitc_pb2.DeleteRequest(key=key)
            stub = bitc_pb2_grpc.BitCdbKeyValueServiceStub(self._conn())
            response = stub.get(request)
            return response
        except grpc.RpcError as rpc_error:
            raise RPCFailedError(
                "RPC Call 'Delete' failed due to: code={}, message={}".format(
                    rpc_error.code(), rpc_error.details()
                )
            )
        except Exception as ex:
            raise RPCFailedError("RPC Call 'Delete' failed due to {}".format(ex))


if __name__ == "__main__":
    client = BitCdbRpcClient()
    print(client.get("test").value)
    client.put("test", "16")
    print(client.get("test").value)
    client.put("test", "19")
    print(client.get("test").value)
