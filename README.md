# bitC-DB

This repo implments a simple Key Value Store based on [BitCask](https://riak.com/assets/bitcask-intro.pdf) storage format.
BitC-DB supports PUT, GET and DELETE methods over gRPC protocol.

The implementation supports hint files, index rebuilding at server startup and compaction of logs as well.

### Dependencies
BitC-DB needs `grpc` package in order to run. Please install it using below command.

`pip install grpcio==1.51.1 grpcio-tools==1.51.1`


### Starting DB server
`server.py` module can be used to start the server.

```
(.bitcvenv) singhpradeepk$ python -m bitc.server --help
usage: BitCdbKeyValueStoreService [-h] --db-dir DB_DIR --port PORT [--merge-interval MERGE_INTERVAL] [--max-cask-file-size MAX_CASK_FILE_SIZE]

bitCDB Key Value Store service based on bitcask

optional arguments:
  -h, --help            show this help message and exit
  --db-dir DB_DIR       database file directory
  --port PORT           Port
  --merge-interval MERGE_INTERVAL
                        File merge interval
  --max-cask-file-size MAX_CASK_FILE_SIZE
                        Max cask file size in bytes
(.bitcvenv) singhpradeepk$ 


python -m bitc.server --db-dir . --port 12345
```
option `--db-dir` specifies the path where data files will be created and `--port` specifies the port number on which server will listen.

### Sending client requests
```
from bitc.client import BitCdbRpcClient

db_client = BitCdbRpcClient(host="127.0.0.1", port=12345,)

print(client.get("test").value)
client.put("test", "16")
print(client.get("test").value)
client.put("test", "19")
print(client.get("test").value)
print(client.delete("test").result)
print(client.get("test").value)
print(client.delete("test").result)
```


**Note:** BitC-DB is only for understanding BitCask peper's implementation and should not be considered a full blown DB.



