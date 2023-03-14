# bitC-DB

This repo implments a simple Key Value Store based on [BitCask](https://riak.com/assets/bitcask-intro.pdf) storage format.
BitC-DB supports PUT, GET and DELETE methods over gRPC protocol.

## Storage

BitC-DB **appends** each record to a data file. Once data file size reaches a threshold, it closes that
file and opens a new file for writing. It never opens closed files for writing again. These older files
will be used  only for reading the records.

Bit Cask persists all records on the disk in following format:
```
+----+-------------+----------+------------+-----+------+
|CRC | Time Stamp  | Key Size | Value Size | key | Value|
+----+-------------+----------+------------+-----+------+
```
It also maintains an in-memory index for each key. This in-mrmory index can be implemented using various
data structures like hash maps, tries, skip lists, R-B Trees, AVL trees etc. BitC-DB uses hashmap for index.

Index format is like below:

```
        +--------+-------------+------------+-------------+-----------+
key --> |file_id | Time Stamp  | Value Size | Value Offset| timestamp |
        +--------+-------------+------------+-------------+-----------+
```

When we need to find a value for given key, we can see the index and find the file and offset where we can find the value.

Over the time number of datafiles will grow. BitC-DB runs a periodic compaction thread, which clubs multiple data files and merge them
into one single file. In the process, it removes all the duplicate entries for keys present in different files and keeps the latest entry only.

Bit Cask like storage is suitable when there are less number of keys (so that they all can fit in memory) having very frequent updates.
This kind of storage is suitable for high writes as compared to reads. No seek is required for writes, as they are always appneded to a file.
Read requires a seek since it needs to read the data randomly from files.

The BitC-DB implementation supports:

* hint files
* index rebuilding at server startup
* compaction of logs

## Running BitC-DB

### Dependencies
BitC-DB uses `grpc` package for communication between client and server. Please install it using below command.

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



