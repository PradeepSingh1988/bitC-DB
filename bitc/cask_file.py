import binascii
import os
import struct
from threading import Lock

from bitc import consts
from bitc.utils import CaskIOException


def calculate_checksum(header, key, value):
    crc = binascii.crc32(header[4:])  # skip crc filed i.e. first four bytes
    crc = binascii.crc32(key, crc)
    crc = binascii.crc32(value, crc)
    return crc


class CaskDataEncoder(object):
    def encode(self, timestamp, key, value):
        header = struct.pack(
            consts.DATA_HEADER_FORMAT, 0, timestamp, len(key), len(value)
        )
        key = str.encode(key)
        value = str.encode(value)
        crc = calculate_checksum(header, key, value)
        return struct.pack(consts.CRC_FORMAT, crc) + header[4:] + key + value

    def decode(self, value_bytes):
        existing_crc, timestamp, key_len, value_len = struct.unpack(
            consts.DATA_HEADER_FORMAT, value_bytes[: consts.DATA_HEADER_SIZE]
        )
        return existing_crc, timestamp, key_len, value_len


class CaskHintEncoder(object):
    def encode(self, timestamp, key, offset, entry_size):
        hint_header = struct.pack(
            consts.HINT_HEADER_FORMAT,
            timestamp,
            len(key),
            entry_size,
            offset,
        )
        key = str.encode(key)
        return hint_header + key

    def decode(self, header):
        (
            timestamp,
            key_len,
            entry_size,
            entry_offset,
        ) = struct.unpack(consts.HINT_HEADER_FORMAT, header)
        return key_len, entry_size, entry_offset, timestamp


class CaskFile(object):
    def __init__(self, path, file_id, read_only, encoder, file_format, os_sync=False):
        self._wfh, self._rfh = None, None
        self._open(path, file_id, read_only, file_format)
        self._id = file_id
        self._os_sync = os_sync
        self._lock = Lock()
        self._encoder = encoder
        self._offset = os.stat(self.name).st_size

    def _open(self, path, file_id, read_only, file_format):
        file_name = os.path.join(path, file_format.format(file_id))
        if not read_only:
            self._wfh = open(file_name, "a+b")
        else:
            if not os.path.exists(file_name):
                raise CaskIOException("file {} not found".format(file_name))
            self._rfh = open(file_name, "r+b")

    @property
    def file_handler(self):
        return self._wfh if self._wfh is not None else self._rfh

    @property
    def file_id(self):
        return self._id

    @property
    def file_type(self):
        return consts.DATA_FILE if ".data" in self.name else consts.HINT_FILE

    @property
    def name(self):
        return self._wfh.name if self._wfh is not None else self._rfh.name

    @property
    def basename(self):
        return (
            os.path.basename(self._wfh.name)
            if self._wfh is not None
            else os.path.basename(self._rfh.name)
        )

    @property
    def size(self):
        with self._lock:
            return self._offset

    def close(self):
        if self._wfh is not None:
            self._wfh.close()
        else:
            self._rfh.close()

    def sync(self):
        if self._wfh is not None:
            os.sync(self._wfh.fileno())

    def read(self, *args, **kwargs):
        raise NotImplementedError()

    def write(self, *args, **kwargs):
        raise NotImplementedError()

    def read_all_entries(self, *args, **kwargs):
        raise NotImplementedError


class CaskDataFile(CaskFile):
    def __init__(self, path, file_id, read_only, os_sync=False):
        super().__init__(
            path,
            file_id,
            read_only,
            CaskDataEncoder(),
            consts.DATA_FILE_NAME_FORMAT,
            os_sync=False,
        )

    def read(self, offset, size):
        fh = self._wfh if self._wfh is not None else self._rfh
        fh.seek(offset, consts.WHENCE_BEGINING)
        value_bytes = fh.read(size)
        crc, _, key_len, value_len = self._encoder.decode(value_bytes)
        if consts.DATA_HEADER_SIZE + key_len + value_len != size:
            raise CaskIOException("Bad Entry Size")
        key = value_bytes[consts.DATA_HEADER_SIZE : consts.DATA_HEADER_SIZE + key_len]
        value = value_bytes[consts.DATA_HEADER_SIZE + key_len :]
        new_crc = calculate_checksum(value_bytes[:14], key, value)
        if new_crc != crc:
            raise CaskIOException("Mismatching CRC")
        return value.decode("utf-8")

    def write(self, timestamp, key, value):
        if self._wfh is None:
            raise CaskIOException("{} is not opened for writing".format(self.name))
        with self._lock:
            entry = self._encoder.encode(timestamp, key, value)
            data_len = self._wfh.write(entry)
            self._wfh.flush()
            if self._os_sync:
                os.fsync(self._wfh.fileno())
            self._offset += data_len

    def read_all_entries(self):
        if self._rfh is None:
            raise CaskIOException("File {} is not opened in RO mode".format(self.name))
        current_offset = self._rfh.tell()
        header = self._rfh.read(consts.DATA_HEADER_SIZE)
        while header:
            existing_crc, timestamp, key_size, value_size = self._encoder.decode(header)
            key = self._rfh.read(key_size)
            value = self._rfh.read(value_size)
            crc = calculate_checksum(header, key, value)
            if crc != existing_crc:
                raise CaskIOException("Mismatching CRC")
            entry_size = consts.DATA_HEADER_SIZE + len(key) + len(value)
            yield key.decode(
                "utf-8"
            ), entry_size, current_offset, timestamp, value.decode("utf-8")
            current_offset = self._rfh.tell()
            header = self._rfh.read(consts.DATA_HEADER_SIZE)


class CaskHintFile(CaskFile):
    def __init__(self, path, file_id, read_only, os_sync=False):
        super().__init__(
            path,
            file_id,
            read_only,
            CaskHintEncoder(),
            consts.HINT_FILE_NAME_FORMAT,
            os_sync,
        )

    def read(self, offset):
        fh = self._wfh if self._wfh is not None else self._rfh
        fh.seek(offset, consts.WHENCE_BEGINING)
        value_bytes = fh.read(consts.HINT_HEADER_SIZE)
        key_len, entry_size, entry_offset, timestamp = self._encoder.decode(value_bytes)
        key = fh.read(key_len).decode("utf-8")
        return key, entry_size, entry_offset, timestamp

    def write(self, key, timestamp, offset, entry_size):
        if self._wfh is None:
            raise CaskIOException("{} is not opened for writing".format(self.name))
        with self._lock:
            entry = self._encoder.encode(timestamp, key, offset, entry_size)
            data_len = self._wfh.write(entry)
            self._wfh.flush()
            if self._os_sync:
                os.fsync(self._wfh.fileno())
            self._offset += data_len

    def read_all_entries(self):
        if self._rfh is None:
            raise CaskIOException("File {} is not opened in RO mode")
        header = self._rfh.read(consts.HINT_HEADER_SIZE)
        while header:
            key_len, entry_size, entry_offset, timestamp = self._encoder.decode(header)
            key = self._rfh.read(key_len).decode("utf-8")
            yield key, entry_size, entry_offset, timestamp
            header = self._rfh.read(consts.HINT_HEADER_SIZE)
