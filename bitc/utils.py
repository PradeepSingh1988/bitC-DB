import glob
import os

from bitc import consts


class CaskIOException(Exception):
    pass


def get_datafiles(file_path):
    return sorted(
        glob.glob(os.path.join(file_path, consts.DATA_FILE_NAME_FORMAT.format("*"))),
        key=lambda x: int(os.path.basename(x).split(".")[0]),
    )


def get_hintfiles(file_path):
    return sorted(
        glob.glob(os.path.join(file_path, consts.HINT_FILE_NAME_FORMAT.format("*"))),
        key=lambda x: int(os.path.basename(x).split(".")[0]),
    )


def get_file_id_from_name(filename):
    return int(filename.split(".")[0])


def get_file_id_from_absolute_path(filepath):
    filename = os.path.basename(filepath)
    return get_file_id_from_name(filename)


def get_hint_filename_for_data_file(data_file):
    return data_file.replace(".data", ".hint")


def has_hint_file(file_path, data_file_name_id):
    return os.path.exists(
        os.path.join(file_path, consts.HINT_FILE_NAME_FORMAT.format(data_file_name_id))
    )
