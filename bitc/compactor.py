# def merge(self, keydir):

#         try:
#             with self._lock:
#                 if self._merge_running:
#                     return False
#                 else:
#                     self._merge_running = True
#                 files_to_merge = utils.get_datafiles(self._file_path)
#                 if not files_to_merge:
#                     return
#                 last_id = utils.get_file_id_from_absolute_path(files_to_merge[-1])
#                 merge_id = last_id + 1
#                 self._next_id = merge_id
#                 self._rotate_files()
#                 with tempfile.TemporaryDirectory(dir=self._file_path) as tempdir:
#                     pass
#         finally:
#             self._merge_running = False