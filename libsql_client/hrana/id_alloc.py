from __future__ import annotations

from typing import Set


# An allocator of non-negative integer ids.
#
# This clever data structure has these "ideal" properties:
# - It consumes memory proportional to the number of used ids (which is optimal).
# - All operations are O(1) time.
# - The allocated ids are small (with a slight modification, we could always
#   provide the smallest possible
# id).
class IdAlloc:
    # Set of all allocated ids
    _used_ids: Set[int]
    # Set of all free ids lower than `len(_used_ids)`
    _free_ids: Set[int]

    def __init__(self) -> None:
        self._used_ids = set()
        self._free_ids = set()

    # Returns an id that was free, and marks it as used.
    def alloc(self) -> int:
        if len(self._free_ids) > 0:
            free_id = self._free_ids.pop()
            self._used_ids.add(free_id)

            # maintain the invariant of `_free_ids`
            if (len(self._used_ids) - 1) not in self._used_ids:
                self._free_ids.add(len(self._used_ids) - 1)
            return free_id

        # the `_free_ids` set is empty, so there are no free ids lower than
        # `len(_used_ids)` this means that `_used_ids` is a set that contains all
        # numbers from 0 to `len(_used_ids) - 1`, so `len(_used_ids)` is free
        free_id = len(self._used_ids)
        self._used_ids.add(free_id)
        return free_id

    def free(self, used_id: int) -> None:
        self._used_ids.remove(used_id)

        # maintain the invariant of `_free_ids`
        self._free_ids.discard(len(self._used_ids))
        if used_id < len(self._used_ids):
            self._free_ids.add(used_id)
