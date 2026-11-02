"""Index helpers for membership tracking."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List

try:  # pragma: no cover - optional dependency
    import pyroaring
except ImportError:  # pragma: no cover - optional dependency
    pyroaring = None  # type: ignore

from util import Interval, coalesce_day_intervals


@dataclass
class MembershipIndex:
    object_ids: List[int]

    def add(self, object_id: int) -> None:
        if self.object_ids and object_id < self.object_ids[-1]:
            raise ValueError("object_ids must be inserted in sorted order")
        if self.object_ids and object_id == self.object_ids[-1]:
            return
        self.object_ids.append(object_id)

    def to_bytes(self) -> bytes:
        if pyroaring is None:
            return ",".join(str(x) for x in self.object_ids).encode("ascii")
        bitmap = pyroaring.BitMap(self.object_ids)
        return bitmap.serialize()


class PresenceRuns:
    """Maintain run-length encoded presence information."""

    def __init__(self) -> None:
        self._intervals: List[Interval] = []

    @property
    def intervals(self) -> List[Interval]:
        return list(self._intervals)

    def add_observation(self, day_id: int) -> None:
        self._intervals = coalesce_day_intervals(self._intervals, day_id)


def write_membership(path: str, object_ids: Iterable[int]) -> None:
    """Write membership data to *path* using the best available encoding."""

    sorted_ids = sorted(set(object_ids))
    if pyroaring is None:
        payload = {
            "encoding": "rle",
            "count": len(sorted_ids),
            "min": sorted_ids[0] if sorted_ids else None,
            "max": sorted_ids[-1] if sorted_ids else None,
            "data": sorted_ids,
        }
        import json
        from pathlib import Path

        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)
        path_obj.write_text(json.dumps(payload))
    else:  # pragma: no cover - requires optional dependency
        bitmap = pyroaring.BitMap(sorted_ids)
        from pathlib import Path

        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)
        path_obj.write_bytes(bitmap.serialize())
