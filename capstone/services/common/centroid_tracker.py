"""Simple centroid-based object tracker with unique ID assignment."""

import math
from collections import OrderedDict


class CentroidTracker:
    def __init__(self, max_disappeared: int = 30, max_distance: int = 100):
        self.next_id = 0
        self.objects: OrderedDict[int, tuple[float, float]] = OrderedDict()
        self.disappeared: OrderedDict[int, int] = OrderedDict()
        self.max_disappeared = max_disappeared
        self.max_distance = max_distance

    def _centroid(self, bbox: list[float]) -> tuple[float, float]:
        x1, y1, x2, y2 = bbox
        return ((x1 + x2) / 2.0, (y1 + y2) / 2.0)

    def _register(self, centroid: tuple[float, float]) -> int:
        tid = self.next_id
        self.objects[tid] = centroid
        self.disappeared[tid] = 0
        self.next_id += 1
        return tid

    def _deregister(self, tid: int) -> None:
        del self.objects[tid]
        del self.disappeared[tid]

    def update(self, detections: list[dict]) -> dict[int, list[float]]:
        """Update tracker with new detections; return {track_id: bbox}."""
        if not detections:
            for tid in list(self.disappeared):
                self.disappeared[tid] += 1
                if self.disappeared[tid] > self.max_disappeared:
                    self._deregister(tid)
            return {}

        new_centroids = [self._centroid(d["bbox"]) for d in detections]
        new_bboxes = [d["bbox"] for d in detections]

        if not self.objects:
            for cx_cy in new_centroids:
                self._register(cx_cy)
            return {tid: new_bboxes[i] for i, tid in enumerate(self.objects)}

        existing_ids = list(self.objects.keys())
        existing_centroids = list(self.objects.values())

        # Pairwise distance matrix
        dist_matrix = [
            [
                math.sqrt((ec[0] - nc[0]) ** 2 + (ec[1] - nc[1]) ** 2)
                for nc in new_centroids
            ]
            for ec in existing_centroids
        ]

        # Greedy matching: find (row, col) of minimum distance, repeated
        matched_rows: set[int] = set()
        matched_cols: set[int] = set()
        matches: list[tuple[int, int]] = []

        # Flatten and sort
        flat = sorted(
            [(dist_matrix[r][c], r, c) for r in range(len(existing_ids)) for c in range(len(new_centroids))],
            key=lambda x: x[0],
        )
        for dist, r, c in flat:
            if r in matched_rows or c in matched_cols:
                continue
            if dist > self.max_distance:
                break
            matches.append((r, c))
            matched_rows.add(r)
            matched_cols.add(c)

        # Update matched tracks
        result: dict[int, list[float]] = {}
        for r, c in matches:
            tid = existing_ids[r]
            self.objects[tid] = new_centroids[c]
            self.disappeared[tid] = 0
            result[tid] = new_bboxes[c]

        # Handle unmatched existing tracks
        for r in range(len(existing_ids)):
            if r not in matched_rows:
                tid = existing_ids[r]
                self.disappeared[tid] += 1
                if self.disappeared[tid] > self.max_disappeared:
                    self._deregister(tid)

        # Register new detections without a match
        for c in range(len(new_centroids)):
            if c not in matched_cols:
                tid = self._register(new_centroids[c])
                result[tid] = new_bboxes[c]

        return result

    @property
    def total_seen(self) -> int:
        return self.next_id
