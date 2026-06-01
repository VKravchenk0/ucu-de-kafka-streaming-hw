from collections import OrderedDict
import numpy as np


class CentroidTracker:
    def __init__(self, max_disappeared: int = 30, max_distance: float = 100.0):
        self.next_id = 0
        self.objects: OrderedDict[int, np.ndarray] = OrderedDict()
        self.disappeared: OrderedDict[int, int] = OrderedDict()
        self.bboxes: dict[int, list[int]] = {}
        self.max_disappeared = max_disappeared
        self.max_distance = max_distance

    def _centroid(self, bbox: list[int]) -> np.ndarray:
        x1, y1, x2, y2 = bbox
        return np.array([(x1 + x2) / 2.0, (y1 + y2) / 2.0])

    def _register(self, centroid: np.ndarray, bbox: list[int]) -> None:
        self.objects[self.next_id] = centroid
        self.disappeared[self.next_id] = 0
        self.bboxes[self.next_id] = bbox
        self.next_id += 1

    def _deregister(self, obj_id: int) -> None:
        del self.objects[obj_id]
        del self.disappeared[obj_id]
        self.bboxes.pop(obj_id, None)

    def update(self, detections: list[list[int]]) -> dict[int, list[int]]:
        if not detections:
            for obj_id in list(self.disappeared):
                self.disappeared[obj_id] += 1
                if self.disappeared[obj_id] > self.max_disappeared:
                    self._deregister(obj_id)
            return dict(self.bboxes)

        new_centroids = np.array([self._centroid(b) for b in detections])

        if not self.objects:
            for i, bbox in enumerate(detections):
                self._register(new_centroids[i], bbox)
            return dict(self.bboxes)

        obj_ids = list(self.objects.keys())
        obj_centroids = np.array(list(self.objects.values()))

        # Pairwise Euclidean distances: rows=existing, cols=new
        D = np.linalg.norm(obj_centroids[:, None] - new_centroids[None, :], axis=2)

        # Greedy matching: sort all distances, assign closest pairs first
        rows_sorted = D.min(axis=1).argsort()
        cols_sorted = D.argmin(axis=1)[rows_sorted]

        used_rows: set[int] = set()
        used_cols: set[int] = set()

        for row, col in zip(rows_sorted, cols_sorted):
            if row in used_rows or col in used_cols:
                continue
            if D[row, col] > self.max_distance:
                continue
            obj_id = obj_ids[row]
            self.objects[obj_id] = new_centroids[col]
            self.disappeared[obj_id] = 0
            self.bboxes[obj_id] = detections[col]
            used_rows.add(row)
            used_cols.add(col)

        unused_rows = set(range(len(obj_ids))) - used_rows
        unused_cols = set(range(len(detections))) - used_cols

        for row in unused_rows:
            obj_id = obj_ids[row]
            self.disappeared[obj_id] += 1
            if self.disappeared[obj_id] > self.max_disappeared:
                self._deregister(obj_id)

        for col in unused_cols:
            self._register(new_centroids[col], detections[col])

        return dict(self.bboxes)
