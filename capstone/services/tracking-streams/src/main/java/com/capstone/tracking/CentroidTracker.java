package com.capstone.tracking;

import com.capstone.tracking.model.TrackerState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

/**
 * Java port of {@code common/centroid_tracker.py}: greedy nearest-centroid matching.
 * Stateless — operates on a {@link TrackerState} loaded from / saved to a Kafka Streams
 * state store, so the same tracker instance can serve every session_id.
 *
 * <p>Note: row order is resolved by a stable sort on each row's minimum distance, whereas
 * the Python version uses {@code np.argsort} (introsort, not stable). Tie-breaking can
 * therefore differ in rare equal-distance cases; this does not affect tracking quality.
 */
public class CentroidTracker {
    private final int maxDisappeared;
    private final double maxDistance;

    public CentroidTracker(int maxDisappeared, double maxDistance) {
        this.maxDisappeared = maxDisappeared;
        this.maxDistance = maxDistance;
    }

    private static double[] centroid(int[] bbox) {
        return new double[]{(bbox[0] + bbox[2]) / 2.0, (bbox[1] + bbox[3]) / 2.0};
    }

    private static double distance(double[] a, double[] b) {
        double dx = a[0] - b[0];
        double dy = a[1] - b[1];
        return Math.sqrt(dx * dx + dy * dy);
    }

    private void register(TrackerState s, double[] centroid, int[] bbox) {
        int id = s.next_id++;
        s.objects.put(id, centroid);
        s.disappeared.put(id, 0);
        s.bboxes.put(id, bbox);
    }

    private void deregister(TrackerState s, int objectId) {
        s.objects.remove(objectId);
        s.disappeared.remove(objectId);
        s.bboxes.remove(objectId);
    }

    /** Mutates {@code state} in place and returns the current track_id -&gt; bbox map. */
    public LinkedHashMap<Integer, int[]> update(TrackerState s, List<int[]> detections) {
        if (detections.isEmpty()) {
            for (Integer objectId : new ArrayList<>(s.disappeared.keySet())) {
                int count = s.disappeared.get(objectId) + 1;
                s.disappeared.put(objectId, count);
                if (count > maxDisappeared) {
                    deregister(s, objectId);
                }
            }
            return new LinkedHashMap<>(s.bboxes);
        }

        double[][] newCentroids = new double[detections.size()][];
        for (int i = 0; i < detections.size(); i++) {
            newCentroids[i] = centroid(detections.get(i));
        }

        if (s.objects.isEmpty()) {
            for (int i = 0; i < detections.size(); i++) {
                register(s, newCentroids[i], detections.get(i));
            }
            return new LinkedHashMap<>(s.bboxes);
        }

        List<Integer> objectIds = new ArrayList<>(s.objects.keySet());
        double[][] objectCentroids = new double[objectIds.size()][];
        for (int i = 0; i < objectIds.size(); i++) {
            objectCentroids[i] = s.objects.get(objectIds.get(i));
        }

        int nRows = objectCentroids.length;
        int nCols = newCentroids.length;
        double[][] distances = new double[nRows][nCols];
        for (int r = 0; r < nRows; r++) {
            for (int c = 0; c < nCols; c++) {
                distances[r][c] = distance(objectCentroids[r], newCentroids[c]);
            }
        }

        double[] rowMin = new double[nRows];
        int[] rowArgmin = new int[nRows];
        for (int r = 0; r < nRows; r++) {
            int best = 0;
            for (int c = 1; c < nCols; c++) {
                if (distances[r][c] < distances[r][best]) {
                    best = c;
                }
            }
            rowMin[r] = distances[r][best];
            rowArgmin[r] = best;
        }

        Integer[] rowsSortedByMinDistance = new Integer[nRows];
        for (int r = 0; r < nRows; r++) {
            rowsSortedByMinDistance[r] = r;
        }
        Arrays.sort(rowsSortedByMinDistance, Comparator.comparingDouble(r -> rowMin[r]));

        Set<Integer> usedRows = new HashSet<>();
        Set<Integer> usedCols = new HashSet<>();

        for (int row : rowsSortedByMinDistance) {
            int col = rowArgmin[row];
            if (usedRows.contains(row) || usedCols.contains(col)) {
                continue;
            }
            if (distances[row][col] > maxDistance) {
                continue;
            }
            int objectId = objectIds.get(row);
            s.objects.put(objectId, newCentroids[col]);
            s.disappeared.put(objectId, 0);
            s.bboxes.put(objectId, detections.get(col));
            usedRows.add(row);
            usedCols.add(col);
        }

        for (int row = 0; row < nRows; row++) {
            if (usedRows.contains(row)) {
                continue;
            }
            int objectId = objectIds.get(row);
            int count = s.disappeared.get(objectId) + 1;
            s.disappeared.put(objectId, count);
            if (count > maxDisappeared) {
                deregister(s, objectId);
            }
        }

        for (int col = 0; col < nCols; col++) {
            if (usedCols.contains(col)) {
                continue;
            }
            register(s, newCentroids[col], detections.get(col));
        }

        return new LinkedHashMap<>(s.bboxes);
    }
}
