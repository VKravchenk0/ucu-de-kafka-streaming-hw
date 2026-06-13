package ucu.de.dsk.capstone.tracking;

import ucu.de.dsk.capstone.tracking.model.TrackerState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

/**
 * Two-pass cascade matching for centroid-based object tracking.
 *
 * <p>Pass 1 — IoU matching: pairs existing tracks with detections whose bounding boxes
 * overlap by at least {@code minIou}. Handles cars approaching the camera (growing bbox,
 * centroid may shift < 100 px while IoU remains high).
 *
 * <p>Pass 2 — centroid fallback: for tracks/detections left unmatched by pass 1, falls
 * back to the original greedy nearest-centroid logic within {@code maxDistance}.
 *
 * <p>Stateless — operates on a {@link TrackerState} loaded from / saved to a Kafka Streams
 * state store, so the same tracker instance can serve every session_id.
 */
public class CentroidTracker {
    private final int maxDisappeared;
    private final double maxDistance;
    private final double minIou;

    public CentroidTracker(int maxDisappeared, double maxDistance, double minIou) {
        this.maxDisappeared = maxDisappeared;
        this.maxDistance = maxDistance;
        this.minIou = minIou;
    }

    private static double[] centroid(int[] bbox) {
        return new double[]{(bbox[0] + bbox[2]) / 2.0, (bbox[1] + bbox[3]) / 2.0};
    }

    private static double centroidDistance(double[] a, double[] b) {
        double dx = a[0] - b[0];
        double dy = a[1] - b[1];
        return Math.sqrt(dx * dx + dy * dy);
    }

    private static double iou(int[] a, int[] b) {
        int interX1 = Math.max(a[0], b[0]);
        int interY1 = Math.max(a[1], b[1]);
        int interX2 = Math.min(a[2], b[2]);
        int interY2 = Math.min(a[3], b[3]);
        int interW = interX2 - interX1;
        int interH = interY2 - interY1;
        if (interW <= 0 || interH <= 0) {
            return 0.0;
        }
        double intersection = (double) interW * interH;
        double areaA = (double) (a[2] - a[0]) * (a[3] - a[1]);
        double areaB = (double) (b[2] - b[0]) * (b[3] - b[1]);
        return intersection / (areaA + areaB - intersection);
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
        int nRows = objectIds.size();
        int nCols = detections.size();

        Set<Integer> usedRows = new HashSet<>();
        Set<Integer> usedCols = new HashSet<>();

        // --- Pass 1: IoU matching ---
        // Build list of (iou, row, col) for all pairs, sort by iou descending.
        List<double[]> iouPairs = new ArrayList<>(nRows * nCols);
        for (int r = 0; r < nRows; r++) {
            int[] existingBbox = s.bboxes.get(objectIds.get(r));
            if (existingBbox == null) continue;
            for (int c = 0; c < nCols; c++) {
                double v = iou(existingBbox, detections.get(c));
                if (v >= minIou) {
                    iouPairs.add(new double[]{v, r, c});
                }
            }
        }
        iouPairs.sort((x, y) -> Double.compare(y[0], x[0])); // descending IoU

        for (double[] pair : iouPairs) {
            int row = (int) pair[1];
            int col = (int) pair[2];
            if (usedRows.contains(row) || usedCols.contains(col)) {
                continue;
            }
            int objectId = objectIds.get(row);
            s.objects.put(objectId, newCentroids[col]);
            s.disappeared.put(objectId, 0);
            s.bboxes.put(objectId, detections.get(col));
            usedRows.add(row);
            usedCols.add(col);
        }

        // --- Pass 2: centroid-distance fallback for remaining unmatched pairs ---
        // Collect unmatched row/col indices.
        List<Integer> unmatchedRows = new ArrayList<>();
        List<Integer> unmatchedCols = new ArrayList<>();
        for (int r = 0; r < nRows; r++) {
            if (!usedRows.contains(r)) unmatchedRows.add(r);
        }
        for (int c = 0; c < nCols; c++) {
            if (!usedCols.contains(c)) unmatchedCols.add(c);
        }

        if (!unmatchedRows.isEmpty() && !unmatchedCols.isEmpty()) {
            double[][] objectCentroids = new double[unmatchedRows.size()][];
            for (int i = 0; i < unmatchedRows.size(); i++) {
                objectCentroids[i] = s.objects.get(objectIds.get(unmatchedRows.get(i)));
            }

            int uRows = unmatchedRows.size();
            int uCols = unmatchedCols.size();
            double[][] distances = new double[uRows][uCols];
            for (int r = 0; r < uRows; r++) {
                for (int c = 0; c < uCols; c++) {
                    distances[r][c] = centroidDistance(objectCentroids[r], newCentroids[unmatchedCols.get(c)]);
                }
            }

            double[] rowMin = new double[uRows];
            int[] rowArgmin = new int[uRows];
            for (int r = 0; r < uRows; r++) {
                int best = 0;
                for (int c = 1; c < uCols; c++) {
                    if (distances[r][c] < distances[r][best]) best = c;
                }
                rowMin[r] = distances[r][best];
                rowArgmin[r] = best;
            }

            Integer[] sortedByMin = new Integer[uRows];
            for (int r = 0; r < uRows; r++) sortedByMin[r] = r;
            Arrays.sort(sortedByMin, Comparator.comparingDouble(r -> rowMin[r]));

            for (int uRow : sortedByMin) {
                int uCol = rowArgmin[uRow];
                int row = unmatchedRows.get(uRow);
                int col = unmatchedCols.get(uCol);
                if (usedRows.contains(row) || usedCols.contains(col)) continue;
                if (distances[uRow][uCol] > maxDistance) continue;
                int objectId = objectIds.get(row);
                s.objects.put(objectId, newCentroids[col]);
                s.disappeared.put(objectId, 0);
                s.bboxes.put(objectId, detections.get(col));
                usedRows.add(row);
                usedCols.add(col);
            }
        }

        // Increment disappeared for tracks that found no match in either pass.
        for (int row = 0; row < nRows; row++) {
            if (usedRows.contains(row)) continue;
            int objectId = objectIds.get(row);
            int count = s.disappeared.get(objectId) + 1;
            s.disappeared.put(objectId, count);
            if (count > maxDisappeared) {
                deregister(s, objectId);
            }
        }

        // Register new tracks for detections that were not matched.
        for (int col = 0; col < nCols; col++) {
            if (usedCols.contains(col)) continue;
            register(s, newCentroids[col], detections.get(col));
        }

        return new LinkedHashMap<>(s.bboxes);
    }
}
