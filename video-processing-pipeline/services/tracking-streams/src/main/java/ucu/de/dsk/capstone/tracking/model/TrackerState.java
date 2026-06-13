package ucu.de.dsk.capstone.tracking.model;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;

/**
 * Persisted (JSON-serialized) per-session state for {@link ucu.de.dsk.capstone.tracking.CentroidTracker}.
 * Map/set iteration order matters for the greedy-matching algorithm, hence LinkedHashMap/Set.
 */
public class TrackerState {
    public int next_id = 0;
    public LinkedHashMap<Integer, double[]> objects = new LinkedHashMap<>();
    public LinkedHashMap<Integer, Integer> disappeared = new LinkedHashMap<>();
    public LinkedHashMap<Integer, int[]> bboxes = new LinkedHashMap<>();
    public LinkedHashSet<Integer> seen = new LinkedHashSet<>();
}
