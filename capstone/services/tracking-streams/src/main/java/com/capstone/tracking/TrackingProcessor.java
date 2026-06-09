package com.capstone.tracking;

import com.capstone.tracking.model.Detection;
import com.capstone.tracking.model.DetectionRecord;
import com.capstone.tracking.model.Track;
import com.capstone.tracking.model.TrackerState;
import com.capstone.tracking.model.TrackingRecord;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Stateful per-session centroid tracking — replaces the Python {@code car-tracker}/
 * {@code person-tracker} services. State (tracker internals + cumulative "seen" set) is
 * keyed by session_id in a persistent {@link KeyValueStore}, so it survives restarts and
 * needs no manual cleanup (unlike the Python version's threading.Timer-based eviction).
 */
public class TrackingProcessor extends ContextualProcessor<String, DetectionRecord, String, TrackingRecord> {
    private final String objectType;
    private final String storeName;
    private final int maxDisappeared;
    private final double maxDistance;
    private final double minIou;

    private CentroidTracker tracker;
    private KeyValueStore<String, TrackerState> store;

    public TrackingProcessor(String objectType, String storeName, int maxDisappeared, double maxDistance, double minIou) {
        this.objectType = objectType;
        this.storeName = storeName;
        this.maxDisappeared = maxDisappeared;
        this.maxDistance = maxDistance;
        this.minIou = minIou;
    }

    @Override
    public void init(ProcessorContext<String, TrackingRecord> context) {
        super.init(context);
        this.tracker = new CentroidTracker(maxDisappeared, maxDistance, minIou);
        this.store = context().getStateStore(storeName);
    }

    @Override
    public void process(Record<String, DetectionRecord> record) {
        DetectionRecord detection = record.value();
        if (detection == null) {
            return;
        }
        String sessionId = detection.session_id;

        TrackerState state = store.get(sessionId);
        if (state == null) {
            state = new TrackerState();
        }

        List<int[]> bboxes = new ArrayList<>();
        if (detection.detections != null) {
            for (Detection d : detection.detections) {
                bboxes.add(d.bbox);
            }
        }

        LinkedHashMap<Integer, int[]> currentTracks = tracker.update(state, bboxes);
        for (Integer trackId : currentTracks.keySet()) {
            state.seen.add(trackId);
        }

        store.put(sessionId, state);

        List<Track> tracksList = new ArrayList<>(currentTracks.size());
        for (var entry : currentTracks.entrySet()) {
            tracksList.add(new Track(entry.getKey(), entry.getValue()));
        }

        TrackingRecord out = new TrackingRecord(
                sessionId,
                detection.frame_number,
                detection.video_timestamp_ms,
                objectType,
                tracksList,
                currentTracks.size(),
                state.seen.size()
        );

        context().forward(record.withValue(out));
    }
}
