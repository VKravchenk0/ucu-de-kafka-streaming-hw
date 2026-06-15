package ucu.de.dsk.capstone.tracking.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TrackingRecord {
    public String session_id;
    public int frame_number;
    public double video_timestamp_ms;
    public String object_type;
    public List<Track> tracks;
    public int in_frame;
    public int total_unique;

    public TrackingRecord() {
    }

    public TrackingRecord(String sessionId, int frameNumber, double videoTimestampMs,
                          String objectType, List<Track> tracks, int inFrame, int totalUnique) {
        this.session_id = sessionId;
        this.frame_number = frameNumber;
        this.video_timestamp_ms = videoTimestampMs;
        this.object_type = objectType;
        this.tracks = tracks;
        this.in_frame = inFrame;
        this.total_unique = totalUnique;
    }
}
