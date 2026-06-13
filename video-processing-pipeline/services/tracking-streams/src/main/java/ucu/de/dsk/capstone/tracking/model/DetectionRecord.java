package ucu.de.dsk.capstone.tracking.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DetectionRecord {
    public String session_id;
    public int frame_number;
    public double video_timestamp_ms;
    public List<Detection> detections;
}
