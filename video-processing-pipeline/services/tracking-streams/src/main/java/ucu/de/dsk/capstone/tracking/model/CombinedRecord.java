package ucu.de.dsk.capstone.tracking.model;

import java.util.List;

public class CombinedRecord {
    public String session_id;
    public int frame_number;
    public double video_timestamp_ms;
    public List<Track> car_tracks;
    public Integer cars_in_frame;
    public Integer cars_total;
    public List<Track> person_tracks;
    public Integer persons_in_frame;
    public Integer persons_total;

    public static CombinedRecord of(TrackingRecord cars, TrackingRecord persons) {
        CombinedRecord out = new CombinedRecord();
        out.session_id = cars.session_id;
        out.frame_number = cars.frame_number;
        out.video_timestamp_ms = cars.video_timestamp_ms;
        out.car_tracks = cars.tracks;
        out.cars_in_frame = cars.in_frame;
        out.cars_total = cars.total_unique;
        if (persons != null) {
            out.person_tracks = persons.tracks;
            out.persons_in_frame = persons.in_frame;
            out.persons_total = persons.total_unique;
        }
        return out;
    }
}
