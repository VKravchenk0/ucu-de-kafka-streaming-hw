package ucu.de.dsk.capstone.tracking.model;

public class Track {
    public int track_id;
    public int[] bbox;

    public Track() {
    }

    public Track(int trackId, int[] bbox) {
        this.track_id = trackId;
        this.bbox = bbox;
    }
}
