# Product Context

## User Goal

Upload a video, watch it play back with bounding boxes drawn around detected cars and people, and see live statistics about how many unique objects have been detected — all without waiting for the entire video to process first.

## Primary User Scenario

1. Navigate to `http://localhost:8080`
2. Drop an MP4 file onto the upload form (or click to browse)
3. Browser redirects to `/view/{session_id}`
4. **Buffering phase** (~4 s): spinner appears, badge shows "Buffering…" while the detection pipeline builds an initial overlay buffer
5. **Playback phase**: video plays with orange bounding boxes for cars/trucks/buses and blue boxes for people; statistics panel updates live
6. **Stall phase** (if pipeline falls behind): video auto-pauses, spinner reappears, resumes after ≥4 s once buffer catches up
7. **Done phase**: badge shows "Done", video plays freely to the end

## UX Guarantees

- The video **never plays a frame without overlays available** — the pipeline must be ahead of the playhead
- The `OVERLAY_STALE_TOLERANCE_MS = 500` exception: the last ~300–500 ms before `vid.duration` may have no overlay (structural gap between last emitted frame and video EOF), and these few frames play without re-triggering a stall
- If processing finishes and `_done` arrives, the video plays freely forever regardless of buffer position

## Live Statistics Panel (sidebar)

| Stat | Source field |
|---|---|
| Cars in frame | `cars_in_frame` (current frame) |
| Cars total | `cars_total` (unique IDs seen in session) |
| People in frame | `persons_in_frame` |
| People total | `persons_total` |

Stats update every animation frame as the playhead advances through `overlayBuffer`.

## Status Badge States

| Badge | CSS class | Meaning |
|---|---|---|
| Buffering… | `badge-buffer` (orange) | Paused, waiting for pipeline |
| Processing | `badge-live` (green) | Playing, pipeline active |
| Done | `badge-done` (yellow) | All frames processed |
| Waiting… | `badge-wait` (gray) | Initial WS connect |

## Developer/Admin Scenario

- Verify GPU is being used: `docker logs car-detector 2>&1 | grep "device="`
- Check consumer lag: `docker exec broker kafka-consumer-groups --bootstrap-server broker:29092 --all-groups --describe`
- Check session stats: `curl -s http://localhost:8002/stats | python3 -m json.tool`

## Page Reload Behaviour

Reloading `/view/{session_id}` reconnects the WebSocket. The server replays the full `_overlay_store` for that session instantly (catch-up phase), then resumes live streaming. No frames are lost from the user's perspective.
