import json
import time
import uuid

from confluent_kafka import Consumer
from websocket import WebSocket, WebSocketTimeoutException

from conftest import E2E_TIMEOUT, KAFKA_BOOTSTRAP

# Extra wall-clock seconds the WebSocket retry loop waits beyond E2E_TIMEOUT.
# _done can arrive 30 s after upload (MIN_DONE_DELAY_S hardcoded in web/main.py).
# Overlays land in _overlay_store shortly after; subsequent reconnects replay them.
_WS_BUFFER_S = 30


def test_records_in_kafka(session_id):
    """Records for the shared session must appear in tracking.combined.

    Uses earliest offset so the consumer catches the message regardless of
    when it was produced relative to subscription time.  The session_id UUID
    ensures no false-positive matches from prior test runs.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"e2e-kafka-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(["tracking.combined"])

    records = []
    deadline = time.time() + E2E_TIMEOUT
    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            key = msg.key().decode() if msg.key() else ""
            if key != session_id:
                continue
            records.append(json.loads(msg.value()))
            break
    finally:
        consumer.close()

    assert records, (
        f"No records in tracking.combined for session_id={session_id} "
        f"within {E2E_TIMEOUT}s"
    )
    first = records[0]
    # SESSION_ID is the Kafka message key (PARTITION BY c.session_id in ksqlDB);
    # FRAME_NUMBER is always present in the value.
    assert "FRAME_NUMBER" in first or "frame_number" in first, (
        f"Unexpected record shape: {first}"
    )


def test_records_via_websocket(session_id):
    """Tracking overlays for the same session must arrive via the WebSocket.

    _done fires 30 s after upload (MIN_DONE_DELAY_S), which can race ahead of
    overlays if the web service's consumer hasn't processed tracking.combined
    yet.  The retry loop reconnects until _overlay_store is populated
    (replay path) or the overlay arrives in the live stream.
    """
    messages = []
    total_deadline = time.time() + E2E_TIMEOUT + _WS_BUFFER_S

    while time.time() < total_deadline and not messages:
        ws = WebSocket()
        ws.connect(f"ws://localhost:8080/ws/{session_id}")
        ws.settimeout(10)
        try:
            attempt_deadline = time.time() + 10
            while time.time() < attempt_deadline:
                try:
                    raw = ws.recv()
                except WebSocketTimeoutException:
                    break
                data = json.loads(raw)
                if data.get("_done"):
                    break
                messages.append(data)
                break
        finally:
            ws.close()

        if not messages:
            time.sleep(2)

    assert messages, (
        f"No overlay messages via WebSocket for session_id={session_id} "
        f"within {E2E_TIMEOUT + _WS_BUFFER_S}s"
    )
    first = messages[0]
    # web/main.py calls _lower() so keys are lowercase here.
    assert "frame_number" in first or "cars_in_frame" in first, (
        f"Unexpected overlay shape: {first}"
    )
