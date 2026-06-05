import json
import logging
import os

import requests
import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KSQLDB_URL = os.environ.get("KSQLDB_URL", "http://ksqldb-server:8088")
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8002"))

app = FastAPI()


def _pull_query(sql: str) -> list[dict]:
    """Execute a ksqlDB pull query and return rows as dicts with lowercased keys.

    ksqlDB /query-stream returns a streaming JSONL response:
      line 1: {"queryId": "...", "columnNames": ["COL1", ...], "columnTypes": [...]}
      line 2+: ["val1", "val2", ...]   (one array per row)
    """
    url = f"{KSQLDB_URL}/query-stream"
    headers = {"Content-Type": "application/vnd.ksqlapi.data.v1+json"}
    resp = requests.post(url, json={"sql": sql}, headers=headers, timeout=10, stream=True)
    resp.raise_for_status()

    columns: list[str] = []
    rows: list[dict] = []
    for raw_line in resp.iter_lines():
        if not raw_line:
            continue
        data = json.loads(raw_line)
        if isinstance(data, dict):
            columns = [c.lower() for c in data.get("columnNames", [])]
        elif isinstance(data, list) and columns:
            rows.append(dict(zip(columns, data)))
    return rows


@app.get("/stats")
def get_stats() -> JSONResponse:
    try:
        car_rows = _pull_query("SELECT session_id, cars_total FROM session_car_stats;")
        person_rows = _pull_query("SELECT session_id, persons_total FROM session_person_stats;")
    except Exception as e:
        logger.error("ksqlDB pull query failed: %s", e)
        return JSONResponse({"error": str(e)}, status_code=503)

    result: dict[str, dict] = {}
    for row in car_rows:
        sid = row["session_id"]
        result[sid] = {"unique_cars": row.get("cars_total") or 0, "unique_persons": 0}
    for row in person_rows:
        sid = row["session_id"]
        if sid not in result:
            result[sid] = {"unique_cars": 0, "unique_persons": 0}
        result[sid]["unique_persons"] = row.get("persons_total") or 0

    return JSONResponse({"sessions": result})


def main() -> None:
    uvicorn.run(app, host="0.0.0.0", port=HTTP_PORT, log_level="warning")


if __name__ == "__main__":
    main()
