import json
import os
import subprocess
import time
from pathlib import Path

import pytest
import requests

CAPSTONE_DIR = Path(__file__).resolve().parent.parent

WEB_URL = os.environ.get("WEB_URL", "http://localhost:8080")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
STARTUP_TIMEOUT = int(os.environ.get("STARTUP_TIMEOUT", "300"))
E2E_TIMEOUT = int(os.environ.get("E2E_TIMEOUT", "60"))

_TEST_VIDEO_ENV = os.environ.get("TEST_VIDEO")
TEST_VIDEO = (
    Path(_TEST_VIDEO_ENV)
    if _TEST_VIDEO_ENV
    else CAPSTONE_DIR / "test-input.mp4"
)


def _http_ok(url: str) -> bool:
    try:
        return requests.get(url, timeout=3).status_code == 200
    except Exception:
        return False


def _poll(fn, timeout: int, interval: float = 3.0, label: str = "") -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if fn():
            return
        time.sleep(interval)
    raise TimeoutError(f"Timed out waiting for: {label}")


@pytest.fixture(scope="session", autouse=True)
def docker_stack():
    cmd = ["docker", "compose", "--profile", "infra", "--profile", "app"]
    subprocess.run([*cmd, "up", "-d"], cwd=CAPSTONE_DIR, check=True)

    _poll(lambda: _http_ok(f"{WEB_URL}/"), STARTUP_TIMEOUT, label="web /")

    yield

    if not os.environ.get("E2E_KEEP_STACK"):
        subprocess.run([*cmd, "down"], cwd=CAPSTONE_DIR, check=True)


@pytest.fixture(scope="session")
def session_id(docker_stack):
    """Upload test-input.mp4 once; share the session_id across both tests."""
    with open(TEST_VIDEO, "rb") as f:
        resp = requests.post(
            f"{WEB_URL}/upload",
            files={"file": ("test-input.mp4", f, "video/mp4")},
            allow_redirects=False,
        )
    assert resp.status_code == 303, f"Upload failed: {resp.status_code} {resp.text}"
    location = resp.headers["location"]
    return location.rstrip("/").split("/")[-1]
