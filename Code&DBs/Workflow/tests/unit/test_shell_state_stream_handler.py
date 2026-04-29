from __future__ import annotations

from datetime import datetime

from surfaces.api.handlers.shell_state_stream_handler import _fetch_events


class _RecordingConn:
    def __init__(self) -> None:
        self.args = None

    def fetch(self, _sql: str, *args):
        self.args = args
        return []


def test_fetch_events_coerces_iso_cursor_to_datetime() -> None:
    conn = _RecordingConn()

    _fetch_events(
        conn,
        session="session-1",
        after="2026-04-29T18:06:57.562651+00:00",
    )

    assert conn.args is not None
    assert isinstance(conn.args[2], datetime)
    assert conn.args[2].isoformat() == "2026-04-29T18:06:57.562651+00:00"


def test_fetch_events_ignores_invalid_cursor() -> None:
    conn = _RecordingConn()

    _fetch_events(conn, session="session-1", after="not-a-date")

    assert conn.args is not None
    assert conn.args[2] is None
