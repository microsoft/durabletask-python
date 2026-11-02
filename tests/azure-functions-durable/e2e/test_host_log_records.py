# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from pathlib import Path

import pytest

from ._harness import FunctionApp


@pytest.mark.parametrize("message", [
    "Starting new 'activity_chain' instance with ID = 'test-instance'.",
    "test-instance: Orchestration activity_chain completed with status: COMPLETED",
])
@pytest.mark.parametrize(("before", "after"), [
    ("", ""),
    ("", "[2026-11-02T19:42:58.870Z] Request GET http://127.0.0.1:10002/"),
    ("[2026-11-02T19:42:58.869Z] Previous record", ""),
    ("[2026-11-02T19:42:58.869Z] Previous record",
     "[2026-11-02T19:42:58.871Z] Next record"),
    ("[2026-11-02T19:42:58Z] Previous record",
     "[2026-11-02T19:42:58.8701234Z] Next record"),
    ("[2026-11-02T19:42:58.869Z] Previous record\n",
     "\n[2026-11-02T19:42:58.871Z] Next record"),
    ("[2026-11-02T19:42:58.869Z] Previous record\r\n",
     "\r\n[2026-11-02T19:42:58.871Z] Next record"),
])
def test_wait_for_host_log_extracts_timestamped_record(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
        message: str, before: str, after: str) -> None:
    app = FunctionApp("dtask_style")
    log_path = tmp_path / "host.log"
    monkeypatch.setattr(app, "_log_path", log_path)
    expected = f"[2026-11-02T19:42:58.870Z] {message}"
    log_path.write_text(before + expected + after, encoding="utf-8")

    record = app.wait_for_host_log(message, timeout=0)

    assert record == expected


@pytest.mark.parametrize("output", [
    "SDK message",
    "[2026-11-02T19:42:58.870Z]\nSDK message",
    "[2026-11-02T19:42:58.870Z] Unrelated record SDK message",
    "[2026-11-02T19:42:58.870Z] SDK message unexpected suffix",
])
def test_wait_for_host_log_preserves_nonmatching_records(
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch, output: str) -> None:
    app = FunctionApp("dtask_style")
    log_path = tmp_path / "host.log"
    monkeypatch.setattr(app, "_log_path", log_path)
    log_path.write_text(output, encoding="utf-8")

    record = app.wait_for_host_log("SDK message", timeout=0)

    assert record == output.splitlines()[-1]
