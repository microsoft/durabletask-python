# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Shared helpers for the Azure Functions Durable end-to-end tests.

These tests launch a *real* Azure Functions host (``func start``) for a sample
function app, backed by Azurite (the local Azure Storage emulator) and the
public Durable Task extension bundle. The Python worker, the host, and the
Durable extension all cooperate exactly as they would in production.

Everything here is stdlib-only (``urllib``, ``subprocess``, ``socket``) so the
harness adds no test dependencies of its own.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

APPS_DIR = Path(__file__).parent / "apps"

# Azurite's well-known blob endpoint. The Durable extension's default Azure
# Storage provider also needs queue/table, but a reachable blob port is a good
# proxy for "Azurite is up" and matches the other e2e suites in this repo.
AZURITE_HOST = "127.0.0.1"
AZURITE_BLOB_PORT = 10000

# How long to wait for the Functions host to become ready, and for an
# orchestration to reach a terminal state. The host cold-start (extension
# bundle download on first run + worker spin-up) dominates the former.
HOST_STARTUP_TIMEOUT_S = 180
ORCHESTRATION_TIMEOUT_S = 60


def find_free_port() -> int:
    """Return an OS-assigned free TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def func_executable() -> Optional[str]:
    """Return the path to the Azure Functions Core Tools (``func``), if installed."""
    return shutil.which("func")


def azurite_is_running() -> bool:
    """Return True if Azurite's blob endpoint accepts TCP connections."""
    try:
        with socket.create_connection((AZURITE_HOST, AZURITE_BLOB_PORT), timeout=2):
            return True
    except OSError:
        return False


@dataclass
class HttpResult:
    status: int
    body: str

    def json(self) -> Any:
        return json.loads(self.body) if self.body else None


def http_request(
        method: str,
        url: str,
        data: Optional[dict[str, Any]] = None,
        timeout: float = 30) -> HttpResult:
    """Perform an HTTP request using urllib, returning status and body.

    HTTP error responses (4xx/5xx) are returned as an :class:`HttpResult`
    rather than raised, so callers can assert on status codes.
    """
    payload = None
    headers: dict[str, str] = {}
    if data is not None:
        payload = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=payload, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return HttpResult(resp.status, resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return HttpResult(e.code, e.read().decode("utf-8"))


class FunctionApp:
    """Manages the lifecycle of a ``func start`` host for a sample app.

    Use as a context manager. On entry it starts the host and blocks until the
    app's ``/api/ping`` route responds; on exit it terminates the whole process
    group and surfaces the captured host log if startup failed.
    """

    def __init__(self, app_name: str, port: Optional[int] = None):
        self.app_dir = APPS_DIR / app_name
        if not self.app_dir.is_dir():
            raise FileNotFoundError(f"Sample app not found: {self.app_dir}")
        self.port = port or find_free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self._process: Optional[subprocess.Popen[str]] = None
        self._log_path = self.app_dir / "_func_host.log"

    @property
    def venv_dir(self) -> Path:
        """The in-app virtual environment directory (``<app>/.venv``)."""
        return self.app_dir / ".venv"

    @property
    def venv_python(self) -> Path:
        """Path to the in-app venv's Python interpreter."""
        if os.name == "nt":
            return self.venv_dir / "Scripts" / "python.exe"
        return self.venv_dir / "bin" / "python"

    def __enter__(self) -> "FunctionApp":
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()

    def start(self) -> None:
        func = func_executable()
        if func is None:
            raise RuntimeError("Azure Functions Core Tools ('func') is not installed.")
        if not self.venv_python.exists():
            raise RuntimeError(
                f"In-app virtual environment not found at {self.venv_dir}. "
                "Provision it first (run the suite via 'nox -s functions_e2e', "
                "which creates a .venv inside each sample app).")

        env = os.environ.copy()
        # Start the host exactly as a developer would after activating the
        # app's OWN virtual environment (``<app>/.venv``). The environment MUST
        # live inside the app directory: the Functions Python worker only
        # prioritizes the app's dependencies (our grpc/protobuf) over its
        # bundled copies when it can locate them relative to the app dir. With
        # the venv outside, both protobuf C-extensions load and the worker
        # crashes natively during indexing. We replicate activation rather than
        # using host-setting overrides (defaultExecutablePath, etc.), which
        # interfere with ``func``'s own Python version detection.
        interpreter_dir = self.venv_python.parent
        env["VIRTUAL_ENV"] = str(self.venv_dir)
        env["PATH"] = str(interpreter_dir) + os.pathsep + env.get("PATH", "")
        # A stray PYTHONHOME would override the venv; activation clears it.
        env.pop("PYTHONHOME", None)

        self._log = open(self._log_path, "w", encoding="utf-8")
        # start_new_session/CREATE_NEW_PROCESS_GROUP lets us reliably terminate
        # the host *and* its child worker processes as a group on teardown.
        creationflags = 0
        start_new_session = False
        if os.name == "nt":
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[attr-defined]
        else:
            start_new_session = True

        self._process = subprocess.Popen(
            [func, "start", "--port", str(self.port)],
            cwd=str(self.app_dir),
            env=env,
            stdout=self._log,
            stderr=subprocess.STDOUT,
            text=True,
            creationflags=creationflags,
            start_new_session=start_new_session,
        )

        try:
            self._wait_until_ready()
        except Exception:
            self.stop()
            raise

    def _wait_until_ready(self) -> None:
        deadline = time.time() + HOST_STARTUP_TIMEOUT_S
        ping_url = f"{self.base_url}/api/ping"
        while time.time() < deadline:
            if self._process is not None and self._process.poll() is not None:
                raise RuntimeError(
                    f"Functions host exited early (code {self._process.returncode}).\n"
                    f"{self._read_log()}")
            self._check_log_for_fatal_errors()
            try:
                result = http_request("GET", ping_url, timeout=5)
                if result.status == 200:
                    return
            except (urllib.error.URLError, OSError):
                pass
            time.sleep(1)
        raise TimeoutError(
            f"Functions host did not become ready within {HOST_STARTUP_TIMEOUT_S}s.\n"
            f"{self._read_log()}")

    # Markers that indicate the host aborted startup (e.g. the app failed to
    # import). Detecting these lets us fail fast with the log rather than
    # blocking for the full startup timeout.
    _FATAL_LOG_MARKERS = (
        "Host startup operation has been canceled",
        "Worker failed to load",
        "Microsoft.Azure.WebJobs.Script: Worker was unable to load",
    )

    def _check_log_for_fatal_errors(self) -> None:
        log = self._read_log()
        for marker in self._FATAL_LOG_MARKERS:
            if marker in log:
                raise RuntimeError(
                    f"Functions host failed to start (matched '{marker}').\n{log}")

    def _read_log(self) -> str:
        try:
            return "----- func host log -----\n" + self._log_path.read_text(encoding="utf-8")
        except OSError:
            return "(no host log captured)"

    def stop(self) -> None:
        proc = self._process
        if proc is not None:
            try:
                self._terminate_process_tree(proc)
            finally:
                self._process = None
        if getattr(self, "_log", None) is not None:
            self._log.close()

    @staticmethod
    def _terminate_process_tree(proc: "subprocess.Popen[str]") -> None:
        """Terminate the func host *and* its child worker processes.

        ``func start`` spawns children (the .NET host and the Python language
        worker); terminating only the top process can orphan them and, on a
        fixed port, block later runs. We first ask the process group to shut
        down gracefully, then force-kill the whole tree.
        """
        try:
            if os.name == "nt":
                proc.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
            else:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            proc.wait(timeout=20)
            return
        except subprocess.TimeoutExpired:
            pass
        except (ProcessLookupError, OSError):
            # Already exited, or the group signal could not be delivered.
            pass

        # Force-kill the entire process tree.
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                capture_output=True, check=False)
        else:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (ProcessLookupError, OSError):
                proc.kill()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pass

    # -- orchestration helpers ---------------------------------------------

    def start_orchestration(self, name: str, body: Any = None) -> str:
        """Start an orchestration via the app's ``/api/start/{name}`` route.

        Returns the new instance ID.
        """
        result = http_request("POST", f"{self.base_url}/api/start/{name}", data={"input": body})
        assert result.status in (200, 202), f"start failed: {result.status} {result.body}"
        return result.json()["id"]

    def get_status(self, instance_id: str) -> dict[str, Any]:
        """Fetch orchestration status via the app's ``/api/status/{id}`` route."""
        result = http_request("GET", f"{self.base_url}/api/status/{instance_id}")
        assert result.status == 200, f"status failed: {result.status} {result.body}"
        return result.json()

    def wait_for_completion(
            self,
            instance_id: str,
            timeout: float = ORCHESTRATION_TIMEOUT_S) -> dict[str, Any]:
        """Poll status until the orchestration reaches a terminal state.

        The terminal-state comparison is case-insensitive so it works for both
        the v1 status names (``Completed``) and the durabletask enum names
        (``COMPLETED``).
        """
        terminal = {"completed", "failed", "terminated", "canceled"}
        deadline = time.time() + timeout
        status: dict[str, Any] = {}
        while time.time() < deadline:
            status = self.get_status(instance_id)
            runtime_status = (status.get("runtimeStatus") or "").lower()
            if runtime_status in terminal:
                return status
            time.sleep(0.5)
        raise TimeoutError(
            f"Orchestration {instance_id} did not complete within {timeout}s; "
            f"last status: {status}")
