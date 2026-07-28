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
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

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


def _wait_for_port(port: int, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.25)
    return False


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


class OtelCollector:
    """Run an OpenTelemetry Collector that writes received spans to JSON."""

    _IMAGE = "otel/opentelemetry-collector-contrib:0.131.1"

    def __init__(self, work_dir: Path):
        self.work_dir = work_dir
        self.config_path = work_dir / "collector.yaml"
        self.output_path = work_dir / "traces.json"
        self.port = find_free_port()
        self.endpoint = f"http://127.0.0.1:{self.port}"
        self.container_name = f"durabletask-tracing-{uuid.uuid4().hex[:12]}"

    @staticmethod
    def is_available() -> bool:
        docker = shutil.which("docker")
        if docker is None:
            return False
        result = subprocess.run(
            [docker, "info"],
            capture_output=True,
            check=False,
            text=True,
        )
        return result.returncode == 0

    def __enter__(self) -> "OtelCollector":
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()

    def start(self) -> None:
        docker = shutil.which("docker")
        if docker is None:
            raise RuntimeError("Docker is required for the tracing E2E test.")

        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.config_path.write_text(
            "receivers:\n"
            "  otlp:\n"
            "    protocols:\n"
            "      grpc:\n"
            "        endpoint: 0.0.0.0:4317\n"
            "exporters:\n"
            "  file:\n"
            "    path: /output/traces.json\n"
            "    format: json\n"
            "service:\n"
            "  pipelines:\n"
            "    traces:\n"
            "      receivers: [otlp]\n"
            "      exporters: [file]\n",
            encoding="utf-8",
        )

        command = [
            docker,
            "run",
            "--detach",
            "--rm",
            "--name",
            self.container_name,
            "--publish",
            f"127.0.0.1:{self.port}:4317",
            "--volume",
            f"{self.config_path.resolve()}:/etc/otelcol-contrib/config.yaml:ro",
            "--volume",
            f"{self.work_dir.resolve()}:/output",
            self._IMAGE,
        ]
        result = subprocess.run(
            command,
            capture_output=True,
            check=False,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"Failed to start OpenTelemetry Collector: {result.stderr}")
        if not _wait_for_port(self.port, timeout=60):
            logs = subprocess.run(
                [docker, "logs", self.container_name],
                capture_output=True,
                check=False,
                text=True,
            )
            self.stop()
            raise RuntimeError(
                f"OpenTelemetry Collector did not start:\n{logs.stdout}\n{logs.stderr}")

    def stop(self) -> None:
        docker = shutil.which("docker")
        if docker is None:
            return
        subprocess.run(
            [docker, "stop", "--time", "10", self.container_name],
            capture_output=True,
            check=False,
            text=True,
        )

    def get_spans(self) -> list[dict[str, Any]]:
        """Read and flatten all OTLP spans exported by the collector."""
        if not self.output_path.exists():
            return []

        spans: list[dict[str, Any]] = []
        for line in self.output_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            for resource_spans in payload.get("resourceSpans", []):
                for scope_spans in resource_spans.get("scopeSpans", []):
                    scope_name = scope_spans.get("scope", {}).get("name", "")
                    for span in scope_spans.get("spans", []):
                        spans.append({**span, "scopeName": scope_name})
        return spans


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


class _FatalStartupError(RuntimeError):
    """Raised when the host aborts startup for a non-transient reason.

    Signals that the app itself failed to load (e.g. an import error), so
    retrying on a different port would be pointless.
    """


class FunctionApp:
    """Manages the lifecycle of a ``func start`` host for a sample app.

    Use as a context manager. On entry it starts the host and blocks until the
    app's ``/api/ping`` route responds; on exit it terminates the whole process
    group and surfaces the captured host log if startup failed.
    """

    # ``func start`` binds the HTTP port itself, some time after we pick a free
    # one. Another process (e.g. the sibling app's host, started moments
    # earlier) can claim that port in the interim, so a transient startup
    # failure is retried on a freshly chosen free port a few times.
    _STARTUP_MAX_ATTEMPTS = 3

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

        env = self._build_env()

        last_exc: Optional[BaseException] = None
        for attempt in range(1, self._STARTUP_MAX_ATTEMPTS + 1):
            self._launch(func, env)
            try:
                self._wait_until_ready()
                return
            except _FatalStartupError:
                # The app itself failed to load; a different port won't help.
                self.stop()
                raise
            except (RuntimeError, TimeoutError) as exc:
                # Likely a port claimed between our selection and func's bind
                # (or a slow cold start). Tear down and retry on a fresh port.
                last_exc = exc
                self.stop()
                if attempt < self._STARTUP_MAX_ATTEMPTS:
                    self.port = find_free_port()
                    self.base_url = f"http://127.0.0.1:{self.port}"
        assert last_exc is not None
        raise last_exc

    def _build_env(self) -> dict[str, str]:
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
        return env

    def _launch(self, func: str, env: dict[str, str]) -> None:
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
                raise _FatalStartupError(
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

    def wait_for_status(
            self,
            instance_id: str,
            expected: str,
            timeout: float = ORCHESTRATION_TIMEOUT_S) -> dict[str, Any]:
        """Poll status until ``runtimeStatus`` equals ``expected`` (case-insensitive)."""
        target = expected.lower()
        deadline = time.time() + timeout
        status: dict[str, Any] = {}
        while time.time() < deadline:
            status = self.get_status(instance_id)
            if (status.get("runtimeStatus") or "").lower() == target:
                return status
            time.sleep(0.5)
        raise TimeoutError(
            f"Orchestration {instance_id} did not reach '{expected}' within "
            f"{timeout}s; last status: {status}")

    # -- event / entity helpers --------------------------------------------

    def raise_event(self, instance_id: str, event: str, data: Any = None) -> None:
        """Raise an external event via the app's ``/api/raise/{id}/{event}`` route."""
        result = http_request(
            "POST", f"{self.base_url}/api/raise/{instance_id}/{event}", data={"data": data})
        assert result.status == 202, f"raise failed: {result.status} {result.body}"

    def read_entity(self, name: str, key: str) -> dict[str, Any]:
        """Read entity state via the app's ``/api/entity/{name}/{key}`` route."""
        result = http_request("GET", f"{self.base_url}/api/entity/{name}/{key}")
        assert result.status == 200, f"entity read failed: {result.status} {result.body}"
        return result.json()

    def list_entities(self, starts_with: Optional[str] = None) -> dict[str, Any]:
        """List entities via the app's ``/api/entities`` route.

        An optional ``starts_with`` filters by the entity instance-id prefix
        (entity IDs are formatted ``@name@key``).
        """
        url = f"{self.base_url}/api/entities"
        if starts_with is not None:
            url += f"?starts_with={urllib.parse.quote(starts_with)}"
        result = http_request("GET", url)
        assert result.status == 200, f"list entities failed: {result.status} {result.body}"
        return result.json()

    def clean_entity_storage(self) -> dict[str, Any]:
        """Trigger entity storage cleanup via the app's ``/api/clean-entities`` route."""
        result = http_request("POST", f"{self.base_url}/api/clean-entities")
        assert result.status == 200, f"clean entities failed: {result.status} {result.body}"
        return result.json()

    def create_schedule(self, schedule_id: str, interval_seconds: float = 2,
                        input: Any = None) -> dict[str, Any]:
        """Create a scheduled task via the app's ``/api/schedule/{id}`` route."""
        result = http_request(
            "POST", f"{self.base_url}/api/schedule/{schedule_id}",
            data={"interval_seconds": interval_seconds, "input": input}, timeout=90)
        assert result.status == 200, f"create schedule failed: {result.status} {result.body}"
        return result.json()

    def describe_schedule(self, schedule_id: str) -> dict[str, Any]:
        """Describe a scheduled task via the app's ``/api/schedule/{id}`` route."""
        result = http_request("GET", f"{self.base_url}/api/schedule/{schedule_id}", timeout=90)
        assert result.status == 200, f"describe schedule failed: {result.status} {result.body}"
        return result.json()

    def delete_schedule(self, schedule_id: str) -> None:
        """Delete a scheduled task via the app's ``/api/schedule/{id}/delete`` route."""
        result = http_request(
            "POST", f"{self.base_url}/api/schedule/{schedule_id}/delete", timeout=90)
        assert result.status == 202, f"delete schedule failed: {result.status} {result.body}"

    def start_export(self, container: str = "exports",
                     job_id: Optional[str] = None,
                     completed_from: Optional[str] = None,
                     mode: Optional[str] = None) -> dict[str, Any]:
        """Start a history-export job via the app's ``/api/export/start`` route.

        ``completed_from`` (an ISO-8601 timestamp) narrows the export window so
        it only covers instances completed at/after that time. ``mode`` selects
        the export mode (``"batch"`` by default; ``"continuous"`` to exercise the
        Functions rejection path).
        """
        data: dict[str, Any] = {"container": container}
        if job_id is not None:
            data["job_id"] = job_id
        if completed_from is not None:
            data["completed_from"] = completed_from
        if mode is not None:
            data["mode"] = mode
        result = http_request("POST", f"{self.base_url}/api/export/start", data=data, timeout=90)
        assert result.status == 200, f"start export failed: {result.status} {result.body}"
        return result.json()

    def wait_for_export(self, job_id: str, timeout: float = 90) -> dict[str, Any]:
        """Poll ``/api/export/status/{id}`` until the export job is terminal."""
        deadline = time.time() + timeout
        payload: dict[str, Any] = {}
        while time.time() < deadline:
            result = http_request("GET", f"{self.base_url}/api/export/status/{job_id}")
            assert result.status == 200, f"export status failed: {result.status} {result.body}"
            payload = result.json()
            if (payload.get("status") or "") in ("Completed", "Failed"):
                return payload
            time.sleep(0.5)
        raise TimeoutError(f"export job {job_id} did not finish within {timeout}s; last: {payload}")

    def signal_entity(self, name: str, key: str, op: str, input: Any = None,
                      delay_seconds: Optional[float] = None) -> None:
        """Signal an entity via the app's ``/api/signal/{name}/{key}/{op}`` route.

        When ``delay_seconds`` is provided, the app schedules the signal for
        future delivery (a delayed/scheduled entity signal).
        """
        data: dict[str, Any] = {"input": input}
        if delay_seconds is not None:
            data["delay_seconds"] = delay_seconds
        result = http_request(
            "POST", f"{self.base_url}/api/signal/{name}/{key}/{op}", data=data)
        assert result.status == 202, f"signal failed: {result.status} {result.body}"

    def wait_for_entity(
            self,
            name: str,
            key: str,
            predicate: Callable[[dict[str, Any]], bool],
            timeout: float = 30) -> dict[str, Any]:
        """Poll the entity read route until ``predicate(payload)`` is true."""
        deadline = time.time() + timeout
        payload: dict[str, Any] = {}
        while time.time() < deadline:
            payload = self.read_entity(name, key)
            if predicate(payload):
                return payload
            time.sleep(0.5)
        raise TimeoutError(f"entity {name}/{key} predicate not met; last: {payload}")
