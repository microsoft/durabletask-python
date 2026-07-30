# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Nox sessions that mirror the repository's GitHub Actions validation.

Run ``nox -s ci`` for a representative local validation sweep. Use a focused
session while developing (for example, ``nox -s lint`` or
``nox -s core_tests-3.10``). The test sessions provision their required
emulators when possible.

Usage:

    nox -s lint
    nox -s typecheck_core
    nox -s typecheck_functions_package
    nox -s core_tests-3.10
    nox -s azuremanaged_tests-3.10
    nox -s functions_unit-3.13
    nox -s functions_e2e

``azure-functions>=2.3.0b2`` is published to PyPI and installed as a declared
dependency of ``azure-functions-durable``, so no local build is required.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tarfile
import time
import uuid
import zipfile
from pathlib import Path
from typing import Sequence

import nox

nox.options.reuse_existing_virtualenvs = True
nox.options.error_on_missing_interpreters = True

REPO_ROOT = Path(__file__).parent
AZUREMANAGED = REPO_ROOT / "durabletask-azuremanaged"
AZURE_FUNCTIONS_DURABLE = REPO_ROOT / "azure-functions-durable"
E2E_APPS_DIR = REPO_ROOT / "tests" / "azure-functions-durable" / "e2e" / "apps"
# Sample apps that need an in-app virtual environment for the E2E suite.
E2E_APPS = ("v1_style", "dtask_style", "tracing")
PYTHON_VERSIONS = ("3.10", "3.11", "3.12", "3.13", "3.14")
DEFAULT_CI_PYTHON = "3.10"


def _install_packages(session: nox.Session, editable: bool = False) -> None:
    """Install durabletask and the azure-functions-durable provider.

    ``azure-functions`` is pulled from PyPI as a declared dependency of the
    provider. When ``editable`` is set, the two local repo packages are
    installed with ``-e`` so source edits are picked up without reinstalling
    (and so ``nox -R`` stays fast).
    """
    if editable:
        session.install("-e", str(REPO_ROOT))
        session.install("-e", str(AZURE_FUNCTIONS_DURABLE))
    else:
        session.install(str(REPO_ROOT))
        session.install(str(AZURE_FUNCTIONS_DURABLE))


def _link_app_venv(session: nox.Session, app_dir: Path) -> None:
    """Point ``<app_dir>/.venv`` at the session virtualenv via a junction/symlink.

    The Azure Functions Python worker only prioritizes the app's dependencies
    over its own bundled ones (grpc/protobuf) when it can locate them *relative
    to the app directory*; a venv outside the app dir leaves the worker's
    bundled protobuf on the path alongside ours and crashes the worker natively
    during indexing.

    Rather than install a full venv per app (slow, and redone every run), we
    reuse the single session virtualenv and expose it inside each app dir as a
    directory junction (Windows) or symlink (POSIX) named ``.venv``. The link
    path is inside the app dir, which is what the worker checks, so isolation
    kicks in while installs happen only once.
    """
    link = app_dir / ".venv"
    target = session.virtualenv.location

    # Clear any stale link or real venv. Never rmtree a junction/symlink -- that
    # would delete the shared session venv it points to.
    is_junction = getattr(os.path, "isjunction", None)
    if is_junction is not None and is_junction(link):
        os.rmdir(link)
    elif os.path.islink(link):
        os.unlink(link)
    elif os.path.isdir(link):
        shutil.rmtree(link)
    elif os.path.exists(link):
        os.unlink(link)

    if os.name == "nt":
        subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), target],
            check=True, capture_output=True)
    else:
        os.symlink(target, str(link), target_is_directory=True)
    session.log(f"Linked {link} -> {target}")


def _is_port_open(port: int) -> bool:
    """Return whether a local TCP listener accepts connections on ``port``."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as connection:
        connection.settimeout(0.25)
        return connection.connect_ex(("127.0.0.1", port)) == 0


def _wait_for_ports(ports: Sequence[int], timeout: int = 30) -> bool:
    """Wait for every local port in ``ports`` to accept connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if all(_is_port_open(port) for port in ports):
            return True
        time.sleep(0.25)
    return False


def _pytest_arguments(
    default_target: str,
    posargs: Sequence[str],
) -> tuple[str, ...]:
    """Keep the default target when positional arguments are only pytest options."""
    if not posargs:
        return (default_target,)

    has_target = any(
        "::" in argument or Path(argument).exists()
        for argument in posargs
    )
    return tuple(posargs) if has_target else (default_target, *posargs)


def _start_azurite(
    session: nox.Session,
    ports: Sequence[int],
    arguments: Sequence[str],
) -> subprocess.Popen[bytes] | None:
    """Start Azurite unless another local instance already owns its ports."""
    open_ports = [port for port in ports if _is_port_open(port)]
    if len(open_ports) == len(ports):
        session.log("Using the Azurite instance already listening locally.")
        return None
    if open_ports:
        session.error(
            "Azurite has only some required ports open "
            f"({', '.join(str(port) for port in open_ports)}). Stop the "
            "existing instance or start one that exposes all required ports "
            f"({', '.join(str(port) for port in ports)}).")

    executable = shutil.which("azurite")
    if executable is None:
        session.error(
            "Azurite is required for this session. Install it with "
            "`npm install -g azurite` and run the session again.")

    command = [
        executable,
        *arguments,
        "--location",
        session.create_tmp(),
    ]
    if os.name == "nt" and executable.endswith(".ps1"):
        command = [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            *command,
        ]

    process = subprocess.Popen(command)
    if _wait_for_ports(ports):
        session.log("Started Azurite for this session.")
        return process

    _stop_process(process)
    session.error("Azurite did not become ready within 30 seconds.")
    return None


def _stop_process(process: subprocess.Popen[bytes] | None) -> None:
    """Stop a service process that this Nox session started."""
    if process is None:
        return

    if os.name == "nt":
        script = f"""
function Stop-ProcessTree([int] $processId) {{
    Get-CimInstance Win32_Process -Filter "ParentProcessId = $processId" |
        ForEach-Object {{ Stop-ProcessTree $_.ProcessId }}
    Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
}}
Stop-ProcessTree {process.pid}
"""
        subprocess.run(
            ["powershell", "-NoProfile", "-Command", script],
            check=True,
        )
    elif process.poll() is None:
        process.terminate()
    process.wait()


def _new_test_namespace(prefix: str) -> str:
    """Create an isolated namespace for a service-backed test session."""
    return f"{prefix}{uuid.uuid4().hex[:20]}"


def _start_dts_emulator(session: nox.Session) -> tuple[str, str]:
    """Start an isolated DTS emulator and return its container name and endpoint."""
    docker = shutil.which("docker")
    if docker is None:
        session.error(
            "Docker is required for this session. Install and start Docker, "
            "then run the session again.")

    docker_info = subprocess.run(
        [docker, "info"], capture_output=True, text=True, check=False)
    if docker_info.returncode != 0:
        session.error(
            "Docker must be running for this session. Start Docker and run the "
            f"session again.\n{docker_info.stderr.strip()}")

    container_name = _new_test_namespace("durabletask-python-dts-")
    subprocess.run(
        [
            docker,
            "run",
            "--name",
            container_name,
            "--rm",
            "-d",
            "-p",
            "127.0.0.1::8080",
            "mcr.microsoft.com/dts/dts-emulator:latest",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        published_port = subprocess.run(
            [docker, "port", container_name, "8080/tcp"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip().rsplit(":", maxsplit=1)[1]
        endpoint = f"http://127.0.0.1:{published_port}"
        if not _wait_for_ports((int(published_port),)):
            session.error("The DTS emulator did not become ready within 30 seconds.")
        session.log(f"Started isolated DTS emulator at {endpoint}.")
        return container_name, endpoint
    except BaseException:
        subprocess.run(
            [docker, "rm", "-f", container_name],
            check=False,
            capture_output=True,
        )
        raise


def _stop_dts_emulator(container_name: str) -> None:
    """Remove the disposable DTS emulator container."""
    docker = shutil.which("docker")
    if docker is not None:
        subprocess.run(
            [docker, "rm", "-f", container_name],
            check=False,
            capture_output=True,
        )


@nox.session
def lint(session: nox.Session) -> None:
    """Run the Flake8 checks required by the GitHub Actions workflows."""
    session.install("flake8")
    targets = session.posargs or (
        "durabletask",
        "durabletask-azuremanaged",
        "azure-functions-durable",
        "tests/durabletask",
        "tests/durabletask-azuremanaged",
        "tests/azure-functions-durable",
        "examples",
    )
    session.run("flake8", *targets)


@nox.session(python=["3.10"])
def typecheck_core(session: nox.Session) -> None:
    """Run strict pyright checks for the core and Azure Managed packages."""
    session.install("-r", "requirements.txt")
    session.install("-r", "examples/requirements.txt")
    for requirements in sorted((REPO_ROOT / "examples").glob("*/requirements.txt")):
        session.install("-r", str(requirements))
    session.install(
        "--force-reinstall",
        "--no-deps",
        str(REPO_ROOT),
        str(AZUREMANAGED),
    )
    session.install(f"{REPO_ROOT}[azure-blob-payloads,opentelemetry]")
    session.install("pyright")
    session.run("pyright", *session.posargs)


@nox.session(python=["3.13"])
def typecheck_functions(session: nox.Session) -> None:
    """Run strict pyright checks for azure-functions-durable."""
    session.install("-r", "requirements.txt")
    _install_packages(session, editable=True)
    session.install("pyright")
    session.run(
        "pyright",
        "-p",
        "azure-functions-durable/pyrightconfig.json",
        *session.posargs,
    )


@nox.session(python=["3.13"])
def typecheck_functions_package(session: nox.Session) -> None:
    """Check the installed Functions and core wheels with strict Pyright."""
    session.install("build", "pyright")
    temp_dir = Path(session.create_tmp())
    artifact_dir = temp_dir / "functions-dist"
    core_artifact_dir = temp_dir / "core-dist"
    if artifact_dir.exists():
        shutil.rmtree(artifact_dir)
    if core_artifact_dir.exists():
        shutil.rmtree(core_artifact_dir)
    session.run(
        "python",
        "-m",
        "build",
        "--outdir",
        str(artifact_dir),
        str(AZURE_FUNCTIONS_DURABLE),
    )
    session.run(
        "python",
        "-m",
        "build",
        "--wheel",
        "--outdir",
        str(core_artifact_dir),
        str(REPO_ROOT),
    )

    wheels = list(artifact_dir.glob("azure_functions_durable-*.whl"))
    sdists = list(artifact_dir.glob("azure_functions_durable-*.tar.gz"))
    core_wheels = list(core_artifact_dir.glob("durabletask-*.whl"))
    if len(wheels) != 1 or len(sdists) != 1 or len(core_wheels) != 1:
        session.error(
            "Expected one provider wheel, provider source distribution, "
            "and core wheel")
    wheel = wheels[0]
    sdist = sdists[0]
    core_wheel = core_wheels[0]
    marker = "azure/durable_functions/py.typed"
    with zipfile.ZipFile(wheel) as archive:
        if marker not in archive.namelist():
            session.error(f"{marker} is missing from {wheel.name}")
    with tarfile.open(sdist, "r:gz") as archive:
        if not any(name.endswith(f"/{marker}") for name in archive.getnames()):
            session.error(f"{marker} is missing from {sdist.name}")

    session.install(str(core_wheel), str(wheel))
    session.run(
        "python",
        "-m",
        "pip",
        "install",
        "--force-reinstall",
        "--no-deps",
        str(core_wheel),
        str(wheel),
    )
    python_path = session.run(
        "python",
        "-c",
        "import sys; print(sys.executable)",
        silent=True,
    ).strip()
    session.run(
        "pyright",
        "--pythonpath",
        python_path,
        "-p",
        "tests/azure-functions-durable/typing/pyrightconfig.json",
    )


@nox.session(python=PYTHON_VERSIONS)
def core_tests(session: nox.Session) -> None:
    """Run core SDK tests against an automatically started Blob Azurite."""
    azurite = _start_azurite(
        session,
        ports=(10000,),
        arguments=("--silent", "--blobPort", "10000"),
    )
    try:
        session.install("-r", "requirements.txt")
        session.install("-e", f"{REPO_ROOT}[azure-blob-payloads]", "aiohttp")
        arguments = _pytest_arguments("tests/durabletask", session.posargs)
        session.run(
            "pytest",
            *arguments,
            "-m",
            "not dts",
            "--verbose",
        )
    finally:
        _stop_process(azurite)


@nox.session(python=PYTHON_VERSIONS)
def azuremanaged_tests(session: nox.Session) -> None:
    """Run Azure Managed tests against a disposable DTS emulator container."""
    container_name, endpoint = _start_dts_emulator(session)
    try:
        session.env["TASKHUB"] = "default"
        session.env["ENDPOINT"] = endpoint
        session.install("-r", "requirements.txt")
        session.install("-r", "examples/requirements.txt")
        session.install(
            "-e",
            str(REPO_ROOT),
            "-e",
            str(AZUREMANAGED),
        )
        arguments = _pytest_arguments(
            "tests/durabletask-azuremanaged",
            session.posargs,
        )
        session.run("pytest", *arguments, "-m", "dts", "--verbose")
    finally:
        _stop_dts_emulator(container_name)


@nox.session(python=["3.13", "3.14"])
def functions_unit(session: nox.Session) -> None:
    """Run the azure-functions-durable unit tests (no func/azurite required)."""
    session.install("-r", "requirements.txt")
    _install_packages(session, editable=True)
    session.install("pytest")
    arguments = _pytest_arguments("tests/azure-functions-durable", session.posargs)
    session.run(
        "pytest", *arguments,
        "-m", "not dts and not azurite and not functions_e2e",
        "--verbose",
    )


@nox.session(python=["3.13"])
def functions_e2e(session: nox.Session) -> None:
    """Run the azure-functions-durable end-to-end tests.

    Requires the Azure Functions Core Tools (``func``). The session starts an
    Azurite instance unless one is already running on all storage ports.

    The SDK is installed once (editable) into the session virtualenv, which is
    then exposed inside each sample app as ``<app>/.venv`` (see
    ``_link_app_venv``) so the Functions host worker loads our grpc/protobuf
    rather than its bundled copies. The harness starts ``func`` with that
    per-app ``.venv`` activated.
    """
    if shutil.which("func") is None:
        session.error(
            "Azure Functions Core Tools is required for this session. "
            "Install it from https://learn.microsoft.com/azure/azure-functions/functions-run-local.")

    azurite = _start_azurite(
        session,
        ports=(10000, 10001, 10002),
        arguments=(
            "--silent",
            "--skipApiVersionCheck",
            "--blobPort",
            "10000",
            "--queuePort",
            "10001",
            "--tablePort",
            "10002",
        ),
    )
    try:
        session.env[
            "AzureFunctionsJobHost__extensions__durableTask__hubName"
        ] = _new_test_namespace("nox")
        session.install("-r", "requirements.txt")
        _install_packages(session, editable=True)
        session.install("pytest", "opentelemetry-exporter-otlp-proto-grpc")
        for app in E2E_APPS:
            _link_app_venv(session, E2E_APPS_DIR / app)
        arguments = _pytest_arguments(
            "tests/azure-functions-durable/e2e",
            session.posargs,
        )
        session.run(
            "pytest", *arguments,
            "-m", "functions_e2e",
        )
    finally:
        session.log(
            "Functions host logs are available at "
            "tests/azure-functions-durable/e2e/apps/*/_func_host.log."
        )
        _stop_process(azurite)


@nox.session(python=False)
def ci(session: nox.Session) -> None:
    """Run the representative local lint, type, test, emulator, and E2E checks.

    Pass a core/Azure Managed Python version after ``--`` to override the
    default representative version, for example ``nox -s ci -- 3.14``.
    """
    if len(session.posargs) > 1 or (
        session.posargs and session.posargs[0] not in PYTHON_VERSIONS
    ):
        session.error(
            "Pass at most one supported Python version after `--`: "
            f"{', '.join(PYTHON_VERSIONS)}.")

    python_version = session.posargs[0] if session.posargs else DEFAULT_CI_PYTHON
    session.notify("lint", ())
    session.notify("typecheck_core", ())
    session.notify("typecheck_functions", ())
    session.notify("typecheck_functions_package", ())
    session.notify(f"core_tests-{python_version}", ())
    session.notify(f"azuremanaged_tests-{python_version}", ())
    session.notify("functions_unit-3.13", ())
    session.notify("functions_e2e", ())
