# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Nox sessions for the azure-functions-durable test suites.

These sessions build **clean, isolated** virtualenvs so the Azure Functions host
worker loads a predictable dependency set. This matters for the end-to-end
suite: the Functions Python worker imports the app (and therefore durabletask +
its native grpc/protobuf) into its own process, and a polluted ambient
environment can cause hard-to-diagnose native failures during indexing. Running
through nox guarantees the same minimal environment locally and in CI.

Usage:

    nox -s functions_unit          # fast unit tests (no func/azurite needed)
    nox -s functions_e2e           # end-to-end tests (needs func + azurite)

``azure-functions>=2.3.0b2`` is published to PyPI and installed as a declared
dependency of ``azure-functions-durable``, so no local build is required.
"""

import os
import shutil
import subprocess

import nox

nox.options.reuse_existing_virtualenvs = True

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
AZURE_FUNCTIONS_DURABLE = os.path.join(REPO_ROOT, "azure-functions-durable")
E2E_APPS_DIR = os.path.join(
    REPO_ROOT, "tests", "azure-functions-durable", "e2e", "apps")
# Sample apps that need an in-app virtual environment for the E2E suite.
E2E_APPS = ("v1_style", "dtask_style")


def _install_packages(session: nox.Session, editable: bool = False) -> None:
    """Install durabletask and the azure-functions-durable provider.

    ``azure-functions`` is pulled from PyPI as a declared dependency of the
    provider. When ``editable`` is set, the two local repo packages are
    installed with ``-e`` so source edits are picked up without reinstalling
    (and so ``nox -R`` stays fast).
    """
    if editable:
        session.install("-e", REPO_ROOT)
        session.install("-e", AZURE_FUNCTIONS_DURABLE)
    else:
        session.install(REPO_ROOT)
        session.install(AZURE_FUNCTIONS_DURABLE)


def _link_app_venv(session: nox.Session, app_dir: str) -> None:
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
    link = os.path.join(app_dir, ".venv")
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
            ["cmd", "/c", "mklink", "/J", link, target],
            check=True, capture_output=True)
    else:
        os.symlink(target, link, target_is_directory=True)
    session.log(f"Linked {link} -> {target}")


@nox.session(python=["3.13"])
def functions_unit(session: nox.Session) -> None:
    """Run the azure-functions-durable unit tests (no func/azurite required)."""
    _install_packages(session)
    session.install("pytest")
    session.run(
        "pytest", "tests/azure-functions-durable",
        "-m", "not dts and not azurite and not functions_e2e",
        *session.posargs)


@nox.session(python=["3.13"])
def functions_e2e(session: nox.Session) -> None:
    """Run the azure-functions-durable end-to-end tests.

    Requires the Azure Functions Core Tools (``func``) and a running Azurite
    instance; the suite skips itself when either is unavailable.

    The SDK is installed once (editable) into the session virtualenv, which is
    then exposed inside each sample app as ``<app>/.venv`` (see
    ``_link_app_venv``) so the Functions host worker loads our grpc/protobuf
    rather than its bundled copies. The harness starts ``func`` with that
    per-app ``.venv`` activated.
    """
    _install_packages(session, editable=True)
    session.install("pytest")
    for app in E2E_APPS:
        _link_app_venv(session, os.path.join(E2E_APPS_DIR, app))
    session.run(
        "pytest", "tests/azure-functions-durable/e2e",
        "-m", "functions_e2e",
        *session.posargs)
