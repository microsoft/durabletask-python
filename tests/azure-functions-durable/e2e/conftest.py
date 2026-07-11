# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Fixtures for the Azure Functions Durable end-to-end suite.

The suite is gated behind the ``functions_e2e`` marker and requires:

- the Azure Functions Core Tools (``func``) on PATH, and
- a running Azurite instance (blob port 10000).

When either prerequisite is missing the whole module is skipped, so the suite
is a no-op for contributors who have not set up the local toolchain. In CI both
are provisioned before the suite runs.

Each sample app gets its own module-scoped Functions host so the two apps are
fully isolated and their hosts start/stop once per test module.
"""

import pytest

from ._harness import FunctionApp, azurite_is_running, func_executable


def _require_prerequisites(app_name: str) -> None:
    if func_executable() is None:
        pytest.skip("Azure Functions Core Tools ('func') is not installed.")
    if not azurite_is_running():
        pytest.skip("Azurite is not running on the default blob port (10000).")
    if not FunctionApp(app_name).venv_python.exists():
        pytest.skip(
            f"In-app virtual environment for '{app_name}' is not provisioned. "
            "Run the suite via 'nox -s functions_e2e', which creates a .venv "
            "inside each sample app.")


@pytest.fixture(scope="module")
def v1_app():
    _require_prerequisites("v1_style")
    with FunctionApp("v1_style") as app:
        yield app


@pytest.fixture(scope="module")
def dtask_app():
    _require_prerequisites("dtask_style")
    with FunctionApp("dtask_style") as app:
        yield app
