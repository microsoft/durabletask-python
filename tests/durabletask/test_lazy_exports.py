# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for the lazy public exports of the ``durabletask`` package.

The ``durabletask`` package initializer resolves its public names lazily (PEP
562) so that importing it - which also happens implicitly when importing
anything from the ``durabletask.azuremanaged`` distribution, because both share
the ``durabletask`` namespace - does not pull in the worker dependency graph.
These tests pin both that behavior and the public surface it has to preserve.
"""

import importlib
import subprocess
import sys

import pytest

import durabletask

EXPECTED_ALL = [
    "ActivityWorkItemFilter",
    "ConcurrencyOptions",
    "EntityWorkItemFilter",
    "GrpcChannelOptions",
    "GrpcRetryPolicyOptions",
    "LargePayloadStorageOptions",
    "OrchestrationWorkItemFilter",
    "PayloadStore",
    "VersioningOptions",
    "WorkItemFilters",
]

# Where each public name is actually defined. Declared independently of the
# package's own lazy-export table so that a wrong mapping is caught rather than
# mirrored back.
EXPECTED_SOURCE_MODULES = {
    "ActivityWorkItemFilter": "durabletask.worker",
    "ConcurrencyOptions": "durabletask.worker",
    "EntityWorkItemFilter": "durabletask.worker",
    "GrpcChannelOptions": "durabletask.grpc_options",
    "GrpcRetryPolicyOptions": "durabletask.grpc_options",
    "LargePayloadStorageOptions": "durabletask.payload.store",
    "OrchestrationWorkItemFilter": "durabletask.worker",
    "PayloadStore": "durabletask.payload.store",
    "VersioningOptions": "durabletask.worker",
    "WorkItemFilters": "durabletask.worker",
}

# Modules that a client-only application should not have to pay for at import
# time. ``durabletask.worker`` is the entry point into the rest of them.
EAGERLY_IMPORTED_WORKER_MODULES = [
    "durabletask.worker",
    "durabletask.internal.type_discovery",
]


def _run_python(code: str) -> "subprocess.CompletedProcess[str]":
    """Run a snippet in a fresh interpreter so ``sys.modules`` starts clean."""
    return subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )


def _assert_ok(result: "subprocess.CompletedProcess[str]") -> None:
    assert result.returncode == 0, (
        f"subprocess failed:\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )


@pytest.mark.parametrize("module_name", EAGERLY_IMPORTED_WORKER_MODULES)
def test_importing_package_does_not_import_worker(module_name: str):
    """A bare ``import durabletask`` must not load the worker graph."""
    result = _run_python(
        "import sys\n"
        "import durabletask\n"
        f"assert {module_name!r} not in sys.modules, sorted(\n"
        "    m for m in sys.modules if m.startswith('durabletask')\n"
        ")\n"
    )
    _assert_ok(result)


def test_importing_package_does_not_import_grpc_or_protobuf():
    """The worker's heavyweight third-party dependencies stay unimported."""
    result = _run_python(
        "import sys\n"
        "import durabletask\n"
        "loaded = [\n"
        "    name\n"
        "    for name in ('grpc', 'google.protobuf', 'opentelemetry')\n"
        "    if name in sys.modules\n"
        "]\n"
        "assert not loaded, loaded\n"
    )
    _assert_ok(result)


def test_importing_azuremanaged_client_does_not_import_worker():
    """The Azure managed client shares the namespace but is client-only."""
    result = _run_python(
        "import sys\n"
        "import durabletask.azuremanaged.client\n"
        "assert 'durabletask.worker' not in sys.modules\n"
    )
    _assert_ok(result)


@pytest.mark.parametrize("name", EXPECTED_ALL)
def test_public_name_is_importable(name: str):
    """Every re-exported symbol is still reachable via ``from durabletask``."""
    result = _run_python(f"from durabletask import {name}\nassert {name} is not None\n")
    _assert_ok(result)


@pytest.mark.parametrize("name", EXPECTED_ALL)
def test_public_name_is_the_same_object_as_in_its_defining_module(name: str):
    """Lazy resolution returns the original object, not a copy or proxy."""
    module = importlib.import_module(EXPECTED_SOURCE_MODULES[name])
    assert getattr(durabletask, name) is getattr(module, name)


def test_all_is_unchanged():
    assert durabletask.__all__ == EXPECTED_ALL


def test_every_public_name_has_a_known_source_module():
    """``__all__`` and the expected source mapping must not drift apart."""
    assert sorted(EXPECTED_SOURCE_MODULES) == sorted(EXPECTED_ALL)


def test_package_name_is_unchanged():
    assert durabletask.PACKAGE_NAME == "durabletask"


def test_dir_includes_public_names():
    """``dir()`` reports lazy names even before they have been resolved."""
    result = _run_python(
        "import durabletask\n"
        f"missing = [name for name in {EXPECTED_ALL!r} if name not in dir(durabletask)]\n"
        "assert not missing, missing\n"
        "assert 'PACKAGE_NAME' in dir(durabletask)\n"
        "assert dir(durabletask) == sorted(dir(durabletask))\n"
    )
    _assert_ok(result)


def test_star_import_binds_all_public_names():
    result = _run_python(
        "import durabletask\n"
        "namespace = {}\n"
        "exec('from durabletask import *', namespace)\n"
        "missing = [name for name in durabletask.__all__ if name not in namespace]\n"
        "assert not missing, missing\n"
    )
    _assert_ok(result)


def test_unknown_attribute_raises_attribute_error():
    with pytest.raises(AttributeError) as excinfo:
        durabletask.ThisAttributeDoesNotExist  # type: ignore[attr-defined]

    assert str(excinfo.value) == (
        "module 'durabletask' has no attribute 'ThisAttributeDoesNotExist'"
    )


def test_hasattr_is_false_for_unknown_attribute():
    assert not hasattr(durabletask, "ThisAttributeDoesNotExist")


def test_submodules_remain_importable_from_the_package():
    """``__getattr__`` must not shadow normal submodule imports."""
    result = _run_python(
        "from durabletask import client, entities, task, worker\n"
        "assert task.__name__ == 'durabletask.task'\n"
    )
    _assert_ok(result)


def test_resolved_name_is_cached_on_the_module():
    result = _run_python(
        "import durabletask\n"
        "assert 'ConcurrencyOptions' not in vars(durabletask)\n"
        "durabletask.ConcurrencyOptions\n"
        "assert 'ConcurrencyOptions' in vars(durabletask)\n"
    )
    _assert_ok(result)


def test_importing_worker_first_does_not_break_the_package():
    """Guard against an import cycle: ``durabletask.worker`` imports the package."""
    result = _run_python(
        "import durabletask.worker\n"
        "from durabletask import ConcurrencyOptions\n"
        "assert ConcurrencyOptions is durabletask.worker.ConcurrencyOptions\n"
    )
    _assert_ok(result)
