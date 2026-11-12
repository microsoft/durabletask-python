# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Tests for deferred Function App dependency warnings."""

import logging
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from azure.durable_functions.internal import dependency_warning, invocation


@pytest.fixture(autouse=True)
def installed_version(monkeypatch):
    monkeypatch.setattr(dependency_warning, "version", lambda name: "2.0.0rc5")


def _warnings(caplog):
    return [record.getMessage() for record in caplog.records if record.levelno >= logging.WARNING]


@pytest.mark.parametrize(
    ("requirement", "warns"),
    [
        ("azure-functions-durable", True),
        ("azure-functions-durable>=2", True),
        ("azure-functions-durable>=2.0.0b1", True),
        ("azure-functions-durable>=2,<4", True),
        ("azure-functions-durable>=2,<3.1", True),
        ("azure-functions-durable>=2.0.0rc5,<3.0.0rc2", True),
        ("azure-functions-durable>=2.0.0rc5,<3.0.0rc0", True),
        ("azure-functions-durable>=2.0.0rc5,<3.0.0a0", True),
        ("azure-functions-durable>=2,<=3,!=3", False),
        ("--pre\nazure-functions-durable>=2.0.0rc5,<=3.0.0,!=3.0.0", True),
        (
            "azure-functions-durable>=2.0.0rc5,<3.0.0rc2,"
            "!=3.0.0.dev0,!=3.0.0a0,!=3.0.0b0,!=3.0.0rc0",
            True,
        ),
        ("azure-functions-durable!=3.*", True),
        ("azure-functions-durable!=3.0.0", True),
        ("azure-functions-durable<3", False),
        ("azure-functions-durable>=2.0.0b1,<3", False),
        ("azure-functions-durable>=2.0.0rc5,<3.0.0.dev0", False),
        ("azure-functions-durable==2.*", False),
        ("azure-functions-durable==2.0.0rc5", False),
        ("azure-functions-durable~=2.0", False),
        ("azure-functions-durable<=2.9.9", False),
        ("azure-functions-durable<4,!=3.*", False),
        ("azure-functions-durable>=3,<4", False),
        ("Azure.Functions_Durable[extra]>=2.0.0b1,<3  # pinned", False),
    ],
)
def test_requirements_warn_only_when_they_allow_a_later_major(tmp_path, caplog, requirement, warns):
    (tmp_path / "requirements.txt").write_text(requirement, encoding="utf-8")
    dependency_warning.warn_if_unbounded(str(tmp_path), logging.getLogger(__name__))

    messages = _warnings(caplog)
    assert bool(messages) is warns
    if warns:
        assert len(messages) == 1
        assert "replay-breaking changes" in messages[0]
        assert "an automatic upgrade could cause existing orchestrations to fail" in messages[0]
        assert "azure-functions-durable>=2.0.0rc5,<3" in messages[0]


def test_warning_tracks_the_installed_major(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(dependency_warning, "version", lambda name: "3.2.1")
    (tmp_path / "requirements.txt").write_text("azure-functions-durable>=3", encoding="utf-8")
    dependency_warning.warn_if_unbounded(str(tmp_path), logging.getLogger(__name__))
    assert "azure-functions-durable>=3.2.1,<4" in _warnings(caplog)[0]


def test_suggested_constraint_preserves_the_installed_epoch(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(dependency_warning, "version", lambda name: "1!2.3")
    (tmp_path / "requirements.txt").write_text("azure-functions-durable", encoding="utf-8")
    dependency_warning.warn_if_unbounded(str(tmp_path), logging.getLogger(__name__))
    assert "azure-functions-durable>=1!2.3,<1!3" in _warnings(caplog)[0]


def test_future_major_prerelease_preserves_the_installed_epoch(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(dependency_warning, "version", lambda name: "1!2.0rc5")
    (tmp_path / "requirements.txt").write_text(
        "azure-functions-durable>=1!2.0rc5,<1!3.0rc2", encoding="utf-8")
    dependency_warning.warn_if_unbounded(str(tmp_path), logging.getLogger(__name__))
    assert "azure-functions-durable>=1!2.0rc5,<1!3" in _warnings(caplog)[0]


def test_a_higher_major_in_an_older_epoch_is_not_an_upgrade(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(dependency_warning, "version", lambda name: "1!2.0")
    (tmp_path / "requirements.txt").write_text("azure-functions-durable>=2,<1!3", encoding="utf-8")
    dependency_warning.warn_if_unbounded(str(tmp_path), logging.getLogger(__name__))
    assert _warnings(caplog) == []


@pytest.mark.parametrize(
    ("contents", "warns"),
    [
        (
            'azure-functions-durable>=2 ; python_version >= "3.0"\n'
            'azure-functions-durable<3 ; python_version < "3.0"\n',
            True,
        ),
        (
            'azure-functions-durable>=2 ; python_version >= "3.0"\n'
            'azure-functions-durable<3 ; python_version >= "3.0"\n',
            False,
        ),
        (
            'azure-functions-durable ; python_version < "3.0"\n',
            False,
        ),
    ],
)
def test_environment_markers_apply_to_each_requirement(tmp_path, caplog, contents, warns):
    (tmp_path / "requirements.txt").write_text(contents, encoding="utf-8")
    dependency_warning.warn_if_unbounded(str(tmp_path), logging.getLogger(__name__))
    assert bool(_warnings(caplog)) is warns


@pytest.mark.parametrize(
    ("requirement_option", "constraint_option"),
    [
        ("-r nested/requirements.txt", "-c constraints.txt"),
        ("--requirement=nested/requirements.txt", "--constraint=constraints.txt"),
        ("-rnested/requirements.txt", "-cconstraints.txt"),
    ],
)
def test_local_requirements_and_constraints_are_combined(
        tmp_path, caplog, requirement_option, constraint_option):
    (tmp_path / "requirements.txt").write_text(
        f"{requirement_option}\n{constraint_option}\n", encoding="utf-8")
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "requirements.txt").write_text("azure-functions-durable\n", encoding="utf-8")
    (tmp_path / "constraints.txt").write_text("azure-functions-durable<3\n", encoding="utf-8")

    dependency_warning.warn_if_unbounded(str(tmp_path), logging.getLogger(__name__))
    assert _warnings(caplog) == []

    (tmp_path / "constraints.txt").write_text("azure-functions-durable>=2\n", encoding="utf-8")
    dependency_warning.warn_if_unbounded(str(tmp_path), logging.getLogger(__name__))
    assert len(_warnings(caplog)) == 1


def test_requirement_include_inside_constraint_resets_constraint_mode(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(dependency_warning, "version", lambda name: "2.1.0")
    (tmp_path / "requirements.txt").write_text("-c constraints.txt\n", encoding="utf-8")
    (tmp_path / "constraints.txt").write_text("-r actual.txt\n", encoding="utf-8")
    (tmp_path / "actual.txt").write_text("azure-functions-durable>=2.1.0\n", encoding="utf-8")

    dependency_warning.warn_if_unbounded(str(tmp_path), logging.getLogger(__name__))

    assert len(_warnings(caplog)) == 1


def test_pre_option_controls_candidate_prerelease_warnings(monkeypatch, tmp_path, caplog):
    monkeypatch.setattr(dependency_warning, "version", lambda name: "2.1.0")
    (tmp_path / "requirements.txt").write_text(
        "azure-functions-durable>=2,<=3,!=3\n", encoding="utf-8")

    dependency_warning.warn_if_unbounded(str(tmp_path), logging.getLogger(__name__))
    assert _warnings(caplog) == []

    (tmp_path / "requirements.txt").write_text(
        "--pre\nazure-functions-durable>=2,<=3,!=3\n", encoding="utf-8")
    dependency_warning.warn_if_unbounded(str(tmp_path), logging.getLogger(__name__))
    assert len(_warnings(caplog)) == 1


def test_duplicate_unbounded_requirements_are_not_a_pin(tmp_path, caplog):
    (tmp_path / "requirements.txt").write_text(
        "azure-functions-durable\nazure-functions-durable\n", encoding="utf-8")
    dependency_warning.warn_if_unbounded(str(tmp_path), logging.getLogger(__name__))
    assert len(_warnings(caplog)) == 1


@pytest.mark.parametrize("option", ["--no-index", "--pre", "-i https://example.invalid", "--index-url=https://example.invalid"])
def test_standalone_index_options_do_not_hide_unpinned_requirements(tmp_path, caplog, option):
    (tmp_path / "requirements.txt").write_text(f"{option}\nazure-functions-durable\n", encoding="utf-8")
    dependency_warning.warn_if_unbounded(str(tmp_path), logging.getLogger(__name__))
    assert len(_warnings(caplog)) == 1


def test_combined_index_options_do_not_hide_constraints(tmp_path, caplog):
    (tmp_path / "requirements.txt").write_text(
        "azure-functions-durable\n--no-index -c constraints.txt\n", encoding="utf-8")
    (tmp_path / "constraints.txt").write_text("azure-functions-durable<3\n", encoding="utf-8")
    dependency_warning.warn_if_unbounded(str(tmp_path), logging.getLogger(__name__))
    assert _warnings(caplog) == []


@pytest.mark.parametrize("lockfile", ["Pipfile.lock", "poetry.lock", "uv.lock", "pdm.lock", "pylock.toml"])
def test_existing_lockfile_does_not_hide_unpinned_requirements(tmp_path, caplog, lockfile):
    (tmp_path / "requirements.txt").write_text("azure-functions-durable", encoding="utf-8")
    (tmp_path / lockfile).write_text("", encoding="utf-8")
    dependency_warning.warn_if_unbounded(str(tmp_path), logging.getLogger(__name__))
    assert len(_warnings(caplog)) == 1
    assert "If a deployment installs these requirements without a lock" in _warnings(caplog)[0]


@pytest.mark.parametrize(
    "contents",
    [
        "azure-functions\n",
        "azure-functions-durable\n-r missing.txt\n",
        "azure-functions-durable\n-c https://example.invalid/constraints.txt\n",
        "azure-functions-durable\n-e .\n",
        "azure-functions-durable @ https://example.invalid/package.whl\n",
        "azure-functions-durable\n-r requirements.txt\n",
        "azure-functions-durable \\\n  --hash=sha256:example\n",
        "azure-functions-durable\ninvalid requirement @@\n",
        "azure-functions-durable; python_version ~= 'not-a-version'\n",
    ],
)
def test_unknown_requirements_do_not_claim_the_app_is_unpinned(tmp_path, caplog, contents):
    (tmp_path / "requirements.txt").write_text(contents, encoding="utf-8")
    dependency_warning.warn_if_unbounded(str(tmp_path), logging.getLogger(__name__))
    assert _warnings(caplog) == []


def test_unreadable_or_missing_requirements_are_diagnostic_only(tmp_path, caplog):
    caplog.set_level(logging.DEBUG)
    logger = logging.getLogger(__name__)
    dependency_warning.warn_if_unbounded(str(tmp_path), logger)
    assert "Cannot read Function App requirements" in caplog.text
    (tmp_path / "requirements.txt").write_bytes(b"\xff")
    dependency_warning.warn_if_unbounded(str(tmp_path), logger)
    assert _warnings(caplog) == []


def test_app_root_prefers_the_function_directory_over_an_unrelated_working_directory(monkeypatch, tmp_path):
    app = tmp_path / "app"
    other = tmp_path / "other"
    nested = app / "functions"
    nested.mkdir(parents=True)
    other.mkdir()
    (app / "host.json").touch()
    (other / "host.json").touch()
    monkeypatch.chdir(other)
    monkeypatch.delenv("AzureWebJobsScriptRoot", raising=False)
    checked = []
    monkeypatch.setattr(dependency_warning, "warn_if_unbounded", lambda root, logger: checked.append(root))

    dependency_warning.warn_for_function_directory(str(nested), logging.getLogger(__name__))

    assert checked == [str(app)]


@pytest.mark.parametrize("configured", [False, True])
def test_app_root_falls_back_to_the_configured_root_or_working_directory(monkeypatch, tmp_path, configured):
    app = tmp_path / "app"
    app.mkdir()
    (app / "host.json").touch()
    generated = tmp_path / "generated"
    generated.mkdir()
    monkeypatch.chdir(app if not configured else tmp_path)
    if configured:
        monkeypatch.setenv("AzureWebJobsScriptRoot", str(app))
    else:
        monkeypatch.delenv("AzureWebJobsScriptRoot", raising=False)
    checked = []
    monkeypatch.setattr(dependency_warning, "warn_if_unbounded", lambda root, logger: checked.append(root))

    dependency_warning.warn_for_function_directory(str(generated), logging.getLogger(__name__))

    assert checked == [str(app)]


def test_background_check_uses_the_host_sdk_log_category(monkeypatch, tmp_path):
    checked = []

    def check(directory, logger):
        checked.append((directory, logger.name))

    monkeypatch.setattr(dependency_warning, "warn_for_function_directory", check)
    invocation._check_dependency_in_background(str(tmp_path))
    assert checked == [(str(tmp_path), "azure.functions.durable")]


def test_app_indexing_does_not_import_the_dependency_check():
    code = (
        "import sys; import azure.durable_functions as df; df.DFApp(); "
        "assert 'azure.durable_functions.internal.dependency_warning' not in sys.modules"
    )
    subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)


def test_check_runs_once_in_the_background_after_an_invocation(monkeypatch, tmp_path):
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    returned = threading.Event()
    checked_directories = []
    context = SimpleNamespace(
        function_directory=str(tmp_path), invocation_id="invocation", thread_local_storage=threading.local())

    def check(directory):
        checked_directories.append(directory)
        started.set()
        try:
            release.wait(timeout=10)
        finally:
            finished.set()

    wrapped = invocation.wrap_invocation(lambda: "result")

    def invoke():
        assert wrapped(context=context) == "result"
        returned.set()

    monkeypatch.setattr(invocation, "_dependency_check_scheduled", False)
    monkeypatch.setattr(invocation, "_check_dependency_in_background", check)
    caller = threading.Thread(target=invoke)
    caller.start()
    try:
        assert started.wait(timeout=5)
        assert returned.wait(timeout=2), "The invocation waited for dependency inspection"
        with ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(lambda _: invoke(), range(32)))
        assert checked_directories == [str(tmp_path)]
    finally:
        release.set()
        caller.join(timeout=5)
        assert finished.wait(timeout=5)


def test_thread_start_failure_is_reported_once_without_failing_the_invocation(monkeypatch, tmp_path, caplog):
    class FailingThread:
        def __init__(self, **kwargs):
            pass

        def start(self):
            raise RuntimeError("thread unavailable")

    monkeypatch.setattr(invocation, "_dependency_check_scheduled", False)
    monkeypatch.setattr(invocation, "Thread", FailingThread)
    context = SimpleNamespace(
        function_directory=str(tmp_path), invocation_id="invocation", thread_local_storage=threading.local())
    wrapped = invocation.wrap_invocation(lambda: "result")
    for _ in range(2):
        assert wrapped(context=context) == "result"
    assert len(_warnings(caplog)) == 1
    assert "Unable to start the Durable Functions dependency check" in _warnings(caplog)[0]


@pytest.mark.parametrize("use_async", [False, True])
async def test_failed_invocations_still_schedule_the_check(monkeypatch, tmp_path, use_async):
    context = SimpleNamespace(
        function_directory=str(tmp_path), invocation_id="invocation", thread_local_storage=threading.local())
    checked = []
    monkeypatch.setattr(invocation, "_schedule_dependency_check", checked.append)

    def fail():
        raise ValueError("invocation failed")

    async def async_fail():
        fail()

    wrapped = invocation.wrap_invocation(async_fail if use_async else fail)
    with pytest.raises(ValueError, match="invocation failed"):
        if use_async:
            await wrapped(context=context)
        else:
            wrapped(context=context)

    assert checked == [context]
    assert invocation.get_invocation_id() is None
