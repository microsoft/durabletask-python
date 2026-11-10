# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Verify the repository's deprecation gate against the shared Functions codec."""

import os
from pathlib import Path
import subprocess
import sys


def test_codec_warning_filters_are_narrow(tmp_path: Path):
    probe = tmp_path / "test_warning_probe.py"
    probe.write_text(
        '''
import warnings

import pytest
import azure.functions._durable_functions as codec


NO_TYPE_ADVISORY = (
    "azure.functions df_loads was called without expected_type.  "
    "Pass the destination type to enable validation and prepare for strict typing "
    "(AZURE_FUNCTIONS_DURABLE_STRICT_TYPING=1).  This message will not be repeated."
)
LOOSE_CODEC_ADVISORY = (
    "azure.functions Durable JSON codec reconstructed a custom object via the "
    "loose-mode object_hook path.  Set AZURE_FUNCTIONS_DURABLE_STRICT_TYPING=1 "
    "and supply expected_type at decode call sites to enable type-validated "
    "deserialization.  This message will not be repeated."
)


class Payload:
    def to_json(self):
        return {"value": 1}

    @classmethod
    def from_json(cls, data):
        return cls()


def test_real_legacy_advisories_are_reported(monkeypatch):
    monkeypatch.setenv("AZURE_FUNCTIONS_DURABLE_STRICT_TYPING", "0")
    monkeypatch.setattr(codec, "_no_expected_type_notice_emitted", False)
    monkeypatch.setattr(codec, "_loose_codec_notice_emitted", False)
    assert isinstance(codec.df_loads(codec.df_dumps(Payload())), Payload)


@pytest.mark.parametrize("message", [
    "An unrelated azure.functions deprecation",
    "azure.functions df_loads was called without expected_type. Another warning",
    "azure.functions Durable JSON codec reconstructed a custom object via "
    "the loose-mode object_hook path. Another warning",
    NO_TYPE_ADVISORY + " More information",
    NO_TYPE_ADVISORY + "\\n",
    LOOSE_CODEC_ADVISORY + " More information",
    LOOSE_CODEC_ADVISORY + "\\n",
])
def test_other_codec_messages_are_errors(message):
    with pytest.raises(DeprecationWarning):
        warnings.warn_explicit(
            message, DeprecationWarning, "codec.py", 1,
            module="azure.functions._durable_functions", registry={})


@pytest.mark.parametrize("module", [
    "azure.functions.other",
    "azure.functions._durable_functions.child",
    "azure.functions._durable_functions\\n",
    "other_package.codec",
])
@pytest.mark.parametrize("message", [NO_TYPE_ADVISORY, LOOSE_CODEC_ADVISORY])
def test_known_messages_from_other_modules_are_errors(module, message):
    with pytest.raises(DeprecationWarning):
        warnings.warn_explicit(
            message, DeprecationWarning, "codec.py", 1,
            module=module, registry={})


def test_pending_deprecations_are_errors():
    with pytest.raises(PendingDeprecationWarning):
        warnings.warn_explicit(
            NO_TYPE_ADVISORY, PendingDeprecationWarning, "codec.py", 1,
            module="azure.functions._durable_functions", registry={})
''',
        encoding="utf-8",
    )
    env = os.environ.copy()
    env.pop("PYTHONWARNINGS", None)
    env.pop("PYTEST_ADDOPTS", None)
    env["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] = "1"
    config = Path(__file__).resolve().parents[2] / "pyproject.toml"
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-c", str(config), str(probe), "-q",
         "-p", "no:cacheprovider", "-p", "pytest_asyncio.plugin"],
        capture_output=True, text=True, env=env, check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "azure.functions df_loads was called without expected_type." in result.stdout
    assert "loose-mode object_hook path." in result.stdout
