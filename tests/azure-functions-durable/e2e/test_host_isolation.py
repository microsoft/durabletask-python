# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import os
import re

import pytest

from ._harness import FunctionApp


def test_sample_hosts_use_independent_task_hubs(monkeypatch: pytest.MonkeyPatch) -> None:
    setting = "AzureFunctionsJobHost__extensions__durableTask__hubName"
    monkeypatch.setenv(setting, "sharedhub")
    apps = [FunctionApp(name) for name in (
        "v1_style", "dtask_style", "tracing", "v1_style")]
    environments = [
        app._build_env()  # pyright: ignore[reportPrivateUsage]
        for app in apps
    ]
    task_hubs = [env[setting] for env in environments]

    assert len(set(task_hubs)) == len(apps)
    assert all(re.fullmatch(r"[A-Za-z][A-Za-z0-9]{2,49}", hub) for hub in task_hubs)
    assert "sharedhub" not in task_hubs
    assert os.environ[setting] == "sharedhub"
    for app, env in zip(apps, environments):
        assert app._build_env()[setting] == env[setting]  # pyright: ignore[reportPrivateUsage]
