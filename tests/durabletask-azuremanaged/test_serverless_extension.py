# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import inspect

from durabletask.azuremanaged.extensions.serverless import (
    DurableTaskSchedulerServerlessWorker,
    build_image_ref,
    build_serverless_activity_declaration,
    build_serverless_worker_heartbeat,
    build_serverless_worker_start,
    resolve_activity_names,
)
from durabletask.azuremanaged.internal import serverless_activities_service_pb2 as pb


def test_resolve_activity_names_trims_and_deduplicates() -> None:
    assert resolve_activity_names([" RemoteHello ", "", "RemoteHello", "Other"]) == [
        "RemoteHello",
        "Other",
    ]


def test_build_image_ref_matches_dotnet_options() -> None:
    assert build_image_ref(container_image=" repo/image:tag ") == "repo/image:tag"
    assert build_image_ref(
        registry_server="example.azurecr.io",
        repository="worker",
        tag="v1") == "example.azurecr.io/worker:v1"
    assert build_image_ref(
        registry_server="example.azurecr.io",
        repository="worker",
        image_digest="sha256:abc") == "example.azurecr.io/worker@sha256:abc"


def test_build_serverless_activity_declaration() -> None:
    declaration = build_serverless_activity_declaration(
        worker_profile_id="preview",
        activity_names=["RemoteHello"],
        container_image="example.azurecr.io/serverless-worker:v1",
        cpu="500m",
        memory="1Gi",
        environment_variables={
            "CUSTOM_ENV": "custom-value",
        },
        max_concurrent_activities=3,
        entrypoint=["python"],
        cmd=["/app/remote_worker.py"])

    assert declaration.worker_profile_id == "preview"
    assert list(declaration.activity_names) == ["RemoteHello"]
    assert declaration.image.image_ref == "example.azurecr.io/serverless-worker:v1"
    assert declaration.image.public_pull is True
    assert declaration.resources.cpu == "500m"
    assert declaration.resources.memory == "1Gi"
    assert declaration.environment_variables["CUSTOM_ENV"] == "custom-value"
    assert declaration.max_concurrent_activities == 3
    assert list(declaration.entrypoint) == ["python"]
    assert list(declaration.cmd) == ["/app/remote_worker.py"]


def test_build_serverless_activity_declaration_preserves_public_pull() -> None:
    declaration = build_serverless_activity_declaration(
        worker_profile_id="preview",
        activity_names=["RemoteHello"],
        container_image="example.azurecr.io/serverless-worker:v1",
        public_pull=False)

    assert declaration.image.public_pull is False


def test_build_serverless_worker_start_and_heartbeat() -> None:
    start = build_serverless_worker_start(
        taskhub="hub",
        worker_profile_id="preview",
        max_activities_count=2,
        activity_names=["RemoteHello"],
        substrate="Sandbox",
        dts_sandbox_identifier="sandbox-1")

    assert start.start.task_hub == "hub"
    assert start.start.worker_profile_id == "preview"
    assert start.start.max_activities_count == 2
    assert start.start.substrate == pb.SUBSTRATE_KIND_SANDBOX
    assert start.start.dts_sandbox_identifier == "sandbox-1"
    assert list(start.start.activity_names) == ["RemoteHello"]

    heartbeat = build_serverless_worker_heartbeat(1)
    assert heartbeat.heartbeat.active_activities_count == 1


def test_serverless_worker_constructor_does_not_expose_runtime_contract() -> None:
    assert list(inspect.signature(DurableTaskSchedulerServerlessWorker).parameters) == []


def test_serverless_worker_does_not_own_wakeup_server(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "http://localhost:8080")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")

    worker = DurableTaskSchedulerServerlessWorker()

    assert not hasattr(worker, "_serverless_wakeup_port")
    assert not hasattr(worker, "_serverless_wakeup_server")


def test_serverless_worker_reads_sandbox_environment_and_registered_activities(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "http://localhost:8080")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")
    monkeypatch.setenv("DTS_WORKER_PROFILE_ID", "env-profile")
    monkeypatch.setenv("DTS_SERVERLESS_MAX_ACTIVITIES", "7")
    monkeypatch.setenv("DTS_SUBSTRATE", "AcaSessionPool")
    monkeypatch.setenv("DTS_SANDBOX_ID", "env-sandbox")

    worker = DurableTaskSchedulerServerlessWorker()
    worker._registry.add_named_activity("EnvActivity", lambda _ctx, value: value)
    worker._registry.add_named_activity("OtherActivity", lambda _ctx, value: value)
    worker._configure_serverless_activity_filters()
    start = next(worker._registration_messages())

    assert worker._host_address == "http://localhost:8080"
    assert worker._serverless_taskhub == "env-hub"
    assert worker._serverless_worker_profile_id == "env-profile"
    assert worker._concurrency_options.maximum_concurrent_activity_work_items == 7
    assert worker._work_item_filters is not None
    assert [activity.name for activity in worker._work_item_filters.activities] == [
        "EnvActivity",
        "OtherActivity",
    ]
    assert start.start.task_hub == "env-hub"
    assert start.start.worker_profile_id == "env-profile"
    assert start.start.max_activities_count == 7
    assert start.start.substrate == pb.SUBSTRATE_KIND_ACA_SESSION_POOL
    assert start.start.dts_sandbox_identifier == "env-sandbox"
    assert list(start.start.activity_names) == ["EnvActivity", "OtherActivity"]


def test_serverless_worker_requires_registered_activities(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "http://localhost:8080")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")

    worker = DurableTaskSchedulerServerlessWorker()

    try:
        worker._configure_serverless_activity_filters()
    except RuntimeError as ex:
        assert "registered activity" in str(ex)
    else:
        raise AssertionError("Expected missing registered activity names to fail.")
