# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import inspect

from azure.core.credentials import AccessToken

import durabletask.azuremanaged.extensions.serverless as serverless
import durabletask.azuremanaged.preview.ondemand_sandbox as sandbox
import durabletask.azuremanaged.preview.ondemand_sandbox.client as sandbox_client
import durabletask.azuremanaged.preview.ondemand_sandbox.worker as sandbox_worker
from durabletask.azuremanaged.preview.ondemand_sandbox import OnDemandSandboxWorker
from durabletask.azuremanaged.preview.ondemand_sandbox import OnDemandSandboxWorkerProfile
from durabletask.azuremanaged.preview.ondemand_sandbox import OnDemandSandboxWorkerProfileOptions
from durabletask.azuremanaged.preview.ondemand_sandbox import on_demand_sandbox_worker_profile
from durabletask.azuremanaged.preview.ondemand_sandbox.client import (
    build_image_ref,
    build_on_demand_sandbox_activity_declaration,
    build_on_demand_sandbox_worker_heartbeat,
    build_on_demand_sandbox_worker_start,
    build_profile_on_demand_sandbox_activity_declarations,
    resolve_activity_names,
)
from durabletask.azuremanaged.internal import serverless_activities_service_pb2 as pb
from durabletask.azuremanaged.internal import serverless_activities_service_pb2_grpc as stubs


def test_resolve_activity_names_trims_and_deduplicates() -> None:
    assert resolve_activity_names([" RemoteHello ", "", "RemoteHello", "Other"]) == [
        "RemoteHello",
        "Other",
    ]


def test_public_on_demand_sandbox_package_exports_customer_entrypoints_only() -> None:
    assert sandbox.__all__ == [
        "OnDemandSandboxWorker",
        "OnDemandSandboxWorkerProfile",
        "OnDemandSandboxWorkerProfileOptions",
        "OnDemandSandboxActivitiesClient",
        "on_demand_sandbox_worker_profile",
    ]
    assert sandbox.OnDemandSandboxWorker is OnDemandSandboxWorker
    assert sandbox.OnDemandSandboxWorkerProfile is OnDemandSandboxWorkerProfile
    assert sandbox.OnDemandSandboxWorkerProfileOptions is OnDemandSandboxWorkerProfileOptions
    assert not hasattr(sandbox, "serverless_activity")


def test_legacy_serverless_package_exports_compatibility_entrypoints_only() -> None:
    assert serverless.__all__ == [
        "ServerlessWorker",
        "ServerlessWorkerProfile",
        "ServerlessWorkerProfileOptions",
        "ServerlessActivitiesClient",
        "serverless_worker_profile",
    ]
    assert serverless.ServerlessWorker is OnDemandSandboxWorker
    assert serverless.ServerlessWorkerProfile is OnDemandSandboxWorkerProfile
    assert serverless.ServerlessWorkerProfileOptions is OnDemandSandboxWorkerProfileOptions


def test_build_profile_on_demand_sandbox_activity_declarations() -> None:
    @on_demand_sandbox_worker_profile("pytest-profile-a")
    class PytestProfileA(OnDemandSandboxWorkerProfile):
        def configure(self, options) -> None:
            options.container_image = "example.azurecr.io/python-worker:v1"
            options.image_pull_managed_identity_client_id = "image-pull-client-id"
            options.scheduler_managed_identity_client_id = "scheduler-client-id"
            options.cpu = "500m"
            options.memory = "1Gi"
            options.max_concurrent_activities = 3
            options.environment_variables["ON_DEMAND_SANDBOX_SAMPLE_MARKER"] = "custom-value"
            options.add_activity("PytestRemoteHello")

    declarations = [
        declaration for declaration in build_profile_on_demand_sandbox_activity_declarations()
        if declaration.worker_profile_id == "pytest-profile-a"
    ]

    declaration = declarations[0]
    assert list(declaration.activity_names) == ["PytestRemoteHello"]
    assert declaration.image.image_ref == "example.azurecr.io/python-worker:v1"
    assert declaration.image.managed_identity_client_id == "image-pull-client-id"
    assert declaration.scheduler_managed_identity_client_id == "scheduler-client-id"
    assert declaration.resources.cpu == "500m"
    assert declaration.resources.memory == "1Gi"
    assert declaration.max_concurrent_activities == 3
    assert declaration.environment_variables["ON_DEMAND_SANDBOX_SAMPLE_MARKER"] == "custom-value"
    assert list(declaration.entrypoint) == []
    assert list(declaration.cmd) == []


def test_build_profile_on_demand_sandbox_activity_declarations_rejects_activity_overlap() -> None:
    @on_demand_sandbox_worker_profile("pytest-overlap-profile-a")
    class PytestOverlapProfileA(OnDemandSandboxWorkerProfile):
        def configure(self, options: OnDemandSandboxWorkerProfileOptions) -> None:
            options.container_image = "example.azurecr.io/python-worker-a:v1"
            options.image_pull_managed_identity_client_id = "image-pull-client-id"
            options.scheduler_managed_identity_client_id = "scheduler-client-id"
            options.add_activity("PytestOverlapRemoteHello")

    @on_demand_sandbox_worker_profile("pytest-overlap-profile-b")
    class PytestOverlapProfileB(OnDemandSandboxWorkerProfile):
        def configure(self, options: OnDemandSandboxWorkerProfileOptions) -> None:
            options.container_image = "example.azurecr.io/python-worker-b:v1"
            options.image_pull_managed_identity_client_id = "image-pull-client-id"
            options.scheduler_managed_identity_client_id = "scheduler-client-id"
            options.add_activity("PytestOverlapRemoteHello")

    try:
        try:
            build_profile_on_demand_sandbox_activity_declarations()
        except ValueError as ex:
            assert "PytestOverlapRemoteHello" in str(ex)
            assert "pytest-overlap-profile-a" in str(ex)
            assert "pytest-overlap-profile-b" in str(ex)
        else:
            raise AssertionError("Expected overlapping on-demand sandbox activity ownership to fail.")
    finally:
        sandbox_client._worker_profiles.pop("pytest-overlap-profile-a", None)
        sandbox_client._worker_profiles.pop("pytest-overlap-profile-b", None)


def test_profile_options_add_activity_accepts_callable() -> None:
    def pytest_callable_remote_hello(_ctx, value):
        return value

    @on_demand_sandbox_worker_profile("pytest-callable-profile")
    class PytestCallableProfile(OnDemandSandboxWorkerProfile):
        def configure(self, options: OnDemandSandboxWorkerProfileOptions) -> None:
            options.container_image = "example.azurecr.io/python-worker:v1"
            options.image_pull_managed_identity_client_id = "image-pull-client-id"
            options.scheduler_managed_identity_client_id = "scheduler-client-id"
            options.add_activity(pytest_callable_remote_hello)

    try:
        declarations = [
            declaration for declaration in build_profile_on_demand_sandbox_activity_declarations()
            if declaration.worker_profile_id == "pytest-callable-profile"
        ]

        declaration = declarations[0]
        assert list(declaration.activity_names) == ["pytest_callable_remote_hello"]
    finally:
        sandbox_client._worker_profiles.pop("pytest-callable-profile", None)


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


def test_build_on_demand_sandbox_activity_declaration() -> None:
    declaration = build_on_demand_sandbox_activity_declaration(
        worker_profile_id="preview",
        activity_names=["RemoteHello"],
        container_image="example.azurecr.io/on-demand-sandbox-worker:v1",
        cpu="500m",
        memory="1Gi",
        environment_variables={
            "CUSTOM_ENV": "custom-value",
        },
        max_concurrent_activities=3,
        image_pull_managed_identity_client_id="image-pull-client-id",
        scheduler_managed_identity_client_id="scheduler-client-id",
        entrypoint=["python"],
        cmd=["/app/remote_worker.py"])

    assert declaration.worker_profile_id == "preview"
    assert list(declaration.activity_names) == ["RemoteHello"]
    assert declaration.image.image_ref == "example.azurecr.io/on-demand-sandbox-worker:v1"
    assert declaration.image.managed_identity_client_id == "image-pull-client-id"
    assert declaration.scheduler_managed_identity_client_id == "scheduler-client-id"
    assert declaration.resources.cpu == "500m"
    assert declaration.resources.memory == "1Gi"
    assert declaration.environment_variables["CUSTOM_ENV"] == "custom-value"
    assert declaration.max_concurrent_activities == 3
    assert list(declaration.entrypoint) == ["python"]
    assert list(declaration.cmd) == ["/app/remote_worker.py"]


def test_build_on_demand_sandbox_activity_declaration_accepts_adc_resource_quantities() -> None:
    for cpu, memory in [
        ("500m", "1024Mi"),
        ("0.5", "1Gi"),
        ("2", "2048"),
    ]:
        declaration = build_on_demand_sandbox_activity_declaration(
            worker_profile_id="preview",
            activity_names=["RemoteHello"],
            container_image="example.azurecr.io/on-demand-sandbox-worker:v1",
            image_pull_managed_identity_client_id="image-pull-client-id",
            scheduler_managed_identity_client_id="scheduler-client-id",
            cpu=cpu,
            memory=memory)

        assert declaration.resources.cpu == cpu
        assert declaration.resources.memory == memory


def test_build_on_demand_sandbox_activity_declaration_rejects_invalid_adc_resource_quantities() -> None:
    for cpu, memory, expected_message in [
        ("0", "1024Mi", "CPU"),
        ("0m", "1024Mi", "CPU"),
        ("500Mi", "1024Mi", "CPU"),
        ("500m", "0", "memory"),
        ("500m", "0Mi", "memory"),
        ("500m", "500m", "memory"),
    ]:
        try:
            build_on_demand_sandbox_activity_declaration(
                worker_profile_id="preview",
                activity_names=["RemoteHello"],
                container_image="example.azurecr.io/on-demand-sandbox-worker:v1",
                image_pull_managed_identity_client_id="image-pull-client-id",
                scheduler_managed_identity_client_id="scheduler-client-id",
                cpu=cpu,
                memory=memory)
        except ValueError as ex:
            assert expected_message in str(ex)
        else:
            raise AssertionError("Expected invalid resource quantity to fail.")


def test_build_on_demand_sandbox_activity_declaration_accepts_single_name() -> None:
    declaration = build_on_demand_sandbox_activity_declaration(
        worker_profile_id="preview",
        activity_names="RemoteHello",
        container_image="example.azurecr.io/on-demand-sandbox-worker:v1",
        image_pull_managed_identity_client_id="image-pull-client-id",
        scheduler_managed_identity_client_id="scheduler-client-id")

    assert list(declaration.activity_names) == ["RemoteHello"]


def test_build_on_demand_sandbox_activity_declaration_requires_scheduler_managed_identity_client_id() -> None:
    try:
        build_on_demand_sandbox_activity_declaration(
            worker_profile_id="preview",
            activity_names=["RemoteHello"],
            container_image="example.azurecr.io/on-demand-sandbox-worker:v1")
    except TypeError as ex:
        assert "scheduler_managed_identity_client_id" in str(ex)
    else:
        raise AssertionError("Expected missing scheduler managed identity client ID to fail.")


def test_build_on_demand_sandbox_activity_declaration_requires_image_pull_managed_identity_client_id() -> None:
    try:
        build_on_demand_sandbox_activity_declaration(
            worker_profile_id="preview",
            activity_names=["RemoteHello"],
            container_image="example.azurecr.io/on-demand-sandbox-worker:v1",
            scheduler_managed_identity_client_id="scheduler-client-id")
    except ValueError as ex:
        assert "ADC uses to pull the worker image" in str(ex)
    else:
        raise AssertionError("Expected missing image pull managed identity client ID to fail.")


def test_build_on_demand_sandbox_worker_start_and_heartbeat() -> None:
    start = build_on_demand_sandbox_worker_start(
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

    heartbeat = build_on_demand_sandbox_worker_heartbeat(1)
    assert heartbeat.heartbeat.active_activities_count == 1


def test_generated_stub_uses_on_demand_sandbox_rpc_paths() -> None:
    channel = _RecordingChannel()
    stub = stubs.OnDemandSandboxActivitiesStub(channel)

    assert stub is not None
    assert channel.methods == [
        "/microsoft.durabletask.ondemandsandbox.OnDemandSandboxActivities/ConnectOnDemandSandboxActivityWorker",
        "/microsoft.durabletask.ondemandsandbox.OnDemandSandboxActivities/DeclareOnDemandSandboxActivities",
        "/microsoft.durabletask.ondemandsandbox.OnDemandSandboxActivities/RemoveOnDemandSandboxActivityDeclaration",
    ]


def test_on_demand_sandbox_worker_constructor_does_not_expose_runtime_contract() -> None:
    assert list(inspect.signature(OnDemandSandboxWorker).parameters) == []


def test_on_demand_sandbox_worker_does_not_own_wakeup_server(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "http://localhost:8080")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")

    worker = OnDemandSandboxWorker()

    assert not hasattr(worker, "_serverless_wakeup_port")
    assert not hasattr(worker, "_serverless_wakeup_server")


def test_on_demand_sandbox_worker_reads_sandbox_environment_and_registered_activities(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "http://localhost:8080")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")
    monkeypatch.setenv("DTS_WORKER_PROFILE_ID", "env-profile")
    monkeypatch.setenv("DTS_ON_DEMAND_SANDBOX_MAX_ACTIVITIES", "7")
    monkeypatch.setenv("DTS_SUBSTRATE", "AcaSessionPool")
    monkeypatch.setenv("DTS_SANDBOX_ID", "env-sandbox")

    worker = OnDemandSandboxWorker()
    worker._registry.add_named_activity("EnvActivity", lambda _ctx, value: value)
    worker._registry.add_named_activity("OtherActivity", lambda _ctx, value: value)
    worker._configure_on_demand_sandbox_activity_filters()
    start = next(worker._registration_messages())

    assert worker._host_address == "http://localhost:8080"
    assert worker._on_demand_sandbox_token_credential is None
    assert worker._on_demand_sandbox_taskhub == "env-hub"
    assert worker._on_demand_sandbox_worker_profile_id == "env-profile"
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


def test_on_demand_sandbox_worker_uses_scheduler_channel_without_credential(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "https://example.scheduler")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")

    worker = OnDemandSandboxWorker()

    assert worker._secure_channel is True
    assert worker._on_demand_sandbox_token_credential is None


def test_on_demand_sandbox_worker_uses_managed_identity_credential_when_injected(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "https://example.scheduler")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")
    monkeypatch.setenv("DTS_AUTHENTICATION", "ManagedIdentity")
    monkeypatch.setenv("DTS_UMI_CLIENT_ID", "worker-client-id")
    monkeypatch.setattr(sandbox_worker, "ManagedIdentityCredential", _FakeManagedIdentityCredential)

    worker = OnDemandSandboxWorker()

    assert worker._secure_channel is True
    assert isinstance(worker._on_demand_sandbox_token_credential, _FakeManagedIdentityCredential)
    assert worker._on_demand_sandbox_token_credential.client_id == "worker-client-id"


def test_on_demand_sandbox_worker_requires_managed_identity_client_id_when_auth_enabled(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "https://example.scheduler")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")
    monkeypatch.setenv("DTS_AUTHENTICATION", "ManagedIdentity")
    monkeypatch.delenv("DTS_UMI_CLIENT_ID", raising=False)

    try:
        OnDemandSandboxWorker()
    except ValueError as ex:
        assert "DTS_UMI_CLIENT_ID" in str(ex)
    else:
        raise AssertionError("Expected missing managed identity client IDs to fail.")


def test_on_demand_sandbox_worker_requires_registered_activities(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "http://localhost:8080")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")

    worker = OnDemandSandboxWorker()

    try:
        worker._configure_on_demand_sandbox_activity_filters()
    except RuntimeError as ex:
        assert "registered activity" in str(ex)
    else:
        raise AssertionError("Expected missing registered activity names to fail.")


class _RecordingChannel:
    def __init__(self) -> None:
        self.methods: list[str] = []

    def stream_unary(self, method, *args, **kwargs):
        self.methods.append(method)
        return object()

    def unary_unary(self, method, *args, **kwargs):
        self.methods.append(method)
        return object()


class _FakeManagedIdentityCredential:
    def __init__(self, client_id=None):
        self.client_id = client_id

    def get_token(self, *scopes, **kwargs):
        return AccessToken("token", 9999999999)
