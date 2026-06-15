# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import inspect

from azure.core.credentials import AccessToken

import durabletask.azuremanaged.preview.sandboxes as sandbox
import durabletask.azuremanaged.preview.sandboxes.worker_profiles as sandbox_worker_profiles
import durabletask.azuremanaged.preview.sandboxes.worker as sandbox_worker
from durabletask.azuremanaged.preview.sandboxes import SandboxWorker
from durabletask.azuremanaged.preview.sandboxes import SandboxActivitiesClient
from durabletask.azuremanaged.preview.sandboxes import SandboxWorkerProfile
from durabletask.azuremanaged.preview.sandboxes import SandboxWorkerProfileOptions
from durabletask.azuremanaged.preview.sandboxes import sandbox_worker_profile
from durabletask.azuremanaged.preview.sandboxes.worker_profiles import (
    _build_sandbox_worker_profile,
    build_sandbox_worker_heartbeat,
    build_sandbox_worker_start,
    build_sandbox_worker_profiles,
)
from durabletask.azuremanaged.preview.sandboxes.helpers import resolve_activities
from durabletask.azuremanaged.preview.sandboxes.helpers import SandboxActivity
from durabletask.azuremanaged.internal import sandbox_service_pb2 as pb
from durabletask.azuremanaged.internal import sandbox_service_pb2_grpc as stubs


def test_resolve_activities_trims_and_deduplicates() -> None:
    assert resolve_activities([
        SandboxActivity(" RemoteHello ", None),
        SandboxActivity("", None),
        SandboxActivity("RemoteHello", None),
        SandboxActivity("remotehello", None),
        SandboxActivity("Other", "v1"),
    ]) == [
        SandboxActivity("RemoteHello", None),
        SandboxActivity("Other", "v1"),
    ]


def test_public_sandbox_package_exports_customer_entrypoints_only() -> None:
    assert sandbox.__all__ == [
        "SandboxWorker",
        "SandboxActivity",
        "SandboxWorkerProfile",
        "SandboxWorkerProfileOptions",
        "SandboxActivitiesClient",
        "sandbox_worker_profile",
    ]
    assert sandbox.SandboxWorker is SandboxWorker
    assert sandbox.SandboxWorkerProfile is SandboxWorkerProfile
    assert sandbox.SandboxWorkerProfileOptions is SandboxWorkerProfileOptions
    legacy_prefix = "server" + "less"
    assert not hasattr(sandbox, f"{legacy_prefix}_activity")
    assert not hasattr(sandbox.SandboxActivitiesClient, f"enable_{legacy_prefix}_activities")
    assert not hasattr(sandbox.SandboxActivitiesClient, f"remove_{legacy_prefix}_activity_worker_profile")
    assert not hasattr(sandbox.SandboxActivitiesClient, f"connect_{legacy_prefix}_activity_worker")
    assert not hasattr(SandboxWorker, f"_configure_{legacy_prefix}_activity_filters")


def test_build_sandbox_worker_profiles() -> None:
    @sandbox_worker_profile("pytest-profile-a")
    class PytestProfileA(SandboxWorkerProfile):
        def configure(self, options) -> None:
            options.container_image = "example.azurecr.io/python-worker:v1"
            options.image_pull_managed_identity_client_id = "image-pull-client-id"
            options.scheduler_managed_identity_client_id = "scheduler-client-id"
            options.cpu = "500m"
            options.memory = "1Gi"
            options.max_concurrent_activities = 3
            options.environment_variables["SANDBOX_SAMPLE_MARKER"] = "custom-value"
            options.add_activity("PytestRemoteHello", version="v1")

    try:
        worker_profiles = [
            worker_profile for worker_profile in build_sandbox_worker_profiles()
            if worker_profile.worker_profile_id == "pytest-profile-a"
        ]

        worker_profile = worker_profiles[0]
        assert [(activity.name, activity.version) for activity in worker_profile.activities] == [
            ("PytestRemoteHello", "v1")]
        assert worker_profile.image.image_ref == "example.azurecr.io/python-worker:v1"
        assert worker_profile.image.managed_identity_client_id == "image-pull-client-id"
        assert worker_profile.scheduler_managed_identity_client_id == "scheduler-client-id"
        assert worker_profile.resources.cpu == "500m"
        assert worker_profile.resources.memory == "1Gi"
        assert worker_profile.max_concurrent_activities == 3
        assert worker_profile.environment_variables["SANDBOX_SAMPLE_MARKER"] == "custom-value"
        assert list(worker_profile.image.entrypoint) == []
        assert list(worker_profile.image.cmd) == []
    finally:
        sandbox_worker_profiles._worker_profiles.pop("pytest-profile-a", None)


def test_sandbox_worker_profile_requires_activity() -> None:
    try:
        try:
            @sandbox_worker_profile("pytest-empty-profile")
            class PytestEmptyProfile(SandboxWorkerProfile):
                def configure(self, options: SandboxWorkerProfileOptions) -> None:
                    options.container_image = "example.azurecr.io/python-worker:v1"
                    options.image_pull_managed_identity_client_id = "image-pull-client-id"
                    options.scheduler_managed_identity_client_id = "scheduler-client-id"
        except ValueError as ex:
            assert "pytest-empty-profile" in str(ex)
            assert "at least one activity" in str(ex)
        else:
            raise AssertionError("Expected empty sandbox worker profile to fail.")
    finally:
        sandbox_worker_profiles._worker_profiles.pop("pytest-empty-profile", None)


def test_build_sandbox_worker_profiles_rejects_activity_overlap() -> None:
    @sandbox_worker_profile("pytest-overlap-profile-a")
    class PytestOverlapProfileA(SandboxWorkerProfile):
        def configure(self, options: SandboxWorkerProfileOptions) -> None:
            options.container_image = "example.azurecr.io/python-worker-a:v1"
            options.image_pull_managed_identity_client_id = "image-pull-client-id"
            options.scheduler_managed_identity_client_id = "scheduler-client-id"
            options.add_activity("PytestOverlapRemoteHello", version=None)

    @sandbox_worker_profile("pytest-overlap-profile-b")
    class PytestOverlapProfileB(SandboxWorkerProfile):
        def configure(self, options: SandboxWorkerProfileOptions) -> None:
            options.container_image = "example.azurecr.io/python-worker-b:v1"
            options.image_pull_managed_identity_client_id = "image-pull-client-id"
            options.scheduler_managed_identity_client_id = "scheduler-client-id"
            options.add_activity("pytestoverlapremotehello", version=None)

    try:
        try:
            build_sandbox_worker_profiles()
        except ValueError as ex:
            assert "pytestoverlapremotehello" in str(ex)
            assert "pytest-overlap-profile-a" in str(ex)
            assert "pytest-overlap-profile-b" in str(ex)
        else:
            raise AssertionError("Expected overlapping sandbox activity ownership to fail.")
    finally:
        sandbox_worker_profiles._worker_profiles.pop("pytest-overlap-profile-a", None)
        sandbox_worker_profiles._worker_profiles.pop("pytest-overlap-profile-b", None)


def test_build_sandbox_worker_profiles_allows_same_activity_name_different_versions() -> None:
    @sandbox_worker_profile("pytest-version-profile-a")
    class PytestVersionProfileA(SandboxWorkerProfile):
        def configure(self, options: SandboxWorkerProfileOptions) -> None:
            options.container_image = "example.azurecr.io/python-worker-a:v1"
            options.image_pull_managed_identity_client_id = "image-pull-client-id"
            options.scheduler_managed_identity_client_id = "scheduler-client-id"
            options.add_activity("PytestVersionedActivity", version="v1")

    @sandbox_worker_profile("pytest-version-profile-b")
    class PytestVersionProfileB(SandboxWorkerProfile):
        def configure(self, options: SandboxWorkerProfileOptions) -> None:
            options.container_image = "example.azurecr.io/python-worker-b:v1"
            options.image_pull_managed_identity_client_id = "image-pull-client-id"
            options.scheduler_managed_identity_client_id = "scheduler-client-id"
            options.add_activity("PytestVersionedActivity", version="v2")

    try:
        worker_profiles = [
            worker_profile for worker_profile in build_sandbox_worker_profiles()
            if worker_profile.worker_profile_id in {"pytest-version-profile-a", "pytest-version-profile-b"}
        ]

        assert [worker_profile.worker_profile_id for worker_profile in worker_profiles] == [
            "pytest-version-profile-a",
            "pytest-version-profile-b",
        ]
        assert [(profile.activities[0].name, profile.activities[0].version) for profile in worker_profiles] == [
            ("PytestVersionedActivity", "v1"),
            ("PytestVersionedActivity", "v2"),
        ]
    finally:
        sandbox_worker_profiles._worker_profiles.pop("pytest-version-profile-a", None)
        sandbox_worker_profiles._worker_profiles.pop("pytest-version-profile-b", None)


def test_profile_options_add_activity_accepts_callable() -> None:
    def pytest_callable_remote_hello(_ctx, value):
        return value

    @sandbox_worker_profile("pytest-callable-profile")
    class PytestCallableProfile(SandboxWorkerProfile):
        def configure(self, options: SandboxWorkerProfileOptions) -> None:
            options.container_image = "example.azurecr.io/python-worker:v1"
            options.image_pull_managed_identity_client_id = "image-pull-client-id"
            options.scheduler_managed_identity_client_id = "scheduler-client-id"
            options.add_activity(pytest_callable_remote_hello, version=None)

    try:
        worker_profiles = [
            worker_profile for worker_profile in build_sandbox_worker_profiles()
            if worker_profile.worker_profile_id == "pytest-callable-profile"
        ]

        worker_profile = worker_profiles[0]
        assert [(activity.name, activity.version) for activity in worker_profile.activities] == [
            ("pytest_callable_remote_hello", "")]
    finally:
        sandbox_worker_profiles._worker_profiles.pop("pytest-callable-profile", None)


def test_build_sandbox_worker_profile() -> None:
    worker_profile = _build_sandbox_worker_profile(
        worker_profile_id="preview",
        activities=[SandboxActivity("RemoteHello", None)],
        container_image="example.azurecr.io/sandboxes-worker:v1",
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

    assert worker_profile.worker_profile_id == "preview"
    assert [(activity.name, activity.version) for activity in worker_profile.activities] == [
        ("RemoteHello", "")]
    assert worker_profile.image.image_ref == "example.azurecr.io/sandboxes-worker:v1"
    assert worker_profile.image.managed_identity_client_id == "image-pull-client-id"
    assert worker_profile.scheduler_managed_identity_client_id == "scheduler-client-id"
    assert worker_profile.resources.cpu == "500m"
    assert worker_profile.resources.memory == "1Gi"
    assert worker_profile.environment_variables["CUSTOM_ENV"] == "custom-value"
    assert worker_profile.max_concurrent_activities == 3
    assert list(worker_profile.image.entrypoint) == ["python"]
    assert list(worker_profile.image.cmd) == ["/app/remote_worker.py"]


def test_build_sandbox_worker_profile_accepts_adc_resource_quantities() -> None:
    for cpu, memory in [
        ("500m", "1024Mi"),
        ("0.5", "1Gi"),
        ("2", "2048"),
    ]:
        worker_profile = _build_sandbox_worker_profile(
            worker_profile_id="preview",
            activities=[SandboxActivity("RemoteHello", None)],
            container_image="example.azurecr.io/sandboxes-worker:v1",
            image_pull_managed_identity_client_id="image-pull-client-id",
            scheduler_managed_identity_client_id="scheduler-client-id",
            cpu=cpu,
            memory=memory)

        assert worker_profile.resources.cpu == cpu
        assert worker_profile.resources.memory == memory


def test_build_sandbox_worker_profile_rejects_invalid_adc_resource_quantities() -> None:
    for cpu, memory, expected_message in [
        ("0", "1024Mi", "CPU"),
        ("0m", "1024Mi", "CPU"),
        ("500.5m", "1024Mi", "CPU"),
        ("500Mi", "1024Mi", "CPU"),
        ("500m", "0", "memory"),
        ("500m", "0Mi", "memory"),
        ("500m", "500m", "memory"),
    ]:
        try:
            _build_sandbox_worker_profile(
                worker_profile_id="preview",
                activities=[SandboxActivity("RemoteHello", None)],
                container_image="example.azurecr.io/sandboxes-worker:v1",
                image_pull_managed_identity_client_id="image-pull-client-id",
                scheduler_managed_identity_client_id="scheduler-client-id",
                cpu=cpu,
                memory=memory)
        except ValueError as ex:
            assert expected_message in str(ex)
        else:
            raise AssertionError("Expected invalid resource quantity to fail.")


def test_build_sandbox_worker_profile_accepts_single_activity() -> None:
    worker_profile = _build_sandbox_worker_profile(
        worker_profile_id="preview",
        activities=[SandboxActivity("RemoteHello", None)],
        container_image="example.azurecr.io/sandboxes-worker:v1",
        image_pull_managed_identity_client_id="image-pull-client-id",
        scheduler_managed_identity_client_id="scheduler-client-id")

    assert [(activity.name, activity.version) for activity in worker_profile.activities] == [
        ("RemoteHello", "")]


def test_build_sandbox_worker_profile_accepts_activity_versions() -> None:
    worker_profile = _build_sandbox_worker_profile(
        worker_profile_id="preview",
        activities=[SandboxActivity("RemoteHello", "v1")],
        container_image="example.azurecr.io/sandboxes-worker:v1",
        image_pull_managed_identity_client_id="image-pull-client-id",
        scheduler_managed_identity_client_id="scheduler-client-id")

    assert [(activity.name, activity.version) for activity in worker_profile.activities] == [
        ("RemoteHello", "v1")]


def test_build_sandbox_worker_profile_requires_scheduler_managed_identity_client_id() -> None:
    try:
        _build_sandbox_worker_profile(
            worker_profile_id="preview",
            activities=[SandboxActivity("RemoteHello", None)],
            container_image="example.azurecr.io/sandboxes-worker:v1")
    except TypeError as ex:
        assert "scheduler_managed_identity_client_id" in str(ex)
    else:
        raise AssertionError("Expected missing scheduler managed identity client ID to fail.")


def test_build_sandbox_worker_profile_requires_image_pull_managed_identity_client_id() -> None:
    try:
        _build_sandbox_worker_profile(
            worker_profile_id="preview",
            activities=[SandboxActivity("RemoteHello", None)],
            container_image="example.azurecr.io/sandboxes-worker:v1",
            scheduler_managed_identity_client_id="scheduler-client-id")
    except ValueError as ex:
        assert "ADC uses to pull the worker image" in str(ex)
    else:
        raise AssertionError("Expected missing image pull managed identity client ID to fail.")


def test_build_sandbox_worker_start_and_heartbeat() -> None:
    start = build_sandbox_worker_start(
        taskhub="hub",
        worker_profile_id="preview",
        max_activities_count=2,
        activities=[SandboxActivity("RemoteHello", None)],
        sandbox_provider="Sandbox",
        dts_sandbox_identifier="sandbox-1")

    assert start.start.task_hub == "hub"
    assert start.start.worker_profile_id == "preview"
    assert start.start.max_activities_count == 2
    assert start.start.sandbox_provider == pb.SANDBOX_PROVIDER_KIND_SANDBOX
    assert start.start.dts_sandbox_identifier == "sandbox-1"
    assert [(activity.name, activity.version) for activity in start.start.activities] == [
        ("RemoteHello", "")]

    heartbeat = build_sandbox_worker_heartbeat(1)
    assert heartbeat.heartbeat.active_activities_count == 1


def test_generated_stub_uses_sandbox_rpc_paths() -> None:
    channel = _RecordingChannel()
    stub = stubs.SandboxActivitiesStub(channel)

    assert stub is not None
    assert channel.methods == [
        "/microsoft.durabletask.sandboxes.SandboxActivities/ConnectSandboxActivityWorker",
        "/microsoft.durabletask.sandboxes.SandboxActivities/DeclareSandboxWorkerProfile",
        "/microsoft.durabletask.sandboxes.SandboxActivities/RemoveSandboxWorkerProfile",
    ]


def test_sandbox_worker_constructor_does_not_expose_runtime_contract() -> None:
    assert list(inspect.signature(SandboxWorker).parameters) == []
    assert "_execute_activity" not in SandboxWorker.__dict__


def test_sandbox_activities_client_does_not_expose_worker_registration_rpc() -> None:
    assert not hasattr(SandboxActivitiesClient, "connect_sandbox_activity_worker")


def test_sandbox_worker_does_not_own_legacy_wakeup_server(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "http://localhost:8080")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")
    monkeypatch.setenv("DTS_WORKER_PROFILE_ID", "env-profile")
    monkeypatch.setenv("DTS_SANDBOX_PROVIDER", "Sandbox")
    _configure_sandbox_worker_auth(monkeypatch)

    worker = SandboxWorker()

    legacy_wakeup_prefix = "_server" + "less_wakeup"
    assert not hasattr(worker, f"{legacy_wakeup_prefix}_port")
    assert not hasattr(worker, f"{legacy_wakeup_prefix}_server")


def test_sandbox_worker_reads_sandbox_environment_and_registered_activities(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "http://localhost:8080")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")
    monkeypatch.setenv("DTS_WORKER_PROFILE_ID", "env-profile")
    monkeypatch.setenv("DTS_SANDBOX_MAX_ACTIVITIES", "7")
    monkeypatch.setenv("DTS_SANDBOX_PROVIDER", "AcaSessionPool")
    monkeypatch.setenv("DTS_SANDBOX_ID", "env-sandbox")
    _configure_sandbox_worker_auth(monkeypatch)

    def EnvActivity(_ctx, value):
        return value

    def OtherActivity(_ctx, value):
        return value

    worker = SandboxWorker()
    worker.add_activity(EnvActivity, version=None)
    worker.add_activity(OtherActivity, version=None)
    worker._configure_sandbox_activity_filters()
    start = next(worker._registration_messages())

    assert worker._sandbox_host_address == "http://localhost:8080"
    assert isinstance(worker._sandbox_token_credential, _FakeManagedIdentityCredential)
    assert worker._sandbox_taskhub == "env-hub"
    assert worker._sandbox_worker_profile_id == "env-profile"
    assert worker.concurrency_options.maximum_concurrent_activity_work_items == 7
    assert worker._work_item_filters is not None
    assert [activity.name for activity in worker._work_item_filters.activities] == [
        "EnvActivity",
        "OtherActivity",
    ]
    assert start.start.task_hub == "env-hub"
    assert start.start.worker_profile_id == "env-profile"
    assert start.start.max_activities_count == 7
    assert start.start.sandbox_provider == pb.SANDBOX_PROVIDER_KIND_ACA_SESSION_POOL
    assert start.start.dts_sandbox_identifier == "env-sandbox"
    assert [(activity.name, activity.version) for activity in start.start.activities] == [
        ("EnvActivity", ""),
        ("OtherActivity", ""),
    ]


def test_sandbox_worker_stop_keeps_handle_for_still_running_registration_thread(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "http://localhost:8080")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")
    monkeypatch.setenv("DTS_WORKER_PROFILE_ID", "env-profile")
    monkeypatch.setenv("DTS_SANDBOX_PROVIDER", "Sandbox")
    _configure_sandbox_worker_auth(monkeypatch)

    class StillRunningThread:
        def __init__(self):
            self.join_timeout = None

        def join(self, timeout=None):
            self.join_timeout = timeout

        def is_alive(self):
            return True

    worker = SandboxWorker()
    thread = StillRunningThread()
    worker._sandbox_registration_thread = thread

    worker._stop_sandbox_registration()

    assert thread.join_timeout == 10
    assert worker._sandbox_registration_stop.is_set()
    assert worker._sandbox_registration_thread is thread


def test_sandbox_worker_requires_managed_identity_authentication(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "https://example.scheduler")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")
    monkeypatch.setenv("DTS_WORKER_PROFILE_ID", "env-profile")
    monkeypatch.setenv("DTS_SANDBOX_PROVIDER", "Sandbox")
    monkeypatch.delenv("DTS_AUTHENTICATION", raising=False)
    monkeypatch.delenv("DTS_UMI_CLIENT_ID", raising=False)

    try:
        SandboxWorker()
    except ValueError as ex:
        assert "DTS_AUTHENTICATION" in str(ex)
        assert "ManagedIdentity" in str(ex)
    else:
        raise AssertionError("Expected missing DTS_AUTHENTICATION to fail.")


def test_sandbox_worker_rejects_invalid_authentication(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "https://example.scheduler")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")
    monkeypatch.setenv("DTS_WORKER_PROFILE_ID", "env-profile")
    monkeypatch.setenv("DTS_SANDBOX_PROVIDER", "Sandbox")
    monkeypatch.setenv("DTS_AUTHENTICATION", "DefaultAzureCredential")
    monkeypatch.setenv("DTS_UMI_CLIENT_ID", "worker-client-id")

    try:
        SandboxWorker()
    except ValueError as ex:
        assert "DTS_AUTHENTICATION" in str(ex)
        assert "ManagedIdentity" in str(ex)
    else:
        raise AssertionError("Expected invalid DTS_AUTHENTICATION to fail.")


def test_sandbox_worker_ignores_legacy_max_activities(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "https://example.scheduler")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")
    monkeypatch.setenv("DTS_WORKER_PROFILE_ID", "env-profile")
    monkeypatch.setenv("DTS_SANDBOX_PROVIDER", "Sandbox")
    monkeypatch.delenv("DTS_SANDBOX_MAX_ACTIVITIES", raising=False)
    monkeypatch.setenv("DTS_" + "SERVER" + "LESS_MAX_ACTIVITIES", "7")
    _configure_sandbox_worker_auth(monkeypatch)

    worker = SandboxWorker()

    assert worker.concurrency_options.maximum_concurrent_activity_work_items == 100


def test_sandbox_worker_rejects_invalid_max_activities(monkeypatch) -> None:
    for value in ["", "0", "-1", "many"]:
        monkeypatch.setenv("DTS_ENDPOINT", "https://example.scheduler")
        monkeypatch.setenv("DTS_TASK_HUB", "env-hub")
        monkeypatch.setenv("DTS_WORKER_PROFILE_ID", "env-profile")
        monkeypatch.setenv("DTS_SANDBOX_PROVIDER", "Sandbox")
        monkeypatch.setenv("DTS_SANDBOX_MAX_ACTIVITIES", value)
        _configure_sandbox_worker_auth(monkeypatch)

        try:
            SandboxWorker()
        except ValueError as ex:
            assert "DTS_SANDBOX_MAX_ACTIVITIES" in str(ex)
            assert "positive integer" in str(ex)
        else:
            raise AssertionError(f"Expected invalid DTS_SANDBOX_MAX_ACTIVITIES={value!r} to fail.")


def test_sandbox_worker_tracks_active_activity_count_with_hooks(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "https://example.scheduler")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")
    monkeypatch.setenv("DTS_WORKER_PROFILE_ID", "env-profile")
    monkeypatch.setenv("DTS_SANDBOX_PROVIDER", "Sandbox")
    _configure_sandbox_worker_auth(monkeypatch)

    worker = SandboxWorker()

    worker._durabletask_on_activity_execution_started(object())
    assert worker._sandbox_active_activities == 1

    worker._durabletask_on_activity_execution_completed(object())
    assert worker._sandbox_active_activities == 0

    worker._durabletask_on_activity_execution_completed(object())
    assert worker._sandbox_active_activities == 0


def test_sandbox_worker_uses_managed_identity_credential_when_injected(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "https://example.scheduler")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")
    monkeypatch.setenv("DTS_WORKER_PROFILE_ID", "env-profile")
    monkeypatch.setenv("DTS_SANDBOX_PROVIDER", "Sandbox")
    monkeypatch.setenv("DTS_AUTHENTICATION", "ManagedIdentity")
    monkeypatch.setenv("DTS_UMI_CLIENT_ID", "worker-client-id")
    monkeypatch.setattr(sandbox_worker, "ManagedIdentityCredential", _FakeManagedIdentityCredential)

    worker = SandboxWorker()

    assert worker._secure_channel is True
    assert isinstance(worker._sandbox_token_credential, _FakeManagedIdentityCredential)
    assert worker._sandbox_token_credential.client_id == "worker-client-id"


def test_sandbox_worker_requires_managed_identity_client_id_when_auth_enabled(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "https://example.scheduler")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")
    monkeypatch.setenv("DTS_WORKER_PROFILE_ID", "env-profile")
    monkeypatch.setenv("DTS_SANDBOX_PROVIDER", "Sandbox")
    monkeypatch.setenv("DTS_AUTHENTICATION", "ManagedIdentity")
    monkeypatch.delenv("DTS_UMI_CLIENT_ID", raising=False)

    try:
        SandboxWorker()
    except ValueError as ex:
        assert "DTS_UMI_CLIENT_ID" in str(ex)
    else:
        raise AssertionError("Expected missing managed identity client IDs to fail.")


def test_sandbox_worker_requires_registered_activities(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "http://localhost:8080")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")
    monkeypatch.setenv("DTS_WORKER_PROFILE_ID", "env-profile")
    monkeypatch.setenv("DTS_SANDBOX_PROVIDER", "Sandbox")
    _configure_sandbox_worker_auth(monkeypatch)

    worker = SandboxWorker()

    try:
        worker._configure_sandbox_activity_filters()
    except RuntimeError as ex:
        assert "registered activity" in str(ex)
    else:
        raise AssertionError("Expected missing registered activity names to fail.")


def test_sandbox_worker_requires_injected_sandbox_provider(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "https://example.scheduler")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")
    monkeypatch.setenv("DTS_WORKER_PROFILE_ID", "env-profile")
    monkeypatch.delenv("DTS_SANDBOX_PROVIDER", raising=False)
    _configure_sandbox_worker_auth(monkeypatch)

    try:
        SandboxWorker()
    except ValueError as ex:
        assert "DTS_SANDBOX_PROVIDER" in str(ex)
    else:
        raise AssertionError("Expected missing DTS_SANDBOX_PROVIDER to fail.")


def test_sandbox_worker_rejects_invalid_sandbox_provider(monkeypatch) -> None:
    monkeypatch.setenv("DTS_ENDPOINT", "https://example.scheduler")
    monkeypatch.setenv("DTS_TASK_HUB", "env-hub")
    monkeypatch.setenv("DTS_WORKER_PROFILE_ID", "env-profile")
    monkeypatch.setenv("DTS_SANDBOX_PROVIDER", "ContainerApp")
    _configure_sandbox_worker_auth(monkeypatch)

    try:
        SandboxWorker()
    except ValueError as ex:
        assert "Sandbox or AcaSessionPool" in str(ex)
    else:
        raise AssertionError("Expected invalid DTS_SANDBOX_PROVIDER to fail.")


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


def _configure_sandbox_worker_auth(monkeypatch) -> None:
    monkeypatch.setenv("DTS_AUTHENTICATION", "ManagedIdentity")
    monkeypatch.setenv("DTS_UMI_CLIENT_ID", "worker-client-id")
    monkeypatch.setattr(sandbox_worker, "ManagedIdentityCredential", _FakeManagedIdentityCredential)
