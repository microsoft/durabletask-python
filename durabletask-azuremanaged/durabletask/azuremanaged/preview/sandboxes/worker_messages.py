# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from typing import Iterable, Optional

from durabletask.azuremanaged.internal import sandbox_service_pb2 as pb
from durabletask.azuremanaged.preview.sandboxes.helpers import SandboxActivity
from durabletask.azuremanaged.preview.sandboxes.helpers import resolve_activities


def build_sandbox_worker_start(
        *,
        taskhub: str,
        worker_profile_id: str,
        max_activities_count: int,
        activities: Iterable[SandboxActivity],
        sandbox_provider: Optional[str] = None,
        dts_sandbox_identifier: Optional[str] = None) -> pb.SandboxActivityWorkerMessage:
    if not taskhub or not taskhub.strip():
        raise ValueError("Sandbox activity worker registration requires a task hub name.")

    if not worker_profile_id or not worker_profile_id.strip():
        raise ValueError("Sandbox activity worker registration requires a worker profile ID.")

    if max_activities_count <= 0:
        raise ValueError("Sandbox activity worker max activity count must be greater than zero.")

    resolved_activities = resolve_activities(activities)
    if not resolved_activities:
        raise ValueError("Sandbox activity worker registration requires at least one registered activity.")

    message = pb.SandboxActivityWorkerMessage(
        start=pb.SandboxActivityWorkerStart(
            task_hub=taskhub.strip(),
            worker_profile_id=worker_profile_id.strip(),
            max_activities_count=max_activities_count,
            sandbox_provider=_parse_sandbox_provider(sandbox_provider),
            dts_sandbox_identifier=(dts_sandbox_identifier or "").strip()))
    message.start.activities.extend([
        pb.SandboxActivity(name=activity.name, version=activity.version or "")
        for activity in resolved_activities
    ])
    return message


def build_sandbox_worker_heartbeat(active_activities_count: int) -> pb.SandboxActivityWorkerMessage:
    if active_activities_count < 0:
        raise ValueError("Sandbox activity worker active activity count cannot be negative.")

    return pb.SandboxActivityWorkerMessage(
        heartbeat=pb.SandboxActivityWorkerHeartbeat(
            active_activities_count=active_activities_count))


def _parse_sandbox_provider(sandbox_provider: Optional[str]) -> "pb.SandboxProviderKind":
    if not sandbox_provider:
        return pb.SANDBOX_PROVIDER_KIND_UNSPECIFIED
    if sandbox_provider.lower() == "sandbox":
        return pb.SANDBOX_PROVIDER_KIND_SANDBOX
    if sandbox_provider.lower() == "acasessionpool":
        return pb.SANDBOX_PROVIDER_KIND_ACA_SESSION_POOL
    return pb.SANDBOX_PROVIDER_KIND_UNSPECIFIED
