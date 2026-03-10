# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, List, Optional, Sequence, TypeVar, Union

import durabletask.internal.helpers as helpers
import durabletask.internal.orchestrator_service_pb2 as pb
import durabletask.internal.shared as shared
from durabletask import task
from durabletask.internal.grpc_interceptor import (
    DefaultAsyncClientInterceptorImpl,
    DefaultClientInterceptorImpl,
)

if TYPE_CHECKING:
    from durabletask.client import (
        EntityQuery,
        OrchestrationQuery,
        OrchestrationState,
        OrchestrationStatus,
    )
    from durabletask.entities import EntityInstanceId

TInput = TypeVar('TInput')
TOutput = TypeVar('TOutput')


def prepare_sync_interceptors(
        metadata: Optional[list[tuple[str, str]]],
        interceptors: Optional[Sequence[shared.ClientInterceptor]]
) -> Optional[list[shared.ClientInterceptor]]:
    """Prepare the list of sync gRPC interceptors, adding a metadata interceptor if needed."""
    result: Optional[list[shared.ClientInterceptor]] = None
    if interceptors is not None:
        result = list(interceptors)
        if metadata is not None:
            result.append(DefaultClientInterceptorImpl(metadata))
    elif metadata is not None:
        result = [DefaultClientInterceptorImpl(metadata)]
    return result


def prepare_async_interceptors(
        metadata: Optional[list[tuple[str, str]]],
        interceptors: Optional[Sequence[shared.AsyncClientInterceptor]]
) -> Optional[list[shared.AsyncClientInterceptor]]:
    """Prepare the list of async gRPC interceptors, adding a metadata interceptor if needed."""
    result: Optional[list[shared.AsyncClientInterceptor]] = None
    if interceptors is not None:
        result = list(interceptors)
        if metadata is not None:
            result.append(DefaultAsyncClientInterceptorImpl(metadata))
    elif metadata is not None:
        result = [DefaultAsyncClientInterceptorImpl(metadata)]
    return result


def build_schedule_new_orchestration_req(
        orchestrator: Union[task.Orchestrator[TInput, TOutput], str], *,
        input: Optional[TInput],
        instance_id: Optional[str],
        start_at: Optional[datetime],
        reuse_id_policy: Optional[pb.OrchestrationIdReusePolicy],
        tags: Optional[dict[str, str]],
        version: Optional[str]) -> pb.CreateInstanceRequest:
    """Build a CreateInstanceRequest for scheduling a new orchestration."""
    name = orchestrator if isinstance(orchestrator, str) else task.get_name(orchestrator)
    return pb.CreateInstanceRequest(
        name=name,
        instanceId=instance_id if instance_id else uuid.uuid4().hex,
        input=helpers.get_string_value(shared.to_json(input) if input is not None else None),
        scheduledStartTimestamp=helpers.new_timestamp(start_at) if start_at else None,
        version=helpers.get_string_value(version),
        orchestrationIdReusePolicy=reuse_id_policy,
        tags=tags
    )


def build_query_instances_req(
        orchestration_query: OrchestrationQuery,
        continuation_token) -> pb.QueryInstancesRequest:
    """Build a QueryInstancesRequest from an OrchestrationQuery."""
    return pb.QueryInstancesRequest(
        query=pb.InstanceQuery(
            runtimeStatus=[status.value for status in orchestration_query.runtime_status] if orchestration_query.runtime_status else None,
            createdTimeFrom=helpers.new_timestamp(orchestration_query.created_time_from) if orchestration_query.created_time_from else None,
            createdTimeTo=helpers.new_timestamp(orchestration_query.created_time_to) if orchestration_query.created_time_to else None,
            maxInstanceCount=orchestration_query.max_instance_count,
            fetchInputsAndOutputs=orchestration_query.fetch_inputs_and_outputs,
            continuationToken=continuation_token
        )
    )


def build_purge_by_filter_req(
        created_time_from: Optional[datetime],
        created_time_to: Optional[datetime],
        runtime_status: Optional[List[OrchestrationStatus]],
        recursive: bool) -> pb.PurgeInstancesRequest:
    """Build a PurgeInstancesRequest for purging orchestrations by filter."""
    return pb.PurgeInstancesRequest(
        purgeInstanceFilter=pb.PurgeInstanceFilter(
            createdTimeFrom=helpers.new_timestamp(created_time_from) if created_time_from else None,
            createdTimeTo=helpers.new_timestamp(created_time_to) if created_time_to else None,
            runtimeStatus=[status.value for status in runtime_status] if runtime_status else None
        ),
        recursive=recursive
    )


def build_query_entities_req(
        entity_query: EntityQuery,
        continuation_token) -> pb.QueryEntitiesRequest:
    """Build a QueryEntitiesRequest from an EntityQuery."""
    return pb.QueryEntitiesRequest(
        query=pb.EntityQuery(
            instanceIdStartsWith=helpers.get_string_value(entity_query.instance_id_starts_with),
            lastModifiedFrom=helpers.new_timestamp(entity_query.last_modified_from) if entity_query.last_modified_from else None,
            lastModifiedTo=helpers.new_timestamp(entity_query.last_modified_to) if entity_query.last_modified_to else None,
            includeState=entity_query.include_state,
            includeTransient=entity_query.include_transient,
            pageSize=helpers.get_int_value(entity_query.page_size),
            continuationToken=continuation_token
        )
    )


def check_continuation_token(resp_token, prev_token, logger: logging.Logger) -> bool:
    """Check if a continuation token indicates more pages. Returns True to continue, False to stop."""
    if resp_token and resp_token.value and resp_token.value != "0":
        logger.info(f"Received continuation token with value {resp_token.value}, fetching next page...")
        if prev_token and prev_token.value and prev_token.value == resp_token.value:
            logger.warning(f"Received the same continuation token value {resp_token.value} again, stopping to avoid infinite loop.")
            return False
        return True
    return False


def log_completion_state(
        logger: logging.Logger,
        instance_id: str,
        state: Optional[OrchestrationState]):
    """Log the final state of a completed orchestration."""
    if not state:
        return
    # Compare against proto constants to avoid circular imports with client.py
    status_val = state.runtime_status.value
    if status_val == pb.ORCHESTRATION_STATUS_FAILED and state.failure_details is not None:
        details = state.failure_details
        logger.info(f"Instance '{instance_id}' failed: [{details.error_type}] {details.message}")
    elif status_val == pb.ORCHESTRATION_STATUS_TERMINATED:
        logger.info(f"Instance '{instance_id}' was terminated.")
    elif status_val == pb.ORCHESTRATION_STATUS_COMPLETED:
        logger.info(f"Instance '{instance_id}' completed.")


def build_raise_event_req(
        instance_id: str,
        event_name: str,
        data: Optional[Any] = None) -> pb.RaiseEventRequest:
    """Build a RaiseEventRequest for raising an orchestration event."""
    return pb.RaiseEventRequest(
        instanceId=instance_id,
        name=event_name,
        input=helpers.get_string_value(shared.to_json(data) if data is not None else None)
    )


def build_terminate_req(
        instance_id: str,
        output: Optional[Any] = None,
        recursive: bool = True) -> pb.TerminateRequest:
    """Build a TerminateRequest for terminating an orchestration."""
    return pb.TerminateRequest(
        instanceId=instance_id,
        output=helpers.get_string_value(shared.to_json(output) if output is not None else None),
        recursive=recursive
    )


def build_signal_entity_req(
        entity_instance_id: EntityInstanceId,
        operation_name: str,
        input: Optional[Any] = None) -> pb.SignalEntityRequest:
    """Build a SignalEntityRequest for signaling an entity."""
    return pb.SignalEntityRequest(
        instanceId=str(entity_instance_id),
        name=operation_name,
        input=helpers.get_string_value(shared.to_json(input) if input is not None else None),
        requestId=str(uuid.uuid4()),
        scheduledTime=None,
        parentTraceContext=None,
        requestTime=helpers.new_timestamp(datetime.now(timezone.utc))
    )
