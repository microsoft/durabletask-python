# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import asyncio
import atexit
import json
import threading

from datetime import datetime, timedelta
from typing import Any, Mapping, Optional, Union, cast
from warnings import deprecated
import azure.functions as func
from urllib.parse import urlparse, quote

from durabletask.client import (
    AsyncTaskHubGrpcClient,
    OrchestrationQuery,
    OrchestrationStatus,
    TaskHubGrpcClient,
)
from durabletask.entities import EntityInstanceId
from durabletask.grpc_options import GrpcChannelOptions
from .internal.azurefunctions_grpc_interceptor import (
    AzureFunctionsAsyncDefaultClientInterceptorImpl,
    AzureFunctionsDefaultClientInterceptorImpl,
)
from .internal.serialization import DEFAULT_FUNCTIONS_DATA_CONVERTER
from .http.http_management_payload import HttpManagementPayload, replace_url_origin
from .internal.compat.durable_orchestration_status import DurableOrchestrationStatus
from .internal.compat.entity_state_response import EntityStateResponse
from .internal.compat.history_projection import project_history
from .internal.compat.orchestration_runtime_status import OrchestrationRuntimeStatus, to_durabletask_statuses
from .internal.compat.purge_history_result import PurgeHistoryResult


_sync_client_cache: dict[str, "SyncDurableFunctionsClient"] = {}
_sync_client_cache_lock = threading.Lock()


def _first_forwarded_value(value: str) -> str:
    return value.split(",", 1)[0].strip().strip('"')


def _get_request_origin(
        request: func.HttpRequest,
        use_forwarded_host: bool) -> str:
    request_url = urlparse(request.url)
    proto = request_url.scheme
    host = request_url.netloc
    if not use_forwarded_host:
        return f"{proto}://{host}"

    request_headers = cast(Mapping[str, str], request.headers)
    headers = {
        name.lower(): value for name, value in request_headers.items()
    }

    forwarded = headers.get("forwarded")
    if forwarded:
        forwarded_values: dict[str, str] = {}
        for pair in forwarded.split(",", 1)[0].split(";"):
            name, separator, value = pair.partition("=")
            if separator:
                forwarded_values[name.strip().lower()] = value.strip().strip('"')

        forwarded_proto = forwarded_values.get("proto")
        if forwarded_proto:
            proto = forwarded_proto
        forwarded_host = forwarded_values.get("host")
        if forwarded_host:
            return f"{proto}://{forwarded_host}"

    forwarded_proto = headers.get("x-forwarded-proto")
    if forwarded_proto:
        first_proto = _first_forwarded_value(forwarded_proto)
        if first_proto:
            proto = first_proto

    forwarded_host = headers.get("x-forwarded-host")
    if forwarded_host:
        first_host = _first_forwarded_value(forwarded_host)
        if first_host:
            host = first_host

    return f"{proto}://{host}"


def _build_http_management_payload(
        instance_id: str,
        management_urls: dict[str, str],
        base_url: str,
        http_base_url: str,
        required_query_string_parameters: str,
        use_forwarded_host: bool,
        request: func.HttpRequest | None) -> HttpManagementPayload:
    encoded_instance_id = quote(instance_id, safe="")
    configured_base_url = http_base_url or base_url
    request_origin: str | None = None
    if request is not None:
        request_origin = _get_request_origin(request, use_forwarded_host)
        if configured_base_url:
            management_base_url = replace_url_origin(
                configured_base_url.rstrip("/"), request_origin)
        else:
            management_base_url = (
                f"{request_origin}/runtime/webhooks/durabletask")
    else:
        management_base_url = configured_base_url.rstrip("/")
    instance_status_url = (
        f"{management_base_url}/instances/{encoded_instance_id}")

    return HttpManagementPayload(
        instance_id,
        instance_status_url,
        required_query_string_parameters,
        management_urls=management_urls,
        request_origin=request_origin)


# Client class used for Durable Functions
class DurableFunctionsClient(AsyncTaskHubGrpcClient):
    """A gRPC client passed to Durable Functions durable client bindings.

    Connects to the Durable Functions runtime using async gRPC and provides methods
    for creating and managing Durable orchestrations, interacting with Durable entities,
    and creating HTTP management payloads and check status responses for use with Durable Functions invocations.
    """
    taskHubName: str
    connectionName: str
    creationUrls: dict[str, str]
    managementUrls: dict[str, str]
    baseUrl: str
    requiredQueryStringParameters: str
    rpcBaseUrl: str
    httpBaseUrl: str
    useForwardedHost: bool
    maxGrpcMessageSizeInBytes: int
    # The host sends this as a .NET TimeSpan string; it is currently stored
    # as-received (see _parse_client_configuration) and is unused, so the raw
    # string form is permitted alongside the timedelta default.
    grpcHttpClientTimeout: timedelta | str

    def __init__(self, client_as_string: str):
        """Initializes a DurableFunctionsClient instance from a JSON string.

        This string will be provided by the Durable Functions host extension upon invocation of the client trigger.

        Args:
            client_as_string (str): A JSON string containing the Durable Functions client configuration.

        Raises:
            json.JSONDecodeError: If the provided string is not valid JSON.
        """
        self._parse_client_configuration(client_as_string)

        interceptors = [AzureFunctionsAsyncDefaultClientInterceptorImpl(self.taskHubName, self.requiredQueryStringParameters)]

        # Only override the gRPC message size limits when the host explicitly
        # provides a value. When unset (0), we leave the gRPC library defaults
        # in place rather than applying a large default of our own.
        channel_options: GrpcChannelOptions | None = None
        if self.maxGrpcMessageSizeInBytes > 0:
            channel_options = GrpcChannelOptions(
                max_receive_message_length=self.maxGrpcMessageSizeInBytes,
                max_send_message_length=self.maxGrpcMessageSizeInBytes)

        # We pass in None for the metadata so we don't construct an additional interceptor in the parent class
        # Since the parent class doesn't use anything metadata for anything else, we can set it as None
        super().__init__(
            host_address=self.rpcBaseUrl,
            secure_channel=False,
            metadata=None,
            interceptors=interceptors,
            channel_options=channel_options,
            data_converter=DEFAULT_FUNCTIONS_DATA_CONVERTER,
            emit_trace_spans=False)

        # The gRPC aio channel is bound to the event loop it is created on. A
        # ``durable_client_input`` decode runs on the worker's invocation loop,
        # so capturing it here lets the post-invocation lifecycle extension
        # close the channel on the right loop after the invocation. ``None``
        # when constructed outside a running loop (e.g. in a unit test); the
        # close is then scheduled on whatever loop is running at close time.
        try:
            self._creation_loop: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            self._creation_loop = None
        self._close_scheduled = False

    def schedule_close(self) -> None:
        """Schedule the underlying gRPC channel to close after the invocation.

        Called by the durable-client lifecycle extension right after the user
        function returns. Each ``durable_client_input`` decode builds a client
        that owns a distinct channel; without this, every invocation leaks one.

        The async channel must be closed on its owning event loop, so the close
        is scheduled there rather than run inline (the extension hook is
        synchronous). Idempotent: repeated calls schedule the close only once.
        """
        if self._close_scheduled:
            return
        self._close_scheduled = True

        loop = self._creation_loop
        if loop is None or loop.is_closed():
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                # No loop available to close on; fall back to GC finalization.
                return
        try:
            loop.call_soon_threadsafe(lambda: loop.create_task(self.close()))
        except RuntimeError:
            # Loop is not running / already shut down; nothing to schedule.
            pass

    def _parse_client_configuration(self, client_as_string: str) -> None:
        """Parses the client configuration JSON string and sets instance variables.

        Args:
            client_as_string (str): A JSON string containing the Durable Functions client configuration.

        Raises:
            json.JSONDecodeError: If the provided string is not valid JSON.
        """
        client = json.loads(client_as_string)

        # Depending on the extension-bundle version, the host may send a field
        # explicitly as ``null`` rather than omitting it. ``dict.get(key,
        # default)`` only substitutes the default for *absent* keys, so a
        # present-but-``null`` value slips through as ``None`` (this crashed the
        # ``maxGrpcMessageSizeInBytes > 0`` guard in __init__ on newer bundles).
        # Use ``... or default`` so an explicit ``null`` -- like a missing key --
        # always collapses to the intended default. Every default here is falsy
        # and no field carries a meaningful falsy value, so this never discards a
        # real value.
        self.taskHubName = client.get("taskHubName") or ""
        self.connectionName = client.get("connectionName") or ""
        self.creationUrls = client.get("creationUrls") or {}
        self.managementUrls = client.get("managementUrls") or {}
        self.baseUrl = client.get("baseUrl") or ""
        self.requiredQueryStringParameters = client.get("requiredQueryStringParameters") or ""
        self.rpcBaseUrl = client.get("rpcBaseUrl") or ""
        self.httpBaseUrl = client.get("httpBaseUrl") or ""
        self.useForwardedHost = client.get("useForwardedHost") or False
        self.maxGrpcMessageSizeInBytes = client.get("maxGrpcMessageSizeInBytes") or 0
        # TODO: convert the string value back to timedelta - annoying regex?
        self.grpcHttpClientTimeout = client.get("grpcHttpClientTimeout") or timedelta(seconds=30)

    def create_check_status_response(self, request: func.HttpRequest, instance_id: str) -> func.HttpResponse:
        """Creates an HTTP response for checking the status of a Durable Function instance.

        Args:
            request (func.HttpRequest): The incoming HTTP request.
            instance_id (str): The ID of the Durable Function instance.
        """
        payload = self._get_client_response_links(request, instance_id)
        return func.HttpResponse(
            body=str(payload),
            status_code=202,
            headers={
                'content-type': 'application/json',
                # Match v1: Location points at statusQueryGetUri, which includes
                # the required query string (webhook key / task hub / connection)
                # so a client that follows the header is authorized.
                'Location': payload['statusQueryGetUri'],
            },
        )

    def create_http_management_payload(
            self,
            request: func.HttpRequest | str | None = None,
            instance_id: str | None = None) -> HttpManagementPayload:
        """Creates an HTTP management payload for a Durable Function instance.

        Two call styles are supported:

        - ``create_http_management_payload(request, instance_id)`` (recommended):
          builds the payload URLs relative to the incoming request's origin.
        - ``create_http_management_payload(instance_id)`` (deprecated V1 style):
          builds the payload URLs from the client binding's base URL when no
          request is available.

        Args:
            request (func.HttpRequest | str | None): The incoming HTTP request, or,
                for backwards compatibility, the instance ID when called with a
                single positional argument.
            instance_id (str | None): The ID of the Durable Function instance.
        """
        # Backwards-compatibility: v1 accepted a single positional ``instance_id``.
        if instance_id is None and isinstance(request, str):
            instance_id = request
            request = None
        if instance_id is None:
            raise TypeError("instance_id is required")
        resolved_request = request if isinstance(request, func.HttpRequest) else None
        return self._get_client_response_links(resolved_request, instance_id)

    def _get_client_response_links(self, request: func.HttpRequest | None, instance_id: str) -> HttpManagementPayload:
        return _build_http_management_payload(
            instance_id,
            self.managementUrls,
            self.baseUrl,
            self.httpBaseUrl,
            self.requiredQueryStringParameters,
            self.useForwardedHost,
            request)

    # ------------------------------------------------------------------
    # Backwards-compatibility shims for the v1 azure-functions-durable
    # DurableOrchestrationClient API. These delegate to the durabletask
    # AsyncTaskHubGrpcClient methods and are deprecated: new code should use
    # the durabletask method names directly.
    # ------------------------------------------------------------------

    @deprecated("start_new is deprecated; use schedule_new_orchestration instead.")
    async def start_new(self,
                        orchestration_function_name: str,
                        instance_id: Optional[str] = None,
                        client_input: Optional[Any] = None,
                        version: Optional[str] = None) -> str:
        """Deprecated alias for :meth:`schedule_new_orchestration`."""
        return await self.schedule_new_orchestration(
            orchestration_function_name,
            input=client_input,
            instance_id=instance_id,
            version=version)

    @deprecated("get_status is deprecated; use get_orchestration_state instead.")
    async def get_status(
            self,
            instance_id: str,
            show_history: bool = False,
            show_history_output: bool = False,
            show_input: bool = False) -> DurableOrchestrationStatus:
        """Deprecated alias for :meth:`get_orchestration_state`.

        Returns a :class:`DurableOrchestrationStatus` wrapping the durabletask
        ``OrchestrationState`` for v1 back-compat. When the instance does not
        exist, a falsy status is returned rather than ``None``.

        When ``show_history`` is true, history is fetched and projected into
        the v1 status-query shape. ``show_history_output`` controls whether
        output-bearing history fields are included. Payloads are fetched to
        preserve the v1 output, custom-status, and failure-detail fields;
        ``show_input`` controls whether the compatibility wrapper exposes the
        orchestration and history inputs.
        """
        state = await self.get_orchestration_state(instance_id, fetch_payloads=True)
        projected_history = None
        if (show_history
                and state is not None
                and state.runtime_status != OrchestrationStatus.PENDING):
            history = await self.get_orchestration_history(instance_id)
            projected_history = project_history(
                history,
                show_input=show_input,
                show_history_output=show_history_output)
        return DurableOrchestrationStatus.from_orchestration_state(
            state,
            include_input=show_input,
            history=projected_history)

    @deprecated("get_status_all is deprecated; use get_all_orchestration_states instead.")
    async def get_status_all(self) -> list[DurableOrchestrationStatus]:
        """Deprecated alias for :meth:`get_all_orchestration_states`."""
        states = await self.get_all_orchestration_states(
            OrchestrationQuery(fetch_inputs_and_outputs=True))
        return [DurableOrchestrationStatus.from_orchestration_state(state) for state in states]

    @deprecated("raise_event is deprecated; use raise_orchestration_event instead.")
    async def raise_event(
            self,
            instance_id: str,
            event_name: str,
            event_data: Any = None,
            task_hub_name: Optional[str] = None,
            connection_name: Optional[str] = None) -> None:
        """Deprecated alias for :meth:`raise_orchestration_event`.

        The ``task_hub_name`` and ``connection_name`` arguments have no
        equivalent in durabletask and are ignored.
        """
        await self.raise_orchestration_event(instance_id, event_name, data=event_data)

    @deprecated("terminate is deprecated; use terminate_orchestration instead.")
    async def terminate(self, instance_id: str, reason: Optional[Any] = None) -> None:
        """Deprecated alias for :meth:`terminate_orchestration`.

        The v1 ``reason`` maps to the durabletask ``output`` argument.
        """
        await self.terminate_orchestration(instance_id, output=reason)

    @deprecated("purge_instance_history is deprecated; use purge_orchestration instead.")
    async def purge_instance_history(self, instance_id: str) -> PurgeHistoryResult:
        """Deprecated alias for :meth:`purge_orchestration`.

        Returns a :class:`PurgeHistoryResult` wrapping the durabletask
        ``PurgeInstancesResult`` for v1 back-compat.
        """
        result = await self.purge_orchestration(instance_id)
        return PurgeHistoryResult.from_purge_result(result)

    @deprecated("suspend is deprecated; use suspend_orchestration instead.")
    async def suspend(self, instance_id: str, reason: Optional[str] = None) -> None:
        """Deprecated alias for :meth:`suspend_orchestration`.

        The v1 ``reason`` argument has no equivalent in durabletask and is
        ignored.
        """
        await self.suspend_orchestration(instance_id)

    @deprecated("resume is deprecated; use resume_orchestration instead.")
    async def resume(self, instance_id: str, reason: Optional[str] = None) -> None:
        """Deprecated alias for :meth:`resume_orchestration`.

        The v1 ``reason`` argument has no equivalent in durabletask and is
        ignored.
        """
        await self.resume_orchestration(instance_id)

    @deprecated("restart is deprecated; use restart_orchestration instead.")
    async def restart(
            self,
            instance_id: str,
            restart_with_new_instance_id: bool = True) -> str:
        """Deprecated alias for :meth:`restart_orchestration`."""
        return await self.restart_orchestration(
            instance_id, restart_with_new_instance_id=restart_with_new_instance_id)

    @deprecated("read_entity_state is deprecated; use get_entity instead.")
    async def read_entity_state(
            self,
            entity_instance_id: Optional[EntityInstanceId] = None,
            task_hub_name: Optional[str] = None,
            connection_name: Optional[str] = None,
            *,
            entityId: Optional[EntityInstanceId] = None) -> EntityStateResponse:
        """Deprecated alias for :meth:`get_entity`.

        Returns an :class:`EntityStateResponse` wrapping the durabletask
        ``EntityMetadata`` for v1 back-compat.

        Accepts the v1 ``entityId`` keyword as an alias for
        ``entity_instance_id``. The ``task_hub_name`` and ``connection_name``
        arguments have no equivalent in durabletask and are ignored.
        """
        resolved_id = entity_instance_id if entity_instance_id is not None else entityId
        if resolved_id is None:
            raise TypeError(
                "read_entity_state() missing required argument: 'entity_instance_id'")
        metadata = await self.get_entity(resolved_id)
        return EntityStateResponse.from_entity_metadata(metadata)

    @deprecated("get_status_by is deprecated; use get_all_orchestration_states instead.")
    async def get_status_by(
            self,
            created_time_from: Optional[datetime] = None,
            created_time_to: Optional[datetime] = None,
            runtime_status: Optional[list[OrchestrationRuntimeStatus]] = None) -> list[DurableOrchestrationStatus]:
        """Deprecated alias for :meth:`get_all_orchestration_states`.

        The v1 ``OrchestrationRuntimeStatus`` values are mapped onto the
        durabletask ``OrchestrationStatus`` enum, and results are wrapped in
        :class:`DurableOrchestrationStatus` for v1 back-compat.
        """
        query = OrchestrationQuery(
            created_time_from=created_time_from,
            created_time_to=created_time_to,
            runtime_status=to_durabletask_statuses(runtime_status),
            fetch_inputs_and_outputs=True)
        states = await self.get_all_orchestration_states(query)
        return [DurableOrchestrationStatus.from_orchestration_state(state) for state in states]

    @deprecated("purge_instance_history_by is deprecated; use purge_orchestrations_by instead.")
    async def purge_instance_history_by(
            self,
            created_time_from: Optional[datetime] = None,
            created_time_to: Optional[datetime] = None,
            runtime_status: Optional[list[OrchestrationRuntimeStatus]] = None) -> PurgeHistoryResult:
        """Deprecated alias for :meth:`purge_orchestrations_by`.

        The v1 ``OrchestrationRuntimeStatus`` values are mapped onto the
        durabletask ``OrchestrationStatus`` enum, and the result is wrapped in
        :class:`PurgeHistoryResult` for v1 back-compat.
        """
        result = await self.purge_orchestrations_by(
            created_time_from=created_time_from,
            created_time_to=created_time_to,
            runtime_status=to_durabletask_statuses(runtime_status))
        return PurgeHistoryResult.from_purge_result(result)

    async def signal_entity(
            self,
            entity_instance_id: Optional[EntityInstanceId] = None,
            operation_name: str = "",
            input: Any = None,
            signal_time: Optional[datetime] = None,
            *,
            entityId: Optional[EntityInstanceId] = None,
            operation_input: Any = None,
            task_hub_name: Optional[str] = None,
            connection_name: Optional[str] = None) -> None:
        """Signal an entity to perform an operation.

        Accepts the durabletask ``input`` argument as well as the v1
        ``operation_input`` alias, and the v1 ``entityId`` keyword as an alias
        for ``entity_instance_id``. The ``task_hub_name`` and ``connection_name``
        arguments have no equivalent in durabletask and are ignored.
        """
        resolved_id = entity_instance_id if entity_instance_id is not None else entityId
        if resolved_id is None:
            raise TypeError(
                "signal_entity() missing required argument: 'entity_instance_id'")
        resolved_input = operation_input if operation_input is not None else input
        await super().signal_entity(
            resolved_id, operation_name, input=resolved_input, signal_time=signal_time)

    @deprecated(
        "get_client_response_links is deprecated; use create_http_management_payload instead.")
    def get_client_response_links(
            self,
            request: Optional[func.HttpRequest],
            instance_id: str) -> HttpManagementPayload:
        """Deprecated alias for :meth:`create_http_management_payload`."""
        return self._get_client_response_links(request, instance_id)

    @deprecated(
        "wait_for_completion_or_create_check_status_response is deprecated; use "
        "wait_for_orchestration_completion together with create_check_status_response instead.")
    async def wait_for_completion_or_create_check_status_response(
            self,
            request: func.HttpRequest,
            instance_id: str,
            timeout_in_milliseconds: int = 10000,
            retry_interval_in_milliseconds: int = 1000) -> func.HttpResponse:
        """Wait for an orchestration to complete, or return a check-status response.

        If the orchestration completes within the timeout, an HTTP response
        containing its output (or failure) is returned; otherwise a
        check-status response is returned.

        The ``retry_interval_in_milliseconds`` argument has no durabletask
        equivalent (durabletask waits server-side) and is ignored.
        """
        if retry_interval_in_milliseconds > timeout_in_milliseconds:
            raise Exception(
                f'Total timeout {timeout_in_milliseconds} (ms) should be bigger than '
                f'retry timeout {retry_interval_in_milliseconds} (ms)')

        try:
            state = await self.wait_for_orchestration_completion(
                instance_id, timeout=timeout_in_milliseconds / 1000)
        except TimeoutError:
            return self.create_check_status_response(request, instance_id)

        if state is None:
            return self.create_check_status_response(request, instance_id)

        if state.runtime_status == OrchestrationStatus.COMPLETED:
            return self._create_http_response(200, state.serialized_output)
        if state.runtime_status == OrchestrationStatus.TERMINATED:
            return self._create_http_response(
                200, DurableOrchestrationStatus.from_orchestration_state(state).to_json())
        if state.runtime_status == OrchestrationStatus.FAILED:
            return self._create_http_response(
                500, DurableOrchestrationStatus.from_orchestration_state(state).to_json())
        return self.create_check_status_response(request, instance_id)

    @deprecated("rewind is deprecated; use rewind_orchestration instead.")
    async def rewind(
            self,
            instance_id: str,
            reason: str,
            task_hub_name: Optional[str] = None,
            connection_name: Optional[str] = None) -> None:
        """Deprecated alias for :meth:`rewind_orchestration`.

        Rewinds a failed orchestration instance to its last known good state,
        removing failed task and sub-orchestration results from the history and
        replaying from the last successful checkpoint.

        The ``task_hub_name`` and ``connection_name`` arguments have no
        equivalent in durabletask and are ignored.
        """
        await self.rewind_orchestration(instance_id, reason=reason)

    @staticmethod
    def _create_http_response(status_code: int, body: Union[str, Any]) -> func.HttpResponse:
        body_as_json = body if isinstance(body, str) else json.dumps(body)
        return func.HttpResponse(
            status_code=status_code,
            body=body_as_json,
            mimetype="application/json",
            headers={"Content-Type": "application/json"})


class SyncDurableFunctionsClient(TaskHubGrpcClient):
    """Synchronous durable client supplied by a Functions durable-client binding."""

    taskHubName: str
    connectionName: str
    creationUrls: dict[str, str]
    managementUrls: dict[str, str]
    baseUrl: str
    requiredQueryStringParameters: str
    rpcBaseUrl: str
    httpBaseUrl: str
    useForwardedHost: bool
    maxGrpcMessageSizeInBytes: int
    grpcHttpClientTimeout: timedelta | str

    def __init__(self, client_as_string: str):
        self._parse_client_configuration(client_as_string)
        interceptors = [AzureFunctionsDefaultClientInterceptorImpl(
            self.taskHubName, self.requiredQueryStringParameters)]
        channel_options: GrpcChannelOptions | None = None
        if self.maxGrpcMessageSizeInBytes > 0:
            channel_options = GrpcChannelOptions(
                max_receive_message_length=self.maxGrpcMessageSizeInBytes,
                max_send_message_length=self.maxGrpcMessageSizeInBytes)
        super().__init__(
            host_address=self.rpcBaseUrl,
            secure_channel=False,
            metadata=None,
            interceptors=interceptors,
            channel_options=channel_options,
            data_converter=DEFAULT_FUNCTIONS_DATA_CONVERTER,
            emit_trace_spans=False)

    @classmethod
    def get_cached(cls, client_as_string: str) -> "SyncDurableFunctionsClient":
        """Get the process-wide client for a durable-client binding configuration.

        Synchronous Functions bindings can be invoked frequently by history
        export fan-out activities. Reusing the gRPC channel avoids creating and
        tearing down a channel for every activity invocation.
        """
        with _sync_client_cache_lock:
            cached = _sync_client_cache.get(client_as_string)
            if cached is None:
                cached = cls(client_as_string)
                _sync_client_cache[client_as_string] = cached
            return cached

    def _parse_client_configuration(self, client_as_string: str) -> None:
        client = json.loads(client_as_string)
        self.taskHubName = client.get("taskHubName") or ""
        self.connectionName = client.get("connectionName") or ""
        self.creationUrls = client.get("creationUrls") or {}
        self.managementUrls = client.get("managementUrls") or {}
        self.baseUrl = client.get("baseUrl") or ""
        self.requiredQueryStringParameters = client.get(
            "requiredQueryStringParameters") or ""
        self.rpcBaseUrl = client.get("rpcBaseUrl") or ""
        self.httpBaseUrl = client.get("httpBaseUrl") or ""
        self.useForwardedHost = client.get("useForwardedHost") or False
        self.maxGrpcMessageSizeInBytes = client.get(
            "maxGrpcMessageSizeInBytes") or 0
        self.grpcHttpClientTimeout = client.get(
            "grpcHttpClientTimeout") or timedelta(seconds=30)

    def create_check_status_response(
            self, request: func.HttpRequest, instance_id: str) -> func.HttpResponse:
        payload = self._get_client_response_links(request, instance_id)
        return func.HttpResponse(
            body=str(payload),
            status_code=202,
            headers={
                "content-type": "application/json",
                "Location": payload["statusQueryGetUri"],
            },
        )

    def create_http_management_payload(
            self,
            request: func.HttpRequest | str | None = None,
            instance_id: str | None = None) -> HttpManagementPayload:
        if instance_id is None and isinstance(request, str):
            instance_id = request
            request = None
        if instance_id is None:
            raise TypeError("instance_id is required")
        resolved_request = request if isinstance(request, func.HttpRequest) else None
        return self._get_client_response_links(resolved_request, instance_id)

    def _get_client_response_links(
            self, request: func.HttpRequest | None,
            instance_id: str) -> HttpManagementPayload:
        return _build_http_management_payload(
            instance_id,
            self.managementUrls,
            self.baseUrl,
            self.httpBaseUrl,
            self.requiredQueryStringParameters,
            self.useForwardedHost,
            request)


def _close_cached_sync_clients() -> None:
    """Release process-wide synchronous durable-client channels on shutdown."""
    with _sync_client_cache_lock:
        clients = list(_sync_client_cache.values())
        _sync_client_cache.clear()
    for client in clients:
        client.close()


atexit.register(_close_cached_sync_clients)
