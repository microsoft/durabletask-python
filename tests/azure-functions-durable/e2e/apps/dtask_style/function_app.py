# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""durabletask-native-style Durable Functions sample app for E2E testing.

The app is composed from blueprints, each covering one concern, and registered
onto a single ``DFApp``. Every orchestrator and entity uses the modern
durabletask authoring style: two-argument orchestrators
(``def orch(ctx, input):``) and entity functions (``def entity(ctx, input):``)
that use the durabletask ``OrchestrationContext`` / ``EntityContext`` API
directly, and the durabletask client method names.

Together with the v1-style app it exercises both authoring surfaces the
compatibility layer supports, end-to-end against a real Functions host.
"""

import os

import azure.functions as func

import azure.durable_functions as df

import activities
import client_routes
import entities
import history_export_routes
import large_payloads
import orchestrators
from durabletask.extensions.azure_blob_payloads import BlobPayloadStore, BlobPayloadStoreOptions

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)
app.configure_large_payloads(payload_store=BlobPayloadStore(BlobPayloadStoreOptions(
    connection_string=os.environ.get("AzureWebJobsStorage", "UseDevelopmentStorage=true"),
    container_name=os.environ.get("E2E_PAYLOAD_CONTAINER", "functions-e2e-payloads"),
)))

app.register_functions(activities.bp)
app.register_functions(entities.bp)
app.register_functions(orchestrators.bp)
app.register_functions(client_routes.bp)
app.register_functions(history_export_routes.bp)
app.register_functions(large_payloads.bp)

# Opt in to durabletask scheduled tasks: registers the schedule entity and
# operation orchestrator so schedules can be managed via ScheduledTaskClient.
app.configure_scheduled_tasks()

# Opt in to durabletask history export: registers the export-job entity, driving
# orchestrator, and activities so export jobs can be driven via ExportHistoryClient.
# The export activities write through this shared writer and resolve their
# durabletask client per-invocation from a durable client binding.
app.configure_history_export(writer=history_export_routes.EXPORT_WRITER)
