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

import azure.functions as func

import azure.durable_functions as df

import activities
import client_routes
import entities
import history_export_routes
import orchestrators

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

app.register_functions(activities.bp)
app.register_functions(entities.bp)
app.register_functions(orchestrators.bp)
app.register_functions(client_routes.bp)
app.register_functions(history_export_routes.bp)

# Opt in to durabletask scheduled tasks: registers the schedule entity and
# operation orchestrator so schedules can be managed via ScheduledTaskClient.
app.configure_scheduled_tasks()

# Opt in to durabletask history export: registers the export-job entity, driving
# orchestrator, and activities so export jobs can be driven via ExportHistoryClient.
app.configure_history_export()
