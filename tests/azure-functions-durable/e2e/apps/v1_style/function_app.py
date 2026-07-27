# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""V1-style Durable Functions sample app for end-to-end testing.

The app is composed from blueprints, each covering one concern, and registered
onto a single ``DFApp``. Every orchestrator and entity uses the classic
``azure-functions-durable`` v1 authoring style (single-argument generator
orchestrators, single-argument entity functions) and the deprecated v1 client
method names.

Splitting the app across blueprints also exercises the Functions blueprint
registration path (``register_functions``) for the durable app.
"""

import azure.functions as func

import azure.durable_functions as df

import activities
import client_routes
import entities
import http_orchestrators
import orchestrators

app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)

app.register_functions(activities.bp)
app.register_functions(entities.bp)
app.register_functions(orchestrators.bp)
app.register_functions(http_orchestrators.bp)
app.register_functions(client_routes.bp)
