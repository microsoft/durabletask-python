# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import azure.durable_functions as df


def create_app() -> df.DFApp:
    return df.DFApp()
