# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Shared logger for the history-export extension.

Submodules that emit log records should import :data:`logger` from
this module rather than calling :func:`logging.getLogger` themselves.
This keeps every emit attributed to the same logger name so that
callers can configure / filter the extension's output in one place.
"""

from __future__ import annotations

import logging

logger = logging.getLogger("durabletask.extensions.history_export")
"""Module-wide logger for the history-export extension."""

__all__ = ["logger"]
