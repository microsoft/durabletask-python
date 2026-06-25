# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Shared test helpers for picking network ports.

Tests start an in-memory backend that binds a TCP port. Hard-coding
fixed ports causes intermittent failures on Windows because Hyper-V (and
other components) reserve large, dynamic ranges of TCP ports. Asking the
OS for a free port avoids those collisions.
"""

from __future__ import annotations

import socket


def find_free_port() -> int:
    """Return a free TCP port by binding to port 0 and reading the assignment.

    Probes IPv6 loopback first to match the backend's ``[::]`` bind (so an
    IPv6-occupied port isn't wrongly reported free), falling back to IPv4.
    """
    if socket.has_ipv6:
        try:
            with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
                s.bind(("::1", 0))
                return s.getsockname()[1]
        except OSError:
            pass  # IPv6 not usable on this host; fall back to IPv4.

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
