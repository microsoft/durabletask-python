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
    """Return a TCP port number that is currently free.

    Binds a socket to port 0 so the OS assigns an available port, then
    releases it and returns the chosen number. There is an inherent race
    between releasing the socket and the caller binding the port, but in
    practice it is reliable for tests and far safer than fixed ports.

    The in-memory backend binds the IPv6 wildcard (``[::]``), so the probe
    uses an IPv6 socket to mirror that bind and avoid returning a port that
    is free on IPv4 but already in use on IPv6. If IPv6 is unavailable, it
    falls back to IPv4.
    """
    if socket.has_ipv6:
        try:
            with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s:
                s.bind(("::", 0))
                return s.getsockname()[1]
        except OSError:
            pass  # IPv6 not usable on this host; fall back to IPv4.

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
