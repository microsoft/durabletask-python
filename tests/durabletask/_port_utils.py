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
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]
