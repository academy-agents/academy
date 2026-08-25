from __future__ import annotations

import enum


class StatusCode(enum.Enum):
    """Http status codes."""

    OKAY = 200
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    TIMEOUT = 408
    TOO_LARGE = 413
    TERMINATED = 419
    NO_RESPONSE = 444
