"""User-facing errors with stable message codes."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class UserFacingError(ValueError):
    def __init__(
        self,
        code: str,
        *,
        params: Mapping[str, Any] | None = None,
        default_message: str | None = None,
    ) -> None:
        self.code = code
        self.params = dict(params or {})
        self.default_message = default_message or code
        super().__init__(self.default_message)
