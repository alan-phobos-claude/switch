from __future__ import annotations

import json
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# Every ordinary call gives up after this long, so a server that accepts the
# connection and never answers cannot hold a send, or anything waiting on it.
REQUEST_TIMEOUT_SECONDS = 15.0

# A long-poll on `/events` is held open by the server until there is something
# to say, and Zulip sends a heartbeat at least once a minute — so this only has
# to outlast the heartbeat interval, not the quiet stretches between messages.
EVENTS_TIMEOUT_SECONDS = 90.0

# Uploads carry the file in the request body, so they get longer than a send.
UPLOAD_TIMEOUT_SECONDS = 120.0

# A multipart file field: (filename, bytes, media type), as httpx takes it.
UploadFile = tuple[str, bytes, str]


class ZulipError(RuntimeError):
    """Zulip answered with `"result": "error"`, or not with JSON at all.

    `code` is Zulip's own error code (`BAD_EVENT_QUEUE_ID`, `RATE_LIMIT_HIT`,
    `REACTION_ALREADY_EXISTS`, …) so a caller can act on the one it expects and
    let every other propagate. `retry_after` is the wait Zulip named with a
    rate-limit refusal, in seconds, and None with anything else.
    """

    def __init__(
        self,
        message: str,
        *,
        code: str,
        status: int,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.status = status
        self.retry_after = retry_after

    @property
    def outcome_unknown(self) -> bool:
        """Whether the request may have taken effect despite the error.

        A server error or a body that is not Zulip's own JSON (a proxy's error
        page) says nothing about whether the request was carried out; a
        Zulip refusal says it was not.
        """
        return self.status >= 500 or self.code == "NOT_JSON"


class ZulipClient:
    """One Zulip account's view of the REST API: the bridge's own, or a bot's.

    Thin on purpose. Parameters that Zulip takes as JSON (lists, dicts) are
    encoded here, so a caller passes Python values and never thinks about which
    fields the server parses as JSON and which as plain form values.
    """

    def __init__(self, http: httpx.AsyncClient, *, email: str, api_key: str) -> None:
        self._http = http
        self._auth = httpx.BasicAuth(email, api_key)
        self.email = email

    async def call(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        files: dict[str, UploadFile] | None = None,
        timeout: float = REQUEST_TIMEOUT_SECONDS,
    ) -> dict[str, Any]:
        encoded = {
            key: json.dumps(value) if isinstance(value, (list, dict, bool)) else value
            for key, value in (params or {}).items()
        }
        url = f"/api/v1/{path.lstrip('/')}"
        if method.upper() in ("GET", "DELETE"):
            response = await self._http.request(
                method, url, params=encoded, auth=self._auth, timeout=timeout
            )
        else:
            response = await self._http.request(
                method,
                url,
                data=encoded,
                files=files,
                auth=self._auth,
                timeout=timeout,
            )
        try:
            body: dict[str, Any] = response.json()
        except ValueError as exc:
            raise ZulipError(
                f"Zulip answered {method} {path} with HTTP {response.status_code} "
                "and a body that is not JSON",
                code="NOT_JSON",
                status=response.status_code,
            ) from exc
        if body.get("result") != "success":
            raise ZulipError(
                f"Zulip refused {method} {path}: {body.get('msg', 'no reason given')}",
                code=str(body.get("code", "UNKNOWN")),
                status=response.status_code,
                retry_after=_retry_after(body, response),
            )
        return body


def _retry_after(body: dict[str, Any], response: httpx.Response) -> float | None:
    raw = body.get("retry-after", response.headers.get("retry-after"))
    if raw is None:
        return None
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return None
