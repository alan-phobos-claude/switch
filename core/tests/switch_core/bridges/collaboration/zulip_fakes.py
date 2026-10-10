"""A fake Zulip server and a bridge wired to it, for the Zulip adapter tests."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs

import httpx

from switch_core.bridges.collaboration.models import (
    InboundAgentJoin,
    InboundCommand,
    InboundInteraction,
    InboundMessage,
    InboundUserJoin,
)
from switch_core.bridges.collaboration.zulip.adapter import (
    ZulipAdapter,
    ZulipConnectionConfig,
    _AgentBot,
)
from switch_core.bridges.collaboration.zulip.client import ZulipClient

SITE = "https://zulip.example.test"
BRIDGE_ID = 10
BRIDGE_EMAIL = "switch-bridge@example.test"

# A route answers with the fields of a success body, or with a whole response
# when it has to say something else: an error, a file, a server failure.
Route = Callable[[dict[str, Any]], "dict[str, Any] | httpx.Response"]


class FakeZulip:
    """Answers the REST calls the adapter makes, and records each one."""

    def __init__(self, routes: dict[tuple[str, str], Route]) -> None:
        self.routes = routes
        self.calls: list[tuple[str, str, dict[str, Any], str]] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/api/v1/")
        content_type = request.headers.get("content-type", "")
        if content_type.startswith("multipart/"):
            params: dict[str, Any] = {"_multipart": request.content}
        else:
            source = (
                request.url.query.decode()
                if request.method in ("GET", "DELETE")
                else request.content.decode()
            )
            params = {
                key: _maybe_json(values[0]) for key, values in parse_qs(source).items()
            }
        auth = request.headers.get("authorization", "")
        self.calls.append((request.method, path, params, auth))
        route = self.routes.get((request.method, path))
        if route is None:
            return error(404, "X", f"no route {request.method} {path}")
        answer = route(params)
        if isinstance(answer, httpx.Response):
            return answer
        return httpx.Response(200, json={"result": "success", "msg": "", **answer})

    def sent(self, method: str, path: str) -> list[dict[str, Any]]:
        return [p for m, pa, p, _ in self.calls if m == method and pa == path]

    def auth_of(self, method: str, path: str) -> list[str]:
        return [a for m, pa, _, a in self.calls if m == method and pa == path]


def error(status: int, code: str, msg: str, **extra: Any) -> httpx.Response:
    return httpx.Response(
        status, json={"result": "error", "code": code, "msg": msg, **extra}
    )


def _maybe_json(value: str) -> Any:
    try:
        return json.loads(value)
    except ValueError:
        return value


def config(**overrides: Any) -> ZulipConnectionConfig:
    return ZulipConnectionConfig(
        **{
            "site": SITE,
            "email": BRIDGE_EMAIL,
            "api_key": "bridge-key",
            **overrides,
        }
    )


class Recorder:
    def __init__(self) -> None:
        self.messages: list[InboundMessage] = []
        self.commands: list[InboundCommand] = []
        self.agent_joins: list[InboundAgentJoin] = []
        self.user_joins: list[InboundUserJoin] = []
        self.presses: list[InboundInteraction] = []

    async def on_message(self, msg: InboundMessage) -> None:
        self.messages.append(msg)

    async def on_command(self, cmd: InboundCommand) -> None:
        self.commands.append(cmd)

    async def on_agent_joined(self, join: InboundAgentJoin) -> None:
        self.agent_joins.append(join)

    async def on_user_joined(self, join: InboundUserJoin) -> None:
        self.user_joins.append(join)

    async def on_app_joined(self, join: object) -> None:
        return None

    async def on_interaction(self, press: InboundInteraction) -> None:
        self.presses.append(press)


def connected(fake: FakeZulip, **overrides: Any) -> tuple[ZulipAdapter, Recorder]:
    """An adapter wired to `fake` as though `start` had run, without polling."""
    adapter = ZulipAdapter(config=config(**overrides))
    http = httpx.AsyncClient(transport=httpx.MockTransport(fake), base_url=SITE)
    adapter._http = http
    adapter._bridge = ZulipClient(http, email=BRIDGE_EMAIL, api_key="k")
    adapter._bridge_user_id = BRIDGE_ID
    recorder = Recorder()
    adapter._on_message = recorder.on_message
    adapter._on_command = recorder.on_command
    adapter._on_agent_joined = recorder.on_agent_joined
    adapter._on_user_joined = recorder.on_user_joined
    adapter._on_app_joined = recorder.on_app_joined
    adapter.set_interaction_handler(recorder.on_interaction)
    return adapter, recorder


def add_bot(
    adapter: ZulipAdapter, name: str, user_id: int, full_name: str
) -> _AgentBot:
    assert adapter._http is not None
    bot = _AgentBot(
        agent_name=name,
        user_id=user_id,
        full_name=full_name,
        client=ZulipClient(
            adapter._http, email=f"{name}-bot@example.test", api_key=f"key-{name}"
        ),
    )
    adapter._bots[name] = bot
    adapter._bot_by_user_id[user_id] = bot
    adapter._realm_bot_ids.add(user_id)
    return bot


def stream_routes() -> dict[tuple[str, str], Route]:
    return {
        ("GET", "streams/7"): lambda p: {
            "stream": {"invite_only": False, "name": "eval"}
        }
    }


def stream_message(**overrides: Any) -> dict[str, Any]:
    return {
        "id": 900,
        "type": "stream",
        "sender_id": 5,
        "sender_email": "pat@example.test",
        "stream_id": 7,
        "display_recipient": "eval",
        "subject": "switch",
        "content": "hello @**Code Reviewer**",
        **overrides,
    }
