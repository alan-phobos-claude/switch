"""Collaboration bridge for Zulip.

How Zulip's model maps onto a Switch room:

- A **stream** (Zulip's "channel") is a bridged channel; its id is the stream id.
- A **topic** is a thread. The connection's `default_topic` is the channel's top
  level, where a message has no thread root. Every other topic is a thread
  rooted at its first message, so replying into a thread means posting into
  that message's topic.
- A **direct message** has no channel id of its own on Zulip, so one is minted
  from its participants: `dm:<sorted user ids>`.
- Each agent is a Zulip bot of its own, created by the bridge account. Zulip
  has no per-message sender override, so a bot per agent is the only way for an
  agent to post under its own name and avatar.

Inbound traffic arrives over Zulip's event queues, which this side long-polls:
one for the bridge account (the streams it is subscribed to, which are the
bridged ones, with subscription changes and reactions), and one per agent bot
for the direct messages people send that bot.

An edit is relayed only when it names an agent the message did not name
before, and then as a message of its own marked as an edit: Switch keeps what
was said rather than rewriting it.

Session activity is drawn with the neutral renderer: a turn's status is one
message edited in place, and a request is a card answered by typing its handle
or by clicking one of the numbered reactions the agent's bot seeds on it (see
`controls`). Zulip limits how long a message can be edited; a publication that
outlives the limit is reported as wedged, and the bridge says so on the
connection while the limit is set.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import mimetypes
import re
import time
import uuid
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Any, ClassVar
from urllib.parse import unquote, urlsplit

import httpx
from pydantic import field_validator

from switch_core.agent_display_name import ZERO_WIDTH_SPACE
from switch_core.bridges.collaboration.adapter import (
    ActivityMark,
    ActivityMarkRefused,
    PlatformAdapter,
    RequestCard,
    RichContent,
    RichContentFailed,
    RichContentThrottled,
    RichContentWedged,
    ThreadUnavailable,
    TurnActivity,
)
from switch_core.bridges.collaboration.models import (
    Attachment,
    AttachmentFailure,
    BridgeConnectionConfig,
    BridgeCredentialError,
    ChannelType,
    DirectoryUser,
    InboundAgentJoin,
    InboundAppJoin,
    InboundCommand,
    InboundInteraction,
    InboundMessage,
    InboundUserJoin,
    OutboundAttachment,
)
from switch_core.bridges.collaboration.session.renderers import (
    INTERRUPT_QUEUED_NOTE,
    MARKDOWN,
)
from switch_core.bridges.collaboration.session.renderers.neutral import (
    render_request,
    turn_status,
)
from switch_core.bridges.collaboration.zulip.client import (
    EVENTS_TIMEOUT_SECONDS,
    UPLOAD_TIMEOUT_SECONDS,
    ZulipClient,
    ZulipError,
)
from switch_core.bridges.collaboration.zulip.controls import (
    NUMBER_EMOJI,
    QUEUED_EMOJI,
    STOP_EMOJI,
    WORKING_EMOJI,
    Offer,
    card_actions,
    card_hint,
    possible_card_actions,
    stop_hint,
    turn_actions,
)
from switch_core.outbound import guarded_async_client

logger = logging.getLogger(__name__)

_DM_PREFIX = "dm:"

# Zulip's own wildcard mentions. Written as `@**all**` and friends; a silent
# mention (`@_**all**`) pages nobody and needs no defusing.
_WILDCARD_MENTION = re.compile(
    r"@\*\*(all|everyone|channel|stream|topic)\*\*", re.IGNORECASE
)

# A person or bot mention as Zulip writes it: `@**Full Name**`, or
# `@**Full Name|123**` when two accounts share a name.
_USER_MENTION = re.compile(r"@_?\*\*([^*|]+?)(?:\|(\d+))?\*\*")

# A person as Switch names them in an outbound body: `@` and their Zulip email,
# which is the username this bridge reports for them.
_EMAIL_MENTION = re.compile(r"@([\w.+-]+@[\w-]+(?:\.[\w-]+)+)")

# An agent named in plain text, the way Switch itself writes `@agent-name`.
_PLAIN_MENTION = re.compile(r"(?<![\w@])@([a-z0-9][a-z0-9._-]*)")

# Separates a message id from the edit it stands for in a message reference:
# an edit relayed into the room is a message of its own, so it needs a
# reference of its own, and that reference still has to lead back to the
# Zulip message for a reaction, a reply or a thread to land on.
_EDIT_SEPARATOR = "~"

# A Switch transport id in an outbound body (`@alice:server`).
_TRANSPORT_MENTION = re.compile(r"@([\w.-]+):\S+")

# A file in a message, as Zulip writes one into the raw Markdown it stores.
_UPLOAD_LINK = re.compile(r"\[([^\]\n]*)\]\((/user_uploads/[^)\s]+)\)")

_SHORT_NAME_INVALID = re.compile(r"[^a-z0-9_-]")

# How long the poller waits before re-registering after the server dropped a
# queue or a poll failed; doubled per consecutive failure up to the ceiling.
_RETRY_INITIAL_SECONDS = 2.0
_RETRY_MAX_SECONDS = 30.0

# The realm Zulip's own system bots (Notification Bot, Welcome Bot) post from.
# What they say is the server narrating, not anyone in the conversation.
_SYSTEM_BOT_REALM = "zulipinternal"

# How far back from a reservation `find_request_card` looks, and how many
# messages it reads: clock skew between Switch and Zulip, and a busy topic.
_RECOVERY_SKEW = timedelta(seconds=30)
_RECOVERY_LIMIT = 100

# How many messages after a card `is_first_reply` reads to decide.
_FIRST_REPLY_WINDOW = 5

# What the realm allows when it could not be asked: Zulip's own defaults.
_DEFAULT_MAX_MESSAGE_LENGTH = 10000
_DEFAULT_MAX_UPLOAD_MIB = 25
_DEFAULT_MAX_AVATAR_MIB = 5

_SEEN_MAX = 1000
_CACHE_MAX = 2000

# How long a stream's public/private type is trusted before it is read again.
_CHANNEL_TYPE_TTL_SECONDS = 300.0

# The longest bot short name the bridge makes, leaving room for `-bot@…`.
_SHORT_NAME_MAX = 40


def _site(value: str) -> str:
    return value.strip().rstrip("/")


class ZulipConnectionConfig(BridgeConnectionConfig):
    # The organization's URL, e.g. `https://chat.example.com`.
    site: str
    # The account the bridge acts as. It must be allowed to create bots and to
    # subscribe other users to streams, which in a default organization means
    # an administrator. Create a dedicated user for it rather than lending it
    # a person's account: every channel the bridge creates is subscribed to it,
    # and removing the connection deactivates every bot it owns.
    email: str
    api_key: str
    # The topic a channel's top-level conversation lives in. Messages here have
    # no thread root; every other topic is a thread of its own.
    default_topic: str = "switch"
    # A human to subscribe to every stream this bridge creates, by user id or
    # login email, so a private stream an agent opened is not invisible to
    # everyone.
    default_member: str | None = None
    verify_tls: bool = True

    @field_validator("site")
    @classmethod
    def _site_is_a_url(cls, value: str) -> str:
        parts = urlsplit(_site(value))
        if parts.scheme.lower() not in ("http", "https") or not parts.hostname:
            raise ValueError("site must be an http(s) URL naming the organization")
        return _site(value)

    @field_validator("default_topic")
    @classmethod
    def _topic_fits(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("default_topic must not be empty")
        if len(value) > 60:
            raise ValueError("default_topic must be at most 60 characters")
        return value


class _AgentBot:
    def __init__(
        self, *, agent_name: str, user_id: int, full_name: str, client: ZulipClient
    ) -> None:
        self.agent_name = agent_name
        self.user_id = user_id
        self.full_name = full_name
        self.client = client
        self.poller: asyncio.Task[None] | None = None


@dataclass(frozen=True)
class _Rendered:
    """A publication drawn for Zulip.

    `text` is what goes on the message. `plain` is the same without the line
    inviting a reaction, for a failure to quote: a click is no way to answer
    something that never posted. `actions` and `value` are the reactions the
    message offers and what a click on one carries.
    """

    text: str
    plain: str
    actions: dict[str, str]
    value: str | None


class _TooLarge(Exception):
    pass


class ZulipAdapter(PlatformAdapter):
    draws_session_activity: ClassVar[bool] = True

    #: A problem somebody has to act on gets its own message, which notifies,
    #: rather than arriving as an edit to a status nobody re-reads.
    separate_attention_slot: ClassVar[bool] = True

    #: A topic notifies only people who follow it or are named in it.
    notifies_only_by_mention: ClassVar[bool] = True
    channel_mention: ClassVar[str | None] = "@**channel**"

    #: The status is the turn's one message; the clock rides along with the
    #: next real change rather than rewriting it every few seconds.
    redraws_for_elapsed_time: ClassVar[bool] = False

    #: Each agent reacts as its own bot, and a Zulip user may hold several
    #: different reactions on one message.
    supports_activity_reactions: ClassVar[bool] = True
    supports_queue_reaction: ClassVar[bool] = True
    activity_reactions_per_agent: ClassVar[bool] = True

    #: A card prints its handle, and a topic can be read back, so a card whose
    #: send was never acknowledged can be found again. A status prints no
    #: handle and has nowhere to carry a hidden marker, so it cannot.
    recovers_uncertain_posts: ClassVar[bool] = True

    #: Zulip linkifies web URLs only, so a `switchdash://` deeplink needs
    #: `GATEWAY_PUBLIC_URL` set for Switch to rewrite it to its https redirect.
    renders_custom_url_schemes: ClassVar[bool] = False

    @classmethod
    def claimed_workspace(cls, connection_config: dict[str, object]) -> str | None:
        parts = urlsplit(_site(str(connection_config["site"])))
        host = (parts.hostname or "").lower()
        if parts.port is not None:
            host = f"{host}:{parts.port}"
        return f"Zulip organization {parts.scheme.lower()}://{host}{parts.path}"

    @classmethod
    def outbound_urls(cls, connection_config: dict[str, object]) -> list[str]:
        return [_site(str(connection_config["site"]))]

    @classmethod
    async def verify_credentials(cls, connection_config: dict[str, object]) -> None:
        config = ZulipConnectionConfig.model_validate(connection_config)
        async with httpx.AsyncClient(
            base_url=config.site, verify=config.verify_tls
        ) as http:
            client = ZulipClient(http, email=config.email, api_key=config.api_key)
            try:
                await client.call("GET", "users/me")
            except ZulipError as exc:
                raise BridgeCredentialError(str(exc)) from exc
            except httpx.HTTPError as exc:
                raise BridgeCredentialError(
                    f"Could not reach Zulip at {config.site}: {exc}"
                ) from exc

    def __init__(self, *, config: ZulipConnectionConfig) -> None:
        super().__init__()
        self._config = config
        self._http: httpx.AsyncClient | None = None
        self._bridge: ZulipClient | None = None
        self._bridge_user_id: int = 0
        self._default_member_id: int | None = None
        # Every bot account in the organization, this bridge's or not, so a
        # post from one is relayed as an app's rather than a person's.
        self._realm_bot_ids: set[int] = set()
        # Zulip email (casefolded) -> user id, and user id -> full name and
        # email: what an outbound `@email` needs to become a mention, and what
        # a reaction, which names only a user id, needs to name its sender.
        self._mention_ids: dict[str, int] = {}
        self._full_names: dict[int, str] = {}
        self._emails: dict[int, str] = {}

        # The realm's own limits, read at start.
        self._max_message_length = _DEFAULT_MAX_MESSAGE_LENGTH
        self._max_upload_bytes = _DEFAULT_MAX_UPLOAD_MIB * 1024 * 1024
        self._max_avatar_bytes = _DEFAULT_MAX_AVATAR_MIB * 1024 * 1024
        self._edit_limit_seconds: int | None = None
        self._editing_allowed = True

        self._bridge_poller: asyncio.Task[None] | None = None
        # Poller name -> (account, queue id) of the queue it is reading, so
        # stopping can tell Zulip to drop them rather than leave them to expire.
        self._queues: dict[str, tuple[ZulipClient, str]] = {}
        self._background: set[asyncio.Task[None]] = set()

        self._bots: dict[str, _AgentBot] = {}
        self._bot_by_user_id: dict[int, _AgentBot] = {}
        self._identity_locks: dict[str, asyncio.Lock] = {}

        # Zulip message id -> the bot that sent it. Zulip lets only a message's
        # sender edit its content — an administrator included — so an update
        # has to go out under the same bot the original did.
        self._sent_by: OrderedDict[int, ZulipClient] = OrderedDict()
        # Zulip message id -> (stream id, topic), for posting a reply into the
        # thread a root message lives in.
        self._message_topics: OrderedDict[int, tuple[int, str]] = OrderedDict()
        # (stream id, topic) -> the id of the topic's first message.
        self._topic_roots: OrderedDict[tuple[int, str], int] = OrderedDict()
        # Message references already relayed: a message id, or for an edit the
        # id and the edit it stands for.
        self._seen: OrderedDict[str, None] = OrderedDict()

        # Published message id -> the reactions it offers as controls.
        self._offers: OrderedDict[int, Offer] = OrderedDict()
        # Published messages whose leftover controls have been cleared since
        # this process started — see `_sync_controls`.
        self._reconciled: OrderedDict[int, None] = OrderedDict()
        # Published message id -> the text last written to it, so a redraw
        # that changes nothing costs no edit.
        self._last_text: OrderedDict[int, str] = OrderedDict()
        # Publications Zulip will no longer let anyone edit.
        self._wedged: OrderedDict[int, None] = OrderedDict()
        # (agent, message, mark) this process has put on a message.
        self._marked: OrderedDict[tuple[str, str, ActivityMark], None] = OrderedDict()
        # Stream id -> (its type, when it was read).
        self._channel_types: dict[str, tuple[ChannelType, float]] = {}
        # (message id, user id, emoji) of clicks already pressed, so the same
        # click delivered by two queues presses once.
        self._pressed: OrderedDict[tuple[int, int, str], None] = OrderedDict()

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def start(
        self,
        on_message: Callable[[InboundMessage], Awaitable[None]],
        on_command: Callable[[InboundCommand], Awaitable[None]],
        on_agent_joined: Callable[[InboundAgentJoin], Awaitable[None]],
        on_user_joined: Callable[[InboundUserJoin], Awaitable[None]],
        on_app_joined: Callable[[InboundAppJoin], Awaitable[None]],
    ) -> None:
        self._on_message = on_message
        self._on_command = on_command
        self._on_agent_joined = on_agent_joined
        self._on_user_joined = on_user_joined
        self._on_app_joined = on_app_joined

        self._http = guarded_async_client(
            self.outbound_policy,
            verify=self._config.verify_tls,
            base_url=self._config.site,
        )
        self._bridge = ZulipClient(
            self._http, email=self._config.email, api_key=self._config.api_key
        )
        try:
            me = await self._bridge.call("GET", "users/me")
        except ZulipError as exc:
            raise BridgeCredentialError(str(exc)) from exc
        self._bridge_user_id = int(me["user_id"])
        if self._config.default_member:
            self._default_member_id = await self._user_id(self._config.default_member)
        members = await self._bridge.call("GET", "users")
        self._realm_bot_ids = {
            int(m["user_id"]) for m in members["members"] if m.get("is_bot")
        }
        for member in members["members"]:
            self._remember_person(
                int(member["user_id"]),
                str(member.get("email", "")),
                str(member.get("full_name", "")),
            )
        await self._read_realm_limits()

        self._bridge_poller = asyncio.create_task(
            self._poll(
                "bridge",
                self._bridge,
                # Only the streams the bridge account is subscribed to: a
                # message from any other would be bridged on arrival, so
                # reading every public stream would adopt all of them.
                {
                    "event_types": [
                        "message",
                        "update_message",
                        "subscription",
                        "reaction",
                    ]
                },
                agent=None,
            ),
            name="zulip-bridge-events",
        )
        logger.info(
            "Zulip adapter connected to %s as %s",
            self._config.site,
            self._config.email,
        )

    async def _read_realm_limits(self) -> None:
        """Read what the organization allows: message length, uploads, edits.

        A failure here leaves Zulip's defaults in place and says so; the limits
        shape what the bridge sends rather than whether it can run.
        """
        bridge = self._require_bridge()
        try:
            realm = await bridge.call(
                "POST",
                "register",
                {"event_types": ["realm"], "fetch_event_types": ["realm"]},
            )
        except (ZulipError, httpx.HTTPError) as exc:
            logger.warning(
                "Could not read the Zulip organization's limits, so Zulip's "
                "defaults are assumed: %s",
                exc,
            )
            return
        self._max_message_length = int(
            realm.get("max_message_length", _DEFAULT_MAX_MESSAGE_LENGTH)
        )
        self._max_upload_bytes = (
            int(realm.get("max_file_upload_size_mib", _DEFAULT_MAX_UPLOAD_MIB))
            * 1024
            * 1024
        )
        self._max_avatar_bytes = (
            int(realm.get("max_avatar_file_size_mib", _DEFAULT_MAX_AVATAR_MIB))
            * 1024
            * 1024
        )
        self._editing_allowed = bool(realm.get("realm_allow_message_editing", True))
        limit = realm.get("realm_message_content_edit_limit_seconds")
        self._edit_limit_seconds = int(limit) if limit else None
        if "queue_id" in realm:
            await self._delete_queue(bridge, str(realm["queue_id"]))

    async def attention(self) -> str | None:
        if not self._editing_allowed:
            return (
                "Message editing is turned off in this Zulip organization, so an "
                "agent's status and request cards cannot be updated once posted. "
                "Turn it on under Organization settings → Message editing."
            )
        if self._edit_limit_seconds is not None:
            minutes = max(1, self._edit_limit_seconds // 60)
            return (
                f"This Zulip organization allows messages to be edited for only "
                f"{minutes} minutes, so an agent's status and request cards stop "
                "updating after that. Set message editing to 'Any time' under "
                "Organization settings → Message editing."
            )
        return None

    async def stop(self) -> None:
        tasks = [self._bridge_poller] + [bot.poller for bot in self._bots.values()]
        tasks += list(self._background)
        for task in tasks:
            if task is not None:
                task.cancel()
        await asyncio.gather(
            *(t for t in tasks if t is not None), return_exceptions=True
        )
        for client, queue_id in list(self._queues.values()):
            await self._delete_queue(client, queue_id)
        self._queues.clear()
        self._bridge_poller = None
        self._bots.clear()
        self._bot_by_user_id.clear()
        if self._http is not None:
            await self._http.aclose()
            self._http = None
        logger.info("Zulip adapter stopped")

    async def withdraw(self) -> None:
        """Deactivate every bot this bridge's account owns.

        The bridge is being removed for good, and its bots would otherwise
        stay in the organization as accounts nobody speaks through. Owned by
        the bridge account means made by it, which is why that account should
        be one kept for the purpose. Deactivation is reversible by an
        organization administrator.
        """
        bridge = self._require_bridge()
        owned = await bridge.call("GET", "bots")
        failed: list[str] = []
        for bot in owned.get("bots", []):
            email = str(bot.get("username", ""))
            try:
                user_id = await self._user_id(email)
                await bridge.call("DELETE", f"bots/{user_id}")
            except (ZulipError, httpx.HTTPError) as exc:
                logger.warning("Could not deactivate Zulip bot %s: %s", email, exc)
                failed.append(email)
        if failed:
            raise RuntimeError(
                "These Zulip bots are still active and can be deactivated under "
                f"Organization settings → Bots: {', '.join(failed)}"
            )

    async def _delete_queue(self, client: ZulipClient, queue_id: str) -> None:
        try:
            await client.call("DELETE", "events", {"queue_id": queue_id}, timeout=5.0)
        except (ZulipError, httpx.HTTPError) as exc:
            logger.debug("Could not delete Zulip event queue %s: %s", queue_id, exc)

    def _require_bridge(self) -> ZulipClient:
        if self._bridge is None:
            raise RuntimeError("Zulip adapter is not started")
        return self._bridge

    def _require_http(self) -> httpx.AsyncClient:
        if self._http is None:
            raise RuntimeError("Zulip adapter is not started")
        return self._http

    def _spawn(self, coro: Awaitable[None], name: str) -> None:
        task: asyncio.Task[None] = asyncio.ensure_future(coro)
        task.set_name(name)
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    # ── Sending ──────────────────────────────────────────────────────────────

    async def send_message(
        self,
        channel_id: str,
        sender_name: str,
        content: str,
        thread_root_id: str | None = None,
        *,
        room_wide_mention: bool = False,
    ) -> str | None:
        client = self._sender(sender_name, channel_id)
        try:
            return await self._post(client, channel_id, content, thread_root_id)
        except (ZulipError, httpx.HTTPError) as exc:
            logger.error("Failed to send Zulip message to %s: %s", channel_id, exc)
            return None

    async def admin_message(
        self,
        channel_id: str,
        content: str,
        thread_root_id: str | None = None,
        *,
        message_type: str | None = None,
        drawn: str | None = None,
    ) -> str | None:
        """A notice from Switch itself, posted as the bridge account.

        In a direct message the bridge account is not a participant, and
        posting there as it would start a different conversation with it added,
        so the notice goes out as the agent's bot in that conversation instead.
        """
        body = self._admin_body(self.translate_outbound(content), drawn)
        try:
            return await self._post(
                self._reader(channel_id), channel_id, body, thread_root_id
            )
        except (ZulipError, httpx.HTTPError) as exc:
            logger.error("Failed to send Zulip notice to %s: %s", channel_id, exc)
            return None

    def _bridge_display_name(self) -> str:
        return self._full_names.get(self._bridge_user_id, "Switch")

    def _sender(self, sender_name: str, channel_id: str) -> ZulipClient:
        bot = self._bots.get(sender_name)
        if bot is not None:
            return bot.client
        logger.warning(
            "No Zulip bot for sender '%s' in %s; posting as the bridge account",
            sender_name,
            channel_id,
        )
        return self._require_bridge()

    async def _post(
        self,
        client: ZulipClient,
        channel_id: str,
        content: str,
        thread_root_id: str | None,
    ) -> str:
        target = await self._target(channel_id, thread_root_id)
        return await self._post_to(client, target, content)

    async def _target(
        self, channel_id: str, thread_root_id: str | None
    ) -> dict[str, Any]:
        """Where a message for this channel and thread goes, as Zulip addresses it."""
        if channel_id.startswith(_DM_PREFIX):
            return {"type": "direct", "to": _dm_user_ids(channel_id)}
        stream_id = int(channel_id)
        topic = await self._topic_for(stream_id, thread_root_id)
        return {"type": "stream", "to": stream_id, "topic": topic}

    async def _post_to(
        self, client: ZulipClient, target: dict[str, Any], content: str
    ) -> str:
        result = await client.call("POST", "messages", {**target, "content": content})
        message_id = int(result["id"])
        _remember(self._sent_by, message_id, client, _CACHE_MAX)
        if target["type"] == "stream":
            _remember(
                self._message_topics,
                message_id,
                (int(target["to"]), str(target["topic"])),
                _CACHE_MAX,
            )
        return str(message_id)

    async def _topic_for(self, stream_id: int, thread_root_id: str | None) -> str:
        """The topic a post belongs in: the root message's, or the default."""
        if thread_root_id is None:
            return self._config.default_topic
        root = _zulip_id(thread_root_id)
        known = self._message_topics.get(root)
        if known is not None:
            return known[1]
        result = await self._require_bridge().call(
            "GET", f"messages/{root}", {"apply_markdown": False}
        )
        message = result["message"]
        topic = str(message.get("subject", self._config.default_topic))
        _remember(self._message_topics, root, (stream_id, topic), _CACHE_MAX)
        return topic

    async def update_message(
        self, channel_id: str, message_ref: str, new_content: str
    ) -> None:
        message_id = _zulip_id(message_ref)
        try:
            client = await self._sender_of(channel_id, message_id)
            await client.call(
                "PATCH", f"messages/{message_id}", {"content": new_content}
            )
        except (ZulipError, httpx.HTTPError) as exc:
            logger.error("Failed to update Zulip message %s: %s", message_ref, exc)

    async def _sender_of(self, channel_id: str, message_id: int) -> ZulipClient:
        """The account that sent a message, which is the only one Zulip lets
        edit it. Read back from Zulip when this process has no record of it —
        it was sent before a restart — and the bridge's own where the sender
        is no account the bridge holds."""
        known = self._sent_by.get(message_id)
        if known is not None:
            return known
        result = await self._reader(channel_id).call(
            "GET", f"messages/{message_id}", {"apply_markdown": False}
        )
        sender = self._bot_by_user_id.get(int(result["message"]["sender_id"]))
        client = sender.client if sender is not None else self._require_bridge()
        _remember(self._sent_by, message_id, client, _CACHE_MAX)
        return client

    def _reader(self, channel_id: str) -> ZulipClient:
        """An account that can see a channel's messages.

        The bridge account for a stream, which it is subscribed to. A direct
        message is between a person and an agent's bot, and the bridge is not
        in it, so there it is the participating bot.
        """
        if channel_id.startswith(_DM_PREFIX):
            for user_id in _dm_user_ids(channel_id):
                bot = self._bot_by_user_id.get(user_id)
                if bot is not None:
                    return bot.client
        return self._require_bridge()

    async def delete_message(self, channel_id: str, message_ref: str) -> None:
        try:
            await self._reader(channel_id).call(
                "DELETE", f"messages/{_zulip_id(message_ref)}"
            )
        except (ZulipError, httpx.HTTPError) as exc:
            logger.error("Failed to delete Zulip message %s: %s", message_ref, exc)

    async def send_typing(
        self, channel_id: str, sender_name: str, is_typing: bool
    ) -> None:
        bot = self._bots.get(sender_name)
        if bot is None:
            return
        await self._typing(bot, channel_id, None, "start" if is_typing else "stop")

    async def notify_working(
        self, channel_id: str, agent_name: str, thread_root_id: str | None
    ) -> None:
        """The typing nudge, in the topic where the agent was asked."""
        bot = self._bots.get(agent_name)
        if bot is None:
            return
        await self._typing(bot, channel_id, thread_root_id, "start")

    async def _typing(
        self, bot: _AgentBot, channel_id: str, thread_root_id: str | None, op: str
    ) -> None:
        try:
            if channel_id.startswith(_DM_PREFIX):
                params: dict[str, Any] = {
                    "type": "direct",
                    "to": _dm_user_ids(channel_id),
                }
            else:
                stream_id = int(channel_id)
                params = {
                    "type": "stream",
                    "stream_id": stream_id,
                    "topic": await self._topic_for(stream_id, thread_root_id),
                }
            await bot.client.call("POST", "typing", {**params, "op": op})
        except (ZulipError, httpx.HTTPError) as exc:
            logger.debug("Failed to send Zulip typing for %s: %s", bot.agent_name, exc)

    # ── Attachments ──────────────────────────────────────────────────────────

    async def send_attachment(
        self,
        channel_id: str,
        sender_name: str,
        filename: str,
        mimetype: str,
        data: bytes,
        caption: str | None = None,
        thread_root_id: str | None = None,
    ) -> str | None:
        return await self.send_attachments(
            channel_id,
            sender_name,
            [OutboundAttachment(filename=filename, mimetype=mimetype, data=data)],
            caption=caption,
            thread_root_id=thread_root_id,
        )

    async def send_attachments(
        self,
        channel_id: str,
        sender_name: str,
        files: list[OutboundAttachment],
        caption: str | None = None,
        thread_root_id: str | None = None,
    ) -> str | None:
        """Upload the files as the sender and post them as one message.

        A file Zulip will not take — over the organization's upload limit, or
        refused — is named in the message rather than dropped.
        """
        client = self._sender(sender_name, channel_id)
        links: list[str] = []
        failed: list[str] = []
        for file in files:
            name = _link_text(file.filename)
            if len(file.data) > self._max_upload_bytes:
                failed.append(name)
                continue
            try:
                uploaded = await client.call(
                    "POST",
                    "user_uploads",
                    files={"file": (file.filename, file.data, file.mimetype)},
                    timeout=UPLOAD_TIMEOUT_SECONDS,
                )
            except (ZulipError, httpx.HTTPError) as exc:
                logger.warning(
                    "Zulip did not take the upload %s for %s: %s",
                    file.filename,
                    channel_id,
                    exc,
                )
                failed.append(name)
                continue
            url = str(uploaded.get("url") or uploaded.get("uri") or "")
            if not url:
                failed.append(name)
                continue
            links.append(f"[{name}]({url})")
        lines: list[str] = []
        if caption:
            lines.append(self.translate_outbound(caption))
        lines.extend(links)
        if failed:
            lines.append(
                self.translate_outbound(
                    f"_sent attachments that couldn't be relayed: {', '.join(failed)}_"
                )
            )
        if not lines:
            return None
        return await self.send_message(
            channel_id, sender_name, "\n".join(lines), thread_root_id
        )

    async def _fetch_attachments(
        self, content: str, client: ZulipClient
    ) -> tuple[list[Attachment], list[AttachmentFailure]]:
        """Download every file a message links to, whatever its type.

        Read as the account that received the message, since that is an
        account allowed to see it. A file over the bridge's size limit, or one
        that cannot be fetched, is reported as a failure rather than dropped.
        """
        attachments: list[Attachment] = []
        failures: list[AttachmentFailure] = []
        seen: set[str] = set()
        for match in _UPLOAD_LINK.finditer(content):
            path = match.group(2)
            if path in seen:
                continue
            seen.add(path)
            filename = match.group(1).strip() or unquote(path.rsplit("/", 1)[-1])
            try:
                data, mimetype = await self._download(client, path, filename)
            except _TooLarge:
                failures.append(
                    AttachmentFailure(
                        filename=filename,
                        reason=f"larger than {self._max_attachment_bytes} bytes",
                    )
                )
                continue
            except (ZulipError, httpx.HTTPError) as exc:
                logger.warning("Could not download Zulip upload %s: %s", path, exc)
                failures.append(
                    AttachmentFailure(
                        filename=filename, reason="could not be downloaded from Zulip"
                    )
                )
                continue
            attachments.append(
                Attachment(filename=filename, mimetype=mimetype, data=data)
            )
        return attachments, failures

    async def _download(
        self, client: ZulipClient, path: str, filename: str
    ) -> tuple[bytes, str]:
        """An upload's bytes, through the short-lived URL Zulip hands out for it."""
        temporary = await client.call("GET", path.lstrip("/"))
        url = str(temporary.get("url") or temporary.get("uri") or "")
        if not url:
            raise ZulipError(
                f"Zulip gave no download URL for {path}", code="NO_URL", status=200
            )
        limit = self._max_attachment_bytes
        # Followed because a server storing uploads in S3 answers with a
        # redirect to the object; each hop is checked by the outbound policy.
        async with self._require_http().stream(
            "GET", url, follow_redirects=True
        ) as response:
            response.raise_for_status()
            declared = response.headers.get("content-length")
            if declared and declared.isdigit() and int(declared) > limit:
                raise _TooLarge()
            chunks: list[bytes] = []
            size = 0
            async for chunk in response.aiter_bytes():
                size += len(chunk)
                if size > limit:
                    raise _TooLarge()
                chunks.append(chunk)
            served = response.headers.get("content-type", "")
        mimetype = (
            served.split(";", 1)[0].strip()
            or mimetypes.guess_type(filename)[0]
            or "application/octet-stream"
        )
        return b"".join(chunks), mimetype

    # ── Session activity ─────────────────────────────────────────────────────

    def rich_fallback_limit(self) -> int:
        return self._max_message_length

    def rich_fallback_text(self, content: RichContent) -> str:
        """What a publication says without the lookups a mention needs."""
        return self._draw(content, mention=None, responder=None).plain

    def _draw(
        self, content: RichContent, *, mention: str | None, responder: str | None
    ) -> _Rendered:
        escape = self._rich_escape
        limit = self.rich_fallback_limit()
        markup = self.rich_markup()
        if isinstance(content, TurnActivity):
            actions = turn_actions(content)
            notes = []
            if actions and content.turn.status == "queued":
                notes.append(INTERRUPT_QUEUED_NOTE)
            if content.notify_unreachable:
                notes.append(self.unnotified_notice())
            tail = "".join(f"\n{line}" for line in notes)
            hint = f"\n{stop_hint()}" if actions else ""
            status = turn_status(
                content.items,
                content.turn,
                escape=escape,
                limit=max(1, limit - len(tail) - len(hint)),
                markup=markup,
                elapsed_seconds=content.elapsed_seconds,
                session_url=content.session_url,
                mention=mention,
                error_summary=content.error_summary,
                tool_detail=True,
            )
            return _Rendered(
                text=f"{status}{hint}{tail}",
                plain=f"{status}{tail}",
                actions=actions,
                value=content.interrupt_turn_id,
            )
        lead = f"{mention}\n" if mention else ""
        tail = f"\n{self.unnotified_notice()}" if content.notify_unreachable else ""
        possible = possible_card_actions(content)
        reserve = len(card_hint(possible)) + 1 if possible else 0
        drawn = render_request(
            content.request,
            content.reference,
            escape=escape,
            limit=max(1, limit - len(lead) - len(tail) - reserve),
            markup=markup,
            responder=responder,
            unavailable_reason=content.unavailable_reason,
            control_label_limit=None,
        )
        actions = card_actions(content, drawn)
        hint = f"\n{card_hint(actions)}" if actions else ""
        return _Rendered(
            text=f"{lead}{drawn.text}{hint}{tail}",
            plain=f"{lead}{drawn.text}{tail}",
            actions=actions,
            value=content.reference.token,
        )

    async def _render_rich(self, content: RichContent) -> _Rendered:
        mention = await self._mention(content.notify_external_id, silent=False)
        responder = (
            await self._mention(content.responder_external_id, silent=True)
            if isinstance(content, RequestCard)
            else None
        )
        return self._draw(content, mention=mention, responder=responder)

    async def post_rich(
        self,
        channel_id: str,
        agent_name: str,
        content: RichContent,
        thread_root_id: str | None = None,
    ) -> str:
        """Post a turn's status or a request's card as the agent's own bot.

        `RichContentFailed` only where Zulip refused: it licenses the caller to
        give up on the publication. A send whose outcome is unknown — a
        timeout, a server error — raises as itself, so the reservation survives
        and `find_request_card` gets the chance to find what landed.
        """
        rendered = await self._render_rich(content)
        bot = self._bots.get(agent_name)
        if bot is None:
            raise RichContentFailed(
                f"No Zulip bot for agent {agent_name!r}, so its activity cannot "
                f"be posted in {channel_id}.",
                text=rendered.plain,
            )
        try:
            target = await self._target(channel_id, thread_root_id)
        except ZulipError as exc:
            if exc.outcome_unknown:
                raise
            raise ThreadUnavailable(
                f"The Zulip message {thread_root_id} this belongs under is gone, "
                f"so there is no topic to post it in: {exc}",
                text=rendered.plain,
            ) from exc
        try:
            ref = await self._post_to(bot.client, target, rendered.text)
        except ZulipError as exc:
            failure = self._rich_failure(exc, rendered.plain)
            if failure is None:
                raise
            raise failure from exc
        message_id = int(ref)
        _remember(self._last_text, message_id, rendered.text, _CACHE_MAX)
        _remember(self._reconciled, message_id, None, _CACHE_MAX)
        await self._sync_controls(
            bot, channel_id, message_id, thread_root_id or ref, rendered
        )
        return ref

    async def update_rich(
        self,
        channel_id: str,
        agent_name: str,
        message_ref: str,
        content: RichContent,
        thread_root_id: str | None,
    ) -> None:
        """Redraw a publication in place, as the bot that posted it.

        A post notifies and an edit does not, so the mention is drawn on the
        first post only. An edit Zulip refuses because the organization's edit
        window has closed is reported once as wedged, and later redraws of that
        message return quietly.
        """
        message_id = int(message_ref)
        if message_id in self._wedged:
            return
        rendered = await self._render_rich(replace(content, notify_external_id=None))
        bot = self._bots.get(agent_name)
        client = bot.client if bot is not None else self._sent_by.get(message_id)
        if client is None:
            raise RichContentFailed(
                f"No Zulip bot for agent {agent_name!r}, so message {message_ref} "
                f"in {channel_id} cannot be updated.",
                text=rendered.plain,
            )
        if self._last_text.get(message_id) != rendered.text:
            try:
                await client.call(
                    "PATCH", f"messages/{message_id}", {"content": rendered.text}
                )
            except ZulipError as exc:
                if _edit_window_closed(exc):
                    _remember(self._wedged, message_id, None, _CACHE_MAX)
                    raise RichContentWedged(
                        f"Zulip no longer allows message {message_ref} in "
                        f"{channel_id} to be edited: {exc}",
                        text=rendered.plain,
                    ) from exc
                failure = self._rich_failure(exc, rendered.plain)
                if failure is None:
                    raise
                raise failure from exc
            _remember(self._last_text, message_id, rendered.text, _CACHE_MAX)
        if bot is not None:
            await self._sync_controls(
                bot, channel_id, message_id, thread_root_id or message_ref, rendered
            )

    def _rich_failure(self, exc: ZulipError, text: str) -> RichContentFailed | None:
        """What a Zulip error means to a publisher, or None where nobody knows
        whether the publication happened and the error should go up as is."""
        if exc.code == "RATE_LIMIT_HIT":
            return RichContentThrottled(retry_after=exc.retry_after or 1.0, text=text)
        if exc.outcome_unknown:
            return None
        return RichContentFailed(str(exc), text=text)

    async def _mention(
        self, external_user_id: str | None, *, silent: bool
    ) -> str | None:
        """`@**Name|id**` for a Zulip user id, or None if it cannot be named.

        Silent (`@_**…**`) where the name is a record rather than a call to
        act — who answered a card — since Zulip notifies a person newly
        mentioned in an edit as well as in a post.
        """
        if not external_user_id or not external_user_id.isdigit():
            return None
        user_id = int(external_user_id)
        name = self._full_names.get(user_id)
        if name is None:
            try:
                user = await self._require_bridge().call("GET", f"users/{user_id}")
            except (ZulipError, httpx.HTTPError) as exc:
                logger.warning("Could not name Zulip user %s: %s", user_id, exc)
                return None
            name = str(user["user"].get("full_name", ""))
            if not name:
                return None
            self._remember_person(user_id, str(user["user"].get("email", "")), name)
        return f"@{'_' if silent else ''}**{name}|{user_id}**"

    async def _sync_controls(
        self,
        bot: _AgentBot,
        channel_id: str,
        message_id: int,
        thread_ref: str,
        rendered: _Rendered,
    ) -> None:
        """Make the reactions on a publication match what it now offers.

        Seeded as the agent's bot, in option order, and taken off once the
        card is settled or the turn has nothing left to stop. A message this
        process has no record of — one posted before a restart — has whatever
        the bot left on it cleared once, so a settled card does not keep
        inviting a click nobody can resolve.

        Never raises. The reactions are a convenience on top of a card that is
        answerable in words, and a publication must not fail over one.
        """
        offer = self._offers.get(message_id)
        seeded = set(offer.seeded) if offer else set()
        try:
            if offer is None and message_id not in self._reconciled:
                seeded = await self._bot_reactions(bot, message_id)
                _remember(self._reconciled, message_id, None, _CACHE_MAX)
            for emoji in rendered.actions:
                if emoji not in seeded:
                    await self._react(bot, message_id, emoji, on=True)
                    seeded.add(emoji)
            for emoji in list(seeded - set(rendered.actions)):
                await self._react(bot, message_id, emoji, on=False)
                seeded.discard(emoji)
        except (ZulipError, httpx.HTTPError) as exc:
            logger.warning(
                "Could not update the reactions on Zulip message %s: %s",
                message_id,
                exc,
            )
        if rendered.actions and rendered.value:
            _remember(
                self._offers,
                message_id,
                Offer(
                    channel_id=channel_id,
                    thread_ref=thread_ref,
                    value=rendered.value,
                    actions=dict(rendered.actions),
                    seeded=seeded,
                ),
                _CACHE_MAX,
            )
        elif seeded and rendered.value:
            # Removal failed part-way: keep the record so the next redraw
            # tries again, but with nothing a click could press.
            _remember(
                self._offers,
                message_id,
                Offer(
                    channel_id=channel_id,
                    thread_ref=thread_ref,
                    value=rendered.value,
                    actions={},
                    seeded=seeded,
                ),
                _CACHE_MAX,
            )
        else:
            self._offers.pop(message_id, None)

    async def _bot_reactions(self, bot: _AgentBot, message_id: int) -> set[str]:
        """The control reactions `bot` already has on a message."""
        result = await bot.client.call(
            "GET", f"messages/{message_id}", {"apply_markdown": False}
        )
        controls = set(NUMBER_EMOJI) | {STOP_EMOJI}
        return {
            str(reaction.get("emoji_name"))
            for reaction in result["message"].get("reactions", [])
            if int(reaction.get("user_id", 0)) == bot.user_id
            and reaction.get("emoji_name") in controls
        }

    async def _react(
        self, bot: _AgentBot, message_id: int, emoji: str, *, on: bool
    ) -> None:
        """Add or remove one of the bot's reactions, settling on either no-op."""
        try:
            await bot.client.call(
                "POST" if on else "DELETE",
                f"messages/{message_id}/reactions",
                {"emoji_name": emoji},
            )
        except ZulipError as exc:
            already = "REACTION_ALREADY_EXISTS" if on else "REACTION_DOES_NOT_EXIST"
            if exc.code != already:
                raise

    async def find_request_card(
        self,
        channel_id: str,
        thread_root_id: str | None,
        token: str,
        created_at: datetime,
        handle: str | None,
    ) -> str | None:
        """Look for a card one of this bridge's bots may already have posted.

        By its heading line: bold from its first character and ending in the
        handle, posted by one of the agents' bots after the reservation was
        made. A status prints no handle, so it stays unconfirmed rather than
        being posted twice. A lookup that cannot be trusted answers None, and
        the question is asked again later.
        """
        if handle is None:
            logger.warning(
                "Cannot look for the Zulip publication marked %s in %s: only a "
                "request card, which prints its handle, can be recognised again.",
                token,
                channel_id,
            )
            return None
        wanted = f"· request {MARKDOWN.code(handle)}"
        after = (created_at - _RECOVERY_SKEW).timestamp()
        try:
            narrow = await self._narrow(channel_id, thread_root_id)
            result = await self._reader(channel_id).call(
                "GET",
                "messages",
                {
                    "anchor": "newest",
                    "num_before": _RECOVERY_LIMIT,
                    "num_after": 0,
                    "narrow": narrow,
                    "apply_markdown": False,
                },
            )
        except (ZulipError, httpx.HTTPError) as exc:
            logger.warning(
                "Could not look for card %s in Zulip %s: %s", handle, channel_id, exc
            )
            return None
        for message in sorted(result.get("messages", []), key=lambda m: m["id"]):
            if int(message.get("sender_id", 0)) not in self._bot_by_user_id:
                continue
            if float(message.get("timestamp", 0)) < after:
                continue
            content = str(message.get("content", ""))
            if any(
                line.startswith("**") and line.endswith(wanted)
                for line in content.split("\n")
            ):
                return str(message["id"])
        return None

    async def is_first_reply(
        self, channel_id: str, root_ref: str, message_ref: str
    ) -> bool:
        """Whether this message is the first thing said after a card.

        Read from the topic the card is in, since a topic is the thread. Never
        raises: this is on the inbound path of every message.
        """
        try:
            root = _zulip_id(root_ref)
            target = _zulip_id(message_ref)
            if channel_id.startswith(_DM_PREFIX):
                narrow = await self._narrow(channel_id, None)
            else:
                narrow = await self._narrow(channel_id, root_ref)
            result = await self._reader(channel_id).call(
                "GET",
                "messages",
                {
                    "anchor": root,
                    "num_before": 0,
                    "num_after": _FIRST_REPLY_WINDOW,
                    "include_anchor": False,
                    "narrow": narrow,
                    "apply_markdown": False,
                },
            )
        except (ZulipError, httpx.HTTPError, ValueError, RuntimeError) as exc:
            logger.warning(
                "Could not read what follows %s in Zulip %s: %s. Treating %s as "
                "not the first reply.",
                root_ref,
                channel_id,
                exc,
                message_ref,
            )
            return False
        replies = sorted(
            int(m["id"]) for m in result.get("messages", []) if int(m["id"]) > root
        )
        return bool(replies) and replies[0] == target

    async def _narrow(
        self, channel_id: str, thread_root_id: str | None
    ) -> list[dict[str, Any]]:
        if channel_id.startswith(_DM_PREFIX):
            return [{"operator": "dm", "operand": _dm_user_ids(channel_id)}]
        stream_id = int(channel_id)
        topic = await self._topic_for(stream_id, thread_root_id)
        return [
            {"operator": "stream", "operand": stream_id},
            {"operator": "topic", "operand": topic},
        ]

    async def mark_activity(
        self,
        channel_id: str,
        message_ref: str,
        *,
        agent_name: str,
        mark: ActivityMark,
        on: bool,
        force: bool = False,
    ) -> None:
        """Put this agent's 👀 or ⏳ on the message it is handling, or take it off.

        `force` is the publisher reconciling after a restart, when this
        process's record of what is already there is empty and wrong. Raises
        when the mark did not happen, so the publisher retries; a refusal no
        retry will change — the bot cannot see the message — is
        `ActivityMarkRefused`.
        """
        key = (agent_name, message_ref, mark)
        if not force and on == (key in self._marked):
            return
        bot = self._bots.get(agent_name)
        if bot is None:
            raise RuntimeError(f"no Zulip bot for {agent_name!r}")
        emoji = WORKING_EMOJI if mark == "working" else QUEUED_EMOJI
        try:
            await self._react(bot, _zulip_id(message_ref), emoji, on=on)
        except ZulipError as exc:
            if exc.status in (400, 403) and exc.code != "RATE_LIMIT_HIT":
                raise ActivityMarkRefused(str(exc)) from exc
            raise
        if on:
            _remember(self._marked, key, None, _CACHE_MAX)
        else:
            self._marked.pop(key, None)

    async def tell_actor(
        self,
        channel_id: str,
        actor_ref: str,
        actor_name: str,
        thread_ref: str | None,
        text: str,
    ) -> None:
        """Tell one person, in a direct message from the bridge, why what they
        did on a card or a status — an answer, a stop — did not take effect.

        Private because only they need to know. Falls back to the card's thread
        when the direct message cannot be sent. Never raises.
        """
        if not actor_ref.isdigit():
            await super().tell_actor(
                channel_id, actor_ref, actor_name, thread_ref, text
            )
            return
        about = "About your reply in Switch"
        if thread_ref is not None and _is_message_ref(thread_ref):
            link = await self.channel_deeplink(channel_id)
            if link:
                about = (
                    f"About [this conversation]({link}/near/{_zulip_id(thread_ref)})"
                )
        body = f"{about}: {self.escape_label_for_body(text)}"
        try:
            await self._require_bridge().call(
                "POST",
                "messages",
                {"type": "direct", "to": [int(actor_ref)], "content": body},
            )
        except (ZulipError, httpx.HTTPError, RuntimeError) as exc:
            logger.warning(
                "Could not message %s privately about their answer: %s", actor_ref, exc
            )
            await super().tell_actor(
                channel_id, actor_ref, actor_name, thread_ref, text
            )

    # ── Channels ─────────────────────────────────────────────────────────────

    async def create_channel(
        self,
        name: str,
        topic: str,
        *,
        channel_type: ChannelType = "channel_public",
    ) -> str:
        if channel_type in ("group", "direct"):
            raise ValueError(
                f"Cannot create {channel_type} channels — they are initiated from the messaging platform"
            )
        bridge = self._require_bridge()
        stream_name = name.strip()[:60]
        # Subscribing to a name that is taken joins the existing stream rather
        # than failing, which would silently bridge someone else's channel.
        if await self._stream_id(stream_name) is not None:
            stream_name = f"{stream_name[:51]}-{uuid.uuid4().hex[:8]}"
        # Zulip refuses a principals list that mixes ids and emails.
        principals: list[int | str] = [self._bridge_user_id]
        if self._default_member_id is not None:
            principals.append(self._default_member_id)
        await bridge.call(
            "POST",
            "users/me/subscriptions",
            {
                "subscriptions": [{"name": stream_name, "description": topic}],
                "principals": principals,
                "invite_only": channel_type == "channel_private",
            },
        )
        stream_id = await self._stream_id(stream_name)
        if stream_id is None:
            raise RuntimeError(
                f"Zulip accepted stream '{stream_name}' but cannot find it afterwards"
            )
        return str(stream_id)

    async def _stream_id(self, stream_name: str) -> int | None:
        try:
            result = await self._require_bridge().call(
                "GET", "get_stream_id", {"stream": stream_name}
            )
        except ZulipError as exc:
            if exc.code == "BAD_REQUEST" or exc.status == 400:
                return None
            raise
        return int(result["stream_id"])

    async def get_channel_type(self, channel_id: str) -> ChannelType:
        if channel_id.startswith(_DM_PREFIX):
            return "direct" if len(_dm_user_ids(channel_id)) <= 2 else "group"
        result = await self._require_bridge().call("GET", f"streams/{int(channel_id)}")
        return (
            "channel_private"
            if result["stream"].get("invite_only")
            else "channel_public"
        )

    async def _cached_channel_type(self, channel_id: str) -> ChannelType:
        """`get_channel_type` for the inbound path, which asks it of every
        message: read again only once the last answer is a few minutes old."""
        cached = self._channel_types.get(channel_id)
        now = time.monotonic()
        if cached is not None and now - cached[1] < _CHANNEL_TYPE_TTL_SECONDS:
            return cached[0]
        channel_type = await self.get_channel_type(channel_id)
        self._channel_types[channel_id] = (channel_type, now)
        return channel_type

    async def channel_deeplink(self, external_channel_id: str) -> str | None:
        if external_channel_id.startswith(_DM_PREFIX):
            ids = ",".join(str(i) for i in _dm_user_ids(external_channel_id))
            return f"{self._config.site}/#narrow/dm/{ids}"
        return f"{self._config.site}/#narrow/channel/{external_channel_id}"

    async def home_deeplink(self) -> str | None:
        return self._config.site

    async def add_agents_to_channel(
        self, channel_id: str, agent_names: list[str]
    ) -> None:
        if channel_id.startswith(_DM_PREFIX):
            return
        user_ids: list[int | str] = []
        for agent_name in agent_names:
            bot = self._bots.get(agent_name)
            if bot is None:
                logger.warning(
                    "No Zulip bot for agent '%s', cannot add it to stream %s",
                    agent_name,
                    channel_id,
                )
                continue
            user_ids.append(bot.user_id)
        if user_ids:
            await self._subscribe(int(channel_id), user_ids)

    async def add_users_to_channel(
        self,
        channel_id: str,
        user_names: list[str],
        user_external_ids: list[str],
    ) -> list[str]:
        if channel_id.startswith(_DM_PREFIX):
            return []
        failed: list[str] = []
        # Zulip identifies a person by user id or email; a paired external id
        # is the user id, and a bare name (the default member) is an email.
        for index, user_name in enumerate(user_names):
            external_id = (
                user_external_ids[index]
                if index < len(user_external_ids)
                else user_name
            )
            principal: int | str = (
                int(external_id) if external_id.isdigit() else user_name
            )
            try:
                await self._subscribe(int(channel_id), [principal])
            except ZulipError:
                logger.exception(
                    "Failed to subscribe '%s' to Zulip stream %s", user_name, channel_id
                )
                failed.append(external_id)
        return failed

    async def _subscribe(self, stream_id: int, principals: list[int | str]) -> None:
        bridge = self._require_bridge()
        stream = await bridge.call("GET", f"streams/{stream_id}")
        await bridge.call(
            "POST",
            "users/me/subscriptions",
            {
                "subscriptions": [{"name": stream["stream"]["name"]}],
                "principals": principals,
            },
        )

    async def get_channel_agent_names(self, channel_id: str) -> list[str]:
        if channel_id.startswith(_DM_PREFIX):
            members = _dm_user_ids(channel_id)
        else:
            result = await self._require_bridge().call(
                "GET", f"streams/{int(channel_id)}/members"
            )
            members = [int(m) for m in result["subscribers"]]
        return [
            self._bot_by_user_id[m].agent_name
            for m in members
            if m in self._bot_by_user_id
        ]

    async def search_directory_users(self, query: str) -> list[DirectoryUser]:
        result = await self._require_bridge().call("GET", "users")
        needle = query.casefold()
        found: list[DirectoryUser] = []
        for member in result["members"]:
            if member.get("is_bot") or not member.get("is_active", True):
                continue
            email = str(member.get("email", ""))
            full_name = str(member.get("full_name", ""))
            if (
                needle
                and needle not in email.casefold()
                and needle not in full_name.casefold()
            ):
                continue
            found.append(
                DirectoryUser(
                    external_user_id=str(member["user_id"]),
                    username=email,
                    display_name=full_name,
                    email=member.get("delivery_email") or None,
                )
            )
        return found[:50]

    # ── Agent identity ───────────────────────────────────────────────────────

    async def create_agent_identity(
        self, agent_name: str, agent_description: str
    ) -> None:
        async with self._identity_locks.setdefault(agent_name, asyncio.Lock()):
            await self._create_agent_identity(agent_name, agent_description)

    async def _create_agent_identity(
        self, agent_name: str, agent_description: str
    ) -> None:
        bridge = self._require_bridge()
        http = self._require_http()
        if agent_name in self._bots:
            return

        label = (await self.agent_rendering(agent_name)).field_label
        short_name = _short_name(agent_name)

        # `GET /bots` lists the bots this account owns, API keys included, so a
        # bot created on an earlier run is adopted rather than duplicated.
        owned = await bridge.call("GET", "bots")
        existing = next(
            (
                b
                for b in owned.get("bots", [])
                if str(b.get("username", "")).startswith(f"{short_name}-bot@")
            ),
            None,
        )
        if existing is not None:
            # `GET /bots` names a bot by its email and carries no user id.
            email = str(existing["username"])
            user_id = await self._user_id(email)
            api_key = str(existing["api_key"])
            if existing.get("full_name") != label:
                await bridge.call("PATCH", f"bots/{user_id}", {"full_name": label})
        else:
            reactivated = await self._reactivate_bot(short_name)
            if reactivated is not None:
                user_id, email, api_key = reactivated
                await bridge.call("PATCH", f"bots/{user_id}", {"full_name": label})
            else:
                created = await bridge.call(
                    "POST",
                    "bots",
                    {"full_name": label, "short_name": short_name, "bot_type": 1},
                )
                user_id = int(created["user_id"])
                api_key = str(created["api_key"])
                email = str(created.get("email") or (await self._user_email(user_id)))

        holder = self._bot_by_user_id.get(user_id)
        if holder is not None and holder.agent_name != agent_name:
            raise RuntimeError(
                f"Zulip bot {email} already speaks for agent {holder.agent_name!r}, "
                f"so it cannot also be {agent_name!r}"
            )

        bot = _AgentBot(
            agent_name=agent_name,
            user_id=user_id,
            full_name=label,
            client=ZulipClient(http, email=email, api_key=api_key),
        )
        self._bots[agent_name] = bot
        self._bot_by_user_id[user_id] = bot
        self._realm_bot_ids.add(user_id)
        self._remember_person(user_id, email, label)
        bot.poller = asyncio.create_task(
            self._poll(
                f"bot:{agent_name}",
                bot.client,
                {
                    "event_types": ["message", "update_message", "reaction"],
                    "narrow": [["is", "dm"]],
                },
                agent=bot,
            ),
            name=f"zulip-bot-events-{agent_name}",
        )
        self._spawn(self._set_avatar(bot), f"zulip-avatar-{agent_name}")
        logger.info("Zulip bot identity ready for agent %s (%s)", agent_name, email)

    async def _reactivate_bot(self, short_name: str) -> tuple[int, str, str] | None:
        """Bring back a bot of this bridge's that removing it deactivated.

        `GET /bots` lists only active bots, and Zulip keeps a deactivated bot's
        name, so without this a bridge registered again could create none of
        its agents' bots. Returns the bot's user id, email and API key, or None
        where there is no such bot.
        """
        bridge = self._require_bridge()
        members = await bridge.call("GET", "users")
        found = next(
            (
                m
                for m in members.get("members", [])
                if m.get("is_bot")
                and not m.get("is_active", True)
                and m.get("bot_owner_id") == self._bridge_user_id
                and str(m.get("email", "")).startswith(f"{short_name}-bot@")
            ),
            None,
        )
        if found is None:
            return None
        user_id = int(found["user_id"])
        await bridge.call("POST", f"users/{user_id}/reactivate")
        owned = await bridge.call("GET", "bots")
        for bot in owned.get("bots", []):
            if str(bot.get("username", "")) == str(found["email"]):
                logger.info("Reactivated Zulip bot %s", found["email"])
                return user_id, str(found["email"]), str(bot["api_key"])
        raise RuntimeError(
            f"Reactivated Zulip bot {found['email']} but it is not among the "
            "bridge account's bots"
        )

    async def _set_avatar(self, bot: _AgentBot) -> None:
        """Give the bot its agent's icon. Best effort: a bot without one still
        speaks, and a failure is logged rather than raised."""
        try:
            url = await self.agent_icon_url(bot.agent_name)
            image = await self._fetch_icon(url)
            if image is None:
                logger.warning(
                    "The icon for %s is over Zulip's %d byte avatar limit; its bot "
                    "keeps the avatar it has",
                    bot.agent_name,
                    self._max_avatar_bytes,
                )
                return
            data, mimetype = image
            extension = mimetypes.guess_extension(mimetype) or ".png"
            await self._require_bridge().call(
                "PATCH",
                f"bots/{bot.user_id}",
                files={"file": (f"avatar{extension}", data, mimetype)},
                timeout=UPLOAD_TIMEOUT_SECONDS,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Could not set the Zulip avatar for %s", bot.agent_name)

    async def _fetch_icon(self, url: str) -> tuple[bytes, str] | None:
        """An icon's bytes and type, or None if it is over the avatar limit.

        The one place the bridge dereferences an agent's icon URL rather than
        handing it to Zulip, so the fetch is bounded: only addresses the
        outbound policy allows, no redirects, and the size checked while
        reading.
        """
        async with guarded_async_client(
            self.outbound_policy, follow_redirects=False, timeout=10.0
        ) as client:
            async with client.stream("GET", url) as response:
                response.raise_for_status()
                chunks: list[bytes] = []
                size = 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > self._max_avatar_bytes:
                        return None
                    chunks.append(chunk)
                served = response.headers.get("content-type", "image/png")
        return b"".join(chunks), served.split(";", 1)[0].strip() or "image/png"

    async def _user_id(self, user: str) -> int:
        if user.isdigit():
            return int(user)
        result = await self._require_bridge().call("GET", f"users/{user}")
        return int(result["user"]["user_id"])

    async def _user_email(self, user_id: int) -> str:
        result = await self._require_bridge().call("GET", f"users/{user_id}")
        return str(result["user"]["email"])

    async def remove_agent_identity(self, agent_name: str) -> None:
        bot = self._bots.pop(agent_name, None)
        if bot is None:
            return
        self._bot_by_user_id.pop(bot.user_id, None)
        if bot.poller is not None:
            bot.poller.cancel()
        queue = self._queues.pop(f"bot:{agent_name}", None)
        if queue is not None:
            await self._delete_queue(*queue)

    # ── Markup ───────────────────────────────────────────────────────────────

    def prime_mention_targets(self, targets: dict[str, str]) -> None:
        for username, external_id in targets.items():
            if username and external_id.isdigit():
                self._mention_ids[username.casefold()] = int(external_id)

    def _remember_person(self, user_id: int, email: str, full_name: str) -> None:
        if email:
            self._mention_ids[email.casefold()] = user_id
            self._emails[user_id] = email
        if full_name:
            self._full_names[user_id] = full_name

    def _render_outbound(self, content: str) -> str:
        def person(match: re.Match[str]) -> str:
            user_id = self._mention_ids.get(match.group(1).casefold())
            if user_id is None:
                return match.group(0)
            name = self._full_names.get(user_id, match.group(1))
            return f"@**{name}|{user_id}**"

        def mention(match: re.Match[str]) -> str:
            bot = self._bots.get(match.group(1))
            if bot is not None:
                return f"@**{bot.full_name}|{bot.user_id}**"
            return f"@{match.group(1)}"

        return _TRANSPORT_MENTION.sub(mention, _EMAIL_MENTION.sub(person, content))

    def defuse_mass_mentions(self, text: str) -> str:
        # Zulip pages only on its own syntax, so the words alone are harmless
        # and are left as written; the `**` form is what has to be broken.
        return _WILDCARD_MENTION.sub(
            lambda m: f"@{ZERO_WIDTH_SPACE}**{m.group(1)}**", text
        )

    def translate_inbound(self, raw_message: str) -> str:
        """Rewrite a mention of an agent's bot to the `@agent` Switch routes on."""

        def mention(match: re.Match[str]) -> str:
            name, user_id = match.group(1), match.group(2)
            bot: _AgentBot | None = None
            if user_id is not None:
                bot = self._bot_by_user_id.get(int(user_id))
            else:
                bot = next(
                    (b for b in self._bots.values() if b.full_name == name), None
                )
            return f"@{bot.agent_name}" if bot is not None else match.group(0)

        return _USER_MENTION.sub(mention, raw_message)

    # ── Events ───────────────────────────────────────────────────────────────

    async def _poll(
        self,
        name: str,
        client: ZulipClient,
        register: dict[str, Any],
        *,
        agent: _AgentBot | None,
    ) -> None:
        """Long-poll one event queue for as long as the bridge runs.

        A queue the server has garbage-collected (`BAD_EVENT_QUEUE_ID`, after
        about ten minutes without a poll) is registered afresh. Anything else
        that goes wrong is logged and retried with backoff, because this loop
        is the bridge's only ear: if it dies, the platform goes quiet with
        nothing to say so.
        """
        delay = _RETRY_INITIAL_SECONDS
        while True:
            try:
                queue = await client.call(
                    "POST", "register", {**register, "apply_markdown": False}
                )
            except asyncio.CancelledError:
                raise
            except (ZulipError, httpx.HTTPError) as exc:
                logger.error(
                    "Could not open a Zulip event queue for %s: %s", client.email, exc
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, _RETRY_MAX_SECONDS)
                continue
            queue_id = str(queue["queue_id"])
            self._queues[name] = (client, queue_id)
            last_event_id = int(queue["last_event_id"])
            delay = _RETRY_INITIAL_SECONDS
            while True:
                # A failed poll is asked again of the same queue from the same
                # place, so nothing that arrived meanwhile is skipped; only a
                # queue Zulip has dropped is replaced.
                try:
                    result = await client.call(
                        "GET",
                        "events",
                        {"queue_id": queue_id, "last_event_id": last_event_id},
                        timeout=EVENTS_TIMEOUT_SECONDS,
                    )
                except asyncio.CancelledError:
                    raise
                except ZulipError as exc:
                    if exc.code == "BAD_EVENT_QUEUE_ID":
                        logger.warning(
                            "Zulip dropped the event queue for %s; opening a new "
                            "one. Events from while it was gone are not delivered.",
                            client.email,
                        )
                        break
                    logger.error(
                        "Zulip event poll for %s failed: %s", client.email, exc
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, _RETRY_MAX_SECONDS)
                    continue
                except httpx.HTTPError as exc:
                    logger.warning(
                        "Zulip event poll for %s failed: %s", client.email, exc
                    )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, _RETRY_MAX_SECONDS)
                    continue
                delay = _RETRY_INITIAL_SECONDS
                for event in result.get("events", []):
                    last_event_id = max(last_event_id, int(event["id"]))
                    try:
                        await self._handle_event(event, agent)
                    except Exception:
                        logger.exception(
                            "Failed to handle Zulip event %s", event.get("type")
                        )

    async def _handle_event(
        self, event: dict[str, Any], agent: _AgentBot | None
    ) -> None:
        kind = event.get("type")
        if kind == "message":
            await self._handle_message(event["message"], agent)
        elif kind == "subscription":
            await self._handle_subscription(event)
        elif kind == "reaction":
            await self._handle_reaction(event)
        elif kind == "update_message":
            await self._handle_edit(event, agent)

    async def _handle_edit(
        self, event: dict[str, Any], agent: _AgentBot | None
    ) -> None:
        """Relay an edit that names an agent the message did not name before.

        Switch takes no edits — a room keeps what was said — so the edited
        message goes in as a message of its own, marked as an edit, rather
        than changing the one already there. Only an edit that brings an agent
        in is relayed: a reworded or corrected message would otherwise be said
        twice, and an agent already named would be asked twice. Zulip's own
        edit window bounds how old a message can be brought back this way.
        """
        if event.get("rendering_only") or "content" not in event:
            return
        if "orig_content" not in event:
            return
        newly_named = self._agents_named(str(event["content"])) - self._agents_named(
            str(event["orig_content"])
        )
        if not newly_named:
            return
        message_id = int(event["message_id"])
        reader = agent.client if agent is not None else self._require_bridge()
        try:
            result = await reader.call(
                "GET", f"messages/{message_id}", {"apply_markdown": False}
            )
        except (ZulipError, httpx.HTTPError) as exc:
            logger.warning(
                "Could not read edited Zulip message %s, so the edit naming %s "
                "is not relayed: %s",
                message_id,
                ", ".join(sorted(newly_named)),
                exc,
            )
            return
        message = result["message"]
        # Only a message's sender can change what it says.
        if int(message.get("sender_id", 0)) != int(event.get("user_id") or 0):
            return
        edit = str(event.get("edit_timestamp") or int(time.time()))
        await self._handle_message(message, agent, edit=edit)

    def _agents_named(self, text: str) -> set[str]:
        """The agents a message names, as Zulip mentions or as `@agent-name`."""
        named: set[str] = set()
        for match in _USER_MENTION.finditer(text):
            name, user_id = match.group(1), match.group(2)
            bot = (
                self._bot_by_user_id.get(int(user_id))
                if user_id is not None
                else next((b for b in self._bots.values() if b.full_name == name), None)
            )
            if bot is not None:
                named.add(bot.agent_name)
        for match in _PLAIN_MENTION.finditer(text):
            if match.group(1) in self._bots:
                named.add(match.group(1))
        return named

    async def _handle_message(
        self,
        message: dict[str, Any],
        agent: _AgentBot | None,
        *,
        edit: str | None = None,
    ) -> None:
        sender_id = int(message["sender_id"])
        if sender_id == self._bridge_user_id or sender_id in self._bot_by_user_id:
            return
        if message.get("sender_realm_str") == _SYSTEM_BOT_REALM:
            return
        if message.get("type") != "stream" and agent is None:
            # A direct message as the bridge account sees it. Either it is to
            # the bridge itself, with nobody to route it to, or the bridge is
            # in a group conversation with a bot, whose own queue delivers it.
            # Decided before it is marked seen, so this copy cannot shadow that
            # one.
            return
        message_id = int(message["id"])
        message_ref = (
            str(message_id) if edit is None else f"{message_id}{_EDIT_SEPARATOR}{edit}"
        )
        if message_ref in self._seen:
            return
        _remember(self._seen, message_ref, None, _SEEN_MAX)

        sender_name = str(message.get("sender_email", sender_id))
        self._remember_person(
            sender_id, sender_name, str(message.get("sender_full_name", ""))
        )
        content = str(message.get("content", ""))
        if edit is not None:
            content = f"{content}\n_(edited)_"
        root_id: str | None = None
        channel_name: str | None = None

        if message.get("type") == "stream":
            stream_id = int(message["stream_id"])
            topic = str(message.get("subject", ""))
            channel_id = str(stream_id)
            channel_name = str(message.get("display_recipient", "")) or None
            channel_type = await self._cached_channel_type(channel_id)
            _remember(self._message_topics, message_id, (stream_id, topic), _CACHE_MAX)
            if not _same_topic(topic, self._config.default_topic):
                root = await self._topic_root(stream_id, topic, message_id)
                root_id = str(root) if root != message_id else None
        else:
            recipients = message.get("display_recipient", [])
            user_ids = sorted(int(r["id"]) for r in recipients)
            channel_id = _DM_PREFIX + ",".join(str(i) for i in user_ids)
            channel_type = "direct" if len(user_ids) <= 2 else "group"

        agent_name = agent.agent_name if agent is not None else None
        stripped = content.strip()
        # An edit is never run as a command: a command is something done once.
        if edit is None and stripped.startswith("!"):
            parts = stripped.split(None, 1)
            await _started(self._on_command)(
                InboundCommand(
                    channel_id=channel_id,
                    channel_type=channel_type,
                    sender_id=str(sender_id),
                    sender_name=sender_name,
                    command=parts[0].lstrip("!"),
                    args=parts[1].strip() if len(parts) > 1 else "",
                    message_ref=str(message_id),
                    root_id=root_id,
                    agent_name=agent_name,
                    channel_name=channel_name,
                )
            )
            return

        reader = agent.client if agent is not None else self._require_bridge()
        # An edit's files went in with the message the first time.
        attachments, failures = (
            await self._fetch_attachments(content, reader) if edit is None else ([], [])
        )
        await _started(self._on_message)(
            InboundMessage(
                channel_id=channel_id,
                channel_type=channel_type,
                sender_id=str(sender_id),
                sender_name=sender_name,
                content=content,
                message_ref=message_ref,
                root_id=root_id,
                agent_name=agent_name,
                channel_name=channel_name,
                attachments=attachments,
                attachment_failures=failures,
                sender_is_app=sender_id in self._realm_bot_ids,
            )
        )

    async def _handle_reaction(self, event: dict[str, Any]) -> None:
        """A click on one of the reactions a publication offers is a press."""
        message_id = int(event.get("message_id", 0))
        user_id = int(event.get("user_id", 0))
        emoji = str(event.get("emoji_name", ""))
        key = (message_id, user_id, emoji)
        if event.get("op") == "remove":
            # Taking a click back and making it again is a second press.
            self._pressed.pop(key, None)
            return
        if event.get("op") != "add":
            return
        # A bot's click decides nothing, as a bot's typed answer does not: a
        # decision is attributed to whoever made it, and an app made none.
        if user_id == self._bridge_user_id or user_id in self._realm_bot_ids:
            return
        offer = self._offers.get(message_id)
        action = offer.actions.get(emoji) if offer is not None else None
        if offer is None or action is None:
            return
        if key in self._pressed:
            return
        _remember(self._pressed, key, None, _SEEN_MAX)
        handler = self._on_interaction
        if handler is None:
            return
        await handler(
            InboundInteraction(
                channel_id=offer.channel_id,
                sender_id=str(user_id),
                sender_name=await self._email_of(user_id),
                action_id=action,
                value=offer.value,
                message_ref=str(message_id),
                thread_ref=offer.thread_ref,
            )
        )

    async def _email_of(self, user_id: int) -> str:
        email = self._emails.get(user_id)
        if email is not None:
            return email
        try:
            email = await self._user_email(user_id)
        except (ZulipError, httpx.HTTPError) as exc:
            logger.warning("Could not read Zulip user %s: %s", user_id, exc)
            return str(user_id)
        self._emails[user_id] = email
        return email

    async def _topic_root(self, stream_id: int, topic: str, message_id: int) -> int:
        # Zulip treats topic names without regard to case.
        key = (stream_id, topic.casefold())
        known = self._topic_roots.get(key)
        if known is not None:
            return known
        result = await self._require_bridge().call(
            "GET",
            "messages",
            {
                "anchor": "oldest",
                "num_before": 0,
                "num_after": 1,
                "narrow": [
                    {"operator": "stream", "operand": stream_id},
                    {"operator": "topic", "operand": topic},
                ],
                "apply_markdown": False,
            },
        )
        messages = result.get("messages", [])
        root = int(messages[0]["id"]) if messages else message_id
        _remember(self._topic_roots, key, root, _CACHE_MAX)
        return root

    async def _handle_subscription(self, event: dict[str, Any]) -> None:
        op = event.get("op")
        if op == "add":
            # The bridge account itself was subscribed to streams.
            for sub in event.get("subscriptions", []):
                await _started(self._on_app_joined)(
                    InboundAppJoin(
                        channel_id=str(sub["stream_id"]),
                        channel_type=(
                            "channel_private"
                            if sub.get("invite_only")
                            else "channel_public"
                        ),
                        channel_name=str(sub.get("name", "")) or None,
                    )
                )
            return
        if op != "peer_add":
            return
        for stream_id in event.get("stream_ids", []):
            channel_id = str(stream_id)
            channel_type = await self._cached_channel_type(channel_id)
            for user_id in event.get("user_ids", []):
                user_id = int(user_id)
                if user_id == self._bridge_user_id:
                    continue
                bot = self._bot_by_user_id.get(user_id)
                if bot is not None:
                    await _started(self._on_agent_joined)(
                        InboundAgentJoin(
                            channel_id=channel_id,
                            channel_type=channel_type,
                            agent_name=bot.agent_name,
                        )
                    )
                    continue
                user = await self._require_bridge().call("GET", f"users/{user_id}")
                if user["user"].get("is_bot"):
                    continue
                await _started(self._on_user_joined)(
                    InboundUserJoin(
                        channel_id=channel_id,
                        channel_type=channel_type,
                        external_user_id=str(user_id),
                        external_username=str(user["user"]["email"]),
                    )
                )


def _edit_window_closed(exc: ZulipError) -> bool:
    """Whether Zulip refused an edit because the organization's edit window
    has passed. Zulip gives this refusal no code of its own, so it is read
    from the message."""
    return "time limit" in str(exc).lower()


def _zulip_id(message_ref: str) -> int:
    """The Zulip message a reference stands for, an edit's included."""
    return int(message_ref.split(_EDIT_SEPARATOR, 1)[0])


def _is_message_ref(message_ref: str) -> bool:
    return message_ref.split(_EDIT_SEPARATOR, 1)[0].isdigit()


def _link_text(filename: str) -> str:
    """A filename safe inside `[…]`: brackets would end the link text early."""
    return filename.replace("[", "(").replace("]", ")") or "file"


def _started[T](handler: T | None) -> T:
    if handler is None:
        raise RuntimeError("Zulip event arrived before the adapter was started")
    return handler


def _dm_user_ids(channel_id: str) -> list[int]:
    return [int(i) for i in channel_id.removeprefix(_DM_PREFIX).split(",") if i]


def _short_name(agent_name: str) -> str:
    """A Zulip bot short name for an agent: the bot's email is `<this>-bot@…`.

    The agent name as it is where Zulip takes it. Where it had to be changed —
    a character Zulip does not take, or cut to length — a digest of the whole
    name is added, so two agents whose names differ only there never share a
    bot.
    """
    cleaned = _SHORT_NAME_INVALID.sub("-", agent_name.lower()).strip("-")
    if cleaned == agent_name and len(cleaned) <= _SHORT_NAME_MAX:
        return cleaned
    digest = hashlib.sha256(agent_name.encode()).hexdigest()[:6]
    return f"{cleaned[: _SHORT_NAME_MAX - 7] or 'agent'}-{digest}"


def _same_topic(a: str, b: str) -> bool:
    """Zulip treats topic names without regard to case."""
    return a.casefold() == b.casefold()


def _remember[K, V](cache: OrderedDict[K, V], key: K, value: V, limit: int) -> None:
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > limit:
        cache.popitem(last=False)
