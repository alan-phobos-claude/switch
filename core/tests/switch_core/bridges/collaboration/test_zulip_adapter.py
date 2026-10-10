from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from switch_core.bridges.collaboration.zulip.adapter import ZulipAdapter
from switch_core.bridges.collaboration.zulip.client import ZulipError

from .zulip_fakes import BRIDGE_ID, SITE, FakeZulip
from .zulip_fakes import add_bot as _add_bot
from .zulip_fakes import config as _config
from .zulip_fakes import connected as _connected
from .zulip_fakes import stream_message as _stream_message
from .zulip_fakes import stream_routes as _stream_routes

# ── Configuration ────────────────────────────────────────────────────────────


def test_site_must_be_an_http_url() -> None:
    with pytest.raises(ValidationError):
        _config(site="zulip.example.test")


def test_site_is_normalised_without_a_trailing_slash() -> None:
    assert _config(site=f"{SITE}/").site == SITE


def test_claimed_workspace_ignores_case_and_trailing_slash() -> None:
    a = ZulipAdapter.claimed_workspace({"site": "https://Zulip.Example.test/"})
    b = ZulipAdapter.claimed_workspace({"site": "https://zulip.example.test"})
    assert a == b


# ── Markup ───────────────────────────────────────────────────────────────────


async def test_inbound_bot_mention_becomes_the_agent_handle() -> None:
    adapter, _ = _connected(FakeZulip({}))
    _add_bot(adapter, "reviewer", 42, "Code Reviewer")

    assert (
        adapter.translate_inbound("hey @**Code Reviewer** look") == "hey @reviewer look"
    )
    assert adapter.translate_inbound("@**Code Reviewer|42** ping") == "@reviewer ping"
    assert adapter.translate_inbound("@**Somebody Else** hi") == "@**Somebody Else** hi"


async def test_outbound_agent_mention_uses_zulip_syntax() -> None:
    adapter, _ = _connected(FakeZulip({}))
    _add_bot(adapter, "reviewer", 42, "Code Reviewer")

    rendered = adapter.translate_outbound(
        "ask @reviewer:switch.local or @alice:switch.local"
    )
    assert rendered == "ask @**Code Reviewer|42** or @alice"


async def test_outbound_person_mention_notifies_them() -> None:
    adapter, _ = _connected(FakeZulip({}))
    adapter.prime_mention_targets({"user8@zulip.example.test": "8"})
    adapter._full_names[8] = "Dev Owner"

    rendered = adapter.translate_outbound(
        "@user8@zulip.example.test I only take instructions from my owner."
    )

    assert rendered == "@**Dev Owner|8** I only take instructions from my owner."


async def test_outbound_unknown_email_is_left_alone() -> None:
    adapter, _ = _connected(FakeZulip({}))
    assert adapter.translate_outbound("mail @x@y.test.") == "mail @x@y.test."


async def test_a_sender_becomes_mentionable() -> None:
    adapter, _ = _connected(FakeZulip(_stream_routes()))

    await adapter._handle_message(_stream_message(sender_full_name="Pat"), agent=None)

    assert adapter.translate_outbound("@pat@example.test hi") == "@**Pat|5** hi"


async def test_wildcard_mentions_are_defused_but_words_are_left() -> None:
    adapter, _ = _connected(FakeZulip({}))
    out = adapter.translate_outbound("@**all** and @**channel**, not @all")
    assert "@**all**" not in out and "@**channel**" not in out
    assert out.endswith("not @all")


# ── Sending ──────────────────────────────────────────────────────────────────


async def test_top_level_post_goes_to_the_default_topic_as_the_agents_bot() -> None:
    fake = FakeZulip({("POST", "messages"): lambda p: {"id": 501}})
    adapter, _ = _connected(fake, default_topic="general")
    _add_bot(adapter, "reviewer", 42, "Code Reviewer")

    ref = await adapter.send_message("7", "reviewer", "hello")

    assert ref == "501"
    [sent] = fake.sent("POST", "messages")
    assert sent == {"content": "hello", "type": "stream", "to": 7, "topic": "general"}
    assert "Basic" in fake.calls[0][3]


async def test_threaded_reply_goes_to_the_root_messages_topic() -> None:
    fake = FakeZulip(
        {
            ("GET", "messages/300"): lambda p: {
                "message": {"id": 300, "subject": "bug 12"}
            },
            ("POST", "messages"): lambda p: {"id": 502},
        }
    )
    adapter, _ = _connected(fake)
    _add_bot(adapter, "reviewer", 42, "Code Reviewer")

    await adapter.send_message("7", "reviewer", "on it", thread_root_id="300")

    assert fake.sent("POST", "messages")[0]["topic"] == "bug 12"


async def test_direct_message_channel_posts_to_its_participants() -> None:
    fake = FakeZulip({("POST", "messages"): lambda p: {"id": 503}})
    adapter, _ = _connected(fake)
    _add_bot(adapter, "reviewer", 42, "Code Reviewer")

    await adapter.send_message("dm:5,42", "reviewer", "hi")

    assert fake.sent("POST", "messages")[0] == {
        "content": "hi",
        "type": "direct",
        "to": [5, 42],
    }


async def test_update_goes_out_under_the_bot_that_sent_the_message() -> None:
    fake = FakeZulip(
        {
            ("POST", "messages"): lambda p: {"id": 504},
            ("PATCH", "messages/504"): lambda p: {},
        }
    )
    adapter, _ = _connected(fake)
    _add_bot(adapter, "reviewer", 42, "Code Reviewer")
    await adapter.send_message("7", "reviewer", "draft")

    await adapter.update_message("7", "504", "final")

    post_auth = fake.calls[0][3]
    patch_auth = fake.calls[1][3]
    assert patch_auth == post_auth


async def test_create_channel_avoids_joining_an_existing_stream() -> None:
    existing = {"Release": 1}
    created: list[str] = []

    def stream_id(p: dict[str, Any]) -> dict[str, Any]:
        name = p["stream"]
        if name in existing:
            return {"stream_id": existing[name]}
        raise AssertionError("unreachable")

    def subscribe(p: dict[str, Any]) -> dict[str, Any]:
        name = p["subscriptions"][0]["name"]
        created.append(name)
        existing[name] = 99
        return {"subscribed": {}, "already_subscribed": {}}

    fake = FakeZulip(
        {
            ("GET", "get_stream_id"): stream_id,
            ("POST", "users/me/subscriptions"): subscribe,
        }
    )
    adapter, _ = _connected(fake, default_member="5")
    adapter._default_member_id = 5

    channel_id = await adapter.create_channel(
        "Release", "ship it", channel_type="channel_private"
    )

    assert channel_id == "99"
    assert created[0].startswith("Release-")
    [sub] = fake.sent("POST", "users/me/subscriptions")
    assert sub["principals"] == [BRIDGE_ID, 5]
    assert sub["invite_only"] is True


async def test_an_existing_bot_is_adopted_on_restart() -> None:
    fake = FakeZulip(
        {
            # The shape Zulip 11 answers with: no `user_id` on an owned bot.
            ("GET", "bots"): lambda p: {
                "bots": [
                    {
                        "username": "reviewer-bot@example.test",
                        "full_name": "reviewer",
                        "api_key": "kept",
                    }
                ]
            },
            ("GET", "users/reviewer-bot@example.test"): lambda p: {
                "user": {"user_id": 42}
            },
        }
    )
    adapter, _ = _connected(fake)

    await adapter._create_agent_identity("reviewer", "reviews code")

    bot = adapter._bots["reviewer"]
    assert (bot.user_id, bot.client.email) == (42, "reviewer-bot@example.test")
    assert adapter.translate_inbound("@**reviewer** hi") == "@reviewer hi"
    assert fake.sent("POST", "bots") == []
    assert bot.poller is not None
    bot.poller.cancel()


# ── Inbound ──────────────────────────────────────────────────────────────────


async def test_default_topic_message_is_top_level() -> None:
    adapter, rec = _connected(FakeZulip(_stream_routes()))

    await adapter._handle_message(_stream_message(), agent=None)

    [msg] = rec.messages
    assert (msg.channel_id, msg.channel_type, msg.root_id) == (
        "7",
        "channel_public",
        None,
    )
    assert msg.sender_name == "pat@example.test"
    assert msg.message_ref == "900"


async def test_other_topic_is_a_thread_rooted_at_its_first_message() -> None:
    routes = _stream_routes()
    routes[("GET", "messages")] = lambda p: {"messages": [{"id": 850}]}
    adapter, rec = _connected(FakeZulip(routes))

    await adapter._handle_message(_stream_message(subject="bug 12"), agent=None)
    await adapter._handle_message(_stream_message(id=850, subject="bug 12"), agent=None)

    assert rec.messages[0].root_id == "850"
    assert rec.messages[1].root_id is None


async def test_own_bots_and_duplicates_are_not_relayed() -> None:
    adapter, rec = _connected(FakeZulip(_stream_routes()))
    _add_bot(adapter, "reviewer", 42, "Code Reviewer")

    await adapter._handle_message(_stream_message(sender_id=42), agent=None)
    await adapter._handle_message(_stream_message(sender_id=BRIDGE_ID), agent=None)
    await adapter._handle_message(_stream_message(), agent=None)
    await adapter._handle_message(_stream_message(), agent=None)

    assert len(rec.messages) == 1


async def test_system_bot_narration_is_not_relayed() -> None:
    adapter, rec = _connected(FakeZulip(_stream_routes()))

    await adapter._handle_message(
        _stream_message(sender_id=6, sender_realm_str="zulipinternal"), agent=None
    )

    assert rec.messages == []


async def test_another_bots_post_is_relayed_as_an_apps() -> None:
    adapter, rec = _connected(FakeZulip(_stream_routes()))
    adapter._realm_bot_ids = {9}

    await adapter._handle_message(_stream_message(sender_id=9), agent=None)
    await adapter._handle_message(_stream_message(id=901), agent=None)

    assert [m.sender_is_app for m in rec.messages] == [True, False]


async def test_bang_message_is_a_command() -> None:
    adapter, rec = _connected(FakeZulip(_stream_routes()))

    await adapter._handle_message(
        _stream_message(content="!invite @reviewer"), agent=None
    )

    [cmd] = rec.commands
    assert (cmd.command, cmd.args) == ("invite", "@reviewer")
    assert rec.messages == []


async def test_direct_message_to_a_bot_names_the_agent() -> None:
    adapter, rec = _connected(FakeZulip({}))
    _add_bot(adapter, "reviewer", 42, "Code Reviewer")
    bot = adapter._bots["reviewer"]

    await adapter._handle_message(
        {
            "id": 901,
            "type": "private",
            "sender_id": 5,
            "sender_email": "pat@example.test",
            "display_recipient": [{"id": 42}, {"id": 5}],
            "content": "hi",
        },
        agent=bot,
    )

    [msg] = rec.messages
    assert (msg.channel_id, msg.channel_type, msg.agent_name) == (
        "dm:5,42",
        "direct",
        "reviewer",
    )


async def test_peer_add_reports_agents_and_people_separately() -> None:
    routes = _stream_routes()
    routes[("GET", "users/5")] = lambda p: {
        "user": {"email": "pat@example.test", "is_bot": False}
    }
    adapter, rec = _connected(FakeZulip(routes))
    _add_bot(adapter, "reviewer", 42, "Code Reviewer")

    await adapter._handle_subscription(
        {
            "type": "subscription",
            "op": "peer_add",
            "stream_ids": [7],
            "user_ids": [42, 5, BRIDGE_ID],
        }
    )

    assert [j.agent_name for j in rec.agent_joins] == ["reviewer"]
    assert [j.external_username for j in rec.user_joins] == ["pat@example.test"]


async def test_zulip_error_carries_the_servers_code() -> None:
    fake = FakeZulip({})
    adapter, _ = _connected(fake)
    assert adapter._bridge is not None
    with pytest.raises(ZulipError) as caught:
        await adapter._bridge.call("GET", "nowhere")
    assert caught.value.code == "X"
