"""Session activity on Zulip: statuses, request cards, reactions, files.

Zulip has no buttons a bot can put on a message, so a card is answered by
typing its handle or by clicking one of the numbered reactions the agent's bot
seeds on it, and a running turn's status carries a stop reaction. What these
tests pin is that a click becomes exactly the press a button would have been —
the card's token, the option's position, the message it was on — that the
reactions come off when there is nothing left to press, and that the failures
a publisher has to tell apart (refused, throttled, wedged, unknown) arrive as
the different things they are.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from switch_core.bridges.collaboration.adapter import (
    ActivityMarkRefused,
    RequestCard,
    RichContentFailed,
    RichContentThrottled,
    RichContentWedged,
    ThreadUnavailable,
    TurnActivity,
)
from switch_core.bridges.collaboration.models import OutboundAttachment
from switch_core.bridges.collaboration.session.renderers import (
    INTERRUPT_ACTION,
    RequestReference,
    position_action,
)
from switch_core.bridges.collaboration.zulip.adapter import ZulipAdapter, _short_name
from switch_core.bridges.collaboration.zulip.client import ZulipError
from switch_core.outbound import OutboundPolicy

from .session_fixtures import _item, _turn, open_request
from .zulip_fakes import (
    BRIDGE_ID,
    SITE,
    FakeZulip,
    Recorder,
    Route,
    add_bot,
    config,
    connected,
    error,
    stream_message,
    stream_routes,
)

BOT_ID = 42
CARD_ID = 501
STATUS_ID = 601


def _card(**fields: Any) -> RequestCard:
    return RequestCard(
        open_request(), RequestReference(token="tok-1", handle="R7"), **fields
    )


def _settled_card() -> RequestCard:
    request = open_request().model_copy(update={"state": "closed"})
    return RequestCard(request, RequestReference(token="tok-1", handle="R7"))


def _activity(**fields: Any) -> TurnActivity:
    items = [_item(kind="assistant-message", title="", text="Looking now.")]
    return TurnActivity(items, _turn("running"), **fields)


def _publishing(message_id: int, **extra: Route) -> dict[tuple[str, str], Route]:
    routes: dict[tuple[str, str], Route] = {
        ("POST", "messages"): lambda p: {"id": message_id},
        ("POST", f"messages/{message_id}/reactions"): lambda p: {},
        ("DELETE", f"messages/{message_id}/reactions"): lambda p: {},
        ("PATCH", f"messages/{message_id}"): lambda p: {},
    }
    routes.update({tuple(k.split(" ", 1)): v for k, v in extra.items()})  # type: ignore[misc]
    return routes


def _reaction(
    op: str, *, user_id: int = 5, emoji: str = "one", message_id: int = CARD_ID
) -> dict[str, Any]:
    return {
        "type": "reaction",
        "op": op,
        "user_id": user_id,
        "message_id": message_id,
        "emoji_name": emoji,
    }


# ── Cards ────────────────────────────────────────────────────────────────────


async def test_a_card_is_posted_by_the_agents_bot_and_invites_a_click() -> None:
    fake = FakeZulip(_publishing(CARD_ID))
    adapter, _ = connected(fake)
    add_bot(adapter, "worker", BOT_ID, "Worker")

    ref = await adapter.post_rich("7", "worker", _card())

    assert ref == str(CARD_ID)
    [posted] = fake.sent("POST", "messages")
    assert posted["topic"] == "switch"
    assert "**Permission needed** · request `R7`" in posted["content"]
    assert "1. Allow once" in posted["content"]
    assert "Or react with :one: :two: to choose." in posted["content"]
    # Seeded in option order, as the bot rather than the bridge.
    assert [
        p["emoji_name"] for p in fake.sent("POST", f"messages/{CARD_ID}/reactions")
    ] == [
        "one",
        "two",
    ]
    assert set(fake.auth_of("POST", f"messages/{CARD_ID}/reactions")) == set(
        fake.auth_of("POST", "messages")
    )


async def test_a_click_on_a_seeded_number_is_the_press_a_button_would_send() -> None:
    adapter, rec = connected(FakeZulip(_publishing(CARD_ID)))
    add_bot(adapter, "worker", BOT_ID, "Worker")
    adapter._emails[5] = "pat@example.test"
    adapter._message_topics[300] = (7, "bug 12")
    await adapter.post_rich("7", "worker", _card(), "300")

    await adapter._handle_event(_reaction("add", emoji="two"), None)

    [press] = rec.presses
    assert press.action_id == position_action(2)
    assert press.value == "tok-1"
    assert (press.channel_id, press.message_ref, press.thread_ref) == (
        "7",
        str(CARD_ID),
        "300",
    )
    assert (press.sender_id, press.sender_name) == ("5", "pat@example.test")


async def test_a_top_level_card_is_its_own_thread_for_a_refusal() -> None:
    adapter, rec = connected(FakeZulip(_publishing(CARD_ID)))
    add_bot(adapter, "worker", BOT_ID, "Worker")
    await adapter.post_rich("7", "worker", _card())

    await adapter._handle_event(_reaction("add"), None)

    assert rec.presses[0].thread_ref == str(CARD_ID)


async def test_bots_and_other_emoji_press_nothing() -> None:
    adapter, rec = connected(FakeZulip(_publishing(CARD_ID)))
    add_bot(adapter, "worker", BOT_ID, "Worker")
    await adapter.post_rich("7", "worker", _card())

    adapter._realm_bot_ids.add(77)
    await adapter._handle_event(_reaction("add", user_id=BOT_ID), None)
    await adapter._handle_event(_reaction("add", user_id=BRIDGE_ID), None)
    # Somebody else's bot in the organization.
    await adapter._handle_event(_reaction("add", user_id=77), None)
    await adapter._handle_event(_reaction("add", emoji="tada"), None)
    await adapter._handle_event(_reaction("add", emoji="three"), None)
    await adapter._handle_event(_reaction("add", message_id=999), None)

    assert rec.presses == []


async def test_one_click_delivered_by_two_queues_presses_once() -> None:
    adapter, rec = connected(FakeZulip(_publishing(CARD_ID)))
    add_bot(adapter, "worker", BOT_ID, "Worker")
    await adapter.post_rich("7", "worker", _card())

    await adapter._handle_event(_reaction("add"), None)
    await adapter._handle_event(_reaction("add"), adapter._bots["worker"])
    assert len(rec.presses) == 1

    # Taking the click back and making it again is a second press.
    await adapter._handle_event(_reaction("remove"), None)
    await adapter._handle_event(_reaction("add"), None)
    assert len(rec.presses) == 2


async def test_a_settled_card_takes_its_reactions_off_and_stops_taking_clicks() -> None:
    fake = FakeZulip(_publishing(CARD_ID))
    adapter, rec = connected(fake)
    add_bot(adapter, "worker", BOT_ID, "Worker")
    await adapter.post_rich("7", "worker", _card())

    await adapter.update_rich("7", "worker", str(CARD_ID), _settled_card(), None)

    [edit] = fake.sent("PATCH", f"messages/{CARD_ID}")
    assert "**Closed** · request `R7`" in edit["content"]
    assert "react" not in edit["content"]
    assert sorted(
        p["emoji_name"] for p in fake.sent("DELETE", f"messages/{CARD_ID}/reactions")
    ) == ["one", "two"]
    await adapter._handle_event(_reaction("add"), None)
    assert rec.presses == []


async def test_a_redraw_that_changes_nothing_costs_no_edit() -> None:
    fake = FakeZulip(_publishing(CARD_ID))
    adapter, _ = connected(fake)
    add_bot(adapter, "worker", BOT_ID, "Worker")
    await adapter.post_rich("7", "worker", _card())

    await adapter.update_rich("7", "worker", str(CARD_ID), _card(), None)

    assert fake.sent("PATCH", f"messages/{CARD_ID}") == []
    assert len(fake.sent("POST", f"messages/{CARD_ID}/reactions")) == 2


async def test_after_a_restart_a_settled_card_has_the_bots_leftovers_cleared() -> None:
    """The process forgot what it seeded; the reactions are still on the card."""
    fake = FakeZulip(
        _publishing(
            CARD_ID,
            **{
                f"GET messages/{CARD_ID}": lambda p: {
                    "message": {
                        "reactions": [
                            {"emoji_name": "one", "user_id": BOT_ID},
                            {"emoji_name": "two", "user_id": BOT_ID},
                            {"emoji_name": "one", "user_id": 5},
                            {"emoji_name": "eyes", "user_id": BOT_ID},
                        ]
                    }
                }
            },
        )
    )
    adapter, _ = connected(fake)
    add_bot(adapter, "worker", BOT_ID, "Worker")

    await adapter.update_rich("7", "worker", str(CARD_ID), _settled_card(), None)
    await adapter.update_rich("7", "worker", str(CARD_ID), _settled_card(), None)

    removed = [
        p["emoji_name"] for p in fake.sent("DELETE", f"messages/{CARD_ID}/reactions")
    ]
    assert sorted(removed) == ["one", "two"]
    # Read back once per message, not on every redraw.
    assert len(fake.sent("GET", f"messages/{CARD_ID}")) == 1


async def test_a_card_that_cannot_seed_its_reactions_still_posts() -> None:
    fake = FakeZulip(
        _publishing(
            CARD_ID,
            **{
                f"POST messages/{CARD_ID}/reactions": lambda p: error(
                    400, "BAD_REQUEST", "Reactions are disabled"
                )
            },
        )
    )
    adapter, _ = connected(fake)
    add_bot(adapter, "worker", BOT_ID, "Worker")

    assert await adapter.post_rich("7", "worker", _card()) == str(CARD_ID)


async def test_the_mention_is_made_on_the_post_and_not_repeated_on_an_edit() -> None:
    fake = FakeZulip(_publishing(CARD_ID))
    adapter, _ = connected(fake)
    add_bot(adapter, "worker", BOT_ID, "Worker")
    adapter._full_names[8] = "Dev Owner"

    await adapter.post_rich("7", "worker", _card(notify_external_id="8"))
    await adapter.update_rich("7", "worker", str(CARD_ID), _settled_card(), None)

    assert fake.sent("POST", "messages")[0]["content"].startswith("@**Dev Owner|8**\n")
    assert "@**Dev Owner" not in fake.sent("PATCH", f"messages/{CARD_ID}")[0]["content"]


# ── Failures ─────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("answer", "raised"),
    [
        (lambda p: error(400, "BAD_REQUEST", "Not subscribed"), RichContentFailed),
        (
            lambda p: error(429, "RATE_LIMIT_HIT", "slow down", **{"retry-after": 2.5}),
            RichContentThrottled,
        ),
        (lambda p: httpx.Response(502, text="bad gateway"), ZulipError),
    ],
)
async def test_a_post_failure_says_whether_anything_could_have_landed(
    answer: Route, raised: type[Exception]
) -> None:
    adapter, _ = connected(FakeZulip({("POST", "messages"): answer}))
    add_bot(adapter, "worker", BOT_ID, "Worker")

    with pytest.raises(raised) as caught:
        await adapter.post_rich("7", "worker", _card())

    if raised is RichContentThrottled:
        assert caught.value.retry_after == 2.5  # type: ignore[attr-defined]
    if raised is ZulipError:
        # Unknown outcome: not a refusal, so the reservation is kept.
        assert not isinstance(caught.value, RichContentFailed)


async def test_a_card_under_a_vanished_root_has_no_thread() -> None:
    adapter, _ = connected(
        FakeZulip(
            {
                ("GET", "messages/300"): lambda p: error(
                    400, "BAD_REQUEST", "Invalid message(s)"
                )
            }
        )
    )
    add_bot(adapter, "worker", BOT_ID, "Worker")

    with pytest.raises(ThreadUnavailable):
        await adapter.post_rich("7", "worker", _card(), "300")


async def test_an_edit_past_the_window_is_wedged_once_then_quiet() -> None:
    fake = FakeZulip(
        _publishing(
            CARD_ID,
            **{
                f"PATCH messages/{CARD_ID}": lambda p: error(
                    400,
                    "BAD_REQUEST",
                    "The time limit for editing this message has passed",
                )
            },
        )
    )
    adapter, _ = connected(fake)
    add_bot(adapter, "worker", BOT_ID, "Worker")
    await adapter.post_rich("7", "worker", _card())

    with pytest.raises(RichContentWedged):
        await adapter.update_rich("7", "worker", str(CARD_ID), _settled_card(), None)
    await adapter.update_rich("7", "worker", str(CARD_ID), _settled_card(), None)

    assert len(fake.sent("PATCH", f"messages/{CARD_ID}")) == 1


# ── Statuses ─────────────────────────────────────────────────────────────────


async def test_a_running_turn_offers_a_stop_and_a_click_stops_that_turn() -> None:
    fake = FakeZulip(_publishing(STATUS_ID))
    adapter, rec = connected(fake)
    add_bot(adapter, "worker", BOT_ID, "Worker")

    await adapter.post_rich("7", "worker", _activity(interrupt_turn_id="turn-1"))
    await adapter._handle_event(
        _reaction("add", emoji="stop_sign", message_id=STATUS_ID), None
    )

    assert ":stop_sign:" in fake.sent("POST", "messages")[0]["content"]
    assert [
        p["emoji_name"] for p in fake.sent("POST", f"messages/{STATUS_ID}/reactions")
    ] == ["stop_sign"]
    [press] = rec.presses
    assert (press.action_id, press.value) == (INTERRUPT_ACTION, "turn-1")


async def test_a_finished_turn_takes_its_stop_off() -> None:
    fake = FakeZulip(_publishing(STATUS_ID))
    adapter, rec = connected(fake)
    add_bot(adapter, "worker", BOT_ID, "Worker")
    await adapter.post_rich("7", "worker", _activity(interrupt_turn_id="turn-1"))

    await adapter.update_rich("7", "worker", str(STATUS_ID), _activity(), None)
    await adapter._handle_event(
        _reaction("add", emoji="stop_sign", message_id=STATUS_ID), None
    )

    assert [
        p["emoji_name"] for p in fake.sent("DELETE", f"messages/{STATUS_ID}/reactions")
    ] == ["stop_sign"]
    assert (
        ":stop_sign:" not in fake.sent("PATCH", f"messages/{STATUS_ID}")[0]["content"]
    )
    assert rec.presses == []


async def test_a_status_whose_send_was_lost_cannot_be_recovered() -> None:
    adapter, _ = connected(FakeZulip({}))

    assert (
        await adapter.find_request_card("7", None, "tok", datetime.now(UTC), None)
        is None
    )


# ── Recovery and first replies ───────────────────────────────────────────────


async def test_a_lost_card_is_found_by_its_heading_from_one_of_the_bots() -> None:
    now = datetime.now(UTC)
    heading = "**Permission needed** · request `R7`"
    adapter, _ = connected(
        FakeZulip(
            {
                ("GET", "messages"): lambda p: {
                    "messages": [
                        # A person quoting the card is not the card.
                        {
                            "id": 1,
                            "sender_id": 5,
                            "timestamp": now.timestamp(),
                            "content": heading,
                        },
                        # Older than the reservation.
                        {
                            "id": 2,
                            "sender_id": BOT_ID,
                            "timestamp": now.timestamp() - 600,
                            "content": heading,
                        },
                        {
                            "id": 3,
                            "sender_id": BOT_ID,
                            "timestamp": now.timestamp(),
                            "content": f"@**Dev|8**\n{heading}\nRun",
                        },
                    ]
                }
            }
        )
    )
    add_bot(adapter, "worker", BOT_ID, "Worker")

    assert await adapter.find_request_card("7", None, "tok-1", now, "R7") == "3"


@pytest.mark.parametrize(
    ("following", "first"),
    [([905, 906], True), ([903, 905], False), ([], False)],
)
async def test_a_bare_answer_counts_only_as_the_first_thing_after_the_card(
    following: list[int], first: bool
) -> None:
    fake = FakeZulip(
        {("GET", "messages"): lambda p: {"messages": [{"id": m} for m in following]}}
    )
    adapter, _ = connected(fake)
    adapter._message_topics[CARD_ID] = (7, "bug 12")

    assert await adapter.is_first_reply("7", str(CARD_ID), "905") is first
    [read] = fake.sent("GET", "messages")
    assert read["anchor"] == CARD_ID
    assert read["include_anchor"] is False
    assert {"operator": "topic", "operand": "bug 12"} in read["narrow"]


async def test_first_reply_is_no_when_zulip_cannot_say() -> None:
    adapter, _ = connected(FakeZulip({}))

    assert await adapter.is_first_reply("7", "abc", "905") is False


# ── Work marks ───────────────────────────────────────────────────────────────


async def test_the_work_marks_are_the_agents_own_and_settle_on_no_ops() -> None:
    fake = FakeZulip(
        {
            ("POST", "messages/900/reactions"): lambda p: error(
                400, "REACTION_ALREADY_EXISTS", "Reaction already exists."
            ),
            ("DELETE", "messages/900/reactions"): lambda p: error(
                400, "REACTION_DOES_NOT_EXIST", "Reaction doesn't exist."
            ),
        }
    )
    adapter, _ = connected(fake)
    add_bot(adapter, "worker", BOT_ID, "Worker")

    await adapter.mark_activity(
        "7", "900", agent_name="worker", mark="working", on=True
    )
    await adapter.mark_activity(
        "7", "900", agent_name="worker", mark="working", on=True
    )
    await adapter.mark_activity("7", "900", agent_name="worker", mark="queued", on=True)
    await adapter.mark_activity(
        "7", "900", agent_name="worker", mark="working", on=False
    )

    assert [p["emoji_name"] for p in fake.sent("POST", "messages/900/reactions")] == [
        "eyes",
        "time_ticking",
    ]
    assert [p["emoji_name"] for p in fake.sent("DELETE", "messages/900/reactions")] == [
        "eyes"
    ]


async def test_a_mark_the_bot_may_not_make_is_refused_for_good() -> None:
    adapter, _ = connected(
        FakeZulip(
            {
                ("POST", "messages/900/reactions"): lambda p: error(
                    400, "BAD_REQUEST", "Invalid message(s)"
                )
            }
        )
    )
    add_bot(adapter, "worker", BOT_ID, "Worker")

    with pytest.raises(ActivityMarkRefused):
        await adapter.mark_activity(
            "7", "900", agent_name="worker", mark="working", on=True
        )


# ── Notices ──────────────────────────────────────────────────────────────────


async def test_a_refused_answer_is_told_privately_with_a_link_to_the_card() -> None:
    fake = FakeZulip({("POST", "messages"): lambda p: {"id": 1}})
    adapter, _ = connected(fake)

    await adapter.tell_actor("7", "5", "Pat", "501", "no request R9 here")

    [dm] = fake.sent("POST", "messages")
    assert dm["type"] == "direct" and dm["to"] == [5]
    assert f"{SITE}/#narrow/channel/7/near/501" in dm["content"]
    assert "no request R9 here" in dm["content"]


async def test_a_switch_notice_comes_from_the_bridge_except_in_a_dm() -> None:
    fake = FakeZulip({("POST", "messages"): lambda p: {"id": 1}})
    adapter, _ = connected(fake)
    add_bot(adapter, "worker", BOT_ID, "Worker")

    await adapter.admin_message("7", "Heads up")
    await adapter.admin_message(f"dm:5,{BOT_ID}", "Heads up")

    bridge_auth, dm_auth = fake.auth_of("POST", "messages")
    assert bridge_auth != dm_auth
    assert fake.sent("POST", "messages")[1]["to"] == [5, BOT_ID]


# ── Files ────────────────────────────────────────────────────────────────────


async def test_files_go_up_as_the_sender_and_out_as_one_message() -> None:
    uploads: list[bytes] = []

    def upload(p: dict[str, Any]) -> dict[str, Any]:
        uploads.append(p["_multipart"])
        return {"url": f"/user_uploads/2/ab/f{len(uploads)}/x"}

    fake = FakeZulip(
        {("POST", "user_uploads"): upload, ("POST", "messages"): lambda p: {"id": 77}}
    )
    adapter, _ = connected(fake)
    add_bot(adapter, "worker", BOT_ID, "Worker")
    adapter._max_upload_bytes = 10

    ref = await adapter.send_attachments(
        "7",
        "worker",
        [
            OutboundAttachment(filename="a[1].txt", mimetype="text/plain", data=b"aa"),
            OutboundAttachment(
                filename="big.bin", mimetype="application/octet-stream", data=b"x" * 11
            ),
        ],
        caption="Here you go",
    )

    assert ref == "77"
    assert len(uploads) == 1 and b'filename="a[1].txt"' in uploads[0]
    content = fake.sent("POST", "messages")[0]["content"]
    assert content.splitlines()[0] == "Here you go"
    assert "[a(1).txt](/user_uploads/2/ab/f1/x)" in content
    assert "big.bin" in content and "couldn't be relayed" in content
    assert fake.auth_of("POST", "user_uploads") == fake.auth_of("POST", "messages")


async def test_a_linked_file_comes_in_through_its_temporary_url() -> None:
    routes = stream_routes()
    routes[("GET", "user_uploads/2/ab/xyz/report.pdf")] = lambda p: {
        "url": "/user_uploads/temporary/tok/report.pdf"
    }
    routes[("GET", "/user_uploads/temporary/tok/report.pdf")] = lambda p: (
        httpx.Response(
            200, content=b"%PDF", headers={"content-type": "application/pdf"}
        )
    )
    routes[("GET", "user_uploads/2/ab/big/huge.bin")] = lambda p: {
        "url": "/user_uploads/temporary/big/huge.bin"
    }
    routes[("GET", "/user_uploads/temporary/big/huge.bin")] = lambda p: httpx.Response(
        200, content=b"x" * 50, headers={"content-length": "50"}
    )
    adapter, rec = connected(FakeZulip(routes))
    adapter.set_max_attachment_bytes(10)

    await adapter._handle_message(
        stream_message(
            content=(
                "see [report.pdf](/user_uploads/2/ab/xyz/report.pdf) and "
                "[huge.bin](/user_uploads/2/ab/big/huge.bin)"
            )
        ),
        agent=None,
    )

    [msg] = rec.messages
    [attachment] = msg.attachments
    assert (attachment.filename, attachment.mimetype, attachment.data) == (
        "report.pdf",
        "application/pdf",
        b"%PDF",
    )
    [failure] = msg.attachment_failures
    assert failure.filename == "huge.bin" and "larger than" in failure.reason


# ── Organization ─────────────────────────────────────────────────────────────


async def test_the_realms_limits_are_read_and_an_edit_window_is_flagged() -> None:
    fake = FakeZulip(
        {
            ("POST", "register"): lambda p: {
                "queue_id": "q1",
                "max_message_length": 9000,
                "max_file_upload_size_mib": 3,
                "realm_allow_message_editing": True,
                "realm_message_content_edit_limit_seconds": 600,
            },
            ("DELETE", "events"): lambda p: {},
        }
    )
    adapter, _ = connected(fake)

    await adapter._read_realm_limits()

    assert adapter.rich_fallback_limit() == 9000
    assert adapter._max_upload_bytes == 3 * 1024 * 1024
    attention = await adapter.attention()
    assert attention is not None and "10 minutes" in attention
    assert fake.sent("DELETE", "events") == [{"queue_id": "q1"}]


async def test_no_edit_window_needs_no_attention() -> None:
    fake = FakeZulip(
        {
            ("POST", "register"): lambda p: {
                "realm_allow_message_editing": True,
                "realm_message_content_edit_limit_seconds": None,
            }
        }
    )
    adapter, _ = connected(fake)

    await adapter._read_realm_limits()

    assert await adapter.attention() is None


async def test_removing_the_bridge_deactivates_the_bots_it_owns() -> None:
    fake = FakeZulip(
        {
            ("GET", "bots"): lambda p: {
                "bots": [{"username": "worker-bot@example.test"}]
            },
            ("GET", "users/worker-bot@example.test"): lambda p: {
                "user": {"user_id": BOT_ID}
            },
            ("DELETE", f"bots/{BOT_ID}"): lambda p: {},
        }
    )
    adapter, _ = connected(fake)

    await adapter.withdraw()

    assert len(fake.sent("DELETE", f"bots/{BOT_ID}")) == 1


# ── Starting and stopping ────────────────────────────────────────────────────


async def test_start_reads_only_the_bridged_streams_and_stop_drops_the_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reading every public stream would bridge each one a message arrived in."""
    registered: list[dict[str, Any]] = []

    def register(p: dict[str, Any]) -> dict[str, Any]:
        registered.append(p)
        return {"queue_id": f"q{len(registered)}", "last_event_id": -1}

    fake = FakeZulip(
        {
            ("GET", "users/me"): lambda p: {"user_id": BRIDGE_ID},
            ("GET", "users"): lambda p: {"members": []},
            ("POST", "register"): register,
            # The poll fails, so the loop backs off instead of spinning.
            ("GET", "events"): lambda p: httpx.Response(503, text="down"),
            ("DELETE", "events"): lambda p: {},
        }
    )
    monkeypatch.setattr(
        "switch_core.bridges.collaboration.zulip.adapter.guarded_async_client",
        lambda policy, **kw: httpx.AsyncClient(
            transport=httpx.MockTransport(fake), base_url=kw["base_url"]
        ),
    )
    adapter = ZulipAdapter(config=config())
    adapter.set_outbound_policy(OutboundPolicy.parse(""))
    rec = Recorder()

    await adapter.start(
        rec.on_message,
        rec.on_command,
        rec.on_agent_joined,
        rec.on_user_joined,
        rec.on_app_joined,
    )
    await asyncio.sleep(0.05)
    await adapter.stop()

    realm, bridge = registered[0], registered[1]
    assert realm["fetch_event_types"] == ["realm"]
    assert bridge["event_types"] == [
        "message",
        "update_message",
        "subscription",
        "reaction",
    ]
    assert "all_public_streams" not in bridge
    assert {"queue_id": "q2"} in fake.sent("DELETE", "events")


async def test_a_failed_poll_is_retried_on_the_same_queue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Registering afresh after a blip would skip what arrived meanwhile."""
    registered: list[str] = []
    polls: list[dict[str, Any]] = []

    def register(p: dict[str, Any]) -> dict[str, Any]:
        registered.append(f"q{len(registered)}")
        return {"queue_id": registered[-1], "last_event_id": 4}

    def events(p: dict[str, Any]) -> Any:
        polls.append(p)
        if len(polls) < 3:
            return httpx.Response(502, text="bad gateway")
        if len(polls) == 3:
            return error(400, "BAD_EVENT_QUEUE_ID", "Bad event queue id")
        return httpx.Response(503, text="down")

    fake = FakeZulip({("POST", "register"): register, ("GET", "events"): events})
    adapter, _ = connected(fake)
    monkeypatch.setattr(
        "switch_core.bridges.collaboration.zulip.adapter._RETRY_INITIAL_SECONDS", 0.001
    )
    assert adapter._bridge is not None
    task = asyncio.create_task(adapter._poll("bridge", adapter._bridge, {}, agent=None))
    for _ in range(100):
        if len(polls) >= 4:
            break
        await asyncio.sleep(0.01)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    # Two failures asked again of the first queue; only the dropped queue
    # was replaced.
    assert [p["queue_id"] for p in polls[:4]] == ["q0", "q0", "q0", "q1"]
    assert all(p["last_event_id"] == 4 for p in polls[:4])


# ── Direct messages ──────────────────────────────────────────────────────────


async def test_a_dm_is_read_back_as_the_bot_in_it() -> None:
    fake = FakeZulip({("GET", "messages"): lambda p: {"messages": [{"id": 905}]}})
    adapter, _ = connected(fake)
    add_bot(adapter, "worker", BOT_ID, "Worker")

    assert await adapter.is_first_reply(f"dm:5,{BOT_ID}", "900", "905") is True
    await adapter.find_request_card(
        f"dm:5,{BOT_ID}", None, "tok", datetime.now(UTC), "R7"
    )

    bot_auth = adapter._bots["worker"].client._auth._auth_header  # type: ignore[attr-defined]
    assert set(fake.auth_of("GET", "messages")) == {bot_auth}
    assert fake.sent("GET", "messages")[0]["narrow"] == [
        {"operator": "dm", "operand": [5, BOT_ID]}
    ]


async def test_a_group_dm_seen_by_the_bridge_first_still_reaches_the_bot() -> None:
    adapter, rec = connected(FakeZulip({}))
    bot = add_bot(adapter, "worker", BOT_ID, "Worker")
    dm = {
        "id": 950,
        "type": "private",
        "sender_id": 5,
        "sender_email": "pat@example.test",
        "display_recipient": [{"id": 5}, {"id": BOT_ID}, {"id": BRIDGE_ID}],
        "content": "hi both",
    }

    await adapter._handle_message(dict(dm), agent=None)
    await adapter._handle_message(dict(dm), agent=bot)

    [msg] = rec.messages
    assert msg.agent_name == "worker"


# ── Edits after a restart ────────────────────────────────────────────────────


async def test_an_edit_after_a_restart_goes_out_as_the_message_sender() -> None:
    fake = FakeZulip(
        {
            ("GET", "messages/700"): lambda p: {"message": {"sender_id": BOT_ID}},
            ("PATCH", "messages/700"): lambda p: {},
        }
    )
    adapter, _ = connected(fake)
    bot = add_bot(adapter, "worker", BOT_ID, "Worker")

    await adapter.update_message("7", "700", "corrected")

    assert fake.auth_of("PATCH", "messages/700") == [
        bot.client._auth._auth_header  # type: ignore[attr-defined]
    ]


# ── Bot identities ───────────────────────────────────────────────────────────


def test_a_name_zulip_takes_as_is_keeps_it_and_an_altered_one_cannot_collide() -> None:
    assert _short_name("claude-eval") == "claude-eval"
    assert _short_name("foo.bar") != _short_name("foo-bar")
    assert _short_name("foo.bar").startswith("foo-bar-")
    long_a, long_b = "a" * 45 + "x", "a" * 45 + "y"
    assert _short_name(long_a) != _short_name(long_b)
    assert len(_short_name(long_a)) <= 40


async def test_a_bot_deactivated_by_an_earlier_removal_is_brought_back() -> None:
    reactivated: list[int] = []

    def bots(p: dict[str, Any]) -> dict[str, Any]:
        if not reactivated:
            return {"bots": []}
        return {"bots": [{"username": "worker-bot@example.test", "api_key": "k2"}]}

    def reactivate(p: dict[str, Any]) -> dict[str, Any]:
        reactivated.append(BOT_ID)
        return {}

    fake = FakeZulip(
        {
            ("GET", "bots"): bots,
            ("GET", "users"): lambda p: {
                "members": [
                    {
                        "user_id": BOT_ID,
                        "email": "worker-bot@example.test",
                        "is_bot": True,
                        "is_active": False,
                        "bot_owner_id": BRIDGE_ID,
                    }
                ]
            },
            ("POST", f"users/{BOT_ID}/reactivate"): reactivate,
            ("PATCH", f"bots/{BOT_ID}"): lambda p: {},
        }
    )
    adapter, _ = connected(fake)

    await adapter._create_agent_identity("worker", "works")

    assert reactivated == [BOT_ID]
    assert fake.sent("POST", "bots") == []
    bot = adapter._bots["worker"]
    assert (bot.user_id, bot.client.email) == (BOT_ID, "worker-bot@example.test")
    for task in [bot.poller, *adapter._background]:
        if task is not None:
            task.cancel()


# ── Topics ───────────────────────────────────────────────────────────────────


async def test_the_default_topic_in_other_case_is_still_the_top_level() -> None:
    adapter, rec = connected(FakeZulip(stream_routes()))

    await adapter._handle_message(stream_message(subject="Switch"), agent=None)

    assert rec.messages[0].root_id is None


# ── Edits ────────────────────────────────────────────────────────────────────


def _edit(**overrides: Any) -> dict[str, Any]:
    return {
        "type": "update_message",
        "user_id": 5,
        "message_id": 900,
        "edit_timestamp": 1700000000,
        "orig_content": "can someone look at this?",
        "content": "@**Worker** can someone look at this?",
        "rendering_only": False,
        **overrides,
    }


def _edited_routes(**message: Any) -> dict[tuple[str, str], Route]:
    routes = stream_routes()
    fields = {"content": "@**Worker** can someone look at this?", **message}
    routes[("GET", "messages/900")] = lambda p: {"message": stream_message(**fields)}
    return routes


async def test_an_edit_that_brings_an_agent_in_is_relayed_as_a_new_message() -> None:
    adapter, rec = connected(FakeZulip(_edited_routes()))
    add_bot(adapter, "worker", BOT_ID, "Worker")
    await adapter._handle_message(
        stream_message(content="can someone look at this?"), agent=None
    )

    await adapter._handle_event(_edit(), None)

    original, edited = rec.messages
    assert original.message_ref == "900"
    assert edited.message_ref == "900~1700000000"
    assert edited.content.endswith("_(edited)_")
    assert adapter.translate_inbound(edited.content).startswith("@worker ")
    assert edited.root_id is None and edited.channel_id == "7"


async def test_an_edit_reference_leads_back_to_the_zulip_message() -> None:
    fake = FakeZulip({("POST", "messages/900/reactions"): lambda p: {}})
    adapter, _ = connected(fake)
    add_bot(adapter, "worker", BOT_ID, "Worker")
    adapter._message_topics[900] = (7, "bug 12")

    await adapter.mark_activity(
        "7", "900~1700000000", agent_name="worker", mark="working", on=True
    )

    assert fake.sent("POST", "messages/900/reactions") == [{"emoji_name": "eyes"}]
    assert await adapter._topic_for(7, "900~1700000000") == "bug 12"


@pytest.mark.parametrize(
    "edit",
    [
        # Reworded, but the agent was already named.
        _edit(orig_content="@**Worker** look", content="@**Worker** look please"),
        # A correction naming nobody.
        _edit(content="can someone look at this, please?"),
        # Zulip re-rendering a link preview, not a person editing.
        _edit(rendering_only=True),
        # A topic move carries no content.
        {k: v for k, v in _edit().items() if k not in ("content", "orig_content")},
    ],
)
async def test_an_edit_that_brings_no_agent_in_is_not_relayed(
    edit: dict[str, Any],
) -> None:
    fake = FakeZulip(_edited_routes())
    adapter, rec = connected(fake)
    add_bot(adapter, "worker", BOT_ID, "Worker")

    await adapter._handle_event(edit, None)

    assert rec.messages == []
    assert fake.sent("GET", "messages/900") == []


async def test_an_agent_named_in_plain_text_counts_as_brought_in() -> None:
    adapter, rec = connected(FakeZulip(_edited_routes()))
    add_bot(adapter, "worker", BOT_ID, "Worker")

    await adapter._handle_event(_edit(content="@worker can someone look?"), None)

    assert len(rec.messages) == 1


async def test_one_edit_delivered_by_two_queues_is_relayed_once() -> None:
    adapter, rec = connected(FakeZulip(_edited_routes()))
    bot = add_bot(adapter, "worker", BOT_ID, "Worker")

    await adapter._handle_event(_edit(), None)
    await adapter._handle_event(_edit(), bot)
    await adapter._handle_event(_edit(edit_timestamp=1700000100), None)

    assert [m.message_ref for m in rec.messages] == [
        "900~1700000000",
        "900~1700000100",
    ]


async def test_an_edit_is_never_run_as_a_command_and_brings_no_files_again() -> None:
    adapter, rec = connected(
        FakeZulip(
            _edited_routes(
                content="!interrupt @**Worker** [a.txt](/user_uploads/2/ab/x/a.txt)"
            )
        )
    )
    add_bot(adapter, "worker", BOT_ID, "Worker")

    await adapter._handle_event(_edit(content="!interrupt @**Worker**"), None)

    [msg] = rec.messages
    assert rec.commands == []
    assert msg.attachments == [] and msg.attachment_failures == []


async def test_an_edit_by_someone_other_than_the_sender_is_not_relayed() -> None:
    adapter, rec = connected(FakeZulip(_edited_routes()))
    add_bot(adapter, "worker", BOT_ID, "Worker")

    await adapter._handle_event(_edit(user_id=99), None)

    assert rec.messages == []
