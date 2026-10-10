"""Reactions standing in for buttons on a Zulip message.

Zulip has no buttons a bot can put on a message, but it has reactions, and a
reaction already behaves like a press: one click, attributed to whoever made
it, delivered as an event. So the bridge seeds the reactions a message offers
as the agent's own bot — one number per option on a request card, a stop sign
on a running turn's status — and a person's click on one becomes the press a
button would have sent.

The number a card's reaction carries is the number printed beside the option
in its body, which is also what a typed answer names, so all three ways of
answering a card mean the same thing by the same number.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from switch_core.bridges.collaboration.adapter import RequestCard, TurnActivity
from switch_core.bridges.collaboration.session.renderers import (
    INTERRUPT_ACTION,
    Drawn,
    offered_controls,
    position_action,
)

# Zulip's names for the keycap digits, in option order. Ten is as far as
# Zulip's emoji go; a card with more options than that is answered in words.
NUMBER_EMOJI = (
    "one",
    "two",
    "three",
    "four",
    "five",
    "six",
    "seven",
    "eight",
    "nine",
    "ten",
)

STOP_EMOJI = "stop_sign"

# The work marks the agent puts on the message it is handling.
WORKING_EMOJI = "eyes"
QUEUED_EMOJI = "time_ticking"


@dataclass
class Offer:
    """What the reactions on one published message mean while it shows them.

    `actions` maps an emoji name to the action id a click on it presses, in the
    order the reactions are seeded. `value` is what the press carries with it:
    the card's token, or the turn a stop names. `seeded` is the subset this
    process has actually put on the message.
    """

    channel_id: str
    thread_ref: str
    value: str
    actions: dict[str, str]
    seeded: set[str] = field(default_factory=set)


def card_actions(content: RequestCard, drawn: Drawn) -> dict[str, str]:
    """The reactions a card offers, or none.

    None when the card cannot be answered where it shows — a form cut short of
    the difference between two options must not take a click on either — and
    when it offers more options than Zulip has numbers for.
    """
    if not drawn.answerable:
        return {}
    return _numbered(content)


def possible_card_actions(content: RequestCard) -> dict[str, str]:
    """The reactions a card would offer if it turns out answerable.

    Known before the card is drawn, so the line inviting the click can be
    charged to the card's budget up front.
    """
    return _numbered(content)


def _numbered(content: RequestCard) -> dict[str, str]:
    controls = offered_controls(content.request)
    if not controls or len(controls) > len(NUMBER_EMOJI):
        return {}
    return {
        NUMBER_EMOJI[control.position - 1]: position_action(control.position)
        for control in controls
    }


def turn_actions(content: TurnActivity) -> dict[str, str]:
    """The stop reaction, while the message names a turn to stop."""
    return {STOP_EMOJI: INTERRUPT_ACTION} if content.interrupt_turn_id else {}


def card_hint(actions: dict[str, str]) -> str:
    emoji = " ".join(f":{name}:" for name in actions)
    return f"Or react with {emoji} to choose."


def stop_hint() -> str:
    return f"React with :{STOP_EMOJI}: to stop the agent's current work."
