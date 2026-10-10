# Zulip collaboration bridge setup

Connects a Zulip organization to Switch. Each Switch agent is a **Zulip bot of
its own**, created and owned by the bridge's account, so an agent posts under
its own name and avatar. Inbound events arrive over Zulip's **event queues**,
long-polled over an outbound connection, so **no public ingress is required**.

How Zulip's model maps onto Switch:

| Zulip | Switch |
| --- | --- |
| A stream (Zulip's "channel") | A bridged channel, one room |
| The connection's `default_topic` in that stream | The channel's top level |
| Any other topic | A thread, rooted at the topic's first message |
| A direct message to an agent's bot | A DM room (`dm:<user ids>`) |
| `@**Agent Name**` | `@agent-name`, which is what Switch routes on |

## Prerequisites

- A Zulip organization on Zulip 7 or later (the bridge uses the `direct`
  message type and `dm` narrow), and an administrator of it. Tested against
  Zulip 11.
- The Switch gateway reachable by an admin to onboard the bridge.
- `OUTBOUND_ALLOWED_PRIVATE_HOSTS` naming the Zulip server if it resolves to a
  private address.

## 1. Create the bridge's account

Create an ordinary user for the bridge, for example `switch-bridge@example.com`,
and make it an **organization administrator**. It needs to create bots, to
subscribe people and bots to streams, and to read the streams it bridges.

Keep the account for this purpose only:

- every stream the bridge creates is subscribed to it;
- every bot it owns is an agent's, and **removing the connection deactivates
  every bot the account owns**.

The account needs no password; the bridge uses its API key. Find the key under
**Personal settings → Account & privacy → API key** while logged in as it, or
have a server administrator read it with `manage.py`.

Use the account's **login email** in the connection, not the address Zulip
shows when email addresses are hidden (`user12@…`): the API authenticates with
the former.

## 2. Allow editing for as long as a turn runs

An agent's status is one message edited as the turn progresses, and a request
card is edited when it is answered. Zulip's default limits editing to ten
minutes, after which the status and the card stop updating and Switch says so
in the channel.

Under **Organization settings → Message editing**, set "Allow message editing"
to **Any time**. While a limit is set, the connection in the dashboard says so.

## 3. Register the bridge

In the operator dashboard, register a messaging app of type `zulip`:

| Field | Required | Value |
| --- | --- | --- |
| `site` | yes | The organization's URL, e.g. `https://chat.example.com` |
| `email` | yes | The bridge account's login email |
| `api_key` | yes | The bridge account's API key |
| `default_topic` | no | The topic used as a stream's top level (default `switch`, at most 60 characters) |
| `default_member` | no | A person, by user id or login email, subscribed to every stream Switch creates |
| `verify_tls` | no | Default `true`; `false` only for a self-signed certificate you trust |

The credentials are checked when the connection is registered.

## 4. Bring channels in

- **Create from Switch.** A room created against the bridge creates a stream,
  public or private, with the bridge account and `default_member` subscribed.
- **Adopt an existing stream.** Subscribe the bridge account to it; the
  subscription is noticed and the stream is bridged.

Adding an agent to a room subscribes its bot to the stream. Address it with
`@**Agent Name**` (Zulip's autocomplete) or `@agent-name`.

## Requests and activity

While an agent works, its bot:

- reacts to the message it is handling with :eyes:, or :time_ticking: while the
  message is queued behind other work;
- keeps one status message up to date with what it is doing;
- posts a **card** when it needs a decision: a permission, or a question.

A card can be answered three ways, all meaning the same option by the same
number:

1. click one of the numbered reactions (:one:, :two:, …) the bot seeds on it;
2. reply with the card's handle and a number, e.g. `R7 1`;
3. reply with just the number, if it is the first thing said after the card.

While a turn is running its status carries :stop_sign:; clicking it stops the
agent's current work, judged by the same rules as `!interrupt`.

Clicks and answers from bots are ignored. When an answer cannot be taken, the
person who gave it is told why in a direct message from the bridge account.

## Files

Files people attach are downloaded and given to the agent, up to the
deployment's media limit; a file over it, or one that cannot be fetched, is
named in the room rather than dropped. Files an agent sends are uploaded as its
bot, within the organization's upload limit.

## Removing the bridge

Removing the connection deactivates the bots the bridge account owns and drops
its event queues. Deactivation is reversible by an organization administrator.
Streams are left as they are.

## Limits

- A card for more than ten options is answered by typing; Zulip's keycap emoji
  stop at ten.
- A card's reactions resolve only while the Switch process that drew it is
  running, or after it has redrawn the card. A click on an older card is
  ignored; the typed answer always works.
- A status whose send was never acknowledged cannot be found again — it prints
  no handle and Zulip has nowhere to hide a marker — so it is left unconfirmed
  rather than posted twice.
- In a private stream with protected history, a bot cannot read messages from
  before it was subscribed.
