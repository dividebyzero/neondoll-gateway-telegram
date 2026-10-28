# NeonDoll Telegram Gateway

Telegram is an independently compiled Core 5 Gateway extension. Core owns the
Wazero runtime, installation identity, capabilities, secrets, durable state,
Session routing, participant identity, permissions, and delivery replay. This
repository owns only Telegram protocol behavior and guest build/test tooling;
adding or updating it never requires rebuilding Core.

## Build

The checked-in artifact is reproducible with TinyGo 0.39:

```sh
make wasm
sha256sum wasm/build/gateway.wasm
```

The artifact exports Gateway ABI v2 and imports only mediated capabilities from
`gateway_host`: logging, outgoing events, HTTPS, redacted configuration, the
scoped Telegram token, and namespaced durable state.

## Install locally

1. Build `wasm/build/gateway.wasm` and verify its SHA-256.
2. Copy it to `<core-data-dir>/extensions/telegram/gateway.wasm`.
3. Copy the object in [`extension/manifest.example.json`](extension/manifest.example.json)
   into Core's `extensions` configuration and update its SHA-256 if necessary.
4. Store the bot token in `<core-secrets-dir>/telegram-main.token` with
   owner-only permissions.
5. Start Core. Its Extension Manager validates integrity, grants capabilities,
   loads the artifact, and assigns the trusted `telegram-main` instance ID.

For another bot, use another Core-assigned `instance_id`, token file, and
conversation mapping. Core namespaces state and secrets per installation.

## Reliability contract

Each Telegram update uses its decimal `update_id` as the stable Core event ID.
Fetched messages are durably stored before emission. The polling cursor moves
only after Core sends:

```json
{"v":0,"type":"inbound.accepted","payload":{"event_id":"42"}}
```

Acknowledgements may arrive out of order, but the cursor advances only through
the accepted prefix. Pending updates retry after restart and later updates
cannot skip an earlier unacknowledged update. The guest accepts at most 16
pending updates, 2 MiB of serialized durable state, and 64 KiB per structured
event. It asks Telegram for no more updates than available queue slots and stops
polling while full. Acknowledgement automatically frees capacity and resumes
polling. Persistence failure leaves Telegram's cursor unchanged, so restart can
recover the update.

All valid Telegram text messages are preserved, including the 4,096-character
maximum, multibyte UTF-8, and JSON escaping. Limits are measured in serialized
bytes: text has a 16 KiB safety bound, the complete event a 64 KiB bound, and
the mediated Bot API response a 4 MiB bound. A deliberately unsupported update
is advanced only through a durable skip record with a diagnostic reason; queue
capacity never causes a skip. Missing, invalid, or bot sender identities are
not emitted as user messages. Core remains the final identity and permission
authority.

Image, document, audio, voice, and video messages emit small `media` references
containing provider file IDs and metadata. Binary content is never embedded or
downloaded by this Gateway. A future separately authorized media capability may
retrieve it from Telegram; the reference itself grants no authority.

For outbound traffic Core sends `session.route`, including its stable
`delivery_id`, followed by `session.event`. Telegram reports the same ID in
`delivery.result` only after the Bot API returns HTTP success, valid JSON,
`ok: true`, and a valid sent-message result. Confirmed delivery IDs are retained
in bounded durable state so a Core retry does not resend a recently confirmed
message.

## Testing

```sh
make verify
```

The `host` package is an independent guest test harness, not a production
runtime. Production always uses Core's generic Extension Manager and Wazero
runtime. Cross-repository compatibility can be checked from a Core checkout:

```sh
TELEGRAM_WASM="$PWD/wasm/build/gateway.wasm"
(cd /path/to/neondoll && \
  NEONDOLL_TELEGRAM_WASM="$TELEGRAM_WASM" \
  go test ./Core/Extension \
    -run TestTelegramABICompatibilityWhenArtifactAvailable -count=1 -v)
```

The guest tests use a mock TLS Bot API to verify polling, acknowledgement,
restart recovery, ordered cursor advancement, outbound correlation, failure
retry, delivery deduplication, and installation isolation.

## Delivery limitation

Telegram's Bot API has no caller-supplied idempotency key. The guest persists a
delivery ID after Telegram confirms `sendMessage` and before reporting success
to Core. A process failure in the narrow interval after Telegram accepts the
request but before the durable write completes can still cause an at-least-once
duplicate on retry. Core and the guest otherwise deduplicate stable delivery
IDs.
