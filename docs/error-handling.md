# Error Handling

## Exception hierarchy

```
MQTTError
  ├── MQTTConnectError      # CONNACK refused (return_code attribute)
  ├── MQTTProtocolError     # malformed or unexpected packet
  ├── MQTTDisconnectedError # connection lost unexpectedly
  ├── MQTTTimeoutError      # PINGRESP or CONNACK timed out
  ├── MQTTSubscribeError    # one or more filters rejected by the broker
  ├── MQTTPublishError      # QoS 1/2 publish rejected by the broker
  └── MQTTInvalidTopicError # topic string failed MQTT validation
```

All exceptions are importable from `zmqtt`:

```python
from zmqtt import (
    MQTTError,
    MQTTConnectError,
    MQTTProtocolError,
    MQTTDisconnectedError,
    MQTTTimeoutError,
    MQTTSubscribeError,
    MQTTPublishError,
    MQTTInvalidTopicError,
)
```

## When each is raised

### `MQTTConnectError`

Raised during `__aenter__` when the broker refuses the connection. The `return_code` attribute holds the CONNACK return code:

```python
from zmqtt import MQTTConnectError

try:
    async with create_client("localhost") as client:
        ...
except MQTTConnectError as e:
    print(f"Broker refused connection: code {e.return_code}")
```

For MQTT 5.0, `e.properties` contains the received `ConnAckProperties`.
Convenience attributes expose the broker's diagnostics:

```python
try:
    async with create_client("localhost", version="5.0") as client:
        ...
except MQTTConnectError as e:
    print(e.return_code, e.reason_string, e.server_reference)
    for key, value in e.user_properties:
        print(key, value)
```

Without properties (including MQTT 3.1.1), `properties`, `reason_string`, and
`server_reference` are `None`, and `user_properties` is `()`. Repeated User
Properties preserve their original order. The positional constructor
`MQTTConnectError(return_code)` and exception text remain unchanged; properties
can also be supplied with the optional keyword argument `properties=...`.
A refused connection does not publish `client.connection_info`.

Common return codes (MQTT 3.1.1):

| Code | Meaning |
|------|---------|
| 1 | Unacceptable protocol version |
| 2 | Client identifier rejected |
| 3 | Server unavailable |
| 4 | Bad username or password |
| 5 | Not authorised |

### `MQTTSubscribeError`

Raised when the broker rejects one or more topic filters in its SUBACK response. This commonly indicates an authorization failure. The `failures` attribute maps each rejected filter to its numeric reason code:

```python
from zmqtt import MQTTSubscribeError

try:
    async with client.subscribe("private/#") as sub:
        ...
except MQTTSubscribeError as e:
    for topic_filter, reason_code in e.failures.items():
        print(f"{topic_filter!r} rejected: 0x{reason_code:02X}")
```

The same exception is raised by `await sub.start()` when using the manual subscription lifecycle.

A rejected UNSUBSCRIBE is not an exception — see [Manual subscription lifecycle](subscribing.md#manual-subscription-lifecycle).

### `MQTTPublishError`

Raised on a `version="5.0"` connection when the broker rejects a QoS 1 or QoS 2
`publish()` — a PUBACK or PUBREC reason code of `0x80` or greater. A common
cause is an authorization denial. The `reason_code` attribute holds the numeric
code, `reason_name` the spec's name for it (`None` for a code zmqtt doesn't
recognize), and `reason_string` the broker's optional Reason String property:

```python
from zmqtt import MQTTPublishError, QoS

try:
    await client.publish("private/topic", b"payload", qos=QoS.AT_LEAST_ONCE)
except MQTTPublishError as e:
    print(f"Publish rejected: 0x{e.reason_code:02X} ({e.reason_name})")
```

For QoS 2, a rejected PUBREC ends the handshake immediately — no PUBREL is
sent, since MQTT 5.0 only permits PUBREL after a PUBREC reason code below
`0x80`. In both cases the packet identifier is released and the connection
remains usable for further operations.

Not raised for QoS 0, and not raised at all on `version="3.1.1"` connections —
PUBACK and PUBREC carry no reason code in that protocol version, so a rejected
publish there completes without error unless the broker instead closes the
connection.

See [PUBACK and PUBREC reason codes](advanced/mqtt5.md#puback-and-pubrec-reason-codes)
for more detail.

### `MQTTProtocolError`

Raised when the broker sends a packet that violates the MQTT spec — wrong packet type in context, malformed header, etc. This usually indicates a broker bug or a mismatch between library version and broker behaviour.

### `MQTTDisconnectedError`

Raised when an operation cannot continue because the connection was lost. If reconnection is enabled (the default), transient connection failures are normally handled automatically.

If reconnection is disabled (`ReconnectConfig(enabled=False)`), an in-progress or subsequent `publish()`, `ping()`, or subscription start may raise `MQTTDisconnectedError`. The exception is not injected asynchronously into unrelated application code.

### `MQTTTimeoutError`

Raised by `client.ping()` when no PINGRESP arrives within the timeout:

```python
try:
    rtt = await client.ping(timeout=5.0)
except MQTTTimeoutError:
    print("Broker not responding")
```

It is also raised by the connect handshake when the broker accepts the TCP
connection but does not send a CONNACK within `mqtt_connect_timeout` seconds
(default 30 s; see [Connecting](connecting.md)). When reconnection is enabled
(the default), this is treated like any other connection failure: the client
backs off and retries rather than surfacing the error.

See [Manual Ping](advanced/ping.md) for the full `ping()` API.

### `MQTTInvalidTopicError`

Raised when a topic string fails MQTT validation. The check happens eagerly —
before any I/O — in `publish()`, `subscribe()`, and `request()`.

**`publish()` — topic name rules:**

- Must not be empty.
- Must not contain `+` or `#` (wildcards are for filters only).
- `$` is only valid as the very first character.

```python
from zmqtt import MQTTInvalidTopicError

try:
    await client.publish("sensors/+/temp", b"22.5")
except MQTTInvalidTopicError as e:
    print(e)  # Wildcards not allowed in publish topic: 'sensors/+/temp'
```

**`subscribe()` — topic filter rules:**

- Must not be empty.
- `#` must be the last character and, if not the only character, must be
  preceded by `/`.
- `+` must occupy an entire level (e.g. `a/+/b` is valid; `a/temp+/b` is not).
- `$` is only valid as the very first character.

```python
try:
    client.subscribe("sensors#")          # missing preceding '/'
    client.subscribe("sensors/temp+/data") # '+' not a full level
except MQTTInvalidTopicError as e:
    print(e)
```

**`request()` — request and response topic rules:**

The request topic and the `response_topic` property both follow the publish-topic
rules. They are validated before zmqtt sends or subscribes to anything:

```python
from zmqtt import MQTTInvalidTopicError

try:
    await client.request(
        "cmd/+",
        b"x",
    )
except MQTTInvalidTopicError as e:
    print(e)
```

The same exception is raised for an invalid custom response topic such as
`PublishProperties(response_topic="reply/+/bad")`.

See [Request / Response](advanced/request-response.md) for details.

## Reconnection interaction

When `ReconnectConfig(enabled=True)` (the default), the client reconnects with
exponential backoff. An active `async for msg in sub` loop keeps waiting and
resumes after the subscription is restored. If all attempts fail, the optional
`on_connection_recovery_failed` callback is invoked once and active subscription
iterators raise the terminal connection error.

When reconnection is disabled, active subscription iterators raise
`MQTTDisconnectedError` after connectivity is lost. A later operation that
requires a live connection raises the same error.

Pending MQTT 5.0 requests have their own timeout and reconnect behaviour; see [Request / Response](advanced/request-response.md#connection-loss-and-reconnection).

See [Reconnection](advanced/reconnection.md) for full details.

---

**See also:** [Connecting](connecting.md) · [Manual Ping](advanced/ping.md) · [Logging](logging.md)
