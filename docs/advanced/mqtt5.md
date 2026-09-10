# MQTT 5.0

## Enabling MQTT 5.0

Pass `version="5.0"` to `create_client()` (see [Version selection](../connecting.md#version-selection)):

```python
from zmqtt import create_client

async with create_client("localhost", version="5.0") as client:
    ...
```

The return type is `MQTTClientV5`, which exposes 5.0-specific methods and accepts 5.0-specific parameters.

## Connection information

`client.connection_info` returns an immutable `ConnectionInfo` snapshot of the
current successful handshake on all client types, including MQTT 3.1.1:

```python
async with create_client("localhost", version="5.0") as client:
    info = client.connection_info
    print(info.connection_id, info.session_present)
    print(info.effective_client_id, info.effective_keepalive)
    print(info.effective_session_expiry_interval)
    if info.properties is not None:
        print(info.properties.response_information)
        print(info.properties.user_properties)
```

`properties` holds raw `ConnAckProperties` without defaults, or `None` when
absent. Repeated User Properties retain their order. Effective values use the
broker's Assigned Client Identifier when CONNECT sent an empty ID, Server Keep
Alive over CONNECT's keepalive, and CONNACK session expiry over CONNECT's expiry
(default `0`). Explicit zero values are preserved; MQTT 3.1.1 has no properties
or session expiry. See the [MQTT 5.0 specification, CONNACK properties](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901086).

Access raises `MQTTDisconnectedError` before connecting, during retries, and
after disconnection or background-loop failure. Each successful handshake gets
a new `connection_id`, starting at 1, even when the MQTT session resumes.
Failed attempts do not increment it; saved snapshots remain unchanged.
Subscription restoration may still be running when the snapshot becomes available.

Effective values describe negotiation; they do not change ping scheduling,
future CONNECT IDs, or enforce broker limits. Response Information is the raw
broker string and does not alter reply topics or `request()`. Requesting it in
CONNECT is not currently exposed by the client configuration.

## Session expiry interval

Controls how long the broker preserves your session after disconnect. `0` (default) means the session ends immediately on disconnect; `0xFFFFFFFF` means the session never expires.

```python
async with create_client("localhost", version="5.0", session_expiry_interval=3600) as client:
    # Session survives for 1 hour after disconnect
    ...
```

## Publish properties

`PublishProperties` can be attached to any `publish()` call on a 5.0 connection:

```python
from zmqtt import PublishProperties

props = PublishProperties(
    message_expiry_interval=300,       # broker discards after 300 s
    content_type="application/json",
    response_topic="replies/my-app",
    correlation_data=b"request-id-42",
    user_properties=(("x-source", "sensor-01"), ("x-region", "eu-west")),
)
await client.publish("data/readings", b'{"temp": 23.4}', properties=props)
```

Received messages expose properties via `msg.properties` (a `PublishProperties` instance or `None`):

```python
async for msg in sub:
    if msg.properties and msg.properties.response_topic:
        await client.publish(
            msg.properties.response_topic,
            b"ok",
            properties=PublishProperties(
                correlation_data=msg.properties.correlation_data,
            ),
        )
```

### `PublishProperties` fields

| Field | Type | Description |
|-------|------|-------------|
| `payload_format_indicator` | `int \| None` | 0 = bytes, 1 = UTF-8 string |
| `message_expiry_interval` | `int \| None` | Seconds until broker discards |
| `topic_alias` | `int \| None` | Integer 1..65535, limited by the server's Topic Alias Maximum |
| `response_topic` | `str \| None` | Topic for response messages |
| `correlation_data` | `bytes \| None` | Request/response correlation token |
| `subscription_identifier` | `int \| None` | Set by broker, not by publisher |
| `content_type` | `str \| None` | MIME type of the payload |
| `user_properties` | `tuple[tuple[str, str], ...]` | Arbitrary key-value pairs |

## Subscribe options (5.0 only)

Additional keyword arguments are available on 5.0 connections:

```python
from zmqtt import RetainHandling

async with client.subscribe(
    "local/events",
    no_local=True,             # do not receive own publishes
    retain_as_published=True,  # preserve the retain flag as published
    retain_handling=RetainHandling.SEND_IF_NOT_EXISTS,
    subscription_identifier=7,
) as sub:
    async for msg in sub:
        if msg.properties is not None:
            print(msg.properties.subscription_identifier)
```

| Option | Type | Description |
|--------|------|-------------|
| `no_local` | `bool` | Skip messages published by this client |
| `retain_as_published` | `bool` | Forward the original retain flag, not the delivery flag |
| `retain_handling` | `RetainHandling` | Control when retained messages are sent for this subscription |
| `subscription_identifier` | `int \| None` | Ask the broker to identify messages caused by this subscription |

!!! warning
    These options require MQTT 5.0. Using a 5.0-only value on a `version="3.1.1"` connection raises `RuntimeError`. A subscription identifier must be between `1` and `268435455`.

The broker copies `subscription_identifier` into matching PUBLISH packets, where it is available as `msg.properties.subscription_identifier`. This is especially useful when separate subscriptions have overlapping filters.

## Request / response (`client.request()`)

Send a request and await exactly one reply in a single call:

```python
reply = await client.request("services/echo", b"hello", timeout=5.0)
print(reply.payload)  # b"hello"
```

zmqtt manages the reply topic subscription, the `response_topic` /
`correlation_data` PUBLISH properties, and cleanup on timeout or
cancellation automatically. See [Request / Response](request-response.md)
for the full API and responder example.

## Low-level AUTH packet (`client.auth()`)

Send one MQTT 5 AUTH packet with reason code `0x18` (Continue Authentication):

```python
await client.auth("SCRAM-SHA-256", data=b"client-first-message")
```

The `method` string is sent as `authentication_method`, and `data` as
`authentication_data`. This call returns after writing the packet; it does not
negotiate the method in CONNECT, wait for a broker AUTH response, or implement a
multi-step mechanism such as SCRAM. Treat it as a low-level building block, not
a complete enhanced-authentication flow.

## CONNACK and DISCONNECT reason codes

In MQTT 5.0, CONNACK and DISCONNECT packets carry a reason code. A failed
CONNACK becomes `MQTTConnectError`, whose `return_code` contains the broker's
code. A broker-initiated DISCONNECT becomes `MQTTDisconnectedError`, with the
reason code included in its message.

Common failed-CONNACK reason codes are:

| Code | Name |
|------|------|
| 0x80 | Unspecified error |
| 0x81 | Malformed packet |
| 0x84 | Unsupported protocol version |
| 0x85 | Client identifier not valid |
| 0x86 | Bad username or password |
| 0x87 | Not authorised |
| 0x88 | Server unavailable |
| 0x8A | Banned |
| 0x8C | Bad authentication method |

See the [MQTT 5.0 spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html) for the full list.

## PUBACK and PUBREC reason codes

PUBACK (QoS 1) and PUBREC (QoS 2) also carry a reason code in MQTT 5.0. A code
below `0x80` — including `0x00` (Success) and `0x10` (No matching
subscribers) — completes `publish()` normally. A code of `0x80` or greater
raises [`MQTTPublishError`](../error-handling.md#mqttpublisherror):

```python
from zmqtt import MQTTPublishError, QoS

try:
    await client.publish("private/topic", b"payload", qos=QoS.AT_LEAST_ONCE)
except MQTTPublishError as e:
    print(f"Publish rejected: 0x{e.reason_code:02X} ({e.reason_name})")
```

`reason_name` is the spec's name for the code. The broker's optional Reason
String property is exposed separately as `reason_string`, and is `None` when
the broker omits it.

This check does not apply to `version="3.1.1"` connections, where PUBACK and
PUBREC carry no reason code.

See the MQTT 5.0 spec for the permitted reason codes:
[PUBACK](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901124)
and [PUBREC](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901134).

---

**See also:** [Connecting — Version selection](../connecting.md#version-selection) · [Publishing](../publishing.md) · [Subscribing](../subscribing.md) · [Error Handling](../error-handling.md)
