# Subscribing

## `client.subscribe()`

`subscribe()` returns a `Subscription` object. Use it as an async context manager — the SUBSCRIBE packet is sent to the broker on entry and UNSUBSCRIBE is sent on exit:

```python
async with client.subscribe("sensors/#") as sub:
    async for msg in sub:
        print(msg.topic, msg.payload.decode())
```

## Manual subscription lifecycle

When nesting context managers does not fit your program structure — for example when subscriptions are added or removed dynamically at runtime — use `start()` and `stop()` directly:

```python
sub = client.subscribe("sensors/#", qos=QoS.AT_LEAST_ONCE)
await sub.start()

async for msg in sub:
    ...

await sub.stop()
```

`stop()` sends UNSUBSCRIBE, stops message delivery and returns the broker's UNSUBACK as an `UnsubscribeResult`, or `None` if none was received — for example after the connection was lost. It never raises: failures and rejections are logged as warnings. Exiting `async with` does the same and discards the result.

On MQTT 5.0 the broker may reject some filters (reason code `0x80` or above); they are listed in `failures`. The subscription stops anyway, but the broker may keep sending messages on a rejected filter — on a persistent session, across reconnects too.

```python
result = await sub.stop()
if result is not None:
    for topic_filter, reason_code in result.failures.items():
        print(f"{topic_filter!r} rejected: 0x{reason_code:02X}")
```

### Buffering and backpressure

Each `Subscription` buffers incoming messages. The size of this buffer is controlled by `receive_buffer_size`; the default is `1000` messages. See [Backpressure](advanced/backpressure.md) for how a full buffer slows message delivery.

- A positive value limits the buffer to that many messages.
- `0` makes the buffer unbounded.

When a bounded buffer is full, message delivery is naturally slowed down instead of allowing memory usage to grow indefinitely.

## Async iterator

`async for` loops indefinitely. Break out when you are done:

```python
async with client.subscribe("commands/+") as sub:
    async for msg in sub:
        handle(msg)
        if msg.topic.endswith("/stop"):
            break
```

## Explicit pull with `get_message()`

`get_message()` returns the next message, suspending until one arrives:

```python
async with client.subscribe("telemetry/device/01") as sub:
    first  = await sub.get_message()
    second = await sub.get_message()
```

Use `get_message()` when you want pull-based control over when to read the next message. To bound memory when the consumer is slow, see [Backpressure](advanced/backpressure.md).

## `Message` fields

| Field | Type | Description |
|-------|------|-------------|
| `topic` | `str` | Full topic the message was published on |
| `payload` | `bytes` | Raw payload bytes |
| `qos` | `QoS` | QoS level of the incoming message |
| `retain` | `bool` | `True` if this is a retained message |
| `properties` | `PublishProperties \| None` | MQTT 5.0 properties; `None` on 3.1.1 |

## Multiple topic filters

Pass multiple filters in a single call. One `Subscription` object covers all of them:

```python
async with client.subscribe("sensors/#", "alerts/+", "status") as sub:
    async for msg in sub:
        ...
```

## QoS on subscribe

```python
async with client.subscribe("data/#", qos=QoS.AT_LEAST_ONCE) as sub:
    ...
```

The broker delivers messages at the lower of the publish QoS and the subscribe QoS.

## `RetainHandling` (MQTT 5.0)

`RetainHandling` controls whether retained messages are sent when you subscribe. Available values:

| Value | Behaviour |
|-------|-----------|
| `SEND_ON_SUBSCRIBE` (default) | Send retained messages on every SUBSCRIBE |
| `SEND_IF_NOT_EXISTS` | Send only if no subscription already exists |
| `DO_NOT_SEND` | Never send retained messages |

```python
from zmqtt import RetainHandling

async with client.subscribe("status/#", retain_handling=RetainHandling.SEND_IF_NOT_EXISTS) as sub:
    ...
```

## Wildcard filter priority

When multiple filters in the same `Subscription` match an incoming topic, zmqtt routes the message to **exactly one** internal queue: the one that corresponds to the *most specific* matching filter. Delivery is never duplicated inside zmqtt.

Specificity is compared level-by-level, left to right:

- A literal segment beats `+`
- `+` beats `#`

**Example:** if you subscribe to both `a/b` and `a/#`, a message on `a/b` is routed to `a/b` only:

```python
async with client.subscribe("a/b", "a/#") as sub:
    # message published to "a/b" → delivered once, matched by "a/b"
    # message published to "a/c" → delivered once, matched by "a/#"
```

### Broker behaviour vs zmqtt behaviour

Some brokers can deliver **multiple** PUBLISH packets to a client when the client has **overlapping subscriptions** that all match the same topic. In practice:

- Some brokers may deliver only one message.
- Others may deliver one message **per matching subscription**.

zmqtt's routing rule above applies **after** packets are received: if multiple PUBLISH packets arrive from the broker, zmqtt will still enqueue each received packet (because they are distinct network deliveries). If only one PUBLISH arrives, zmqtt ensures it is delivered to the most specific matching filter queue.

### Tie-breaking

When two filters tie (same specificity), zmqtt logs a `WARNING` and routes the message to whichever filter was registered first.

## Duplicate-filter guard

!!! warning
    Subscribing the same filter string across two separate `Subscription` objects logs a `WARNING`. The second subscription gets no queue for that filter — it will receive `get_message()` results from other filters only. The SUBSCRIBE is still forwarded to the broker.

```python
async with client.subscribe("data/temp") as sub1:
    async with client.subscribe("data/temp") as sub2:  # WARNING logged
        # sub2 receives nothing for "data/temp"
        ...
```

Prefer multiple filters in a single `subscribe()` call rather than multiple overlapping subscriptions. See [Logging](logging.md) for how to observe this warning at runtime.

---

**See also:** [Manual Ack](advanced/manual-ack.md) · [Backpressure](advanced/backpressure.md) · [MQTT 5.0](advanced/mqtt5.md) · [Scaling](advanced/scaling.md)
