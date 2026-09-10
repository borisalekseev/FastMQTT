# Backpressure

## `receive_buffer_size`

By default, the internal message queue for each `Subscription` is bounded to `1000`. Set `receive_buffer_size` to change it:

```python
async with client.subscribe("telemetry/#", receive_buffer_size=100) as sub:
    async for msg in sub:
        await slow_process(msg)
```

`receive_buffer_size` is passed directly to `asyncio.Queue(maxsize=...)`. When the queue is full, `put()` blocks.

## How flow control works

The library's read loop reads packets from the TCP stream and dispatches them. When a `Subscription` queue is full:

1. The read loop blocks on `queue.put()` to that subscription's queue.
2. The read loop stops reading new data from the socket.
3. The TCP receive buffer fills.
4. The TCP stack signals backpressure to the broker via window size reduction.

The result is bounded client-side memory and flow control back to the broker
connection. What happens beyond that connection is broker-specific: the broker
may slow publishers, queue messages, or apply an overload policy. A bounded
zmqtt queue alone does not guarantee that every upstream message is preserved.

## When to use it

Use `receive_buffer_size` when:

- Your message handler is slow (I/O-bound, database writes, etc.) and you want to bound memory usage.
- You need bounded in-process buffering and prefer slowing the connection to
  growing memory without limit.
- You are implementing a consumer that must apply backpressure to upstream producers.

Set it to `0` (unbounded) when:

- Message arrival rate is low or bounded.
- Another layer enforces a reliable upper bound on outstanding work.

An unbounded queue does not drop or log excess messages; it keeps allocating
memory. If dropping is part of your overload policy, implement that policy
explicitly in application code.

## When a message is dropped

A full buffer blocks delivery, and that is what applies backpressure — but it
only works while something is draining the buffer. Once you call `stop()`, the
subscription has said it is leaving, so delivery to it stops blocking: a blocked
delivery would stall every other subscription sharing the connection.

From that point messages still reach the subscription while its buffer has room.
Only when the buffer is completely full is an arriving message dropped and
logged at `WARNING`. Whatever is already buffered stays readable after `stop()`
returns.

This is permanent for the connection, which matters when the broker rejects the
unsubscribe: the filter stays subscribed and keeps delivering, but never blocks
again. A burst that outruns your reading loses messages even while you are
reading. Size `receive_buffer_size` for that burst, or retry `stop()` until the
broker accepts it. Only a reconnect restores blocking delivery.

Messages are also dropped for a subscription that was cancelled, or whose
`stop()` was cancelled: cancelling gives the subscription up.

### What a drop costs

| QoS | Cost |
| --- | --- |
| 0 | The message is gone. There is no acknowledgement to withhold. |
| 1 | The message is **not** acknowledged, so the broker keeps it and redelivers it when the session resumes. That needs `clean_session=False`; under the default `clean_session=True` the session is discarded on disconnect and the message is gone. No broker redelivers within a live connection ([MQTT 5 §4.4]). |
| 2 | The exchange is acknowledged at PUBREC, before delivery. A message whose PUBREL arrives after `stop()` is placed in the buffer, but if the buffer is full at that moment it is dropped and still completed, so the broker will not redeliver it. |

Draining a subscription before calling `stop()` avoids all of this.

[MQTT 5 §4.4]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901240

## Request / response

MQTT 5.0 request routing has a separate limit. `max_pending_requests` defaults
to `1000` and caps the number of concurrent `request()` calls. Additional calls
wait before publishing, applying backpressure to the caller. Every response is
routed to a single recipient: a matching response completes only its request.
An unmatched or late response follows normal routing and may be buffered by at
most one regular `Subscription`. On a resumed persistent session, a message that
arrives before its local subscription is declared uses the separate bounded
[session replay buffer](persistent-sessions.md#startup-replay).

!!! warning
    Applying backpressure affects all topics multiplexed on the same TCP connection. A slow consumer on one `Subscription` will stall delivery to all other subscriptions on the same client. If you need independent flow control per topic, use separate clients or scale your application using shared subscriptions or emqs `$queue`.

---

**See also:** [Subscribing](../subscribing.md) · [Manual Ack](manual-ack.md)
