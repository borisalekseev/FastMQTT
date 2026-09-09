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

Blocking on a full queue assumes someone will drain it. Between `stop()`
sending UNSUBSCRIBE and the broker answering, that assumption no longer holds:
the consumer has left, so a blocked delivery would hold the read loop — and
every other subscription on the connection — indefinitely.

In that window, and only when the buffer is completely full, an arriving
message is dropped and logged at `WARNING`. Messages already buffered are kept
and can still be read after `stop()` returns. If the broker rejects the
unsubscribe, the subscription stays active and delivery goes back to blocking.

A message is also dropped, again at `WARNING`, when it arrives for a filter
whose consumer is already gone — a subscription cancelled mid-flight, or a
QoS 2 exchange whose PUBREL lands after `stop()`.

A dropped message at QoS 1 or 2 is still acknowledged, so the broker will not
redeliver it. Draining the subscription before calling `stop()` avoids the
window entirely.

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
