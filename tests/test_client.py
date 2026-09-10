"""Unit tests for MQTTClient construction and connect-retry behaviour."""

import asyncio
import ssl
from collections import deque
from typing import TYPE_CHECKING

import pytest

from zmqtt import (
    MQTTClient,
    MQTTTimeoutError,
    QoS,
    ReconnectConfig,
    Subscription,
    Will,
    WillProperties,
    create_client,
)
from zmqtt._internal._compat import ExceptionGroup
from zmqtt._internal.packets.codec import encode
from zmqtt._internal.packets.connect import ConnAck
from zmqtt._internal.packets.ping import PingResp
from zmqtt._internal.packets.publish import PubAck, Publish, PubRel
from zmqtt._internal.packets.reader import PacketBuffer
from zmqtt._internal.packets.subscribe import SubAck, UnsubAck
from zmqtt._internal.transport.base import Transport
from zmqtt.errors import MQTTDisconnectedError, MQTTProtocolError, MQTTUnsubscribeError

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine


def test_mqtt_connect_timeout_default_is_30s() -> None:
    client = MQTTClient("localhost")
    assert client._mqtt_connect_timeout == 30.0


@pytest.mark.parametrize("bad", [0, -1, -0.5, float("nan")])
def test_non_positive_mqtt_connect_timeout_raises(bad: float) -> None:
    with pytest.raises(ValueError, match="mqtt_connect_timeout must be positive"):
        create_client("localhost", mqtt_connect_timeout=bad)


def test_create_client_accepts_will() -> None:
    will = Will(
        topic="status/client",
        payload=b"offline",
        qos=QoS.AT_LEAST_ONCE,
        retain=True,
    )

    client = create_client("localhost", will=will)

    assert isinstance(client, MQTTClient)


def test_mqtt_v311_rejects_will_properties() -> None:
    will = Will(
        topic="status/client",
        payload=b"offline",
        qos=QoS.AT_LEAST_ONCE,
        retain=True,
        properties=WillProperties(content_type="text/plain"),
    )

    with pytest.raises(RuntimeError, match=r"will properties require MQTT 5\.0"):
        MQTTClient("localhost", version="3.1.1", will=will)


class FakeTransport:
    """Minimal Transport: read() hangs until fed; tracks close()."""

    def __init__(self, feed: bytes | None = None) -> None:
        self.sent: list[bytes] = []
        self.packet_sent = asyncio.Event()
        self.closed = False
        self._rx: deque[bytes] = deque()
        if feed is not None:
            self._rx.append(feed)

    def feed(self, data: bytes) -> None:
        self._rx.append(data)

    async def read(self, n: int) -> bytes:  # noqa: ARG002
        while not self._rx:  # noqa: ASYNC110
            await asyncio.sleep(0)
        return self._rx.popleft()

    async def write(self, data: bytes) -> None:
        self.sent.append(data)
        self.packet_sent.set()

    async def close(self) -> None:
        self.closed = True

    @property
    def is_connected(self) -> bool:
        return not self.closed


class BreakableTransport(FakeTransport):
    """FakeTransport whose reads start failing once the peer goes away."""

    def __init__(self, feed: bytes | None = None) -> None:
        super().__init__(feed)
        self.broken = False

    async def read(self, n: int) -> bytes:  # noqa: ARG002
        while not self._rx:
            if self.broken:
                msg = "Connection lost"
                raise MQTTDisconnectedError(msg)
            await asyncio.sleep(0)
        return self._rx.popleft()


CONNACK_V5 = encode(ConnAck(session_present=False, return_code=0), version="5.0")


def v5_client(transport: FakeTransport) -> MQTTClient:
    """MQTT 5.0 client whose only transport is the given fake."""

    async def factory(host: str, port: int, tls: ssl.SSLContext | bool | None) -> Transport:  # noqa: ARG001
        return transport

    return MQTTClient("localhost", version="5.0", transport_factory=factory)


async def start_subscription(
    subscription: Subscription,
    transport: FakeTransport,
    *,
    packet_id: int = 1,
    return_codes: tuple[int, ...] = (0x00,),
) -> None:
    """Complete start() against a SUBACK fed once SUBSCRIBE is on the wire."""
    transport.packet_sent.clear()
    started = asyncio.create_task(subscription.start())
    await transport.packet_sent.wait()
    transport.feed(encode(SubAck(packet_id=packet_id, return_codes=return_codes), version="5.0"))
    await started


def publish_v5(topic: str, payload: bytes) -> bytes:
    return encode(
        Publish(topic=topic, payload=payload, qos=QoS.AT_MOST_ONCE, retain=False, dup=False),
        version="5.0",
    )


def publish_qos1_v5(topic: str, payload: bytes, *, packet_id: int) -> bytes:
    return encode(
        Publish(topic=topic, payload=payload, qos=QoS.AT_LEAST_ONCE, retain=False, dup=False, packet_id=packet_id),
        version="5.0",
    )


def publish_qos2_v5(topic: str, payload: bytes, *, packet_id: int) -> bytes:
    return encode(
        Publish(topic=topic, payload=payload, qos=QoS.EXACTLY_ONCE, retain=False, dup=False, packet_id=packet_id),
        version="5.0",
    )


def acknowledged_packet_ids(transport: FakeTransport) -> list[int]:
    """Packet ids the client PUBACKed, in the order they went on the wire."""
    buf = PacketBuffer(version="5.0")
    for chunk in transport.sent:
        buf.feed(chunk)
    return [packet.packet_id for packet in buf if isinstance(packet, PubAck)]


async def hold_subscription(
    client: MQTTClient,
    topic: str,
    entered: asyncio.Event,
    blocker: asyncio.Event,
) -> None:
    """Hold a subscription open until released, for cancellation tests."""
    async with client.subscribe(topic):
        entered.set()
        await blocker.wait()


async def fail_inside_subscription(client: MQTTClient, topic: str, error: Exception) -> None:
    """Raise from the body of a subscription context manager."""
    async with client.subscribe(topic):
        raise error


async def start_consumer(
    coro: "Coroutine[object, object, None]",
    transport: FakeTransport,
    entered: asyncio.Event,
    *,
    packet_id: int = 1,
) -> "asyncio.Task[None]":
    """Run a consumer coroutine up to the point where it holds its subscription."""
    transport.packet_sent.clear()
    task = asyncio.create_task(coro)
    await transport.packet_sent.wait()
    transport.feed(encode(SubAck(packet_id=packet_id, return_codes=(0x00,)), version="5.0"))
    await entered.wait()

    return task


async def test_connect_retries_after_connack_timeout() -> None:
    connack = encode(ConnAck(session_present=False, return_code=0), version="3.1.1")
    transports = [
        FakeTransport(),  # 1st attempt: never CONNACKs -> times out
        FakeTransport(feed=connack),  # 2nd attempt: succeeds
    ]
    made: list[FakeTransport] = []

    async def factory(host: str, port: int, tls: ssl.SSLContext | bool | None) -> Transport:  # noqa: ARG001
        transport = transports[len(made)]
        made.append(transport)
        return transport

    client = MQTTClient(
        "localhost",
        mqtt_connect_timeout=0.05,
        reconnect=ReconnectConfig(initial_delay=0.0, max_attempts=None),
        transport_factory=factory,
    )

    await client._connect_with_retry()

    assert len(made) == 2  # the first (timed-out) attempt was retried
    assert made[0].closed  # the dead transport's fd was released
    assert client._protocol is not None


async def test_mqtt_connect_timeout_gives_up_after_max_attempts() -> None:
    made: list[FakeTransport] = []

    async def factory(host: str, port: int, tls: ssl.SSLContext | bool | None) -> Transport:  # noqa: ARG001
        transport = FakeTransport()  # never fed -> CONNACK never arrives -> times out
        made.append(transport)
        return transport

    client = MQTTClient(
        "localhost",
        mqtt_connect_timeout=0.05,
        reconnect=ReconnectConfig(initial_delay=0.0, max_attempts=1),
        transport_factory=factory,
    )

    with pytest.raises(MQTTTimeoutError):
        await client._connect_with_retry()

    assert len(made) == 1  # gave up after the single allowed attempt
    assert made[0].closed  # transport still cleaned up on give-up


async def test_stop_completes_when_messages_precede_unsuback() -> None:
    """A full subscription queue must not block a following UNSUBACK."""

    transport = FakeTransport(feed=CONNACK_V5)
    client = v5_client(transport)
    subscription = client.subscribe("topic", receive_buffer_size=1)
    async with client:
        await start_subscription(subscription, transport)
        transport.packet_sent.clear()

        stopped = asyncio.create_task(subscription.stop())
        await transport.packet_sent.wait()
        transport.feed(publish_v5("topic", b"first"))
        transport.feed(publish_v5("topic", b"second"))
        transport.feed(encode(UnsubAck(packet_id=1, reason_codes=(0x00,)), version="5.0"))
        await asyncio.wait_for(stopped, timeout=0.2)

        assert subscription not in client._subscriptions


async def test_full_subscription_queue_delivers_messages_after_consumer_makes_room() -> None:
    """A bounded queue applies backpressure instead of dropping a QoS 1 message."""

    transport = FakeTransport(feed=CONNACK_V5)
    client = v5_client(transport)
    subscription = client.subscribe("topic", auto_ack=False, receive_buffer_size=1)
    async with client:
        await start_subscription(subscription, transport)
        for packet_id, payload in ((2, b"first"), (3, b"second")):
            transport.feed(publish_qos1_v5("topic", payload, packet_id=packet_id))

        first = await asyncio.wait_for(subscription.get_message(), timeout=0.2)
        await first.ack()
        second = await asyncio.wait_for(subscription.get_message(), timeout=0.2)
        await second.ack()

        assert [first.payload, second.payload] == [b"first", b"second"]


async def test_cancellation_completes_when_unsuback_is_withheld() -> None:
    """Cancellation must complete when the broker withholds UNSUBACK."""

    transport = FakeTransport(feed=CONNACK_V5)
    client = v5_client(transport)
    entered = asyncio.Event()
    blocker = asyncio.Event()
    async with client:
        task = await start_consumer(hold_subscription(client, "topic", entered, blocker), transport, entered)
        packets_before_cancellation = len(transport.sent)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=0.2)
        packets_after_cancellation = len(transport.sent)
        transport.packet_sent.clear()
        ping = asyncio.create_task(client.ping(timeout=0.2))
        await transport.packet_sent.wait()
        transport.feed(encode(PingResp(), version="5.0"))
        rtt = await ping

        assert packets_after_cancellation == packets_before_cancellation
        assert rtt >= 0


async def test_cancellation_leaves_no_registered_filter() -> None:
    transport = FakeTransport(feed=CONNACK_V5)
    client = v5_client(transport)
    entered = asyncio.Event()
    blocker = asyncio.Event()
    async with client:
        task = await start_consumer(hold_subscription(client, "topic", entered, blocker), transport, entered)
        protocol = client._protocol
        assert protocol is not None

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=0.2)

        assert not protocol._state.subscriptions.contains("topic")


async def test_stop_reports_unsuback_rejection_and_keeps_observed_filter() -> None:
    """Cleanup surfaces one error; an observed filter keeps its broker subscription."""

    allowed_filter = "allowed"
    denied_filter = "denied"
    reply_filter = "reply"
    transport = FakeTransport(feed=CONNACK_V5)
    client = v5_client(transport)
    subscription = client.subscribe(allowed_filter, denied_filter, reply_filter)
    async with client:
        await start_subscription(subscription, transport, return_codes=(0x00, 0x00, 0x00))
        protocol = client._protocol
        assert protocol is not None
        index = protocol._state.subscriptions
        await protocol.add_response_observer(reply_filter)
        transport.packet_sent.clear()
        stopped = asyncio.create_task(subscription.stop())
        await transport.packet_sent.wait()
        transport.feed(encode(UnsubAck(packet_id=1, reason_codes=(0x00, 0x87)), version="5.0"))
        with pytest.raises(MQTTUnsubscribeError) as exc_info:
            await stopped
        observed_registered = index.contains(reply_filter)
        observed_consumed = index.has_consumer(reply_filter)
        rejected_consumed = index.has_consumer(denied_filter)
        accepted_registered = index.contains(allowed_filter)
        packets_after_unsubscribe = len(transport.sent)
        replacement = client.subscribe(allowed_filter)
        await start_subscription(replacement, transport)
        replacement_packets = len(transport.sent) - packets_after_unsubscribe
        transport.packet_sent.clear()

        retried = asyncio.create_task(subscription.stop())
        await transport.packet_sent.wait()
        transport.feed(encode(UnsubAck(packet_id=1, reason_codes=(0x87,)), version="5.0"))
        with pytest.raises(MQTTUnsubscribeError) as retry_exc_info:
            await retried
        transport.feed(publish_v5(allowed_filter, b"replacement-still-active"))
        replacement_message = await asyncio.wait_for(replacement.get_message(), timeout=0.2)

        assert exc_info.value.failures == {denied_filter: 0x87}
        assert subscription._registered_filters == [denied_filter]
        assert observed_registered
        assert not observed_consumed
        assert rejected_consumed
        assert not accepted_registered
        assert replacement_packets == 1  # replacement SUBSCRIBE only
        assert retry_exc_info.value.failures == {denied_filter: 0x87}
        assert replacement_message.payload == b"replacement-still-active"


async def test_resubscribing_a_refused_filter_is_refused() -> None:
    """A filter its owner failed to release must not hand back a silent, dead subscription."""

    denied_filter = "denied"
    transport = FakeTransport(feed=CONNACK_V5)
    client = v5_client(transport)
    subscription = client.subscribe(denied_filter)
    async with client:
        await start_subscription(subscription, transport)
        transport.packet_sent.clear()
        stopped = asyncio.create_task(subscription.stop())
        await transport.packet_sent.wait()
        transport.feed(encode(UnsubAck(packet_id=1, reason_codes=(0x87,)), version="5.0"))
        with pytest.raises(MQTTUnsubscribeError):
            await stopped
        packets_after_refusal = len(transport.sent)

        with pytest.raises(RuntimeError, match="still held by a subscription"):
            await client.subscribe(denied_filter).start()

        assert len(transport.sent) == packets_after_refusal  # refused before reaching the wire


async def test_stop_sends_no_packet_for_an_observed_filter() -> None:
    """An observed filter is released locally, so its verdict needs no round-trip."""

    reply_filter = "reply"
    transport = FakeTransport(feed=CONNACK_V5)
    client = v5_client(transport)
    subscription = client.subscribe(reply_filter)
    async with client:
        await start_subscription(subscription, transport)
        protocol = client._protocol
        assert protocol is not None
        await protocol.add_response_observer(reply_filter)
        packets_before_stop = len(transport.sent)

        await asyncio.wait_for(subscription.stop(), timeout=0.2)

        assert len(transport.sent) == packets_before_stop
        assert protocol._state.subscriptions.contains(reply_filter)
        assert not protocol._state.subscriptions.has_consumer(reply_filter)
        assert subscription not in client._subscriptions


async def test_stop_after_unexpected_drop_detaches_subscription() -> None:
    """Stopping between connections completes locally instead of raising."""

    transport = BreakableTransport(feed=CONNACK_V5)
    client = v5_client(transport)
    subscription = client.subscribe("topic")
    await client.connect()
    await start_subscription(subscription, transport)
    transport.broken = True
    await asyncio.sleep(0.05)

    await asyncio.wait_for(subscription.stop(), timeout=1.0)

    assert subscription not in client._subscriptions
    assert subscription._registered_filters == []


async def test_cancelled_stop_gives_up_the_subscription() -> None:
    """A cancelled stop() is a consumer that is done, not one that changed its mind."""

    transport = FakeTransport(feed=CONNACK_V5)
    client = v5_client(transport)
    subscription = client.subscribe("topic", receive_buffer_size=1)
    async with client:
        await start_subscription(subscription, transport)
        transport.packet_sent.clear()

        stopped = asyncio.create_task(subscription.stop())
        await transport.packet_sent.wait()
        stopped.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stopped
        transport.feed(publish_v5("topic", b"first"))
        transport.packet_sent.clear()
        ping = asyncio.create_task(client.ping(timeout=0.2))
        await transport.packet_sent.wait()
        transport.feed(encode(PingResp(), version="5.0"))

        assert await ping >= 0
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(subscription.get_message(), timeout=0.2)


async def test_refused_response_observer_release_keeps_the_filter() -> None:
    reply_filter = "reply"
    transport = FakeTransport(feed=CONNACK_V5)
    client = v5_client(transport)
    subscription = client.subscribe(reply_filter)
    async with client:
        await start_subscription(subscription, transport)
        protocol = client._protocol
        assert protocol is not None
        await protocol.add_response_observer(reply_filter)
        await subscription.stop()
        transport.packet_sent.clear()

        released = asyncio.create_task(protocol.remove_response_observer(reply_filter))
        await transport.packet_sent.wait()
        transport.feed(encode(UnsubAck(packet_id=1, reason_codes=(0x87,)), version="5.0"))
        await released

        assert protocol._state.subscriptions.contains(reply_filter)


async def test_body_error_and_unexpected_cleanup_failure_are_combined() -> None:
    """Any cleanup failure is combined with the body error, not just a rejected UNSUBACK."""

    transport = FakeTransport(feed=CONNACK_V5)
    client = v5_client(transport)
    body_error = RuntimeError("application failed")
    async with client:
        transport.packet_sent.clear()
        task = asyncio.create_task(fail_inside_subscription(client, "topic", body_error))
        await transport.packet_sent.wait()
        transport.feed(encode(SubAck(packet_id=1, return_codes=(0x00,)), version="5.0"))
        transport.packet_sent.clear()
        await transport.packet_sent.wait()

        # A reason-code count that does not match the request reaches cleanup as
        # something other than MQTTUnsubscribeError.
        transport.feed(encode(UnsubAck(packet_id=1, reason_codes=(0x00, 0x00)), version="5.0"))
        with pytest.raises(ExceptionGroup) as exc_info:
            await asyncio.wait_for(task, timeout=0.2)

        errors = exc_info.value.exceptions
        assert errors[0] is body_error
        assert isinstance(errors[1], MQTTProtocolError)


async def test_dropped_message_is_not_acknowledged() -> None:
    """A QoS 1 message the client throws away must stay redeliverable."""

    transport = FakeTransport(feed=CONNACK_V5)
    client = v5_client(transport)
    subscription = client.subscribe("topic", receive_buffer_size=1)
    async with client:
        await start_subscription(subscription, transport)
        transport.feed(publish_qos1_v5("topic", b"buffered", packet_id=10))
        await asyncio.sleep(0)
        transport.packet_sent.clear()

        stopped = asyncio.create_task(subscription.stop())
        await transport.packet_sent.wait()
        transport.feed(publish_qos1_v5("topic", b"dropped", packet_id=11))
        await asyncio.sleep(0)
        transport.feed(encode(UnsubAck(packet_id=1, reason_codes=(0x00,)), version="5.0"))
        await asyncio.wait_for(stopped, timeout=0.2)

        assert acknowledged_packet_ids(transport) == [10]


async def test_message_without_a_subscriber_is_acknowledged() -> None:
    """A message no filter matches is undeliverable, not abandoned."""

    transport = FakeTransport(feed=CONNACK_V5)
    client = v5_client(transport)
    subscription = client.subscribe("topic")
    async with client:
        await start_subscription(subscription, transport)

        transport.feed(publish_qos1_v5("nobody/listens", b"orphan", packet_id=12))
        await asyncio.sleep(0)

        assert 12 in acknowledged_packet_ids(transport)


async def test_qos2_message_completed_after_stop_is_still_readable() -> None:
    """A QoS 2 exchange the client committed to at PUBREC belongs in the buffer."""

    transport = FakeTransport(feed=CONNACK_V5)
    client = v5_client(transport)
    subscription = client.subscribe("topic")
    async with client:
        await start_subscription(subscription, transport)
        transport.feed(publish_qos2_v5("topic", b"in-flight", packet_id=7))
        await asyncio.sleep(0)
        transport.packet_sent.clear()

        stopped = asyncio.create_task(subscription.stop())
        await transport.packet_sent.wait()
        transport.feed(encode(UnsubAck(packet_id=1, reason_codes=(0x00,)), version="5.0"))
        await asyncio.wait_for(stopped, timeout=0.2)
        transport.feed(encode(PubRel(packet_id=7), version="5.0"))
        message = await asyncio.wait_for(subscription.get_message(), timeout=0.2)

        assert message.payload == b"in-flight"


def cancel_stop(stopping: "asyncio.Task[None]", transport: FakeTransport) -> None:  # noqa: ARG001
    stopping.cancel()


def answer_without_a_verdict(stopping: "asyncio.Task[None]", transport: FakeTransport) -> None:  # noqa: ARG001
    """Two reason codes for one filter: an answer the client cannot act on."""
    transport.feed(encode(UnsubAck(packet_id=1, reason_codes=(0x00, 0x00)), version="5.0"))


@pytest.mark.parametrize(
    ("end_stop", "expected_error"),
    [
        (cancel_stop, asyncio.CancelledError),
        (answer_without_a_verdict, MQTTProtocolError),
    ],
)
async def test_departed_consumer_does_not_stall_the_connection(
    end_stop: "Callable[[asyncio.Task[None], FakeTransport], None]",
    expected_error: type[BaseException],
) -> None:
    """However stop() ends, the consumer is gone and must not starve the connection."""

    transport = FakeTransport(feed=CONNACK_V5)
    client = v5_client(transport)
    abandoned = client.subscribe("abandoned", receive_buffer_size=1)
    other = client.subscribe("other")
    async with client:
        await start_subscription(abandoned, transport)
        await start_subscription(other, transport)
        transport.packet_sent.clear()

        stopping = asyncio.create_task(abandoned.stop())
        await transport.packet_sent.wait()
        end_stop(stopping, transport)
        with pytest.raises(expected_error):
            await asyncio.wait_for(stopping, timeout=0.2)
        for index in range(3):
            transport.feed(publish_v5("abandoned", f"m{index}".encode()))
        transport.feed(publish_v5("other", b"hello"))
        message = await asyncio.wait_for(other.get_message(), timeout=0.2)

        assert message.payload == b"hello"
