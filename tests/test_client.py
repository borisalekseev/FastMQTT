"""Unit tests for MQTTClient construction and connect-retry behaviour."""

import asyncio
import ssl
from collections import deque

import pytest

from zmqtt import MQTTClient, MQTTTimeoutError, QoS, ReconnectConfig, Will, WillProperties, create_client
from zmqtt._internal._compat import ExceptionGroup
from zmqtt._internal.packets.codec import encode
from zmqtt._internal.packets.connect import ConnAck
from zmqtt._internal.packets.ping import PingResp
from zmqtt._internal.packets.publish import Publish
from zmqtt._internal.packets.subscribe import SubAck, UnsubAck
from zmqtt._internal.transport.base import Transport
from zmqtt.errors import MQTTSubscribeError, MQTTUnsubscribeError


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

    packet_id = 1
    transport = FakeTransport(
        feed=encode(ConnAck(session_present=False, return_code=0), version="5.0"),
    )

    async def factory(host: str, port: int, tls: ssl.SSLContext | bool | None) -> Transport:  # noqa: ARG001
        return transport

    client = MQTTClient("localhost", version="5.0", transport_factory=factory)
    subscription = client.subscribe("topic", receive_buffer_size=1)

    async with client:
        transport.packet_sent.clear()
        started = asyncio.create_task(subscription.start())
        await transport.packet_sent.wait()
        transport.feed(encode(SubAck(packet_id=packet_id, return_codes=(0x00,)), version="5.0"))
        await started

        transport.packet_sent.clear()
        stopped = asyncio.create_task(subscription.stop())
        await transport.packet_sent.wait()
        transport.feed(
            encode(
                Publish(
                    topic="topic",
                    payload=b"first",
                    qos=QoS.AT_MOST_ONCE,
                    retain=False,
                    dup=False,
                ),
                version="5.0",
            ),
        )

        transport.feed(
            encode(
                Publish(
                    topic="topic",
                    payload=b"second",
                    qos=QoS.AT_MOST_ONCE,
                    retain=False,
                    dup=False,
                ),
                version="5.0",
            ),
        )
        transport.feed(encode(UnsubAck(packet_id=packet_id, reason_codes=(0x00,)), version="5.0"))
        await asyncio.wait_for(stopped, timeout=0.2)
        detached = subscription not in client._subscriptions

    assert detached


async def test_full_subscription_queue_delivers_messages_after_consumer_makes_room() -> None:
    """A bounded queue applies backpressure instead of dropping a QoS 1 message."""

    subscription_packet_id = 1
    transport = FakeTransport(
        feed=encode(ConnAck(session_present=False, return_code=0), version="5.0"),
    )

    async def factory(host: str, port: int, tls: ssl.SSLContext | bool | None) -> Transport:  # noqa: ARG001
        return transport

    client = MQTTClient("localhost", version="5.0", transport_factory=factory)
    subscription = client.subscribe("topic", auto_ack=False, receive_buffer_size=1)

    async with client:
        transport.packet_sent.clear()
        started = asyncio.create_task(subscription.start())
        await transport.packet_sent.wait()
        transport.feed(encode(SubAck(packet_id=subscription_packet_id, return_codes=(0x00,)), version="5.0"))
        await started

        transport.feed(
            encode(
                Publish(
                    topic="topic",
                    payload=b"first",
                    qos=QoS.AT_LEAST_ONCE,
                    retain=False,
                    dup=False,
                    packet_id=2,
                ),
                version="5.0",
            ),
        )
        transport.feed(
            encode(
                Publish(
                    topic="topic",
                    payload=b"second",
                    qos=QoS.AT_LEAST_ONCE,
                    retain=False,
                    dup=False,
                    packet_id=3,
                ),
                version="5.0",
            ),
        )

        first = await asyncio.wait_for(subscription.get_message(), timeout=0.2)
        await first.ack()
        second = await asyncio.wait_for(subscription.get_message(), timeout=0.2)
        await second.ack()

    assert [first.payload, second.payload] == [b"first", b"second"]


async def test_cancellation_completes_when_unsuback_is_withheld() -> None:
    """Cancellation must complete when the broker withholds UNSUBACK."""

    packet_id = 1
    transport = FakeTransport(
        feed=encode(ConnAck(session_present=False, return_code=0), version="5.0"),
    )
    entered = asyncio.Event()
    blocker = asyncio.Event()

    async def factory(host: str, port: int, tls: ssl.SSLContext | bool | None) -> Transport:  # noqa: ARG001
        return transport

    async def consume(client: MQTTClient) -> None:
        async with client.subscribe("topic"):
            entered.set()
            await blocker.wait()

    client = MQTTClient("localhost", version="5.0", transport_factory=factory)

    async with client:
        transport.packet_sent.clear()
        task = asyncio.create_task(consume(client))
        await transport.packet_sent.wait()
        transport.feed(encode(SubAck(packet_id=packet_id, return_codes=(0x00,)), version="5.0"))
        await entered.wait()

        packets_before_cancellation = len(transport.sent)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=0.2)
        assert len(transport.sent) == packets_before_cancellation

        transport.packet_sent.clear()
        ping = asyncio.create_task(client.ping(timeout=0.2))
        await transport.packet_sent.wait()
        transport.feed(encode(PingResp(), version="5.0"))
        rtt = await ping

    assert rtt >= 0


async def test_stop_preserves_unsuback_error_when_observer_resubscribe_fails() -> None:
    """A failed observer replacement must preserve errors and filter ownership."""

    packet_id = 1
    allowed_filter = "allowed"
    denied_filter = "denied"
    reply_filter = "reply"
    transport = FakeTransport(
        feed=encode(ConnAck(session_present=False, return_code=0), version="5.0"),
    )

    async def factory(host: str, port: int, tls: ssl.SSLContext | bool | None) -> Transport:  # noqa: ARG001
        return transport

    client = MQTTClient("localhost", version="5.0", transport_factory=factory)
    subscription = client.subscribe(allowed_filter, denied_filter, reply_filter)

    async with client:
        transport.packet_sent.clear()
        started = asyncio.create_task(subscription.start())
        await transport.packet_sent.wait()
        transport.feed(encode(SubAck(packet_id=packet_id, return_codes=(0x00, 0x00, 0x00)), version="5.0"))
        await started
        protocol = client._protocol
        assert protocol is not None
        await protocol.add_response_observer(reply_filter)

        transport.packet_sent.clear()
        stopped = asyncio.create_task(subscription.stop())
        await transport.packet_sent.wait()
        transport.feed(encode(UnsubAck(packet_id=packet_id, reason_codes=(0x00, 0x87)), version="5.0"))
        transport.packet_sent.clear()
        await transport.packet_sent.wait()
        transport.feed(encode(SubAck(packet_id=packet_id, return_codes=(0x87,)), version="5.0"))
        with pytest.raises(ExceptionGroup) as exc_info:
            await stopped

        replacement = client.subscribe(allowed_filter)
        transport.packet_sent.clear()
        replacement_started = asyncio.create_task(replacement.start())
        await transport.packet_sent.wait()
        transport.feed(encode(SubAck(packet_id=packet_id, return_codes=(0x00,)), version="5.0"))
        await replacement_started

        transport.packet_sent.clear()
        retried = asyncio.create_task(subscription.stop())
        await transport.packet_sent.wait()
        transport.feed(encode(UnsubAck(packet_id=packet_id, reason_codes=(0x87,)), version="5.0"))
        with pytest.raises(MQTTUnsubscribeError):
            await retried

        transport.feed(
            encode(
                Publish(
                    topic=allowed_filter,
                    payload=b"replacement-still-active",
                    qos=QoS.AT_MOST_ONCE,
                    retain=False,
                    dup=False,
                ),
                version="5.0",
            ),
        )
        replacement_message = await asyncio.wait_for(replacement.get_message(), timeout=0.2)

    errors = exc_info.value.exceptions
    assert any(isinstance(error, MQTTUnsubscribeError) for error in errors)
    assert any(isinstance(error, MQTTSubscribeError) for error in errors)
    assert replacement_message.topic == allowed_filter
    assert replacement_message.payload == b"replacement-still-active"
