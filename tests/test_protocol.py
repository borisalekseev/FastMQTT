"""Tests for zmqtt.protocol — pure-logic and error-path tests only.

E2E observable behavior (QoS flows, subscribe/receive, ack) lives in
tests/test_brokers/_base.py and runs against real brokers.
"""

import asyncio
import contextlib
import logging
from collections import deque
from typing import Literal

import pytest

from zmqtt._internal.packets.codec import encode
from zmqtt._internal.packets.connect import ConnAck, Connect
from zmqtt._internal.packets.disconnect import Disconnect
from zmqtt._internal.packets.properties import PublishProperties
from zmqtt._internal.packets.publish import PubAck, Publish, PubRec, PubRel
from zmqtt._internal.packets.reader import PacketBuffer
from zmqtt._internal.packets.subscribe import SubAck, Subscribe, SubscriptionRequest
from zmqtt._internal.protocol import (
    _PUBLISH_REASON_NAMES,
    MQTTProtocol,
    _raise_on_rejected_filters,
)
from zmqtt._internal.state import SessionState
from zmqtt._internal.subscription_index import SubscriptionEntry
from zmqtt._internal.topic_matching import _DEFAULT_STRIPPED_PREFIXES, _shared_filter_to_actual
from zmqtt._internal.types.message import Message
from zmqtt._internal.types.qos import QoS
from zmqtt.errors import (
    MQTTConnectError,
    MQTTDisconnectedError,
    MQTTProtocolError,
    MQTTPublishError,
    MQTTSubscribeError,
    MQTTTimeoutError,
)

PUBLISH_FAILURE_CODES = [code for code in _PUBLISH_REASON_NAMES if code >= 0x80]


class FakeTransport:
    """In-memory transport: read() drains from rx_queue; write() appends to sent."""

    def __init__(self) -> None:
        self.sent: list[bytes] = []
        self._rx: deque[bytes | Exception] = deque()
        self._closed = False

    def feed(self, data: bytes) -> None:
        self._rx.append(data)

    async def read(self, n: int) -> bytes:  # noqa: ARG002
        while not self._rx:  # noqa: ASYNC110
            await asyncio.sleep(0)
        item = self._rx.popleft()
        if isinstance(item, Exception):
            raise item
        return item

    async def write(self, data: bytes) -> None:
        self.sent.append(data)

    async def close(self) -> None:
        self._closed = True

    @property
    def is_connected(self) -> bool:
        return not self._closed


class FakeRequestClaim:
    def __init__(self, message: Message, observed: list[Message]) -> None:
        self._message = message
        self._observed = observed

    def deliver(self) -> None:
        self._observed.append(self._message)


class FakeRequestRouter:
    def __init__(
        self,
        observed: list[Message],
        correlation_data: bytes | None = None,
    ) -> None:
        self._observed = observed
        self._correlation_data = correlation_data

    def claim(self, message: Message) -> FakeRequestClaim | None:
        properties = message.properties
        if self._correlation_data is not None and (
            properties is None or properties.correlation_data != self._correlation_data
        ):
            return None
        return FakeRequestClaim(message, self._observed)


def make_protocol(
    keepalive: int = 60,
    ping_timeout: float = 5.0,
    stripped_prefixes: tuple[str, ...] = _DEFAULT_STRIPPED_PREFIXES,
    version: Literal["3.1.1", "5.0"] = "3.1.1",
) -> tuple[MQTTProtocol, FakeTransport]:
    transport = FakeTransport()
    state = SessionState()
    protocol = MQTTProtocol(
        transport,
        state,
        keepalive=keepalive,
        ping_timeout=ping_timeout,
        stripped_prefixes=stripped_prefixes,
        version=version,
    )
    return protocol, transport


async def _run_read_loop(protocol: MQTTProtocol) -> asyncio.Task[None]:
    task: asyncio.Task[None] = asyncio.create_task(protocol._read_loop())
    await asyncio.sleep(0)
    return task


async def _stop_task(task: asyncio.Task[None]) -> None:
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def test_connect_refused_raises() -> None:
    protocol, transport = make_protocol()
    connect = Connect(client_id="test", clean_session=True, keepalive=60)
    transport.feed(
        encode(ConnAck(session_present=False, return_code=4), version="3.1.1"),
    )
    with pytest.raises(MQTTConnectError) as exc_info:
        await protocol.connect(connect)
    assert exc_info.value.return_code == 4


async def test_connect_wrong_packet_raises() -> None:
    protocol, transport = make_protocol()
    connect = Connect(client_id="test", clean_session=True, keepalive=60)
    transport.feed(encode(PubAck(packet_id=1), version="3.1.1"))
    with pytest.raises(MQTTProtocolError):
        await protocol.connect(connect)


async def test_connect_timeout_raises() -> None:
    transport = FakeTransport()
    protocol = MQTTProtocol(transport, SessionState(), connect_timeout=0.05)
    connect = Connect(client_id="test", clean_session=True, keepalive=60)
    # Transport is never fed -> CONNACK never arrives -> the bounded wait fires.
    with pytest.raises(MQTTTimeoutError):
        await protocol.connect(connect)


async def test_connect_succeeds_within_timeout() -> None:
    transport = FakeTransport()
    protocol = MQTTProtocol(transport, SessionState(), connect_timeout=5.0)
    connect = Connect(client_id="test", clean_session=True, keepalive=60)
    transport.feed(encode(ConnAck(session_present=False, return_code=0), version="3.1.1"))
    ack = await protocol.connect(connect)
    assert isinstance(ack, ConnAck)
    assert ack.return_code == 0
    assert ack.session_present is False


async def test_ping_timeout_raises() -> None:
    protocol, _ = make_protocol(keepalive=0, ping_timeout=0.05)
    ping_task = asyncio.create_task(protocol._ping_loop())
    with pytest.raises(MQTTTimeoutError):
        await ping_task


async def test_deliver_no_match_logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    """A response-topic interest does not suppress the no-recipient warning."""
    protocol, _ = make_protocol()
    protocol._state.subscriptions.add_response_observer("unknown/topic")

    with caplog.at_level(logging.WARNING, logger="zmqtt.protocol"):
        await protocol._handle_publish(
            Publish(
                topic="unknown/topic",
                payload=b"x",
                qos=QoS.AT_MOST_ONCE,
                retain=False,
                dup=False,
            ),
        )

    assert "unknown/topic" in caplog.text


async def test_stripped_prefix_filter_receives_messages() -> None:
    """A group-less decorator ($queue, $exclusive, ...) is stripped for matching.

    The broker delivers on the real topic, so without stripping the prefix every
    message is dropped with "No subscriber".
    """
    protocol, _ = make_protocol()
    entry = SubscriptionEntry(
        queue=asyncio.Queue(),
        actual_filter=_shared_filter_to_actual("$queue/sensors/+/state", _DEFAULT_STRIPPED_PREFIXES),
    )
    protocol._state.subscriptions.add("$queue/sensors/+/state", entry)

    await protocol._handle_publish(
        Publish(
            topic="sensors/dev-1/state",
            payload=b"x",
            qos=QoS.AT_MOST_ONCE,
            retain=False,
            dup=False,
        ),
    )

    assert entry.queue.qsize() == 1


def test_shared_prefixes_are_stripped_for_matching() -> None:
    strip = _DEFAULT_STRIPPED_PREFIXES
    # $share carries a group; the group-less decorators do not.
    assert _shared_filter_to_actual("$share/group/a/+/b", strip) == "a/+/b"
    assert _shared_filter_to_actual("$queue/a/+/b", strip) == "a/+/b"
    assert _shared_filter_to_actual("$exclusive/a/+/b", strip) == "a/+/b"
    assert _shared_filter_to_actual("a/+/b", strip) == "a/+/b"
    # A malformed $share prefix is left as-is rather than guessed at.
    assert _shared_filter_to_actual("$share/only-group", strip) == "$share/only-group"


def test_unknown_prefixes_are_not_stripped() -> None:
    """The allowlist fails safe: a real namespace or an unconfigured decorator is
    left untouched (a loud "No subscriber" beats a silent mis-route), until the
    broker's decorator is added to the allowlist.
    """
    strip = _DEFAULT_STRIPPED_PREFIXES
    assert _shared_filter_to_actual("$SYS/#", strip) == "$SYS/#"
    assert _shared_filter_to_actual("$q/a/b", strip) == "$q/a/b"
    assert _shared_filter_to_actual("$q/a/b", ("$q",)) == "a/b"


async def test_configured_prefix_reaches_subscribe() -> None:
    """A prefix added via stripped_prefixes is applied to the stored actual_filter."""
    protocol, transport = make_protocol(stripped_prefixes=("$q",))

    async def subscribe() -> None:
        await protocol.subscribe(
            [SubscriptionRequest(topic_filter="$q/sensors/+/state", qos=QoS.AT_MOST_ONCE)],
            queue=asyncio.Queue(),
        )

    task = asyncio.create_task(subscribe())
    await asyncio.sleep(0)
    transport.feed(encode(SubAck(packet_id=1, return_codes=(0x00,)), version="3.1.1"))
    read = await _run_read_loop(protocol)
    await task
    await _stop_task(read)

    entry = protocol._state.subscriptions.get("$q/sensors/+/state")
    assert entry is not None
    assert entry.actual_filter == "sensors/+/state"


async def test_subscription_identifier_routes_delivery() -> None:
    """The broker's echoed identifier picks the exact subscription that matched.

    Two overlapping filters — a $share subscription and its plain twin — normalise
    to the same actual filter, so filter matching alone cannot tell them apart:
    one used to receive everything (both broker copies) and the other starved.
    """
    protocol, _ = make_protocol()
    shared = SubscriptionEntry(
        queue=asyncio.Queue(),
        actual_filter="demo/+/state",
        subscription_identifier=1,
    )
    plain = SubscriptionEntry(
        queue=asyncio.Queue(),
        actual_filter="demo/+/state",
        subscription_identifier=2,
    )
    protocol._state.subscriptions.add("$share/g/demo/+/state", shared)
    protocol._state.subscriptions.add("demo/+/state", plain)

    for echoed, entry in ((1, shared), (2, plain)):
        await protocol._handle_publish(
            Publish(
                topic="demo/dev-1/state",
                payload=b"x",
                qos=QoS.AT_MOST_ONCE,
                retain=False,
                dup=False,
                properties=PublishProperties(subscription_identifier=echoed),
            ),
        )
        assert entry.queue.qsize() == 1

    assert shared.queue.qsize() == 1
    assert plain.queue.qsize() == 1


async def test_unknown_subscription_identifier_falls_back_to_topic_match(
    caplog: pytest.LogCaptureFixture,
) -> None:
    protocol, _ = make_protocol()
    entry = SubscriptionEntry(queue=asyncio.Queue(), actual_filter="demo/#")
    protocol._state.subscriptions.add("demo/#", entry)

    with caplog.at_level(logging.WARNING):
        await protocol._handle_publish(
            Publish(
                topic="demo/device/state",
                payload=b"x",
                qos=QoS.AT_MOST_ONCE,
                retain=False,
                dup=False,
                properties=PublishProperties(subscription_identifier=999),
            ),
        )

    assert entry.queue.qsize() == 1
    assert "identifier 999" in caplog.text


async def test_matching_response_bypasses_regular_subscription() -> None:
    response_topic = "demo/responses"
    state = SessionState()
    observed: list[Message] = []

    protocol = MQTTProtocol(
        FakeTransport(),
        state,
        version="5.0",
        request_router=FakeRequestRouter(observed, b"expected"),
    )
    regular = SubscriptionEntry(queue=asyncio.Queue(), actual_filter=response_topic)
    state.subscriptions.add(response_topic, regular)
    state.subscriptions.add_response_observer(response_topic)

    def response(payload: bytes, correlation_data: bytes) -> Publish:
        return Publish(
            topic=response_topic,
            payload=payload,
            qos=QoS.AT_MOST_ONCE,
            retain=False,
            dup=False,
            properties=PublishProperties(correlation_data=correlation_data),
        )

    await protocol._handle_publish(response(b"matched", b"expected"))
    await protocol._handle_publish(response(b"unmatched", b"unknown"))

    assert [message.payload for message in observed] == [b"matched"]
    assert (await regular.queue.get()).payload == b"unmatched"
    assert regular.queue.empty()


def test_recipient_selects_auto_ack_policy() -> None:
    protocol, _ = make_protocol()
    automatic = SubscriptionEntry(
        queue=asyncio.Queue(),
        auto_ack=True,
        actual_filter="demo/+/state",
        subscription_identifier=1,
    )
    manual = SubscriptionEntry(
        queue=asyncio.Queue(),
        auto_ack=False,
        actual_filter="demo/+/state",
        subscription_identifier=2,
    )
    protocol._state.subscriptions.add("$share/g/demo/+/state", automatic)
    protocol._state.subscriptions.add("demo/+/state", manual)

    def publish(identifier: int) -> Publish:
        return Publish(
            topic="demo/device/state",
            payload=b"x",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            dup=False,
            packet_id=1,
            properties=PublishProperties(subscription_identifier=identifier),
        )

    assert protocol._select_recipient(publish(1)).auto_ack
    assert not protocol._select_recipient(publish(2)).auto_ack

    response_protocol = MQTTProtocol(
        FakeTransport(),
        protocol._state,
        request_router=FakeRequestRouter([]),
    )
    assert response_protocol._select_recipient(publish(2)).auto_ack


async def test_multi_filter_subscription_delivers_once_per_publish() -> None:
    """One subscribe() with MANY filters is ONE subscription: one identifier, one
    per-filter entry (each with its own relay queue, all draining into the same
    application queue). A broker PUBLISH echoing that identifier used to be
    enqueued once PER ENTRY — 20 filters meant 20 duplicates of every message
    (found live: a device ack recorded 20 times per command). It must land in
    exactly ONE entry: the one whose filter matches the topic."""
    protocol, _ = make_protocol()
    filters = ["demo/+/server/state", "demo/+/server/status", "demo/+/server/state/ack"]
    entries = {}
    for topic_filter in filters:
        entry = SubscriptionEntry(
            queue=asyncio.Queue(),
            actual_filter=topic_filter,
            subscription_identifier=1,
        )
        entries[topic_filter] = entry
        protocol._state.subscriptions.add(f"$share/g/{topic_filter}", entry)

    await protocol._handle_publish(
        Publish(
            topic="demo/dev-1/server/state/ack",
            payload=b"x",
            qos=QoS.AT_MOST_ONCE,
            retain=False,
            dup=False,
            properties=PublishProperties(subscription_identifier=1),
        ),
    )
    sizes = {f: e.queue.qsize() for f, e in entries.items()}
    assert sum(sizes.values()) == 1  # once per subscription, not once per filter
    assert sizes["demo/+/server/state/ack"] == 1  # and via the filter that matched


async def test_subscribe_sends_identifier_in_properties() -> None:
    """The identifier must actually go out on the wire in the SUBSCRIBE packet."""
    protocol, transport = make_protocol(version="5.0")

    async def subscribe() -> None:
        await protocol.subscribe(
            [SubscriptionRequest(topic_filter="a/b", qos=QoS.AT_LEAST_ONCE)],
            queue=asyncio.Queue(),
            subscription_identifier=7,
        )

    task = asyncio.create_task(subscribe())
    await asyncio.sleep(0)
    transport.feed(encode(SubAck(packet_id=1, return_codes=(0x01,)), version="5.0"))
    read = await _run_read_loop(protocol)
    await task
    await _stop_task(read)

    buf = PacketBuffer(version="5.0")
    buf.feed(transport.sent[0])
    (packet,) = list(buf)
    assert isinstance(packet, Subscribe)
    assert packet.properties is not None
    assert packet.properties.subscription_identifier == 7
    entry = protocol._state.subscriptions.get("a/b")
    assert entry is not None
    assert entry.subscription_identifier == 7
    assert protocol._state.subscriptions.by_identifier(7) == [("a/b", entry)]


async def test_broker_disconnect_is_a_disconnection() -> None:
    """A broker-initiated DISCONNECT must take the reconnect path.

    Brokers send it on session takeover (same client id — every rolling deploy),
    keepalive timeout, admin kick and rate limiting. Raised as a protocol error it
    killed the run task silently: iterators hung forever and nothing reconnected.
    """
    protocol, transport = make_protocol()

    transport.feed(encode(Disconnect(reason_code=0x8E), version="3.1.1"))

    with pytest.raises(MQTTDisconnectedError):
        await asyncio.wait_for(protocol._read_loop(), timeout=1)


async def test_dead_protocol_refuses_new_operations() -> None:
    """Operations after the run loop died must fail fast, not hang.

    _cancel_pending() fails futures that already exist; an unsubscribe() issued
    afterwards (e.g. Subscription.__aexit__ on a dead client) used to create a
    fresh future that nothing would ever resolve.
    """
    protocol, _ = make_protocol()
    protocol._dead = True

    with pytest.raises(MQTTDisconnectedError):
        await protocol.unsubscribe(["a/b"])

    with pytest.raises(MQTTDisconnectedError):
        await protocol.subscribe(
            [SubscriptionRequest(topic_filter="a/b", qos=QoS.AT_MOST_ONCE)],
            queue=asyncio.Queue(),
        )


def test_suback_failure_codes_raise() -> None:
    """A rejected filter (SUBACK >= 0x80, e.g. an ACL denial) must not look successful."""
    filters = [
        SubscriptionRequest(topic_filter="allowed/topic", qos=QoS.AT_LEAST_ONCE),
        SubscriptionRequest(topic_filter="$SYS/#", qos=QoS.AT_LEAST_ONCE),
    ]
    suback = SubAck(packet_id=1, return_codes=(0x01, 0x80))

    with pytest.raises(MQTTSubscribeError) as exc_info:
        _raise_on_rejected_filters(filters, suback)

    assert exc_info.value.failures == {"$SYS/#": 0x80}


def test_suback_granted_codes_pass() -> None:
    filters = [SubscriptionRequest(topic_filter="a/b", qos=QoS.EXACTLY_ONCE)]
    suback = SubAck(packet_id=1, return_codes=(0x02,))

    _raise_on_rejected_filters(filters, suback)  # no raise


async def test_suback_failure_rolls_back_subscription_index() -> None:
    protocol, transport = make_protocol(version="5.0")

    task = asyncio.create_task(
        protocol.subscribe(
            [SubscriptionRequest(topic_filter="denied/topic", qos=QoS.AT_LEAST_ONCE)],
            queue=asyncio.Queue(),
            subscription_identifier=7,
        ),
    )
    await asyncio.sleep(0)
    transport.feed(encode(SubAck(packet_id=1, return_codes=(0x87,)), version="5.0"))
    read = await _run_read_loop(protocol)

    with pytest.raises(MQTTSubscribeError):
        await task
    await _stop_task(read)

    assert not protocol._state.subscriptions.contains("denied/topic")
    assert protocol._state.subscriptions.by_identifier(7) == []


async def test_inbound_qos2_manual_ack_duplicate_ignored() -> None:
    """Broker retransmit before app calls ack() must not re-queue the message."""
    protocol, transport = make_protocol()
    transport.feed(
        encode(ConnAck(session_present=False, return_code=0), version="3.1.1"),
    )
    await protocol.connect(Connect(client_id="c", clean_session=True, keepalive=60))

    queue: asyncio.Queue[Message] = asyncio.Queue()
    protocol._state.subscriptions.add(
        "t/#",
        SubscriptionEntry(
            queue=queue,
            auto_ack=False,
            actual_filter="t/#",
        ),
    )
    transport.sent.clear()

    publish = Publish(
        topic="t/x",
        payload=b"once",
        qos=QoS.EXACTLY_ONCE,
        retain=False,
        dup=False,
        packet_id=11,
    )
    transport.feed(encode(publish, version="3.1.1"))
    read_task = await _run_read_loop(protocol)
    message = await asyncio.wait_for(queue.get(), timeout=1.0)

    # Broker retransmits PUBLISH before app calls ack()
    transport.feed(
        encode(
            Publish(
                topic="t/x",
                payload=b"once",
                qos=QoS.EXACTLY_ONCE,
                retain=False,
                dup=True,
                packet_id=11,
            ),
            version="3.1.1",
        ),
    )
    await asyncio.sleep(0.05)

    assert queue.empty()
    assert transport.sent == []  # no PUBREC for duplicate

    await message.ack()
    await protocol._handle_pubrel(PubRel(packet_id=11))

    assert queue.empty()

    await _stop_task(read_task)


@pytest.mark.parametrize("reason_code", PUBLISH_FAILURE_CODES)
async def test_publish_qos2_rejected_pubrec_raises_no_pubrel_and_releases_packet_id(reason_code: int) -> None:
    """publish() raises MQTTPublishError on a rejected PUBREC, sends no PUBREL, and frees the packet_id."""
    protocol, transport = make_protocol(version="5.0")
    transport.feed(encode(ConnAck(session_present=False, return_code=0), version="5.0"))
    await protocol.connect(Connect(client_id="c", clean_session=True, keepalive=60))
    transport.sent.clear()
    read_task = await _run_read_loop(protocol)
    publish_task = asyncio.create_task(
        protocol.publish(
            Publish(topic="t/x", payload=b"payload", qos=QoS.EXACTLY_ONCE, retain=False, dup=False, packet_id=0),
        ),
    )
    await asyncio.sleep(0)
    pid = next(iter(protocol._state.inflight_qos2_out))
    sent_before_ack = len(transport.sent)

    transport.feed(encode(PubRec(packet_id=pid, reason_code=reason_code), version="5.0"))
    with pytest.raises(MQTTPublishError) as exc_info:
        await publish_task

    assert exc_info.value.reason_code == reason_code
    assert exc_info.value.reason_name == _PUBLISH_REASON_NAMES[reason_code]
    assert pid not in protocol._state.inflight_qos2_out
    assert len(transport.sent) == sent_before_ack  # no PUBREL sent
    assert protocol._state.packet_ids.acquire() == pid  # proves release: id is reused
    await _stop_task(read_task)
