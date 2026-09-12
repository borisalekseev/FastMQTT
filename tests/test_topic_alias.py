"""Tests for MQTT 5 Topic Alias support (§3.3.2.3.4, issue #83)."""

import asyncio
import contextlib

import pytest

from zmqtt import MQTTTopicAliasError
from zmqtt._internal.packets.codec import decode, encode
from zmqtt._internal.packets.connect import ConnAck, Connect
from zmqtt._internal.packets.disconnect import Disconnect
from zmqtt._internal.packets.properties import (
    ConnAckProperties,
    ConnectProperties,
    PublishProperties,
)
from zmqtt._internal.packets.publish import Publish
from zmqtt._internal.protocol import MQTTProtocol
from zmqtt._internal.topic_aliases import (
    TopicAliasError,
    TopicAliasTable,
    resolve_incoming,
)
from zmqtt._internal.types.qos import QoS
from zmqtt.client import MQTTClient
from zmqtt.errors import MQTTProtocolError

from .test_client import FakeTransport as ClientFakeTransport
from .test_protocol import FakeTransport, _run_read_loop, _stop_task, make_protocol


def _feed_connack(
    transport: FakeTransport,
    *,
    topic_alias_maximum: int | None,
    maximum_qos: int | None = None,
) -> None:
    props = None
    if topic_alias_maximum is not None or maximum_qos is not None:
        props = ConnAckProperties(topic_alias_maximum=topic_alias_maximum, maximum_qos=maximum_qos)
    transport.feed(encode(ConnAck(session_present=False, return_code=0, properties=props), version="5.0"))


async def _connected_protocol(
    *, topic_alias_maximum: int | None = None, incoming_maximum: int = 0
) -> tuple[MQTTProtocol, FakeTransport]:
    protocol, transport = make_protocol(version="5.0", incoming_topic_alias_maximum=incoming_maximum)
    _feed_connack(transport, topic_alias_maximum=topic_alias_maximum)
    await protocol.connect(Connect(client_id="c", clean_session=True, keepalive=60))
    transport.sent.clear()
    return protocol, transport


def _alias_publish(alias: int, topic: str = "", qos: QoS = QoS.AT_MOST_ONCE) -> Publish:
    return Publish(
        topic=topic,
        payload=b"p",
        qos=qos,
        retain=False,
        dup=False,
        properties=PublishProperties(topic_alias=alias),
    )


# ---------------------------------------------------------------------------
# TopicAliasTable unit semantics


def test_table_set_get_roundtrip() -> None:
    table = TopicAliasTable(limit=5)
    table.set(3, "sensors/temp")
    assert table.get(3) == "sensors/temp"
    assert table.get(4) is None


def test_table_rebind_updates_topic() -> None:
    table = TopicAliasTable(limit=5)
    table.set(3, "a/b")
    table.set(3, "c/d")
    assert table.get(3) == "c/d"


def test_table_rejects_alias_above_limit() -> None:
    table = TopicAliasTable(limit=2)
    with pytest.raises(TopicAliasError):
        table.set(3, "a/b")


def test_table_rejects_alias_zero() -> None:
    table = TopicAliasTable(limit=5)
    with pytest.raises(TopicAliasError):
        table.set(0, "a/b")


def test_table_clear_forgets_bindings() -> None:
    table = TopicAliasTable(limit=5)
    table.set(1, "a")
    table.clear()
    assert table.get(1) is None


# ---------------------------------------------------------------------------
# resolve_incoming semantics


def test_resolve_incoming_registers_full_topic() -> None:
    table = TopicAliasTable(limit=5)
    assert resolve_incoming("sensors/temp", 2, table) == "sensors/temp"
    assert table.get(2) == "sensors/temp"


def test_resolve_incoming_resolves_alias_only() -> None:
    table = TopicAliasTable(limit=5)
    resolve_incoming("sensors/temp", 2, table)
    assert resolve_incoming("", 2, table) == "sensors/temp"


def test_resolve_incoming_unknown_alias_raises() -> None:
    table = TopicAliasTable(limit=10)
    with pytest.raises(TopicAliasError, match="unknown"):
        resolve_incoming("", 7, table)


def test_resolve_incoming_empty_topic_without_alias_raises() -> None:
    with pytest.raises(TopicAliasError, match="without a Topic Alias"):
        resolve_incoming("", None, TopicAliasTable(limit=5))


def test_resolve_incoming_alias_above_limit_raises() -> None:
    table = TopicAliasTable(limit=3)
    with pytest.raises(TopicAliasError, match="above our advertised"):
        resolve_incoming("t", 4, table)


# ---------------------------------------------------------------------------
# Protocol: CONNACK negotiation + reset on new connection


async def test_connack_without_alias_maximum_disables_outgoing() -> None:
    protocol, _transport = await _connected_protocol(topic_alias_maximum=None)
    assert protocol._outgoing_aliases.limit == 0
    with pytest.raises(MQTTTopicAliasError, match="Topic Alias Maximum"):
        await protocol.publish(_alias_publish(1, "a/b"))


async def test_connack_sets_outgoing_limit() -> None:
    protocol, _ = await _connected_protocol(topic_alias_maximum=10)
    assert protocol._outgoing_aliases.limit == 10
    await protocol.publish(_alias_publish(10, "a/b"))
    assert protocol._outgoing_aliases.get(10) == "a/b"


async def test_outgoing_alias_above_server_limit_raises() -> None:
    protocol, _ = await _connected_protocol(topic_alias_maximum=2)
    with pytest.raises(MQTTTopicAliasError, match="exceeds"):
        await protocol.publish(_alias_publish(3, "a/b"))


async def test_outgoing_alias_zero_raises() -> None:
    protocol, _ = await _connected_protocol(topic_alias_maximum=5)
    with pytest.raises(MQTTTopicAliasError, match=r"1\.\.65535"):
        await protocol.publish(_alias_publish(0, "a/b"))


async def test_alias_only_publish_resolves_registered_alias() -> None:
    protocol, transport = await _connected_protocol(topic_alias_maximum=5)
    read_task = await _run_read_loop(protocol)
    try:
        # Register alias 2 with a full topic, then reuse it with an empty topic.
        await protocol.publish(_alias_publish(2, "sensors/temp"))
        transport.sent.clear()
        task = asyncio.create_task(protocol.publish(_alias_publish(2, "")))
        await asyncio.sleep(0)
        assert len(transport.sent) == 1
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    finally:
        await _stop_task(read_task)


async def test_alias_only_wire_form_keeps_topic_empty() -> None:
    # §3.3.2.3.4 payload saving: the wire PUBLISH must carry the alias and an
    # EMPTY Topic Name — the resolved topic must not leak back onto the wire.
    protocol, transport = await _connected_protocol(topic_alias_maximum=5)
    await protocol.publish(_alias_publish(2, "sensors/temp"))
    transport.sent.clear()
    await protocol.publish(_alias_publish(2, ""))
    assert len(transport.sent) == 1
    decoded = decode(transport.sent[0], version="5.0")
    assert decoded is not None
    wire, _consumed = decoded
    assert isinstance(wire, Publish)
    assert wire.topic == ""
    assert wire.properties is not None
    assert wire.properties.topic_alias == 2


async def test_alias_only_publish_unknown_alias_raises() -> None:
    protocol, _ = await _connected_protocol(topic_alias_maximum=5)
    with pytest.raises(MQTTTopicAliasError, match="unregistered"):
        await protocol.publish(_alias_publish(2, ""))


# ---------------------------------------------------------------------------
# Protocol: incoming resolution


def _feed_incoming_publish(transport: FakeTransport, packet: Publish) -> None:
    transport.feed(encode(packet, version="5.0"))


async def test_incoming_alias_only_is_resolved_before_routing() -> None:
    protocol, transport = await _connected_protocol(topic_alias_maximum=5, incoming_maximum=5)
    # Establish alias 4 with a full topic from the peer.
    _feed_incoming_publish(transport, _alias_publish(4, "sensors/temp"))
    # Alias-only follow-up resolves to the full topic.
    _feed_incoming_publish(transport, _alias_publish(4, ""))
    read_task = await _run_read_loop(protocol)
    try:
        await asyncio.sleep(0.05)
        # No protocol error; subscription routing would see sensors/temp.
    finally:
        await _stop_task(read_task)
    assert protocol._incoming_aliases.get(4) == "sensors/temp"


async def test_incoming_alias_above_advertised_maximum_is_protocol_error() -> None:
    protocol, transport = await _connected_protocol(topic_alias_maximum=5, incoming_maximum=3)
    read_task = await _run_read_loop(protocol)
    _feed_incoming_publish(transport, _alias_publish(4, "t"))
    with pytest.raises(MQTTProtocolError, match="above our advertised"):
        await asyncio.wait_for(read_task, timeout=2)
    # §3.14.2.2.1: we told the peer why — DISCONNECT 0x82 went out first.
    assert transport.sent, "expected a DISCONNECT before the teardown"
    decoded = decode(transport.sent[-1], version="5.0")
    assert decoded is not None
    wire, _consumed = decoded
    assert isinstance(wire, Disconnect)
    assert wire.reason_code == 0x82


async def test_incoming_unknown_alias_only_is_protocol_error() -> None:
    protocol, transport = await _connected_protocol(topic_alias_maximum=5, incoming_maximum=10)
    read_task = await _run_read_loop(protocol)
    _feed_incoming_publish(transport, _alias_publish(9, ""))
    with pytest.raises(MQTTProtocolError, match="unknown Topic Alias"):
        await asyncio.wait_for(read_task, timeout=2)


async def test_new_connection_clears_alias_state() -> None:
    # Two CONNACKs through ONE protocol object (the reconnect path builds a
    # new MQTTProtocol each attempt, but _await_connack must still reset the
    # tables defensively — resumed sessions included).
    protocol, transport = make_protocol(version="5.0")
    _feed_connack(transport, topic_alias_maximum=5)
    await protocol.connect(Connect(client_id="c", clean_session=True, keepalive=60))
    await protocol.publish(_alias_publish(2, "sensors/temp"))
    assert protocol._outgoing_aliases.get(2) == "sensors/temp"

    # Second handshake on the same object: broker grants a different limit.
    _feed_connack(transport, topic_alias_maximum=3)
    await protocol.connect(Connect(client_id="c", clean_session=True, keepalive=60))
    assert protocol._outgoing_aliases.get(2) is None
    assert protocol._outgoing_aliases.limit == 3
    assert protocol._incoming_aliases.get(2) is None


async def test_incoming_disabled_by_default_zero_limit() -> None:
    # incoming_maximum defaults to 0: any alias from the server is a violation.
    protocol, transport = await _connected_protocol(topic_alias_maximum=5)
    read_task = await _run_read_loop(protocol)
    _feed_incoming_publish(transport, _alias_publish(1, "t"))
    with pytest.raises(MQTTProtocolError, match="above our advertised"):
        await asyncio.wait_for(read_task, timeout=2)


# ---------------------------------------------------------------------------
# Client plumbing


async def test_client_advertises_topic_alias_maximum_in_connect() -> None:
    captured_packet: dict[str, ConnectProperties | None] = {}

    original_connect = MQTTProtocol.connect

    async def _capture_connect(self: MQTTProtocol, packet: Connect) -> ConnAck:
        captured_packet["props"] = packet.properties
        return await original_connect(self, packet)

    MQTTProtocol.connect = _capture_connect  # type: ignore[method-assign]
    try:

        async def factory(host: str, port: int, tls: object) -> ClientFakeTransport:  # noqa: ARG001
            return ClientFakeTransport(
                feed=encode(
                    ConnAck(session_present=False, return_code=0, properties=ConnAckProperties()),
                    version="5.0",
                )
            )

        client = MQTTClient(
            "h", transport_factory=factory, version="5.0", topic_alias_maximum=7, mqtt_connect_timeout=1.0
        )
        await client._connect()
    finally:
        MQTTProtocol.connect = original_connect  # type: ignore[method-assign]
    props = captured_packet["props"]
    assert props is not None
    assert props.topic_alias_maximum == 7
