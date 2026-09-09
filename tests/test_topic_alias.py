"""Outgoing Topic Alias validation through the public client and real codec."""

import asyncio
import ssl

import pytest

from tests.test_connection_info import transport_factory
from tests.test_protocol import FakeTransport
from zmqtt import ConnAckProperties, MQTTClient, MQTTDisconnectedError, PublishProperties, QoS, ReconnectConfig
from zmqtt._internal.packets.codec import encode
from zmqtt._internal.packets.connect import ConnAck
from zmqtt._internal.packets.publish import Publish
from zmqtt._internal.packets.reader import PacketBuffer
from zmqtt._internal.transport.base import Transport


def feed_connack(transport: FakeTransport, properties: ConnAckProperties | None, *, resumed: bool = False) -> None:
    transport.feed(encode(ConnAck(session_present=resumed, return_code=0, properties=properties), version="5.0"))


@pytest.mark.parametrize("alias", [-1, 0, 65_536])
@pytest.mark.parametrize("qos", list(QoS))
async def test_rejects_alias_outside_protocol_range(alias: int, qos: QoS) -> None:
    transport = FakeTransport()
    feed_connack(transport, ConnAckProperties(topic_alias_maximum=65_535))
    async with MQTTClient("localhost", version="5.0", transport_factory=transport_factory(transport)) as client:
        sent = list(transport.sent)
        with pytest.raises(ValueError, match="topic_alias must be between 1 and 65535"):
            await asyncio.wait_for(
                client.publish("test/topic", b"payload", qos=qos, properties=PublishProperties(topic_alias=alias)),
                timeout=1,
            )
        assert transport.sent == sent
        assert client._protocol is not None
        assert not client._protocol._state.inflight_qos1
        assert not client._protocol._state.inflight_qos2_out


@pytest.mark.parametrize(
    ("properties", "alias"),
    [
        (None, 1),
        (ConnAckProperties(reason_string="welcome"), 1),
        (ConnAckProperties(topic_alias_maximum=0), 1),
        (ConnAckProperties(topic_alias_maximum=3), 4),
    ],
)
@pytest.mark.parametrize("qos", list(QoS))
async def test_rejects_alias_exceeding_server_limit(properties: ConnAckProperties | None, alias: int, qos: QoS) -> None:
    transport = FakeTransport()
    feed_connack(transport, properties)
    async with MQTTClient("localhost", version="5.0", transport_factory=transport_factory(transport)) as client:
        sent = list(transport.sent)
        with pytest.raises(ValueError, match="exceeds server Topic Alias Maximum"):
            await asyncio.wait_for(
                client.publish("test/topic", b"payload", qos=qos, properties=PublishProperties(topic_alias=alias)),
                timeout=1,
            )
        assert transport.sent == sent


@pytest.mark.parametrize(("maximum", "alias"), [(1, 1), (3, 1), (3, 3), (65_535, 65_535)])
async def test_accepts_alias_within_server_limit(maximum: int, alias: int) -> None:
    transport = FakeTransport()
    feed_connack(transport, ConnAckProperties(topic_alias_maximum=maximum))
    properties = PublishProperties(topic_alias=alias)
    async with MQTTClient("localhost", version="5.0", transport_factory=transport_factory(transport)) as client:
        await client.publish("test/topic", "payload", properties=properties)
        buffer = PacketBuffer(version="5.0")
        buffer.feed(transport.sent[-1])
        assert list(buffer) == [
            Publish(
                topic="test/topic",
                payload=b"payload",
                qos=QoS.AT_MOST_ONCE,
                retain=False,
                dup=False,
                properties=properties,
            )
        ]


@pytest.mark.parametrize("maximum", [None, 0, 3])
@pytest.mark.parametrize("properties", [None, PublishProperties(content_type="text/plain")])
async def test_publication_without_alias_is_independent_of_server_limit(
    maximum: int | None, properties: PublishProperties | None
) -> None:
    transport = FakeTransport()
    feed_connack(transport, ConnAckProperties(topic_alias_maximum=maximum))
    async with MQTTClient("localhost", version="5.0", transport_factory=transport_factory(transport)) as client:
        await client.publish("test/topic", b"payload", properties=properties)
        buffer = PacketBuffer(version="5.0")
        buffer.feed(transport.sent[-1])
        assert list(buffer) == [
            Publish(
                topic="test/topic",
                payload=b"payload",
                qos=QoS.AT_MOST_ONCE,
                retain=False,
                dup=False,
                properties=properties,
            )
        ]


async def test_mqtt_v311_still_rejects_publish_properties() -> None:
    transport = FakeTransport()
    transport.feed(encode(ConnAck(session_present=False, return_code=0), version="3.1.1"))
    async with MQTTClient("localhost", transport_factory=transport_factory(transport)) as client:
        sent = list(transport.sent)
        with pytest.raises(RuntimeError, match=r"properties require MQTT 5\.0"):
            await client.publish("test/topic", b"payload", properties=PublishProperties(topic_alias=1))
        assert transport.sent == sent
        await client.publish("test/topic", b"without alias")


@pytest.mark.parametrize("maximum", [None, 0, 1, 5])
@pytest.mark.parametrize("resumed", [False, True])
async def test_reconnect_uses_new_alias_limit(maximum: int | None, resumed: bool) -> None:
    first, second = FakeTransport(), FakeTransport()
    feed_connack(first, ConnAckProperties(topic_alias_maximum=3))
    feed_connack(second, ConnAckProperties(topic_alias_maximum=maximum), resumed=resumed)
    transports = iter([first, second])
    reconnecting = asyncio.Event()
    allow_reconnect = asyncio.Event()

    async def factory(host: str, port: int, tls: ssl.SSLContext | bool | None) -> Transport:  # noqa: ARG001
        transport = next(transports)
        if transport is second:
            reconnecting.set()
            await allow_reconnect.wait()
        return transport

    client = MQTTClient(
        "localhost",
        version="5.0",
        clean_session=False,
        session_expiry_interval=60,
        transport_factory=factory,
        reconnect=ReconnectConfig(initial_delay=0),
    )
    async with client:
        properties = PublishProperties(topic_alias=3)
        await client.publish("test/topic", b"first", properties=properties)
        first._rx.append(MQTTDisconnectedError("lost"))
        await asyncio.wait_for(reconnecting.wait(), timeout=1)
        sent = list(first.sent)
        with pytest.raises(MQTTDisconnectedError):
            await client.publish("test/topic", b"during reconnect", properties=properties)
        assert first.sent == sent
        allow_reconnect.set()

        async def wait_for_reconnect() -> None:
            while client._connection_info is None or client._connection_info.connection_id != 2:  # noqa: ASYNC110
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_reconnect(), timeout=1)
        if maximum is None or maximum < 3:
            sent = list(second.sent)
            with pytest.raises(ValueError, match="exceeds server Topic Alias Maximum"):
                await client.publish("test/topic", b"second", properties=properties)
            assert second.sent == sent
        if maximum:
            await client.publish("test/topic", b"second", properties=PublishProperties(topic_alias=maximum))
        await client.publish("test/topic", b"without alias")
