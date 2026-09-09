"""Tests for MQTT 5 Maximum QoS enforcement (server limit from CONNACK)."""

import asyncio
import contextlib

import pytest

from zmqtt import MQTTQoSExceededError
from zmqtt._internal.packets.codec import encode
from zmqtt._internal.packets.connect import ConnAck, Connect
from zmqtt._internal.packets.properties import ConnAckProperties
from zmqtt._internal.packets.publish import Publish
from zmqtt._internal.protocol import MQTTProtocol
from zmqtt._internal.types.qos import QoS
from zmqtt.errors import MQTTPublishError

from .test_protocol import FakeTransport, _run_read_loop, _stop_task, make_protocol


def _feed_connack(transport: FakeTransport, *, maximum_qos: int | None) -> None:
    properties = ConnAckProperties(maximum_qos=maximum_qos) if maximum_qos is not None else None
    transport.feed(encode(ConnAck(session_present=False, return_code=0, properties=properties), version="5.0"))


async def _connected_protocol(maximum_qos: int | None) -> tuple[MQTTProtocol, FakeTransport]:
    protocol, transport = make_protocol(version="5.0")
    _feed_connack(transport, maximum_qos=maximum_qos)
    await protocol.connect(Connect(client_id="c", clean_session=True, keepalive=60))
    transport.sent.clear()
    return protocol, transport


@pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
async def test_publish_above_maximum_qos_raises_before_send(qos: QoS) -> None:
    protocol, transport = await _connected_protocol(maximum_qos=0)

    with pytest.raises(MQTTQoSExceededError) as exc_info:
        await protocol.publish(
            Publish(topic="t/x", payload=b"p", qos=qos, retain=False, dup=False, packet_id=0),
        )

    assert exc_info.value.requested_qos == int(qos)
    assert exc_info.value.maximum_qos == 0
    assert transport.sent == []  # nothing left the wire


async def test_publish_qos2_above_maximum_qos_1_raises() -> None:
    protocol, transport = await _connected_protocol(maximum_qos=1)

    with pytest.raises(MQTTQoSExceededError):
        await protocol.publish(
            Publish(topic="t/x", payload=b"p", qos=QoS.EXACTLY_ONCE, retain=False, dup=False, packet_id=0),
        )
    assert transport.sent == []


async def test_publish_at_or_below_maximum_qos_is_sent() -> None:
    protocol, transport = await _connected_protocol(maximum_qos=1)

    read_task = await _run_read_loop(protocol)
    try:
        task = asyncio.create_task(
            protocol.publish(
                Publish(
                    topic="t/x",
                    payload=b"p",
                    qos=QoS.AT_LEAST_ONCE,
                    retain=False,
                    dup=False,
                    packet_id=0,
                ),
            ),
        )
        await asyncio.sleep(0)
        assert len(transport.sent) == 1  # the PUBLISH went out
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, MQTTPublishError):
            await task
    finally:
        await _stop_task(read_task)


async def test_absent_maximum_qos_defaults_to_qos2() -> None:
    protocol, transport = await _connected_protocol(maximum_qos=None)

    read_task = await _run_read_loop(protocol)
    try:
        task = asyncio.create_task(
            protocol.publish(
                Publish(
                    topic="t/x",
                    payload=b"p",
                    qos=QoS.EXACTLY_ONCE,
                    retain=False,
                    dup=False,
                    packet_id=0,
                ),
            ),
        )
        await asyncio.sleep(0)
        assert len(transport.sent) == 1  # no local rejection: absent limit means QoS 2 ok
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, MQTTPublishError):
            await task
    finally:
        await _stop_task(read_task)


async def test_qos0_always_allowed_under_maximum_qos_0() -> None:
    protocol, transport = await _connected_protocol(maximum_qos=0)

    await protocol.publish(
        Publish(topic="t/x", payload=b"p", qos=QoS.AT_MOST_ONCE, retain=False, dup=False, packet_id=0),
    )
    assert len(transport.sent) == 1


async def test_v5_connack_with_properties_but_no_maximum_qos() -> None:
    # Properties present, Maximum QoS absent: spec default is QoS 2 — no limit.
    protocol, transport = make_protocol(version="5.0")
    transport.feed(
        encode(
            ConnAck(
                session_present=False,
                return_code=0,
                properties=ConnAckProperties(session_expiry_interval=60),
            ),
            version="5.0",
        )
    )
    await protocol.connect(Connect(client_id="c", clean_session=True, keepalive=60))
    assert protocol._max_publish_qos is None
    transport.sent.clear()

    read_task = await _run_read_loop(protocol)
    try:
        task = asyncio.create_task(
            protocol.publish(
                Publish(
                    topic="t/x",
                    payload=b"p",
                    qos=QoS.EXACTLY_ONCE,
                    retain=False,
                    dup=False,
                    packet_id=0,
                ),
            ),
        )
        await asyncio.sleep(0)
        assert len(transport.sent) == 1
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, MQTTPublishError):
            await task
    finally:
        await _stop_task(read_task)


async def test_v311_connack_never_applies_limit() -> None:
    # 3.1.1 has no CONNACK properties; QoS 2 must go through untouched.
    protocol, transport = make_protocol(version="3.1.1")
    transport.feed(encode(ConnAck(session_present=False, return_code=0), version="3.1.1"))
    await protocol.connect(Connect(client_id="c", clean_session=True, keepalive=60))
    transport.sent.clear()

    read_task = await _run_read_loop(protocol)
    try:
        task = asyncio.create_task(
            protocol.publish(
                Publish(
                    topic="t/x",
                    payload=b"p",
                    qos=QoS.EXACTLY_ONCE,
                    retain=False,
                    dup=False,
                    packet_id=0,
                ),
            ),
        )
        await asyncio.sleep(0)
        assert len(transport.sent) == 1
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, MQTTPublishError):
            await task
    finally:
        await _stop_task(read_task)


async def test_reconnect_refreshes_limit() -> None:
    # Fresh protocol object per connection mirrors _connect_with_retry, which
    # builds a new MQTTProtocol each attempt; verify the new limit applies.
    protocol1, _ = await _connected_protocol(maximum_qos=None)
    assert protocol1._max_publish_qos is None

    protocol2, transport2 = await _connected_protocol(maximum_qos=0)
    assert protocol2._max_publish_qos == 0
    with pytest.raises(MQTTQoSExceededError):
        await protocol2.publish(
            Publish(topic="t/x", payload=b"p", qos=QoS.AT_LEAST_ONCE, retain=False, dup=False, packet_id=0),
        )
    assert transport2.sent == []


async def test_subscribe_qos_is_not_limited() -> None:
    # §3.2.2.3.4: Maximum QoS constrains PUBLISH only; a SUBSCRIBE may still
    # request a higher QoS. Sanity-check the guard did not bleed into state.
    protocol, _ = await _connected_protocol(maximum_qos=0)
    assert protocol._max_publish_qos == 0
    # subscribe() itself talks to the broker; the limit must not pre-reject it.
    # (Full SUBSCRIBE flow is covered by broker E2E tests; here we only assert
    # the protocol object stores no subscription-side cap.)
    assert not hasattr(protocol, "_max_subscribe_qos")
