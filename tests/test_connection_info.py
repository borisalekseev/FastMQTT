"""Public handshake snapshots exercised through the real MQTT protocol."""

import asyncio
import ssl
from collections.abc import Callable
from dataclasses import FrozenInstanceError
from typing import Literal

import pytest

from tests.test_protocol import FakeTransport
from zmqtt import (
    ConnAckProperties,
    ConnectionInfo,
    MQTTClient,
    MQTTClientV5,
    MQTTClientV311,
    MQTTConnectError,
    MQTTDisconnectedError,
    MQTTTimeoutError,
    ReconnectConfig,
    create_client,
)
from zmqtt._internal.packets.codec import encode
from zmqtt._internal.packets.connect import ConnAck, Connect
from zmqtt._internal.packets.reader import PacketBuffer
from zmqtt._internal.transport.base import Transport
from zmqtt.client import TransportFactory


def transport_factory(transport: FakeTransport) -> TransportFactory:
    async def factory(host: str, port: int, tls: ssl.SSLContext | bool | None) -> Transport:  # noqa: ARG001
        return transport

    return factory


def assert_disconnected(client: MQTTClientV311 | MQTTClientV5) -> None:
    assert client.connection_info is None


async def wait_until(predicate: Callable[[], bool]) -> None:
    async def wait() -> None:
        while not predicate():  # noqa: ASYNC110
            await asyncio.sleep(0)

    await asyncio.wait_for(wait(), timeout=2)


async def test_full_connack_snapshot_is_immutable() -> None:
    properties = ConnAckProperties(
        session_expiry_interval=120,
        receive_maximum=12,
        maximum_qos=1,
        retain_available=False,
        maximum_packet_size=4096,
        assigned_client_identifier="broker-id",
        topic_alias_maximum=8,
        reason_string="welcome",
        wildcard_subscription_available=False,
        subscription_identifier_available=True,
        shared_subscription_available=False,
        server_keep_alive=30,
        response_information="reply-prefix/raw",
        server_reference="broker.example",
        authentication_method="example",
        authentication_data=b"data",
        user_properties=(("key", "first"), ("key", "second")),
    )
    transport = FakeTransport()
    transport.feed(encode(ConnAck(session_present=True, return_code=0, properties=properties), version="5.0"))
    client = create_client("localhost", version="5.0", transport_factory=transport_factory(transport))
    assert_disconnected(client)
    async with client:
        info = client.connection_info
        assert info == ConnectionInfo(
            connection_id=1,
            session_present=True,
            return_code=0,
            properties=properties,
            effective_client_id="broker-id",
            effective_keepalive=30,
            effective_session_expiry_interval=120,
        )
        assert info is not None
        assert info.properties is not None
        with pytest.raises(FrozenInstanceError):
            info.connection_id = 2  # type: ignore[misc]
        with pytest.raises(FrozenInstanceError):
            info.properties.reason_string = "changed"  # type: ignore[misc]
        with pytest.raises(TypeError):
            info.properties.user_properties[0][0] = "changed"  # type: ignore[index]
        with pytest.raises(AttributeError):
            client.connection_info = None  # type: ignore[misc]
    assert_disconnected(client)
    assert info.properties == properties


@pytest.mark.parametrize("version", ["3.1.1", "5.0"])
@pytest.mark.parametrize("client_id", ["", "explicit"])
@pytest.mark.parametrize("expiry", [0, 600])
@pytest.mark.parametrize(
    "properties",
    [
        None,
        ConnAckProperties(),
        ConnAckProperties(reason_string="welcome"),
        ConnAckProperties(server_keep_alive=0, session_expiry_interval=0),
    ],
)
async def test_connection_info_fallbacks(
    version: Literal["3.1.1", "5.0"], client_id: str, expiry: int, properties: ConnAckProperties | None
) -> None:
    transport = FakeTransport()
    transport.feed(encode(ConnAck(session_present=False, return_code=0, properties=properties), version=version))
    client = MQTTClient(
        "localhost",
        version=version,
        client_id=client_id,
        keepalive=45,
        session_expiry_interval=expiry,
        transport_factory=transport_factory(transport),
    )
    async with client:
        info = client.connection_info
        assert info is not None
        assert info.connection_id == 1
        assert info.effective_client_id == client_id
        overridden = version == "5.0" and properties is not None and properties.server_keep_alive is not None
        assert info.effective_keepalive == (0 if overridden else 45)
        assert info.effective_session_expiry_interval == ((0 if overridden else expiry) if version == "5.0" else None)
        assert info.properties == (properties if version == "5.0" and properties != ConnAckProperties() else None)


async def test_explicit_client_id_takes_precedence() -> None:
    transport = FakeTransport()
    transport.feed(
        encode(
            ConnAck(
                session_present=False,
                return_code=0,
                properties=ConnAckProperties(assigned_client_identifier="assigned"),
            ),
            version="5.0",
        )
    )
    client = create_client(
        "localhost", version="5.0", client_id="explicit", transport_factory=transport_factory(transport)
    )
    async with client:
        assert client.connection_info is not None
        assert client.connection_info.effective_client_id == "explicit"


async def test_reconnect_replaces_snapshot_after_failed_attempt() -> None:
    transports = [FakeTransport(), FakeTransport()]
    for transport in transports:
        transport.feed(
            encode(
                ConnAck(
                    session_present=True,
                    return_code=0,
                    properties=ConnAckProperties(assigned_client_identifier="assigned"),
                ),
                version="5.0",
            )
        )
    attempts = 0
    retry_entered = asyncio.Event()
    allow_retry = asyncio.Event()

    async def factory(host: str, port: int, tls: ssl.SSLContext | bool | None) -> Transport:  # noqa: ARG001
        nonlocal attempts
        attempts += 1
        if attempts == 2:
            assert_disconnected(client)
            retry_entered.set()
            await allow_retry.wait()
            msg = "temporary connection failure"
            raise OSError(msg)
        assert_disconnected(client)
        return transports[0 if attempts == 1 else 1]

    client = create_client(
        "localhost",
        version="5.0",
        transport_factory=factory,
        reconnect=ReconnectConfig(initial_delay=0, max_attempts=2),
    )
    async with client:
        old = client.connection_info
        assert old is not None
        transports[0]._rx.append(MQTTDisconnectedError("lost"))
        await asyncio.wait_for(retry_entered.wait(), timeout=2)
        assert_disconnected(client)
        allow_retry.set()
        await wait_until(lambda: client.connection_info is not None)
        new = client.connection_info
        assert new is not None
        assert new.connection_id == 2
        assert new is not old
        assert old.connection_id == 1
        assert old.effective_client_id == "assigned"
        assert attempts == 3
        for transport in transports:
            buffer = PacketBuffer(version="5.0")
            buffer.feed(transport.sent[0])
            packet = next(iter(buffer))
            assert isinstance(packet, Connect)
            assert packet.client_id == ""
    assert_disconnected(client)


@pytest.mark.parametrize("failure", [MQTTDisconnectedError("lost"), MQTTTimeoutError("timeout"), ValueError("crashed")])
async def test_background_failure_clears_snapshot(failure: Exception) -> None:
    transport = FakeTransport()
    transport.feed(encode(ConnAck(session_present=False, return_code=0), version="3.1.1"))
    observed: list[ConnectionInfo | None] = []

    async def callback() -> None:
        observed.append(client.connection_info)

    client = MQTTClient(
        "localhost",
        transport_factory=transport_factory(transport),
        reconnect=ReconnectConfig(enabled=False),
        on_connection_recovery_failed=callback,
    )
    async with client:
        assert client.connection_info is not None
        transport._rx.append(failure)
        assert client._run_task is not None
        with pytest.raises(type(failure), match=str(failure)):
            await asyncio.wait_for(asyncio.shield(client._run_task), timeout=2)
        assert_disconnected(client)
        assert observed == ([] if isinstance(failure, ValueError) else [None])


@pytest.mark.parametrize("failure", ["refusal", "timeout"])
async def test_failed_recovery_clears_snapshot_before_callback(failure: str) -> None:
    first, retry = FakeTransport(), FakeTransport()
    first.feed(encode(ConnAck(session_present=False, return_code=0), version="5.0"))
    if failure == "refusal":
        retry.feed(encode(ConnAck(session_present=False, return_code=0x87), version="5.0"))
    transports = iter([first, retry])
    observed: list[ConnectionInfo | None] = []

    async def factory(host: str, port: int, tls: ssl.SSLContext | bool | None) -> Transport:  # noqa: ARG001
        assert_disconnected(client)
        return next(transports)

    async def callback() -> None:
        observed.append(client.connection_info)

    client = MQTTClient(
        "localhost",
        version="5.0",
        transport_factory=factory,
        mqtt_connect_timeout=0.02,
        reconnect=ReconnectConfig(initial_delay=0, max_attempts=1),
        on_connection_recovery_failed=callback,
    )
    async with client:
        first._rx.append(MQTTDisconnectedError("lost"))
        assert client._run_task is not None
        with pytest.raises(MQTTConnectError if failure == "refusal" else MQTTTimeoutError):
            await asyncio.wait_for(asyncio.shield(client._run_task), timeout=2)
        assert observed == [None]
        assert_disconnected(client)
        assert not retry.is_connected


@pytest.mark.parametrize(("version", "code"), [("3.1.1", 5), ("5.0", 0x87)])
async def test_connection_refusal_diagnostics(version: Literal["3.1.1", "5.0"], code: int) -> None:
    props = (
        ConnAckProperties(
            reason_string="denied", server_reference="other.example", user_properties=(("key", "one"), ("key", "two"))
        )
        if version == "5.0"
        else None
    )
    transport = FakeTransport()
    transport.feed(encode(ConnAck(session_present=False, return_code=code, properties=props), version=version))
    client = MQTTClient("localhost", version=version, transport_factory=transport_factory(transport))
    with pytest.raises(MQTTConnectError) as caught:
        await client.connect()
    error = caught.value
    assert error.return_code == code
    assert str(error) == f"Connection refused: return code {code}"
    assert error.properties == props
    assert error.reason_string == (props.reason_string if props else None)
    assert error.server_reference == (props.server_reference if props else None)
    assert error.user_properties == (props.user_properties if props else ())
    assert_disconnected(client)
    assert not transport.is_connected
    legacy = MQTTConnectError(code)
    assert legacy.properties is None
    assert legacy.reason_string is None
    assert legacy.server_reference is None
    assert legacy.user_properties == ()


async def test_initial_timeout_has_no_snapshot() -> None:
    transport = FakeTransport()
    client = create_client(
        "localhost",
        transport_factory=transport_factory(transport),
        mqtt_connect_timeout=0.02,
        reconnect=ReconnectConfig(enabled=False),
    )
    with pytest.raises(MQTTTimeoutError):
        await client.connect()
    assert_disconnected(client)
    assert not transport.is_connected


async def test_immediate_disconnect_and_next_connect() -> None:
    transport = FakeTransport()
    client = create_client("localhost", transport_factory=transport_factory(transport))
    for connection_id in (1, 2):
        transport.feed(encode(ConnAck(session_present=False, return_code=0), version="3.1.1"))
        await client.connect()
        assert client.connection_info is not None
        assert client.connection_info.connection_id == connection_id
        await client.disconnect()
        assert_disconnected(client)
