"""High-level MQTT client — public API layer."""

import asyncio
import contextlib
import dataclasses
import logging
import os
import ssl
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Final, Literal, Protocol, overload

from zmqtt._internal._compat import Self, defer_cancellation
from zmqtt._internal.packets.auth import Auth
from zmqtt._internal.packets.connect import ConnAck, Connect, Will
from zmqtt._internal.packets.properties import (
    AuthProperties,
    ConnAckProperties,
    ConnectProperties,
    PublishProperties,
    UnsubAckProperties,
)
from zmqtt._internal.packets.publish import Publish
from zmqtt._internal.packets.subscribe import SubscriptionRequest, UnsubAck
from zmqtt._internal.protocol import MQTTProtocol
from zmqtt._internal.request_response import _RequestDispatcher
from zmqtt._internal.state import SessionState
from zmqtt._internal.topic_matching import _DEFAULT_STRIPPED_PREFIXES
from zmqtt._internal.transport.base import Transport
from zmqtt._internal.transport.tcp import open_tcp
from zmqtt._internal.transport.tls import open_tls
from zmqtt._internal.types.message import Message
from zmqtt._internal.types.qos import QoS
from zmqtt._internal.types.retain_handling import RetainHandling
from zmqtt._internal.types.topic import validate_publish, validate_response_topic, validate_subscribe_topic
from zmqtt.errors import MQTTConnectError, MQTTDisconnectedError, MQTTTimeoutError

__all__ = (
    "ConnectionInfo",
    "MQTTClient",
    "MQTTClientV5",
    "MQTTClientV311",
    "ReconnectConfig",
    "Subscription",
    "Transport",
    "UnsubscribeResult",
    "create_client",
)

TransportFactory = Callable[[str, int, ssl.SSLContext | bool | None], Awaitable[Transport]]

# MQTT 5.0 §3.8.2.1.2: a subscription identifier is a variable-byte integer.
_MAX_SUBSCRIPTION_IDENTIFIER = 268_435_455

_UNSUBACK_REJECTION_THRESHOLD: Final = 0x80

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True, kw_only=True)
class ConnectionInfo:
    """Immutable snapshot of a successful CONNECT/CONNACK handshake.

    Attributes:
        connection_id: Successful network connection number within this client,
            starting at 1. A resumed MQTT session still gets a new number.
        session_present: Whether the broker resumed an existing MQTT session.
        return_code: Successful CONNACK return code (0).
        properties: Raw CONNACK properties, without substituted defaults.
        effective_client_id: Sent client ID, or the broker-assigned ID if empty.
        effective_keepalive: Server Keep Alive, falling back to CONNECT keepalive.
        effective_session_expiry_interval: CONNACK session expiry, falling back
            to CONNECT and then 0. None for MQTT 3.1.1.
    """

    connection_id: int
    session_present: bool
    return_code: int
    properties: ConnAckProperties | None
    effective_client_id: str
    effective_keepalive: int
    effective_session_expiry_interval: int | None

    @classmethod
    def from_connack(
        cls,
        connect_packet: Connect,
        connack: ConnAck,
        *,
        version: Literal["3.1.1", "5.0"],
        connection_id: int,
    ) -> Self:
        """Build a snapshot from a successful CONNECT/CONNACK exchange."""
        properties = connack.properties
        client_id = connect_packet.client_id
        keepalive = connect_packet.keepalive
        expiry = None
        if version == "5.0":
            expiry = 0
            if connect_packet.properties is not None and connect_packet.properties.session_expiry_interval is not None:
                expiry = connect_packet.properties.session_expiry_interval
        if properties is not None:
            if not client_id and properties.assigned_client_identifier is not None:
                client_id = properties.assigned_client_identifier
            if properties.server_keep_alive is not None:
                keepalive = properties.server_keep_alive
            if version == "5.0" and properties.session_expiry_interval is not None:
                expiry = properties.session_expiry_interval
        return cls(
            connection_id=connection_id,
            session_present=connack.session_present,
            return_code=connack.return_code,
            properties=properties,
            effective_client_id=client_id,
            effective_keepalive=keepalive,
            effective_session_expiry_interval=expiry,
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class UnsubscribeResult:
    """The broker's UNSUBACK for the filters sent in one UNSUBSCRIBE.

    Attributes:
        topic_filters: Filters sent to the broker, in request order.
        reason_codes: One code per filter. Empty on MQTT 3.1.1, whose UNSUBACK
            carries none.
        properties: Raw UNSUBACK properties.
    """

    topic_filters: tuple[str, ...]
    reason_codes: tuple[int, ...]
    properties: UnsubAckProperties | None

    @classmethod
    def from_unsuback(cls, topic_filters: tuple[str, ...], unsuback: UnsubAck) -> Self:
        return cls(topic_filters=topic_filters, reason_codes=unsuback.reason_codes, properties=unsuback.properties)

    @property
    def failures(self) -> dict[str, int]:
        """Rejected filters mapped to their reason codes (>= 0x80)."""
        return {
            f: code
            for f, code in zip(self.topic_filters, self.reason_codes, strict=False)
            if code >= _UNSUBACK_REJECTION_THRESHOLD
        }

    @property
    def reason_string(self) -> str | None:
        return self.properties.reason_string if self.properties is not None else None

    @property
    def user_properties(self) -> tuple[tuple[str, str], ...]:
        return self.properties.user_properties if self.properties is not None else ()


@dataclass(frozen=True, slots=True, kw_only=True)
class ReconnectConfig:
    """Configuration for automatic reconnection on connection loss.

    Attributes:
        enabled: Whether to reconnect automatically. Set to ``False`` to let
            exceptions propagate immediately on disconnection.
        initial_delay: Seconds to wait before the first reconnection attempt.
        max_delay: Upper bound (seconds) for the exponential back-off delay.
        backoff_factor: Multiplier applied to the delay after each failed attempt.
        max_attempts: Maximum total number of connection attempts before
            giving up. ``None`` retries indefinitely.
    """

    enabled: bool = True
    initial_delay: float = 1.0
    max_delay: float = 60.0
    backoff_factor: float = 2.0
    max_attempts: int | None = 5


async def _default_transport_factory(
    host: str,
    port: int,
    tls: ssl.SSLContext | bool | None,
) -> Transport:
    if not tls:
        return await open_tcp(host, port)
    if tls is True:
        return await open_tls(host, port)
    return await open_tls(host, port, tls)


class MQTTClientV311(Protocol):
    async def __aenter__(self) -> Self: ...

    async def __aexit__(self, *exc: object) -> None: ...

    async def publish(
        self,
        topic: str,
        payload: bytes | str,
        *,
        qos: QoS = QoS.AT_MOST_ONCE,
        retain: bool = False,
    ) -> None: ...

    def subscribe(
        self,
        *filters: str,
        qos: QoS = QoS.AT_MOST_ONCE,
        auto_ack: bool = True,
        receive_buffer_size: int = 1000,
    ) -> "Subscription": ...

    @property
    def connection_info(self) -> ConnectionInfo: ...

    async def connect(self) -> None: ...

    async def disconnect(self) -> None: ...

    async def ping(self, timeout: float = 10.0) -> float: ...


class MQTTClientV5(Protocol):
    """Type-safe view of MQTTClient for MQTT 5.0 connections."""

    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, *exc: object) -> None: ...

    @property
    def connection_info(self) -> ConnectionInfo: ...

    async def connect(self) -> None: ...

    async def disconnect(self) -> None: ...

    async def publish(
        self,
        topic: str,
        payload: bytes | str,
        *,
        qos: QoS = QoS.AT_MOST_ONCE,
        retain: bool = False,
        properties: PublishProperties | None = None,
    ) -> None: ...

    def subscribe(
        self,
        *filters: str,
        qos: QoS = QoS.AT_MOST_ONCE,
        auto_ack: bool = True,
        receive_buffer_size: int = 1000,
        no_local: bool = False,
        retain_as_published: bool = False,
        retain_handling: RetainHandling = RetainHandling.SEND_ON_SUBSCRIBE,
        subscription_identifier: int | None = None,
    ) -> "Subscription": ...

    async def auth(self, method: str, data: bytes | None = None) -> None: ...

    async def ping(self, timeout: float = 10.0) -> float: ...

    async def request(
        self,
        topic: str,
        payload: bytes | str,
        *,
        qos: QoS = QoS.AT_MOST_ONCE,
        timeout: float = 30.0,
        properties: PublishProperties | None = None,
    ) -> "Message": ...


class Subscription:
    """Async context manager for an active topic subscription.

    Registers filters on enter, unsubscribes on exit. Messages are available
    via get_message() or async iteration. Survives reconnection transparently —
    the queue keeps buffering and delivery resumes when the connection restores.
    """

    def __init__(
        self,
        client: "MQTTClient",
        filters: list[str],
        qos: QoS,
        auto_ack: bool = True,
        receive_buffer_size: int = 1000,
        no_local: bool = False,
        retain_as_published: bool = False,
        retain_handling: RetainHandling = RetainHandling.SEND_ON_SUBSCRIBE,
        subscription_identifier: int | None = None,
    ) -> None:
        self._client = client
        self._filters = filters
        self._qos = qos
        self._auto_ack = auto_ack
        self._queue: asyncio.Queue[Message] = asyncio.Queue(receive_buffer_size)
        self._no_local = no_local
        self._retain_as_published = retain_as_published
        self._retain_handling = retain_handling
        self._subscription_identifier = subscription_identifier
        self._registered_filters: list[str] = []

    async def __aenter__(self) -> Self:
        """Register the subscription filters with the broker.

        Raises:
            MQTTDisconnectedError: If the client is not currently connected.
        """
        if self._client._protocol is None:
            msg = "Not connected"
            raise MQTTDisconnectedError(msg)
        self._client._subscriptions.append(self)
        await self._do_subscribe(self._client._protocol)
        return self

    async def __aexit__(self, *exc: object) -> None:
        """Same as :meth:`stop`, discarding its result; skips UNSUBSCRIBE if the body was cancelled."""
        await self._stop(cancelled=isinstance(exc[1], asyncio.CancelledError))

    async def _stop(self, *, cancelled: bool) -> UnsubscribeResult | None:
        if self not in self._client._subscriptions:
            return None
        self._client._subscriptions.remove(self)
        protocol = self._client._protocol
        if cancelled or not self._registered_filters or protocol is None:
            return None
        try:
            sent = await protocol.unsubscribe(self._registered_filters)
        except Exception:  # noqa: BLE001 - filters are already released locally, so nothing is left to retry
            log.warning("Unsubscribe of %s failed; stopped locally", self._registered_filters, exc_info=True)
            return None
        return UnsubscribeResult.from_unsuback(*sent) if sent is not None else None

    async def _do_subscribe(self, protocol: MQTTProtocol) -> None:
        reqs = [
            SubscriptionRequest(
                topic_filter=f,
                qos=self._qos,
                no_local=self._no_local,
                retain_as_published=self._retain_as_published,
                retain_handling=self._retain_handling,
            )
            for f in self._filters
        ]
        _, queues = await protocol.subscribe(
            reqs,
            auto_ack=self._auto_ack,
            queue=self._queue,
            subscription_identifier=self._subscription_identifier,
        )
        self._registered_filters = list(queues.keys())

    async def _reconnect(self, protocol: MQTTProtocol) -> None:
        """Re-subscribe on a fresh protocol after reconnection."""
        await self._do_subscribe(protocol)

    async def start(self) -> None:
        """Register the subscription filters with the broker.
        Equivalent to entering the async context manager.
        Must be paired with a corresponding :meth:`stop` call to send
        UNSUBSCRIBE and release internal resources.

        Example::

            sub = client.subscribe("sensors/#", qos=QoS.AT_LEAST_ONCE)
            await sub.start()
            # ... later
            await sub.stop()

        Raises:
            MQTTDisconnectedError: If the client is not currently connected.
        """
        await self.__aenter__()

    async def stop(self) -> UnsubscribeResult | None:
        """Unsubscribe from all filters and stop message delivery.

        Equivalent to exiting the async context manager. Sends UNSUBSCRIBE to
        the broker. Never raises: failures and rejected filters are logged as
        warnings.

        Example::

            await sub.stop()

        Returns:
            The broker's UNSUBACK, or ``None`` if none was received.
        """
        return await self._stop(cancelled=False)

    async def get_message(self) -> Message:
        """Wait for and return the next message from the subscription queue.

        Raises:
            Exception: The terminal error that stopped the client run loop.
        """
        failure_signal = self._client._subscription_failure
        if failure_signal is None:
            message = await self._queue.get()
            await self._notify_capacity_available()
            return message
        if failure_signal.done():
            raise failure_signal.result()

        message_task = asyncio.create_task(self._queue.get())
        try:
            done, _ = await asyncio.wait(
                (message_task, failure_signal),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if failure_signal in done:
                raise failure_signal.result()
            message = message_task.result()
            await self._notify_capacity_available()
            return message
        finally:
            if not message_task.done():
                message_task.cancel()
            await asyncio.gather(message_task, return_exceptions=True)

    async def _notify_capacity_available(self) -> None:
        protocol = self._client._protocol
        if protocol is not None:
            await protocol.inbound.drain()

    def __aiter__(self) -> AsyncIterator[Message]:
        """Return self as the async iterator."""
        return self

    async def __anext__(self) -> Message:
        """Return the next message, suspending until one is available."""
        return await self.get_message()


class MQTTClient:
    """Asyncio MQTT client with automatic reconnection.

    Use as an async context manager. subscribe() returns a Subscription that
    is itself an async context manager.
    """

    def __init__(
        self,
        host: str,
        port: int = 1883,
        *,
        client_id: str = "",
        keepalive: int = 60,
        clean_session: bool = True,
        username: str | None = None,
        password: str | None = None,
        will: Will | None = None,
        tls: ssl.SSLContext | bool | None = None,
        reconnect: ReconnectConfig | None = None,
        on_connection_recovery_failed: Callable[[], Awaitable[None]] | None = None,
        mqtt_connect_timeout: float = 30.0,
        transport_factory: TransportFactory | None = None,
        version: Literal["3.1.1", "5.0"] = "3.1.1",
        session_expiry_interval: int = 0,
        stripped_prefixes: tuple[str, ...] = _DEFAULT_STRIPPED_PREFIXES,
        max_pending_requests: int = 1000,
        session_replay_buffer_size: int = 1000,
        session_replay_timeout: float = 30.0,
    ) -> None:
        """Create an MQTT client.

        The client must be used as an async context manager to establish the
        connection::

            async with MQTTClient("broker.example.com") as client:
                await client.publish("sensors/temp", "22.5")

        Prefer :func:`create_client` for version-typed access.

        Args:
            host: Broker hostname or IP address.
            port: TCP port. Defaults to ``1883`` (``8883`` is conventional for TLS).
            client_id: Client identifier sent in CONNECT. An empty string lets the
                broker assign one.
            keepalive: Keepalive interval in seconds. ``0`` disables keepalive.
            clean_session: Start with a clean session (MQTT 3.1.1) or discard any
                existing session state on connect.
            username: Optional username for broker authentication.
            password: Optional plain-text password for broker authentication.
            will: Last Will published by the broker if the connection closes
                unexpectedly.
            tls: TLS configuration. Pass ``True`` for default TLS, an
                :class:`ssl.SSLContext` for custom settings, or ``False`` (default)
                for a plain TCP connection.
            reconnect: Reconnection policy. Defaults to
                :class:`ReconnectConfig` with exponential back-off enabled.
            on_connection_recovery_failed: Async callback invoked once when a
                connection that was established successfully cannot be restored.
                Initial connection failures and clean disconnects do not invoke it.
            mqtt_connect_timeout: Seconds to wait for the broker's CONNACK during
                the MQTT CONNECT/CONNACK handshake — distinct from the TCP socket
                connect — before raising :exc:`MQTTTimeoutError`. Must be positive.
                Defaults to ``30.0``.
            transport_factory: Override the low-level transport. Useful for testing.
            version: MQTT protocol version to use. Either ``"3.1.1"`` (default) or
                ``"5.0"``.
            session_expiry_interval: MQTT 5.0 session expiry interval in seconds.
                ``0`` means the session expires on disconnect.
            stripped_prefixes: Group-less subscription prefixes the broker strips
                before delivery — matched against incoming PUBLISH topics with the
                prefix removed. Defaults to ``("$queue", "$exclusive")``; add a
                broker-specific decorator here instead of patching the library.
                ``$share/<group>/`` is always handled; real namespaces the broker
                delivers on unchanged (e.g. ``$SYS``) must not be listed.
            max_pending_requests: Maximum concurrent MQTT 5.0 ``request()`` calls.
                Additional calls wait until capacity is available.
            session_replay_buffer_size: Maximum unmatched messages held while a
                resumed persistent session waits for local subscriptions. ``0``
                makes the buffer unbounded. Defaults to ``1000``.
            session_replay_timeout: Seconds a persistent-session message may
                remain in the replay buffer. Remaining messages are dropped
                without acknowledgement after the timeout. Defaults to ``30.0``.
        """
        if not mqtt_connect_timeout > 0:
            msg = "mqtt_connect_timeout must be positive"
            raise ValueError(msg)
        if session_replay_buffer_size < 0:
            msg = "session_replay_buffer_size must be non-negative"
            raise ValueError(msg)
        if not session_replay_timeout > 0:
            msg = "session_replay_timeout must be positive"
            raise ValueError(msg)
        if will is not None and will.properties is not None and version != "5.0":
            msg = "will properties require MQTT 5.0"
            raise RuntimeError(msg)
        self._host = host
        self._port = port
        self._client_id = client_id
        self._keepalive = keepalive
        self._clean_session = clean_session
        self._username = username
        self._password = password
        self._will = will
        self._tls = tls
        self._reconnect = reconnect or ReconnectConfig()
        self._on_connection_recovery_failed = on_connection_recovery_failed
        self._mqtt_connect_timeout = mqtt_connect_timeout
        self._transport_factory: TransportFactory = transport_factory or _default_transport_factory
        self._version: Final = version
        self._session_expiry_interval = session_expiry_interval
        self._stripped_prefixes = stripped_prefixes
        self._request_dispatcher = _RequestDispatcher(max_pending_requests)
        self._session_replay_buffer_size = session_replay_buffer_size
        self._session_replay_timeout = session_replay_timeout
        self._connection_info: ConnectionInfo | None = None
        self._connection_id = 0
        self._protocol: MQTTProtocol | None = None
        self._subscriptions: list[Subscription] = []
        self._run_task: asyncio.Task[None] | None = None
        self._subscription_failure: asyncio.Future[BaseException] | None = None

    @property
    def connection_info(self) -> ConnectionInfo:
        """Current successful handshake.

        Subscription restoration may still be in progress. Previously returned
        snapshots remain valid after disconnection. Effective values describe
        negotiation; they do not change ping scheduling or future CONNECT IDs.

        Raises:
            MQTTDisconnectedError: If no successful connection is active,
                including before connecting and during reconnection.
        """
        if self._connection_info is None:
            msg = "No active connection information"
            raise MQTTDisconnectedError(msg)
        return self._connection_info

    async def __aenter__(self) -> Self:
        """Connect to the broker and start the background run loop."""
        await self._connect_with_retry()
        self._subscription_failure = asyncio.get_running_loop().create_future()
        self._run_task = asyncio.create_task(self._run_loop())
        self._run_task.add_done_callback(self._notify_subscription_failure)
        return self

    def _notify_subscription_failure(self, run_task: asyncio.Task[None]) -> None:
        """Wake subscription consumers if the client run loop failed."""
        self._connection_info = None
        if run_task.cancelled():
            return
        failure = run_task.exception()
        signal = self._subscription_failure
        if failure is not None and signal is not None and not signal.done():
            signal.set_result(failure)

    async def __aexit__(self, *exc: object) -> None:
        """Disconnect cleanly and cancel the run loop."""
        self._connection_info = None
        async with defer_cancellation():
            await self._request_dispatcher.cancel_pending()
            if self._run_task is not None:
                self._run_task.cancel()
                await asyncio.gather(self._run_task, return_exceptions=True)
                self._run_task = None
            if self._protocol is not None:
                await self._protocol.disconnect()
                self._protocol = None

    async def connect(self) -> None:
        """Connect to the broker and start the background run loop.

        Equivalent to entering the async context manager.
        Must be paired with a corresponding :meth:`disconnect` call to send
        DISCONNECT, close the socket, and stop the background run loop.

        Example::

            client = create_client("broker.example.com")
            await client.connect()
            # ... use the client
            await client.disconnect()

        Raises:
            MQTTConnectError: If the broker refuses the connection.
        """
        await self.__aenter__()

    async def disconnect(self) -> None:
        """Disconnect cleanly and stop the background run loop.

        Equivalent to exiting the async context manager. Sends DISCONNECT,
        closes the socket, and cancels the internal run loop task. Safe to
        call even if the connection has already been lost.
        """
        await self.__aexit__(None, None, None)

    async def publish(
        self,
        topic: str,
        payload: bytes | str,
        *,
        qos: QoS = QoS.AT_MOST_ONCE,
        retain: bool = False,
        properties: PublishProperties | None = None,
    ) -> None:
        """Publish a message to *topic*.

        Args:
            topic: Topic string. Must not contain wildcards.
            payload: Message body. ``str`` values are UTF-8 encoded automatically.
            qos: Delivery guarantee level. Defaults to ``AT_MOST_ONCE``.
            retain: Ask the broker to retain the message for future subscribers.
            properties: MQTT 5.0 publish properties. Raises if used with MQTT 3.1.1.

        Raises:
            MQTTInvalidTopicError: If *topic* is empty, contains wildcards, or has
                ``$`` in a non-leading position.
            MQTTDisconnectedError: If the client is not currently connected.
            RuntimeError: If *properties* is supplied on an MQTT 3.1.1 connection.
            MQTTPublishError: If the broker rejects a QoS 1/2 publish. Not raised for QoS 0 or MQTT 3.1.1.
        """
        validate_publish(topic)
        if self._protocol is None:
            msg = "Not connected"
            raise MQTTDisconnectedError(msg)
        if properties is not None and self._version != "5.0":
            msg = "properties require MQTT 5.0"
            raise RuntimeError(msg)
        if isinstance(payload, str):
            payload = payload.encode()
        await self._protocol.publish(
            Publish(
                topic=topic,
                payload=payload,
                qos=qos,
                retain=retain,
                dup=False,
                properties=properties,
            ),
        )

    async def ping(self, timeout: float = 10.0) -> float:
        """Send a PINGREQ and return the round-trip time in seconds.

        Args:
            timeout: Seconds to wait for PINGRESP before raising
                :exc:`MQTTTimeoutError`.

        Returns:
            RTT in seconds.

        Raises:
            MQTTDisconnectedError: If the client is not currently connected.
            MQTTTimeoutError: If no PINGRESP is received within *timeout* seconds.
        """
        if self._protocol is None:
            msg = "Not connected"
            raise MQTTDisconnectedError(msg)
        return await self._protocol.ping(timeout=timeout)

    def subscribe(
        self,
        *filters: str,
        qos: QoS = QoS.AT_MOST_ONCE,
        auto_ack: bool = True,
        receive_buffer_size: int = 1000,
        no_local: bool = False,
        retain_as_published: bool = False,
        retain_handling: RetainHandling = RetainHandling.SEND_ON_SUBSCRIBE,
        subscription_identifier: int | None = None,
    ) -> Subscription:
        """Create a :class:`Subscription` for one or more topic filters.

        The returned object must be used as an async context manager to activate
        the subscription and unsubscribe on exit::

            async with client.subscribe("sensors/#", qos=QoS.AT_LEAST_ONCE) as sub:
                async for msg in sub:
                    print(msg.topic, msg.payload)

        Args:
            *filters: One or more MQTT topic filters. Wildcards ``+`` (single level)
                and ``#`` (multi-level) are supported.
            qos: Maximum QoS level requested from the broker.
            auto_ack: Automatically send PUBACK/PUBREC upon receipt. Set to
                ``False`` to acknowledge manually via :meth:`Message.ack`.
            receive_buffer_size: Maximum messages buffered per internal queue.
                When the buffers are full the read loop stops pulling from the
                socket, so a slow consumer pushes back on the broker through the
                TCP window instead of growing memory. ``0`` makes the queues
                unbounded; the default is ``1000``.
            no_local: Do not receive messages published by this client (MQTT 5.0
                only).
            retain_as_published: Preserve the retain flag on forwarded messages
                (MQTT 5.0 only).
            retain_handling: Control when the broker sends retained messages for
                this subscription (MQTT 5.0 only).
            subscription_identifier: Numeric identifier (1..268435455) sent in the
                SUBSCRIBE properties (MQTT 5.0 only). The broker echoes it on every
                PUBLISH this subscription causes: incoming messages are routed to
                the exact subscription that matched (essential when filters
                overlap, e.g. a ``$share/...`` subscription plus its plain twin),
                and the value is readable on ``Message.properties``.

        Raises:
            MQTTInvalidTopicError: If any filter is empty, has ``$`` in a non-leading
                position, or contains a malformed wildcard (e.g. ``sensors#`` or
                ``a/b#/c``).
            RuntimeError: If an MQTT 5.0-only subscription option is used on an
                MQTT 3.1.1 connection.
        """
        for f in filters:
            validate_subscribe_topic(f)
        uses_v5_option = no_local or retain_as_published or retain_handling is not RetainHandling.SEND_ON_SUBSCRIBE
        if uses_v5_option and self._version != "5.0":
            msg = "no_local, retain_as_published, and retain_handling require MQTT 5.0"
            raise RuntimeError(msg)
        if subscription_identifier is not None:
            if self._version != "5.0":
                msg = "subscription_identifier requires MQTT 5.0"
                raise RuntimeError(msg)
            if not 1 <= subscription_identifier <= _MAX_SUBSCRIPTION_IDENTIFIER:
                msg = "subscription_identifier must be in 1..268435455"
                raise ValueError(msg)
        return Subscription(
            self,
            list(filters),
            qos,
            auto_ack,
            receive_buffer_size,
            no_local,
            retain_as_published,
            retain_handling,
            subscription_identifier,
        )

    async def request(
        self,
        topic: str,
        payload: bytes | str,
        *,
        qos: QoS = QoS.AT_MOST_ONCE,
        timeout: float = 30.0,
        properties: PublishProperties | None = None,
    ) -> Message:
        """Send a request and wait for exactly one reply (MQTT 5.0 only).

        Publishes *payload* to *topic* with a ``response_topic`` property, then
        waits for a message whose ``correlation_data`` matches the request.
        Other messages on the response topic remain available to regular
        subscriptions and do not complete this request.

        Both ``response_topic`` and ``correlation_data`` are taken from
        *properties* when set; otherwise they are generated automatically
        (a unique ``_zmqtt/reply/<32 hex chars>`` topic and 16 random bytes
        respectively).

        Args:
            topic: Request topic. Must not contain wildcards.
            payload: Request body. ``str`` values are UTF-8 encoded automatically.
            qos: QoS for the outgoing request publish.
            timeout: Seconds to wait for the reply before raising
                ``asyncio.TimeoutError``.
            properties: Publish properties for the request. ``response_topic``
                selects the reply topic (must not contain wildcards
                [MQTT-3.3.2-14]). ``correlation_data`` is forwarded as-is to
                the responder.

        Returns:
            The matching response ``Message``.

        Raises:
            RuntimeError: If the client is not using MQTT 5.0.
            MQTTInvalidTopicError: If ``properties.response_topic`` contains wildcards.
            MQTTDisconnectedError: If the request cannot start because the client
                is disconnected, or the client is stopped while waiting.
            ValueError: If the same response topic and correlation data are
                already used by another active request.
            asyncio.TimeoutError: If no matching reply arrives within *timeout*
                seconds.
        """
        if self._version != "5.0":
            msg = "request() requires MQTT 5.0"
            raise RuntimeError(msg)

        if properties is not None and properties.response_topic is not None:
            reply_topic = properties.response_topic
            validate_response_topic(reply_topic)
        else:
            reply_topic = f"_zmqtt/reply/{os.urandom(16).hex()}"

        if properties is not None and properties.correlation_data is not None:
            corr = properties.correlation_data
        else:
            corr = os.urandom(16)

        if properties is not None:
            req_props = dataclasses.replace(
                properties,
                response_topic=reply_topic,
                correlation_data=corr,
            )
        else:
            req_props = PublishProperties(
                response_topic=reply_topic,
                correlation_data=corr,
            )

        pending = await self._request_dispatcher.register(reply_topic, corr)
        try:
            await self.publish(topic, payload, qos=qos, properties=req_props)
            return await asyncio.wait_for(pending.future, timeout=timeout)
        finally:
            await pending.close()

    async def auth(self, method: str, data: bytes | None = None) -> None:
        """Send an AUTH packet for enhanced authentication (MQTT 5.0 only).

        Args:
            method: Authentication method name negotiated with the broker.
            data: Optional authentication data to include in the packet.

        Raises:
            RuntimeError: If the client is not using MQTT 5.0.
            MQTTDisconnectedError: If the client is not currently connected.
        """
        if self._version != "5.0":
            msg = "AUTH requires MQTT 5.0"
            raise RuntimeError(msg)
        if self._protocol is None:
            msg = "Not connected"
            raise MQTTDisconnectedError(msg)

        props = AuthProperties(authentication_method=method, authentication_data=data)
        await self._protocol.send_auth(Auth(reason_code=0x18, properties=props))

    async def _connect(self) -> None:
        transport = await self._transport_factory(self._host, self._port, self._tls)
        protocol = MQTTProtocol(
            transport,
            SessionState(),
            keepalive=self._keepalive,
            connect_timeout=self._mqtt_connect_timeout,
            version=self._version,
            stripped_prefixes=self._stripped_prefixes,
            request_router=self._request_dispatcher,
            session_replay_buffer_size=self._session_replay_buffer_size,
            session_replay_timeout=self._session_replay_timeout,
        )
        connect_props = None
        if self._version == "5.0":
            connect_props = ConnectProperties(
                session_expiry_interval=self._session_expiry_interval,
            )
        connect_packet = Connect(
            client_id=self._client_id,
            clean_session=self._clean_session,
            keepalive=self._keepalive,
            username=self._username,
            password=self._password.encode() if self._password is not None else None,
            will=self._will,
            properties=connect_props,
        )
        try:
            connack = await protocol.connect(connect_packet)
        except BaseException:
            await transport.close()
            raise
        info = ConnectionInfo.from_connack(
            connect_packet,
            connack,
            version=self._version,
            connection_id=self._connection_id + 1,
        )
        self._protocol = protocol
        self._connection_info = info
        self._connection_id = info.connection_id
        self._request_dispatcher.bind(protocol)

    async def _connect_with_retry(self, *, wait_before_first_attempt: bool = False) -> None:
        delay = self._reconnect.initial_delay
        if wait_before_first_attempt:
            await asyncio.sleep(delay)
            delay = min(delay * self._reconnect.backoff_factor, self._reconnect.max_delay)

        attempt = 0
        while True:
            try:
                await self._connect()
            except MQTTConnectError:  # noqa: PERF203
                raise
            except (OSError, MQTTTimeoutError):
                attempt += 1
                max_a = self._reconnect.max_attempts
                if not self._reconnect.enabled or (max_a is not None and attempt >= max_a):
                    raise
                log.warning("Connection failed, retrying in %.1fs", delay, exc_info=True)
                await asyncio.sleep(delay)
                delay = min(delay * self._reconnect.backoff_factor, self._reconnect.max_delay)
            else:
                return

    async def _run_loop(self) -> None:
        subs_to_restore: list[Subscription] = []

        while True:
            if self._protocol is None:
                msg = "Not connected yet"
                raise RuntimeError(msg)
            protocol_run_task = asyncio.create_task(self._protocol.run())
            try:
                # Run the protocol as a sub-task so _read_loop is live while we
                # re-subscribe.  For the first connection subs_to_restore is empty,
                # so this collapses to the original "await protocol.run()" pattern.
                await self._protocol.started_event.wait()
                if subs_to_restore:
                    for sub in subs_to_restore:
                        await sub._reconnect(self._protocol)
                    subs_to_restore = []
                await self._request_dispatcher.restore()
                await protocol_run_task

            except (MQTTDisconnectedError, MQTTTimeoutError):
                self._connection_info = None
                if not self._reconnect.enabled:
                    await self._notify_connection_recovery_failed()
                    raise
                # Close the dead transport to release the file descriptor.
                with contextlib.suppress(Exception):
                    await self._protocol._transport.close()
            else:
                return  # clean disconnect — protocol.disconnect() was called
            finally:
                self._connection_info = None
                protocol_run_task.cancel()
                await asyncio.gather(protocol_run_task, return_exceptions=True)

            subs_to_restore = list(self._subscriptions)
            log.warning("Connection lost, reconnecting...")
            try:
                await self._connect_with_retry(wait_before_first_attempt=True)
            except (MQTTConnectError, MQTTTimeoutError, OSError):
                await self._notify_connection_recovery_failed()
                raise
            log.info("Successfully reconnected")

    async def _notify_connection_recovery_failed(self) -> None:
        if self._on_connection_recovery_failed is not None:
            await self._on_connection_recovery_failed()


# No version= argument: defaults to "3.1.1", so the return type is MQTTClientV311.
@overload
def create_client(
    host: str,
    port: int = ...,
    *,
    client_id: str = ...,
    keepalive: int = ...,
    clean_session: bool = ...,
    username: str | None = ...,
    password: str | None = ...,
    will: Will | None = ...,
    tls: ssl.SSLContext | bool = ...,
    reconnect: ReconnectConfig | None = ...,
    on_connection_recovery_failed: Callable[[], Awaitable[None]] | None = ...,
    mqtt_connect_timeout: float = ...,
    transport_factory: TransportFactory | None = ...,
    session_expiry_interval: int = ...,
    max_pending_requests: int = ...,
    session_replay_buffer_size: int = ...,
    session_replay_timeout: float = ...,
) -> MQTTClientV311: ...


@overload
def create_client(
    host: str,
    port: int = ...,
    *,
    client_id: str = ...,
    keepalive: int = ...,
    clean_session: bool = ...,
    username: str | None = ...,
    password: str | None = ...,
    will: Will | None = ...,
    tls: ssl.SSLContext | bool = ...,
    reconnect: ReconnectConfig | None = ...,
    on_connection_recovery_failed: Callable[[], Awaitable[None]] | None = ...,
    mqtt_connect_timeout: float = ...,
    transport_factory: TransportFactory | None = ...,
    session_expiry_interval: int = ...,
    max_pending_requests: int = ...,
    session_replay_buffer_size: int = ...,
    session_replay_timeout: float = ...,
    version: Literal["3.1.1"],
) -> MQTTClientV311: ...


@overload
def create_client(
    host: str,
    port: int = ...,
    *,
    client_id: str = ...,
    keepalive: int = ...,
    clean_session: bool = ...,
    username: str | None = ...,
    password: str | None = ...,
    will: Will | None = ...,
    tls: ssl.SSLContext | bool = ...,
    reconnect: ReconnectConfig | None = ...,
    on_connection_recovery_failed: Callable[[], Awaitable[None]] | None = ...,
    mqtt_connect_timeout: float = ...,
    transport_factory: TransportFactory | None = ...,
    session_expiry_interval: int = ...,
    max_pending_requests: int = ...,
    session_replay_buffer_size: int = ...,
    session_replay_timeout: float = ...,
    version: Literal["5.0"],
) -> MQTTClientV5: ...


def create_client(
    host: str,
    port: int = 1883,
    *,
    client_id: str = "",
    keepalive: int = 60,
    clean_session: bool = True,
    username: str | None = None,
    password: str | None = None,
    will: Will | None = None,
    tls: ssl.SSLContext | bool = False,
    reconnect: ReconnectConfig | None = None,
    on_connection_recovery_failed: Callable[[], Awaitable[None]] | None = None,
    mqtt_connect_timeout: float = 30.0,
    transport_factory: TransportFactory | None = None,
    session_expiry_interval: int = 0,
    max_pending_requests: int = 1000,
    session_replay_buffer_size: int = 1000,
    session_replay_timeout: float = 30.0,
    version: Literal["3.1.1", "5.0"] = "3.1.1",
) -> MQTTClientV311 | MQTTClientV5:
    """Create a version-typed MQTT client.

    Returns MQTTClientV311 when version="3.1.1", MQTTClientV5 when version="5.0".
    The concrete type is always MQTTClient; the return type is a Protocol view.
    """
    return MQTTClient(
        host,
        port,
        client_id=client_id,
        keepalive=keepalive,
        clean_session=clean_session,
        username=username,
        password=password,
        will=will,
        tls=tls,
        reconnect=reconnect,
        on_connection_recovery_failed=on_connection_recovery_failed,
        mqtt_connect_timeout=mqtt_connect_timeout,
        transport_factory=transport_factory,
        version=version,
        session_expiry_interval=session_expiry_interval,
        max_pending_requests=max_pending_requests,
        session_replay_buffer_size=session_replay_buffer_size,
        session_replay_timeout=session_replay_timeout,
    )
