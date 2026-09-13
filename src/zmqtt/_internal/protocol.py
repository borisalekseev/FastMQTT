"""Protocol engine: ties codec, transport, and session state together."""

import asyncio
import contextlib
import dataclasses
import logging
from collections.abc import AsyncGenerator, Iterable
from typing import Final, Literal

from zmqtt._internal import topic_matching
from zmqtt._internal.inbound import InboundPublishFlow
from zmqtt._internal.packets.auth import Auth
from zmqtt._internal.packets.codec import AnyPacket, encode
from zmqtt._internal.packets.connect import ConnAck, Connect
from zmqtt._internal.packets.disconnect import Disconnect
from zmqtt._internal.packets.ping import PingReq, PingResp
from zmqtt._internal.packets.properties import SubscribeProperties
from zmqtt._internal.packets.publish import PubAck, PubComp, Publish, PubRec, PubRel
from zmqtt._internal.packets.reader import PacketBuffer
from zmqtt._internal.packets.subscribe import (
    SubAck,
    Subscribe,
    SubscriptionRequest,
    UnsubAck,
    Unsubscribe,
)
from zmqtt._internal.routing import InboundRecipient, RequestRouter
from zmqtt._internal.state import (
    OutboundQoS2Flight,
    OutboundQoS2State,
    QoS1Flight,
    SessionState,
)
from zmqtt._internal.subscription_index import SubscriptionEntry
from zmqtt._internal.transport.base import Transport
from zmqtt._internal.types.message import Message
from zmqtt._internal.types.qos import QoS
from zmqtt.errors import (
    MQTTConnectError,
    MQTTDisconnectedError,
    MQTTProtocolError,
    MQTTPublishError,
    MQTTQoSExceededError,
    MQTTSubscribeError,
    MQTTTimeoutError,
)

log = logging.getLogger("zmqtt.protocol")

# see: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901124
_PUBLISH_REASON_NAMES: Final[dict[int, str]] = {
    0x00: "Success",
    0x10: "No matching subscribers",
    0x80: "Unspecified error",
    0x83: "Implementation specific error",
    0x87: "Not authorized",
    0x90: "Topic Name invalid",
    0x91: "Packet Identifier in use",
    0x97: "Quota exceeded",
    0x99: "Payload format invalid",
}

# MQTT 5.0 §3.11.3
_UNSUBACK_REASON_NAMES: Final[dict[int, str]] = {
    0x80: "Unspecified error",
    0x83: "Implementation specific error",
    0x87: "Not authorized",
    0x8F: "Topic Filter invalid",
    0x91: "Packet Identifier in use",
}


class _SubscriptionGuard:
    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.users = 0


class _SubscriptionGuards:
    """Serialize ownership changes only when topic filters overlap exactly."""

    def __init__(self) -> None:
        self._guards: dict[str, _SubscriptionGuard] = {}

    @contextlib.asynccontextmanager
    async def hold(self, filters: Iterable[str]) -> AsyncGenerator[None]:
        entries: list[tuple[str, _SubscriptionGuard]] = []
        for filter_ in sorted(set(filters)):
            guard = self._guards.setdefault(filter_, _SubscriptionGuard())
            guard.users += 1
            entries.append((filter_, guard))

        acquired = 0
        try:
            for _, guard in entries:
                await guard.lock.acquire()
                acquired += 1
            yield
        finally:
            for _, guard in reversed(entries[:acquired]):
                guard.lock.release()
            for filter_, guard in entries:
                guard.users -= 1
                if guard.users == 0:
                    self._guards.pop(filter_, None)


def _raise_on_rejected_filters(filters: list[SubscriptionRequest], suback: SubAck) -> None:
    """Surface SUBACK failure codes (>= 0x80) instead of ignoring them.

    A rejected filter — most commonly an authorization denial — otherwise looks
    exactly like a successful subscription and silently never receives anything.
    Return codes map to filters by position (MQTT 3.1.1 §3.9.3, 5.0 §3.9.2.1).
    """
    # strict=False: a non-conforming broker answering with a short code list
    # should not turn into a ValueError here.
    failures = {req.topic_filter: code for req, code in zip(filters, suback.return_codes, strict=False) if code >= 0x80}
    if failures:
        raise MQTTSubscribeError(failures)


def _warn_on_rejected_unsubscribe(filters: list[str], unsuback: UnsubAck) -> None:
    rejected = [
        f"{f!r} (0x{code:02X} {_UNSUBACK_REASON_NAMES.get(code, 'Unknown')})"
        for f, code in zip(filters, unsuback.reason_codes, strict=False)
        if code >= 0x80
    ]
    if not rejected:
        return
    reason_string = unsuback.properties.reason_string if unsuback.properties is not None else None
    log.warning(
        "Broker rejected unsubscribe of %s%s; it may keep delivering messages on these filters",
        ", ".join(rejected),
        f" ({reason_string})" if reason_string else "",
    )


def _publish_error(packet: PubAck | PubRec) -> MQTTPublishError:
    return MQTTPublishError(
        packet.reason_code,
        _PUBLISH_REASON_NAMES.get(packet.reason_code),
        packet.properties.reason_string if packet.properties is not None else None,
    )


def _raise_on_rejected_puback(puback: PubAck) -> None:
    """Surface PUBACK failure codes (>= 0x80) instead of ignoring them.

    A rejected publish otherwise looks exactly like a successful publish.
    """
    if puback.reason_code >= 0x80:
        err = _publish_error(puback)
        log.debug("QoS 1 ack received with error for packet_id=%d: %s", puback.packet_id, err)
        raise err


class MQTTProtocol:
    """MQTT protocol engine.

    Lifecycle:
      1. ``await protocol.connect(packet)``  — handshake, returns ConnAck
      2. ``await protocol.run()``            — read loop + ping loop (runs until disconnect)
      3. ``await protocol.disconnect()``     — clean shutdown

    QoS flows and subscription management are available between steps 1 and 3.
    """

    def __init__(
        self,
        transport: Transport,
        state: SessionState,
        keepalive: int = 60,
        ping_timeout: float = 10.0,
        connect_timeout: float = 30.0,
        version: Literal["3.1.1", "5.0"] = "3.1.1",
        stripped_prefixes: tuple[str, ...] = topic_matching._DEFAULT_STRIPPED_PREFIXES,
        request_router: RequestRouter | None = None,
        session_replay_buffer_size: int = 1000,
        session_replay_timeout: float = 30.0,
    ) -> None:
        self._transport = transport
        self._state = state
        self._keepalive = keepalive
        self._ping_timeout = ping_timeout
        self._connect_timeout = connect_timeout
        self._version: Final = version
        self._stripped_prefixes = stripped_prefixes
        self.inbound = InboundPublishFlow(
            connection=self,
            state=state,
            request_router=request_router,
            session_replay_buffer_size=session_replay_buffer_size,
            session_replay_timeout=session_replay_timeout,
        )
        self._buf = PacketBuffer(version=version)
        self._ping_waiters: list[asyncio.Future[None]] = []
        self._subscription_guards = _SubscriptionGuards()
        self._disconnecting = False
        self._dead = False
        # MQTT 5 §3.2.2.3.4: absent Maximum QoS means the server accepts QoS 2,
        # and 3.1.1 has no CONNACK properties at all — default to EXACTLY_ONCE
        # so publish() never has to branch on None.
        self._max_publish_qos: QoS = QoS.EXACTLY_ONCE
        self.started_event = asyncio.Event()

    async def connect(self, packet: Connect) -> ConnAck:
        """Send CONNECT, read and return CONNACK. Raises on failure.

        Per MQTT 5.0 section 3.2, if the CONNACK is not received within
        ``connect_timeout`` seconds the connection is presumed dead and
        ``MQTTTimeoutError`` is raised (mirrors ``ping()``'s PINGRESP timeout).
        """
        log.debug("Connecting client %r", packet.client_id)
        await self._send(self._encode(packet))
        try:
            # No asyncio.shield (unlike ping): on timeout the transport is closed and
            # replaced by _connect_with_retry, so keeping the read coroutine alive is pointless.
            return await asyncio.wait_for(self._await_connack(), timeout=self._connect_timeout)
        except asyncio.TimeoutError as e:
            msg = "CONNACK not received within timeout"
            raise MQTTTimeoutError(msg) from e

    async def _await_connack(self) -> ConnAck:
        while True:
            data = await self._transport.read(4096)
            self._buf.feed(data)
            for pkt in self._buf:
                if not isinstance(pkt, ConnAck):
                    msg = f"Expected CONNACK, got {pkt!r}"
                    raise MQTTProtocolError(msg)
                if pkt.return_code != 0:
                    raise MQTTConnectError(pkt.return_code, properties=pkt.properties)
                log.info("Connected with session_present=%s", pkt.session_present)
                if self._version == "5.0" and pkt.properties is not None:
                    # Properties present but Maximum QoS absent: spec default is QoS 2.
                    max_qos = pkt.properties.maximum_qos
                    self._max_publish_qos = QoS.EXACTLY_ONCE if max_qos is None else QoS(max_qos)
                else:  # 3.1.1 has no CONNACK properties: no limit.
                    self._max_publish_qos = QoS.EXACTLY_ONCE
                self.inbound.begin_session(session_present=pkt.session_present)
                return pkt

    async def run(self) -> None:
        """Run read loop and ping loop concurrently until disconnection."""
        read_task = asyncio.create_task(self._read_loop())
        ping_task = asyncio.create_task(self._ping_loop())
        self.started_event.set()
        try:
            await asyncio.gather(read_task, ping_task)
        except BaseException:
            read_task.cancel()
            ping_task.cancel()
            await asyncio.gather(read_task, ping_task, return_exceptions=True)
            raise
        finally:
            self.started_event.clear()
            self._dead = True
            self._cancel_pending()

    async def disconnect(self) -> None:
        """Send DISCONNECT and close the transport."""
        self._disconnecting = True
        with contextlib.suppress(Exception):
            await self._send(self._encode(Disconnect()))
        await self._transport.close()
        log.info("Disconnected")

    def _cancel_pending(self) -> None:  # noqa: C901
        """Fail all futures awaiting broker responses — called when run() exits."""
        exc = MQTTDisconnectedError("Connection lost")
        for sub_f in self._state.pending_subs.values():
            if not sub_f.done():
                sub_f.set_exception(exc)
        for unsub_f in self._state.pending_unsubs.values():
            if not unsub_f.done():
                unsub_f.set_exception(exc)
        for q1_flight in self._state.inflight_qos1.values():
            if not q1_flight.future.done():
                q1_flight.future.set_exception(exc)
        for q2_flight in self._state.inflight_qos2_out.values():
            if not q2_flight.future.done():
                q2_flight.future.set_exception(exc)
        for ping_f in self._ping_waiters:
            if not ping_f.done():
                ping_f.set_exception(exc)
        self._ping_waiters.clear()
        self.inbound.clear()

    def _ensure_alive(self) -> None:
        """Refuse new operations once the run loop has exited.

        _cancel_pending() fails every future that existed when the loop died, but
        an operation started afterwards would create a fresh future that nothing
        will ever resolve — the caller would hang forever (a dead client used to
        hang even in __aexit__, inside unsubscribe()).
        """
        if self._dead:
            msg = "Connection lost"
            raise MQTTDisconnectedError(msg)

    async def publish(self, packet: Publish) -> PubAck | PubComp | None:
        """
        Publish a message. Returns PubAck (QoS 1), PubComp (QoS 2), or None (QoS 0).
        """
        self._ensure_alive()
        max_qos = self._max_publish_qos
        if packet.qos > max_qos:
            raise MQTTQoSExceededError(requested=int(packet.qos), maximum=int(max_qos))
        match packet.qos:
            case QoS.AT_MOST_ONCE:
                await self._send(self._encode(packet))
                log.debug("Published QoS 0 to topic %r", packet.topic)
                return None

            case QoS.AT_LEAST_ONCE:
                loop = asyncio.get_running_loop()
                pid = self._state.packet_ids.acquire()
                packet = dataclasses.replace(packet, packet_id=pid)
                future: asyncio.Future[PubAck] = loop.create_future()
                self._state.inflight_qos1[pid] = QoS1Flight(
                    packet_id=pid,
                    publish=packet,
                    future=future,
                )
                await self._send(self._encode(packet))
                log.debug("Published QoS 1 to topic %r with packet_id=%d", packet.topic, pid)
                ack = await future
                _raise_on_rejected_puback(ack)
                return ack

            case QoS.EXACTLY_ONCE:
                loop = asyncio.get_running_loop()
                pid = self._state.packet_ids.acquire()
                packet = dataclasses.replace(packet, packet_id=pid)
                future2: asyncio.Future[PubComp] = loop.create_future()
                self._state.inflight_qos2_out[pid] = OutboundQoS2Flight(
                    packet_id=pid,
                    publish=packet,
                    state=OutboundQoS2State.PENDING_PUBREC,
                    future=future2,
                )
                await self._send(self._encode(packet))
                log.debug("Published QoS 2 to topic %r with packet_id=%d", packet.topic, pid)
                return await future2

    async def subscribe(
        self,
        filters: list[SubscriptionRequest],
        *,
        queue: asyncio.Queue[Message],
        auto_ack: bool = True,
        subscription_identifier: int | None = None,
    ) -> tuple[SubAck, dict[str, asyncio.Queue[Message]]]:
        """Send SUBSCRIBE and return (SubAck, {filter: queue}) after broker ACK.

        Queues are registered before SUBSCRIBE is sent so no messages are lost.
        Duplicate filters (already subscribed) are logged as warnings and skipped;
        they are still included in the SUBSCRIBE packet sent to the broker.

        ``queue`` is shared by all entries in this subscription. When it is full,
        delivery blocks, which stalls the read loop and ultimately pushes back on
        the broker through the TCP window.

        ``subscription_identifier`` (MQTT 5) is sent in the SUBSCRIBE properties;
        the broker echoes it on every PUBLISH this subscription causes, which lets
        ``_deliver`` attribute the message to the exact subscription that matched
        instead of guessing by filter specificity.
        """
        async with self._subscription_guards.hold(req.topic_filter for req in filters):
            result = await self._subscribe(
                filters,
                auto_ack=auto_ack,
                queue=queue,
                subscription_identifier=subscription_identifier,
            )
        await self.inbound.drain()
        return result

    async def _subscribe(
        self,
        filters: list[SubscriptionRequest],
        *,
        auto_ack: bool,
        queue: asyncio.Queue[Message],
        subscription_identifier: int | None,
    ) -> tuple[SubAck, dict[str, asyncio.Queue[Message]]]:
        new_entries: dict[str, SubscriptionEntry] = {}

        for req in filters:
            f = req.topic_filter
            if self._state.subscriptions.contains(f):
                log.warning("Filter %r already subscribed (ignored)", f)
            else:
                new_entries[f] = SubscriptionEntry(
                    queue=queue,
                    auto_ack=auto_ack,
                    actual_filter=topic_matching._shared_filter_to_actual(f, self._stripped_prefixes),
                    subscription_identifier=subscription_identifier,
                )

        self._state.subscriptions.add_many(new_entries)
        subscribed = False
        try:
            suback = await self._send_subscribe(filters, subscription_identifier)
            subscribed = True
            return suback, {f: entry.queue for f, entry in new_entries.items()}
        finally:
            if not subscribed:
                for f in new_entries:
                    self._state.subscriptions.remove(f)

    async def unsubscribe(self, filters: list[str]) -> tuple[tuple[str, ...], UnsubAck] | None:
        """Remove queues and unsubscribe filters without response observers.

        Returns the filters sent to the broker with its UNSUBACK, or ``None``
        when every broker subscription must stay active for request/response
        routing.
        """
        async with self._subscription_guards.hold(filters):
            return await self._unsubscribe(filters)

    async def _unsubscribe(self, filters: list[str]) -> tuple[tuple[str, ...], UnsubAck] | None:
        self._ensure_alive()
        observed_filters = [f for f in filters if self._state.subscriptions.has_response_observer(f)]
        broker_filters = [f for f in filters if f not in observed_filters]
        for f in filters:
            self._state.subscriptions.remove(f)

        acknowledged = None
        if broker_filters:
            unsuback = await self._send_unsubscribe(broker_filters)
            _warn_on_rejected_unsubscribe(broker_filters, unsuback)
            acknowledged = (tuple(broker_filters), unsuback)
        if observed_filters:
            requests = [SubscriptionRequest(topic_filter=f, qos=QoS.AT_MOST_ONCE) for f in observed_filters]
            try:
                await self._send_subscribe(requests, subscription_identifier=None)
            except Exception:  # noqa: BLE001 - must not discard the UNSUBACK already received
                log.warning("Restoring response observers %s failed", observed_filters, exc_info=True)
        return acknowledged

    async def add_response_observer(self, topic: str) -> None:
        """Keep an exact response topic subscribed for pending requests."""
        async with self._subscription_guards.hold([topic]):
            await self._add_response_observer(topic)

    async def _add_response_observer(self, topic: str) -> None:
        self._ensure_alive()
        if not self._state.subscriptions.add_response_observer(topic):
            return
        if self._state.subscriptions.contains(topic):
            return
        request = SubscriptionRequest(topic_filter=topic, qos=QoS.AT_MOST_ONCE)
        subscribed = False
        try:
            await self._send_subscribe([request], subscription_identifier=None)
            subscribed = True
        finally:
            if not subscribed:
                self._state.subscriptions.remove_response_observer(topic)

    async def remove_response_observer(self, topic: str) -> None:
        """Release an exact response topic unless an application owns it."""
        async with self._subscription_guards.hold([topic]):
            await self._remove_response_observer(topic)

    async def _remove_response_observer(self, topic: str) -> None:
        if not self._state.subscriptions.remove_response_observer(topic):
            return
        if self._state.subscriptions.contains(topic) or self._dead:
            return
        unsuback = await self._send_unsubscribe([topic])
        _warn_on_rejected_unsubscribe([topic], unsuback)

    async def _send_subscribe(
        self,
        filters: list[SubscriptionRequest],
        subscription_identifier: int | None,
    ) -> SubAck:
        self._ensure_alive()
        loop = asyncio.get_running_loop()
        pid = self._state.packet_ids.acquire()
        future: asyncio.Future[SubAck] = loop.create_future()
        self._state.pending_subs[pid] = future
        properties = (
            SubscribeProperties(subscription_identifier=subscription_identifier)
            if subscription_identifier is not None
            else None
        )
        try:
            await self._send(
                self._encode(
                    Subscribe(packet_id=pid, subscriptions=tuple(filters), properties=properties),
                ),
            )
            log.debug("Sent SUBSCRIBE with packet_id=%d", pid)
            suback = await future
            _raise_on_rejected_filters(filters, suback)
            return suback
        finally:
            self._state.pending_subs.pop(pid, None)
            self._state.packet_ids.release(pid)

    async def _send_unsubscribe(self, filters: list[str]) -> UnsubAck:
        self._ensure_alive()
        pid = self._state.packet_ids.acquire()
        loop = asyncio.get_running_loop()
        future: asyncio.Future[UnsubAck] = loop.create_future()
        self._state.pending_unsubs[pid] = future
        try:
            await self._send(
                encode(
                    Unsubscribe(packet_id=pid, topic_filters=tuple(filters)),
                    version=self._version,
                ),
            )
            log.debug("Sent UNSUBSCRIBE with packet_id=%d", pid)
            return await future
        finally:
            self._state.pending_unsubs.pop(pid, None)
            self._state.packet_ids.release(pid)

    async def ping(self, timeout: float | None = None) -> float:
        """Send PINGREQ and return RTT in seconds when PINGRESP is received."""
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        self._ping_waiters.append(future)
        t0 = loop.time()
        await self._send(self._encode(PingReq()))
        log.debug("Sent PINGREQ")
        try:
            await asyncio.wait_for(asyncio.shield(future), timeout=timeout)
        except asyncio.TimeoutError as e:
            self._ping_waiters.remove(future)
            msg = "PINGRESP not received within timeout"
            raise MQTTTimeoutError(msg) from e
        return loop.time() - t0

    async def send_auth(self, packet: Auth) -> None:
        """Send an AUTH packet (MQTT 5.0 enhanced authentication)."""
        if self._version != "5.0":
            msg = f"Feature is not supported for mqtt protocol version {self._version}"
            raise RuntimeError(
                msg,
            )
        await self._send(self._encode(packet))
        log.debug("Sent AUTH with reason_code=%d", packet.reason_code)

    async def _read_loop(self) -> None:
        while True:
            for packet in self._buf:
                await self._dispatch(packet)
            try:
                data = await self._transport.read(4096)
            except MQTTDisconnectedError:
                if self._disconnecting:
                    return
                raise
            self._buf.feed(data)

    async def _ping_loop(self) -> None:
        while True:
            await asyncio.sleep(self._keepalive)
            await self.ping(timeout=self._ping_timeout)

    async def _dispatch(self, packet: AnyPacket) -> None:  # noqa: C901
        log.debug("Received %r", packet)
        match packet:
            case Publish():
                await self._handle_publish(packet)
            case PubAck():
                await self._handle_puback(packet)
            case PubRec():
                await self._handle_pubrec(packet)
            case PubRel():
                await self._handle_pubrel(packet)
            case PubComp():
                await self._handle_pubcomp(packet)
            case SubAck():
                await self._handle_suback(packet)
            case UnsubAck():
                await self._handle_unsuback(packet)
            case PingResp():
                self._handle_pingresp()
            case Disconnect(reason_code=reason_code):
                # A broker-initiated DISCONNECT (session takeover, keepalive timeout,
                # admin kick) is a disconnection, not a protocol violation — raise it
                # as MQTTDisconnectedError so it takes the reconnect path.
                msg = f"Broker sent DISCONNECT (reason code 0x{reason_code:02X})"
                raise MQTTDisconnectedError(msg)
            case Auth():
                if self._version != "5.0":
                    msg = "Received AUTH packet in MQTT 3.1.1 session"
                    raise MQTTProtocolError(
                        msg,
                    )
                # AUTH exchange is handled by the caller via auth(); ignore here.
            case _:
                msg = f"Unexpected packet from broker: {packet!r}"
                raise MQTTProtocolError(msg)

    def _select_recipient(self, publish: Publish) -> InboundRecipient:
        return self.inbound.select_recipient(publish)

    async def _handle_publish(self, packet: Publish) -> None:
        await self.inbound.handle_publish(packet)

    async def _handle_pubrel(self, packet: PubRel) -> None:
        await self.inbound.handle_pubrel(packet)

    async def _handle_puback(self, packet: PubAck) -> None:
        flight = self._state.inflight_qos1.pop(packet.packet_id, None)
        if flight is None:
            msg = f"PUBACK for unknown packet_id {packet.packet_id}"
            raise MQTTProtocolError(msg)
        self._state.packet_ids.release(packet.packet_id)
        if not flight.future.done():
            flight.future.set_result(packet)
        log.debug("QoS 1 ack received for packet_id=%d", packet.packet_id)

    async def _handle_pubrec(self, packet: PubRec) -> None:
        flight = self._state.inflight_qos2_out.get(packet.packet_id)
        if flight is None:
            msg = f"PUBREC for unknown packet_id {packet.packet_id}"
            raise MQTTProtocolError(msg)
        if flight.state is not OutboundQoS2State.PENDING_PUBREC:
            msg = f"PUBREC in wrong state {flight.state} for packet_id {packet.packet_id}"
            raise MQTTProtocolError(
                msg,
            )
        # Unlike PUBACK/SUBACK, a negative PUBREC has no follow-up packet — MQTT 5
        # terminates the QoS 2 flow here instead of sending PUBCOMP. The error must
        # be raised from the handler because it also decides not to send PUBREL below.
        if packet.reason_code >= 0x80:
            self._state.inflight_qos2_out.pop(packet.packet_id, None)
            self._state.packet_ids.release(packet.packet_id)
            err = _publish_error(packet)
            if not flight.future.done():
                flight.future.set_exception(err)
            log.debug("QoS 2 ack received with error for packet_id=%d: %s", packet.packet_id, err)
            return

        flight.state = OutboundQoS2State.PENDING_PUBCOMP
        await self._send(self._encode(PubRel(packet_id=packet.packet_id)))
        log.debug("QoS 2 PUBREC received, sent PUBREL for packet_id=%d", packet.packet_id)

    async def _handle_pubcomp(self, packet: PubComp) -> None:
        flight = self._state.inflight_qos2_out.pop(packet.packet_id, None)
        if flight is None:
            msg = f"PUBCOMP for unknown packet_id {packet.packet_id}"
            raise MQTTProtocolError(msg)
        self._state.packet_ids.release(packet.packet_id)
        if not flight.future.done():
            flight.future.set_result(packet)
        log.debug("QoS 2 complete for packet_id=%d", packet.packet_id)

    async def _handle_suback(self, packet: SubAck) -> None:
        future = self._state.pending_subs.get(packet.packet_id)
        if future is None:
            msg = f"SUBACK for unknown packet_id {packet.packet_id}"
            raise MQTTProtocolError(msg)
        future.set_result(packet)

    async def _handle_unsuback(self, packet: UnsubAck) -> None:
        future = self._state.pending_unsubs.get(packet.packet_id)
        if future is None:
            msg = f"UNSUBACK for unknown packet_id {packet.packet_id}"
            raise MQTTProtocolError(
                msg,
            )
        future.set_result(packet)

    def _handle_pingresp(self) -> None:
        if self._ping_waiters:
            f = self._ping_waiters.pop(0)
            if not f.done():
                f.set_result(None)
        log.debug("PINGRESP received")

    def _encode(self, packet: AnyPacket) -> bytes:
        return encode(packet, version=self._version)

    async def send_packet(self, packet: AnyPacket) -> None:
        """Send an encoded packet on behalf of an internal protocol flow."""
        await self._send(self._encode(packet))

    async def abort(self) -> None:
        """Close the transport after a terminal inbound-flow failure."""
        await self._transport.close()

    async def _send(self, data: bytes) -> None:
        log.debug("Sending %d bytes", len(data))
        await self._transport.write(data)
