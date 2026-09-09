"""Inbound PUBLISH routing, acknowledgement, and persistent-session replay."""

import asyncio
import contextlib
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Protocol, TypeAlias, cast

from zmqtt._internal.packets.codec import AnyPacket
from zmqtt._internal.packets.publish import PubAck, PubComp, Publish, PubRec, PubRel
from zmqtt._internal.routing import InboundRecipient, RequestRouter
from zmqtt._internal.state import InboundQoS2Flight, InboundQoS2State, SessionState
from zmqtt._internal.subscription_index import SubscriptionEntry, SubscriptionSelection
from zmqtt._internal.types.message import Message
from zmqtt._internal.types.qos import QoS
from zmqtt.errors import MQTTProtocolError

log = logging.getLogger("zmqtt.protocol")


class InboundConnection(Protocol):
    """Wire operations needed by the inbound publish flow."""

    async def send_packet(self, packet: AnyPacket) -> None: ...

    async def abort(self) -> None: ...


class _NoSessionReplay:
    """Null Object used when the broker did not resume a session."""

    async def accept(
        self,
        publish: Publish,
        recipient: InboundRecipient,
    ) -> tuple[InboundRecipient, "_ReplayState"]:
        if recipient.request is None and recipient.subscription is None:
            log.warning("No subscriber for topic %r", publish.topic)
        return recipient, self

    async def drain(self, delivery: "InboundPublishFlow") -> "_ReplayState":
        del delivery
        return self

    def contains_packet_id(self, packet_id: int) -> bool:
        del packet_id
        return False

    def clear(self) -> "_ReplayState":
        return self


class _PersistentReplayIdle:
    """Persistent session with no messages waiting for a local recipient."""

    def __init__(
        self,
        *,
        buffer_size: int,
        timeout: float,
        connection: InboundConnection,
        owner: "InboundPublishFlow",
    ) -> None:
        self._buffer_size = buffer_size
        self._timeout = timeout
        self._connection = connection
        self._owner = owner

    async def accept(
        self,
        publish: Publish,
        recipient: InboundRecipient,
    ) -> tuple[InboundRecipient | None, "_ReplayState"]:
        if recipient.request is not None or recipient.subscription is not None:
            return recipient, self

        buffered = _PersistentReplayBuffered(
            buffer_size=self._buffer_size,
            timeout=self._timeout,
            connection=self._connection,
            owner=self._owner,
            idle=self,
        )
        return await buffered.accept(publish, recipient)

    async def drain(self, delivery: "InboundPublishFlow") -> "_ReplayState":
        del delivery
        return self

    def contains_packet_id(self, packet_id: int) -> bool:
        del packet_id
        return False

    def clear(self) -> "_ReplayState":
        return _NO_SESSION_REPLAY


class _PersistentReplayBuffered:
    """Persistent session with unmatched messages awaiting local subscriptions."""

    def __init__(
        self,
        *,
        buffer_size: int,
        timeout: float,
        connection: InboundConnection,
        owner: "InboundPublishFlow",
        idle: _PersistentReplayIdle,
    ) -> None:
        self._buffer_size = buffer_size
        self._timeout = timeout
        self._connection = connection
        self._owner = owner
        self._idle = idle
        self._publishes: deque[Publish] = deque()
        self._packet_ids: set[int] = set()
        self._topics: dict[str, int] = {}
        self._message_count = 0
        self._delivery_lock = asyncio.Lock()
        self._expiration_task: asyncio.Task[None] | None = None

    async def accept(
        self,
        publish: Publish,
        recipient: InboundRecipient,
    ) -> tuple[InboundRecipient | None, "_ReplayState"]:
        if recipient.request is not None:
            return recipient, self
        if recipient.subscription is not None and publish.topic not in self._topics:
            return recipient, self
        if publish.packet_id is not None and publish.packet_id in self._packet_ids:
            return None, self
        if self._buffer_size and self._message_count >= self._buffer_size:
            msg = f"Persistent-session replay buffer limit of {self._buffer_size} messages exceeded"
            await self._connection.abort()
            raise MQTTProtocolError(msg)

        self._publishes.append(publish)
        self._message_count += 1
        self._topics[publish.topic] = self._topics.get(publish.topic, 0) + 1
        if publish.packet_id is not None:
            self._packet_ids.add(publish.packet_id)
        if self._expiration_task is None:
            self._expiration_task = asyncio.create_task(
                self._expire_after_timeout(),
                name="zmqtt-session-replay-expiration",
            )
        log.debug("Buffered persistent-session replay for topic %r", publish.topic)
        return None, self

    async def drain(self, delivery: "InboundPublishFlow") -> "_ReplayState":
        async with self._delivery_lock:
            await self._deliver_pending(delivery)
            if self._message_count:
                return self
            self._cancel_expiration()
            return self._idle

    async def expire(self, delivery: "InboundPublishFlow") -> "_ReplayState":
        """Deliver newly matched messages, then drop the rest without ACK."""
        async with self._delivery_lock:
            await self._deliver_pending(delivery)
            dropped = self._drop_pending()
            if dropped:
                log.warning(
                    "Dropped %d persistent-session replay messages after %.1f seconds without delivery",
                    dropped,
                    self._timeout,
                )
                return _NO_SESSION_REPLAY
            return self._idle

    def contains_packet_id(self, packet_id: int) -> bool:
        return packet_id in self._packet_ids

    def clear(self) -> "_ReplayState":
        self._cancel_expiration()
        self._publishes.clear()
        self._packet_ids.clear()
        self._topics.clear()
        self._message_count = 0
        return _NO_SESSION_REPLAY

    async def _expire_after_timeout(self) -> None:
        try:
            await asyncio.sleep(self._timeout)
            await self._owner.expire_replay(self)
        except asyncio.CancelledError:
            return
        except Exception:
            log.exception("Failed to expire persistent-session replay buffer")
            with contextlib.suppress(Exception):
                await self._connection.abort()
        finally:
            if self._expiration_task is asyncio.current_task():
                self._expiration_task = None

    def _cancel_expiration(self) -> None:
        task = self._expiration_task
        self._expiration_task = None
        if task is not None and task is not asyncio.current_task():
            task.cancel()

    async def _deliver_pending(self, delivery: "InboundPublishFlow") -> None:
        unmatched: deque[Publish] = deque()
        pending: deque[Publish] = deque()
        try:
            while self._publishes:
                pending = self._publishes
                self._publishes = deque()
                while pending:
                    publish = pending.popleft()
                    recipient = delivery.select_recipient(publish)
                    subscription = recipient.subscription
                    if recipient.request is None and subscription is None:
                        unmatched.append(publish)
                        continue
                    queue = subscription[1].queue if subscription is not None else None
                    if queue is not None and queue.full():
                        unmatched.append(publish)
                        continue
                    try:
                        await delivery.deliver_replay(publish, recipient)
                    except BaseException:
                        unmatched.append(publish)
                        raise
                    else:
                        self._discard(publish)
        finally:
            unmatched.extend(pending)
            unmatched.extend(self._publishes)
            self._publishes = unmatched

    def _drop_pending(self) -> int:
        dropped = self._message_count
        self._publishes.clear()
        self._packet_ids.clear()
        self._topics.clear()
        self._message_count = 0
        return dropped

    def _discard(self, publish: Publish) -> None:
        self._message_count -= 1
        topic_count = self._topics[publish.topic] - 1
        if topic_count:
            self._topics[publish.topic] = topic_count
        else:
            self._topics.pop(publish.topic)
        if publish.packet_id is not None:
            self._packet_ids.discard(publish.packet_id)


_ReplayState: TypeAlias = _NoSessionReplay | _PersistentReplayIdle | _PersistentReplayBuffered
_NO_SESSION_REPLAY: _ReplayState = _NoSessionReplay()


class InboundPublishFlow:
    """Own the complete lifecycle of incoming MQTT application messages.

    The flow selects one application recipient, executes inbound QoS handshakes,
    and holds unmatched messages replayed by a resumed persistent session until
    a compatible local subscription becomes available. Connection lifecycle,
    packet decoding, and outbound publishing remain MQTTProtocol concerns.
    """

    def __init__(
        self,
        *,
        connection: InboundConnection,
        state: SessionState,
        request_router: RequestRouter | None,
        session_replay_buffer_size: int,
        session_replay_timeout: float,
    ) -> None:
        self._connection = connection
        self._state = state
        self._request_router = request_router
        self._session_replay_buffer_size = session_replay_buffer_size
        self._session_replay_timeout = session_replay_timeout
        self._replay: _ReplayState = _NO_SESSION_REPLAY

    def begin_session(self, *, session_present: bool) -> None:
        """Set whether this connection resumed broker-side session state."""
        self._replay.clear()
        self._replay = (
            _PersistentReplayIdle(
                buffer_size=self._session_replay_buffer_size,
                timeout=self._session_replay_timeout,
                connection=self._connection,
                owner=self,
            )
            if session_present
            else _NO_SESSION_REPLAY
        )

    def clear(self) -> None:
        """Discard connection-local replay state without acknowledging it."""
        self._replay = self._replay.clear()

    async def drain(self) -> None:
        """Deliver held messages for which a recipient now has capacity."""
        replay = self._replay
        next_replay = await replay.drain(self)
        if self._replay is replay:
            self._replay = next_replay

    async def expire_replay(self, replay: _PersistentReplayBuffered) -> None:
        """Expire a buffered state only if it is still active."""
        if self._replay is not replay:
            return
        next_replay = await replay.expire(self)
        if self._replay is replay:
            self._replay = next_replay

    async def handle_publish(self, packet: Publish) -> None:
        """Route one inbound PUBLISH through its QoS receive flow."""
        if packet.qos is QoS.AT_MOST_ONCE:
            recipient = await self._select_recipient_or_buffer(packet)
            if recipient is not None:
                await self._deliver(recipient, ack_callback=None)
            return None
        if packet.qos is QoS.AT_LEAST_ONCE:
            return await self._handle_qos1_publish(packet)
        if packet.qos is QoS.EXACTLY_ONCE:
            return await self._handle_qos2_publish(packet)
        return None

    async def handle_pubrel(self, packet: PubRel) -> None:
        """Complete an inbound QoS 2 exchange."""
        flight = self._state.inflight_qos2_in.pop(packet.packet_id, None)
        if flight is None:
            msg = f"PUBREL for unknown packet_id {packet.packet_id}"
            raise MQTTProtocolError(msg)
        await self._connection.send_packet(PubComp(packet_id=packet.packet_id))
        if not flight.delivered:
            await self._deliver(flight.recipient, ack_callback=None)

    def select_recipient(self, publish: Publish) -> InboundRecipient:
        """Select the exclusive request or subscription recipient for a PUBLISH."""
        message = self._make_message(publish)
        if self._request_router is not None:
            request = self._request_router.claim(message)
            if request is not None:
                return InboundRecipient(
                    message=message,
                    request=request,
                )

        selection = self._select_subscription(publish)
        if selection.identifier_missing:
            identifier = publish.properties.subscription_identifier if publish.properties else None
            log.warning(
                "No subscription with identifier %r for topic %r, falling back to filter matching",
                identifier,
                publish.topic,
            )
        if selection.tied_filters:
            log.warning(
                "Multiple equally-specific subscribers for %r: %s, delivering to first",
                publish.topic,
                list(selection.tied_filters),
            )
        return InboundRecipient(
            message=message,
            subscription=selection.recipient,
        )

    def _select_subscription(self, publish: Publish) -> SubscriptionSelection:
        identifier = publish.properties.subscription_identifier if publish.properties else None
        if identifier is None:
            return self._state.subscriptions.select(topic=publish.topic)
        return self._state.subscriptions.select_by_identifier(
            topic=publish.topic,
            identifier=identifier,
        )

    async def _select_recipient_or_buffer(self, publish: Publish) -> InboundRecipient | None:
        replay = self._replay
        selected = self.select_recipient(publish)
        recipient, next_replay = await replay.accept(publish, selected)
        if self._replay is replay:
            self._replay = next_replay
        return recipient

    async def deliver_replay(
        self,
        publish: Publish,
        recipient: InboundRecipient,
    ) -> None:
        """Deliver one message selected by the active replay state."""
        if publish.qos is QoS.AT_MOST_ONCE:
            await self._deliver(recipient, ack_callback=None)
        elif publish.qos is QoS.AT_LEAST_ONCE:
            await self._deliver_qos1_publish(publish, recipient)
        elif publish.qos is QoS.EXACTLY_ONCE:
            await self._accept_qos2_publish(publish, recipient)

    async def _handle_qos1_publish(self, packet: Publish) -> None:
        if packet.packet_id is None:
            msg = "Cannot publish without packet id"
            raise ValueError(msg)
        if self._replay.contains_packet_id(packet.packet_id):
            return
        recipient = await self._select_recipient_or_buffer(packet)
        if recipient is None:
            return
        await self._deliver_qos1_publish(packet, recipient)

    async def _deliver_qos1_publish(
        self,
        packet: Publish,
        recipient: InboundRecipient,
    ) -> None:
        if packet.packet_id is None:
            msg = "Cannot publish without packet id"
            raise ValueError(msg)
        if recipient.auto_ack:
            await self._deliver(recipient, ack_callback=None)
            await self._connection.send_packet(PubAck(packet_id=packet.packet_id))
        else:
            acked = False

            async def _puback() -> None:
                nonlocal acked
                if acked:
                    return
                acked = True
                await self._connection.send_packet(PubAck(packet_id=cast("int", packet.packet_id)))

            await self._deliver(recipient, ack_callback=_puback)

    async def _handle_qos2_publish(self, packet: Publish) -> None:
        if packet.packet_id is None:
            msg = "Cannot publish without packet id"
            raise ValueError(msg)
        if packet.packet_id in self._state.inflight_qos2_in:
            # PUBREC already sent — resend it (duplicate PUBLISH after PUBREC)
            await self._connection.send_packet(PubRec(packet_id=packet.packet_id))
            return
        if packet.packet_id in self._state.pending_ack_qos2_in:
            # Duplicate PUBLISH while app hasn't called msg.ack() yet — ignore
            return
        if self._replay.contains_packet_id(packet.packet_id):
            return
        recipient = await self._select_recipient_or_buffer(packet)
        if recipient is None:
            return
        await self._accept_qos2_publish(packet, recipient)

    async def _accept_qos2_publish(
        self,
        packet: Publish,
        recipient: InboundRecipient,
    ) -> None:
        if packet.packet_id is None:
            msg = "Cannot publish without packet id"
            raise ValueError(msg)
        if recipient.auto_ack:
            self._state.inflight_qos2_in[packet.packet_id] = InboundQoS2Flight(
                packet_id=packet.packet_id,
                recipient=recipient,
                state=InboundQoS2State.PENDING_PUBREL,
            )
            await self._connection.send_packet(PubRec(packet_id=packet.packet_id))
        else:
            self._state.pending_ack_qos2_in.add(packet.packet_id)
            pid = packet.packet_id

            async def _pubrec() -> None:
                self._state.pending_ack_qos2_in.discard(pid)
                self._state.inflight_qos2_in[pid] = InboundQoS2Flight(
                    packet_id=pid,
                    recipient=recipient,
                    state=InboundQoS2State.PENDING_PUBREL,
                    delivered=True,
                )
                await self._connection.send_packet(PubRec(packet_id=pid))

            await self._deliver(recipient, ack_callback=_pubrec)

    async def _deliver(
        self,
        recipient: InboundRecipient,
        ack_callback: Callable[[], Awaitable[None]] | None,
    ) -> None:
        if recipient.request is not None:
            recipient.request.deliver()
            return

        subscription = recipient.subscription
        if subscription is None:
            return

        filter_, entry = subscription
        queue = entry.queue
        if queue is None:
            log.warning("Dropped message for topic %r: filter %r has no consumer", recipient.message.topic, filter_)
            return

        message = recipient.message
        if not entry.auto_ack and ack_callback is not None:
            message._ack_callback = ack_callback  # noqa: SLF001 - internal delivery contract

        try:
            queue.put_nowait(message)
        except asyncio.QueueFull:
            if not await self._put_when_room(entry, queue, message, filter_):
                return
        log.debug("Delivered message for topic %r to filter %r", message.topic, filter_)

    async def _put_when_room(
        self,
        entry: SubscriptionEntry,
        queue: "asyncio.Queue[Message]",
        message: Message,
        filter_: str,
    ) -> bool:
        """Wait for buffer space, abandoning the message if the owner detaches.

        Blocking is the backpressure contract, but it must not outlive the consumer:
        with nothing draining the queue this put would hold the read loop forever.
        """
        put = asyncio.ensure_future(queue.put(message))
        detached = asyncio.ensure_future(entry.detached.wait())
        try:
            done, _ = await asyncio.wait({put, detached}, return_when=asyncio.FIRST_COMPLETED)
        except BaseException:
            put.cancel()
            detached.cancel()
            raise
        detached.cancel()
        if put in done:
            put.result()
            return True

        put.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await put
        log.warning(
            "Dropped message for topic %r: filter %r detached while its buffer was full",
            message.topic,
            filter_,
        )
        return False

    @staticmethod
    def _make_message(publish: Publish) -> Message:
        return Message(
            topic=publish.topic,
            payload=publish.payload,
            qos=publish.qos,
            retain=publish.retain,
            properties=publish.properties,
        )
