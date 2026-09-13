"""Base class for E2E broker tests.

Subclasses set host/port/version and get all tests for free.
Run with:  pytest -m broker
"""

import abc
import asyncio
import contextlib
import logging
import ssl
import uuid
from collections.abc import AsyncGenerator
from dataclasses import FrozenInstanceError
from typing import ClassVar, Literal

import pytest

from zmqtt import (
    MQTTClient,
    MQTTDisconnectedError,
    MQTTProtocolError,
    PublishProperties,
    QoS,
    ReconnectConfig,
    RetainHandling,
    Subscription,
    Will,
    WillProperties,
    create_client,
)
from zmqtt._internal.transport.base import Transport
from zmqtt._internal.transport.tcp import open_tcp


@pytest.mark.broker
class BrokerTestBase(abc.ABC):
    host: ClassVar[str] = "127.0.0.1"
    port: ClassVar[int] = 1883
    version: ClassVar[Literal["3.1.1", "5.0"]] = "3.1.1"
    supports_persistent_sessions: ClassVar[bool] = True

    @abc.abstractmethod
    async def handle_sub_duplicates(
        self,
        *,
        sub: Subscription,
        n_duplicates: int,
    ) -> None: ...

    @pytest.fixture
    async def mqtt_client(self) -> AsyncGenerator[MQTTClient]:
        async with MQTTClient(
            self.host,
            self.port,
            client_id=f"zmqtt-test-{uuid.uuid4().hex[:8]}",
            version=self.version,
        ) as client:
            yield client

    async def trigger_session_takeover(self, *, client_id: str) -> None:
        """Make the broker drop a connection by claiming its client identifier.

        A losing MQTT 5 takeover can return DISCONNECT (0x8E) while the client
        awaits CONNACK. MQTT 3.1.1 can only close the connection.
        """
        takeover_error = MQTTProtocolError if self.version == "5.0" else MQTTDisconnectedError

        with contextlib.suppress(takeover_error):
            async with MQTTClient(
                self.host,
                self.port,
                client_id=client_id,
                reconnect=ReconnectConfig(enabled=False),
                version=self.version,
            ):
                pass

    async def force_tcp_disconnect(self, client: MQTTClient) -> None:
        """Close the TCP connection without sending MQTT DISCONNECT."""
        protocol = client._protocol
        assert protocol is not None
        await protocol._transport.close()

    def persistent_client(
        self,
        *,
        client_id: str,
        session_replay_timeout: float = 30.0,
    ) -> MQTTClient:
        return MQTTClient(
            self.host,
            self.port,
            client_id=client_id,
            clean_session=False,
            reconnect=ReconnectConfig(enabled=False),
            version=self.version,
            session_expiry_interval=60 if self.version == "5.0" else 0,
            session_replay_timeout=session_replay_timeout,
        )

    async def test_ping(self, mqtt_client: MQTTClient) -> None:
        await mqtt_client.ping()

    async def test_publish_qos0(self, mqtt_client: MQTTClient, topic: str) -> None:
        await mqtt_client.publish(topic, b"hello")

    async def test_publish_qos1(self, mqtt_client: MQTTClient, topic: str) -> None:
        await mqtt_client.publish(topic, b"hello", qos=QoS.AT_LEAST_ONCE)

    async def test_publish_qos2(self, mqtt_client: MQTTClient, topic: str) -> None:
        await mqtt_client.publish(topic, b"hello", qos=QoS.EXACTLY_ONCE)

    async def test_subscribe_receive_qos0(
        self,
        mqtt_client: MQTTClient,
        topic: str,
    ) -> None:
        async with mqtt_client.subscribe(topic) as sub:
            await mqtt_client.publish(topic, b"payload-qos0")
            msg = await sub.get_message()

        assert msg.topic == topic
        assert msg.payload == b"payload-qos0"

    async def test_subscribe_receive_qos1(
        self,
        mqtt_client: MQTTClient,
        topic: str,
    ) -> None:
        async with mqtt_client.subscribe(topic, qos=QoS.AT_LEAST_ONCE) as sub:
            await mqtt_client.publish(topic, b"payload-qos1", qos=QoS.AT_LEAST_ONCE)
            msg = await asyncio.wait_for(sub.get_message(), timeout=5.0)
        assert msg.payload == b"payload-qos1"

    async def test_subscribe_receive_qos2(
        self,
        mqtt_client: MQTTClient,
        topic: str,
    ) -> None:
        async with mqtt_client.subscribe(topic, qos=QoS.EXACTLY_ONCE) as sub:
            await mqtt_client.publish(topic, b"payload-qos2", qos=QoS.EXACTLY_ONCE)
            msg = await asyncio.wait_for(sub.get_message(), timeout=5.0)

        assert msg.payload == b"payload-qos2"

    async def test_retain_handling_do_not_send(self, mqtt_client: MQTTClient, topic: str) -> None:
        if self.version != "5.0":
            pytest.skip("Retain handling requires MQTT 5.0")
        await mqtt_client.publish(topic, b"retained", qos=QoS.AT_LEAST_ONCE, retain=True)

        async with mqtt_client.subscribe(
            topic,
            qos=QoS.AT_LEAST_ONCE,
            retain_handling=RetainHandling.DO_NOT_SEND,
        ) as sub:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(sub.get_message(), timeout=0.2)

    async def test_subscribe_wildcard(
        self,
        mqtt_client: MQTTClient,
        topic: str,
    ) -> None:
        prefix = f"{topic}/wild"
        async with mqtt_client.subscribe(f"{prefix}/#") as sub:
            await mqtt_client.publish(f"{prefix}/a/b", b"w1")
            await mqtt_client.publish(f"{prefix}/c", b"w2")
            msgs = [await asyncio.wait_for(sub.get_message(), timeout=5.0) for _ in range(2)]

        assert {m.payload for m in msgs} == {b"w1", b"w2"}
        assert all(m.topic.startswith(prefix) for m in msgs)

    async def test_unsubscribe_removes_only_exact_filter(
        self,
        mqtt_client: MQTTClient,
        topic: str,
    ) -> None:
        wildcard = mqtt_client.subscribe(f"{topic}/+", qos=QoS.AT_LEAST_ONCE)
        concrete = mqtt_client.subscribe(f"{topic}/concrete", qos=QoS.AT_LEAST_ONCE)
        await wildcard.start()
        await concrete.start()

        await wildcard.stop()
        await mqtt_client.publish(f"{topic}/concrete", b"exact-remains", qos=QoS.AT_LEAST_ONCE)

        message = await asyncio.wait_for(concrete.get_message(), timeout=5.0)
        assert message.payload == b"exact-remains"

    async def test_stop_returns_broker_acknowledgement(self, mqtt_client: MQTTClient, topic: str) -> None:
        sub = mqtt_client.subscribe(f"{topic}/a", f"{topic}/b")
        await sub.start()

        result = await sub.stop()

        assert result is not None
        assert result.topic_filters == (f"{topic}/a", f"{topic}/b")
        assert result.reason_codes == ((0x00, 0x00) if self.version == "5.0" else ())
        assert result.failures == {}

    async def test_stop_after_disconnect_returns_none(self, mqtt_client: MQTTClient, topic: str) -> None:
        sub = mqtt_client.subscribe(topic)
        await sub.start()
        await mqtt_client.disconnect()

        result = await sub.stop()

        assert result is None

    async def test_repeated_stop_returns_none(self, mqtt_client: MQTTClient, topic: str) -> None:
        sub = mqtt_client.subscribe(topic)
        await sub.start()
        await sub.stop()

        result = await sub.stop()

        assert result is None

    async def test_stop_after_connection_loss_logs_failure(self, topic: str, caplog: pytest.LogCaptureFixture) -> None:
        async with MQTTClient(
            self.host,
            self.port,
            reconnect=ReconnectConfig(enabled=False),
            version=self.version,
        ) as client:
            sub = client.subscribe(topic)
            await sub.start()
            await self.force_tcp_disconnect(client)

            with caplog.at_level(logging.WARNING, logger="zmqtt.client"):
                result = await sub.stop()

        assert result is None
        assert any(record.exc_info for record in caplog.records if record.name == "zmqtt.client")

    async def test_unsubscribe_identifier_preserves_other_subscription(
        self,
        mqtt_client: MQTTClient,
        topic: str,
    ) -> None:
        if self.version != "5.0":
            pytest.skip("Subscription identifiers require MQTT 5.0")

        concrete_topic = f"{topic}/concrete"
        wildcard = mqtt_client.subscribe(
            f"{topic}/+",
            qos=QoS.AT_LEAST_ONCE,
            subscription_identifier=1,
        )
        concrete = mqtt_client.subscribe(
            concrete_topic,
            qos=QoS.AT_LEAST_ONCE,
            subscription_identifier=2,
        )
        await wildcard.start()
        await concrete.start()

        await wildcard.stop()
        await mqtt_client.publish(concrete_topic, b"identifier-remains", qos=QoS.AT_LEAST_ONCE)

        message = await asyncio.wait_for(concrete.get_message(), timeout=5.0)
        await concrete.stop()
        assert message.payload == b"identifier-remains"
        assert message.properties is not None
        assert message.properties.subscription_identifier == 2

    async def test_resubscribe_same_filter_uses_new_subscription(
        self,
        mqtt_client: MQTTClient,
        topic: str,
    ) -> None:
        topic_filter = f"{topic}/resubscribe"
        first = mqtt_client.subscribe(
            topic_filter,
            qos=QoS.AT_LEAST_ONCE,
            subscription_identifier=1 if self.version == "5.0" else None,
        )
        await first.start()
        await first.stop()

        async with mqtt_client.subscribe(
            topic_filter,
            qos=QoS.AT_LEAST_ONCE,
            subscription_identifier=2 if self.version == "5.0" else None,
        ) as second:
            await mqtt_client.publish(topic_filter, b"resubscribed", qos=QoS.AT_LEAST_ONCE)
            message = await asyncio.wait_for(second.get_message(), timeout=5.0)

        assert message.payload == b"resubscribed"
        if self.version == "5.0":
            assert message.properties is not None
            assert message.properties.subscription_identifier == 2

    async def test_message_ordering(self, mqtt_client: MQTTClient, topic: str) -> None:
        payloads = [str(i).encode() for i in range(5)]
        async with mqtt_client.subscribe(topic, qos=QoS.AT_LEAST_ONCE) as sub:
            for p in payloads:
                await mqtt_client.publish(topic, p, qos=QoS.AT_LEAST_ONCE)
            received = [(await asyncio.wait_for(sub.get_message(), timeout=5.0)).payload for _ in payloads]

        assert received == payloads

    async def test_shared_subscription(self, topic: str) -> None:
        group = f"zmqtt-sh-{uuid.uuid4().hex[:8]}"
        # Strip leading slash to avoid $share/group//topic (double slash) which some brokers reject
        bare_topic = topic.lstrip("/")
        shared_filter = f"$share/{group}/{bare_topic}"

        async with (
            MQTTClient(self.host, self.port, version=self.version) as c1,
            MQTTClient(self.host, self.port, version=self.version) as c2,
            MQTTClient(
                self.host, self.port, client_id=f"zmqtt-pub-{uuid.uuid4().hex[:8]}", version=self.version
            ) as pub,
            c1.subscribe(shared_filter, qos=QoS.AT_LEAST_ONCE) as sub1,
            c2.subscribe(shared_filter, qos=QoS.AT_LEAST_ONCE) as sub2,
        ):
            for i in range(10):
                await pub.publish(bare_topic, f"msg-{i}".encode(), qos=QoS.AT_LEAST_ONCE)

            remain_msgs = 10
            e = asyncio.Event()

            async def drain(sub: Subscription) -> None:
                nonlocal remain_msgs
                async for _ in sub:
                    remain_msgs -= 1
                    if remain_msgs < 1:
                        e.set()

            task1 = asyncio.create_task(drain(sub1))
            task2 = asyncio.create_task(drain(sub2))
            await asyncio.wait_for(e.wait(), timeout=5.0)

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(sub1.get_message(), timeout=0.2)

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(sub2.get_message(), timeout=0.2)
            task1.cancel()
            task2.cancel()

    async def test_subscription_identifier_overlapping(self, topic: str) -> None:
        """MQTT 5 subscription identifiers route overlapping subscriptions: a
        ``$share`` subscription (id 1) and its plain twin (id 2) on the same
        topic are indistinguishable by client-side filter matching alone — the
        broker's echoed identifier tags each delivery with the subscription
        that caused it, so each receives its own copy.

        On 3.1.1 the parameter itself must refuse loudly (v5-only feature).
        """
        bare_topic = topic.lstrip("/")
        group = f"zmqtt-si-{uuid.uuid4().hex[:8]}"

        async with MQTTClient(self.host, self.port, version=self.version) as client:
            if self.version != "5.0":
                with pytest.raises(RuntimeError, match=r"requires MQTT 5\.0"):
                    client.subscribe(bare_topic, qos=QoS.AT_LEAST_ONCE, subscription_identifier=1)
                return

            async with (
                client.subscribe(
                    f"$share/{group}/{bare_topic}",
                    qos=QoS.AT_LEAST_ONCE,
                    subscription_identifier=1,
                ) as shared,
                client.subscribe(bare_topic, qos=QoS.AT_LEAST_ONCE, subscription_identifier=2) as plain,
                MQTTClient(self.host, self.port, version=self.version) as publisher,
            ):
                for i in range(3):
                    await publisher.publish(bare_topic, f"m{i}".encode(), qos=QoS.AT_LEAST_ONCE)

                shared_msgs = [await asyncio.wait_for(shared.get_message(), timeout=5.0) for _ in range(3)]
                plain_msgs = [await asyncio.wait_for(plain.get_message(), timeout=5.0) for _ in range(3)]

        assert {m.properties.subscription_identifier for m in shared_msgs if m.properties} == {1}
        assert {m.properties.subscription_identifier for m in plain_msgs if m.properties} == {2}

    async def test_reconnect_subscription_survives(self, topic: str) -> None:
        client_id = f"zmqtt-reconnect-{uuid.uuid4().hex[:8]}"
        reconnect = ReconnectConfig(enabled=True, initial_delay=0.5, max_delay=1.0)

        async with (
            MQTTClient(
                self.host,
                self.port,
                client_id=client_id,
                reconnect=reconnect,
                version=self.version,
            ) as client,
            client.subscribe(topic) as sub,
        ):
            await self.trigger_session_takeover(client_id=client_id)

            async with MQTTClient(self.host, self.port, version=self.version) as publisher:
                for _ in range(50):
                    await publisher.publish(topic, b"after-reconnect")
                    try:
                        msg = await asyncio.wait_for(sub.get_message(), timeout=0.1)
                    except asyncio.TimeoutError:
                        await asyncio.sleep(0.1)
                    else:
                        break
                else:
                    pytest.fail("Subscription did not recover within 10 s")

        assert msg.payload == b"after-reconnect"

    async def test_subscription_survives_single_allowed_reconnect_attempt(self, topic: str) -> None:
        client = MQTTClient(
            self.host,
            self.port,
            client_id=f"zmqtt-single-reconnect-{uuid.uuid4().hex[:8]}",
            reconnect=ReconnectConfig(initial_delay=0.0, max_attempts=1),
            version=self.version,
        )

        async with (
            client,
            client.subscribe(topic) as subscription,
            MQTTClient(self.host, self.port, version=self.version) as publisher,
        ):
            message_task = asyncio.create_task(anext(subscription))
            await asyncio.sleep(0)

            await self.force_tcp_disconnect(client)
            await asyncio.sleep(0.5)
            await publisher.publish(topic, b"after-single-reconnect")
            message = await message_task

        assert message.payload == b"after-single-reconnect"

    @pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
    async def test_persistent_session_replay_waits_for_subscription(
        self,
        topic: str,
        qos: QoS,
    ) -> None:
        if not self.supports_persistent_sessions:
            pytest.skip("Broker test configuration does not retain persistent sessions")
        client_id = f"zmqtt-session-replay-{uuid.uuid4().hex[:8]}"
        original = self.persistent_client(client_id=client_id)
        await original.connect()
        original_subscription = original.subscribe(topic, qos=qos)
        await original_subscription.start()
        await original.disconnect()

        async with MQTTClient(self.host, self.port, version=self.version) as publisher:
            await publisher.publish(topic, b"while-offline", qos=qos)

        resumed = self.persistent_client(client_id=client_id)
        await resumed.connect()
        await asyncio.sleep(0.2)
        replay_subscription = resumed.subscribe(topic, qos=qos)
        await replay_subscription.start()
        message = await asyncio.wait_for(replay_subscription.get_message(), timeout=5.0)
        assert message.payload == b"while-offline"
        assert message.qos is qos
        await replay_subscription.stop()
        await resumed.disconnect()

    async def test_persistent_session_replay_preserves_manual_ack(
        self,
        topic: str,
    ) -> None:
        if not self.supports_persistent_sessions:
            pytest.skip("Broker test configuration does not retain persistent sessions")
        client_id = f"zmqtt-session-manual-ack-{uuid.uuid4().hex[:8]}"
        original = self.persistent_client(client_id=client_id)
        await original.connect()
        original_subscription = original.subscribe(topic, qos=QoS.AT_LEAST_ONCE)
        await original_subscription.start()
        await original.disconnect()

        async with MQTTClient(self.host, self.port, version=self.version) as publisher:
            await publisher.publish(topic, b"ack-after-replay", qos=QoS.AT_LEAST_ONCE)

        resumed = self.persistent_client(client_id=client_id)
        await resumed.connect()
        await asyncio.sleep(0.2)
        manual_subscription = resumed.subscribe(
            topic,
            qos=QoS.AT_LEAST_ONCE,
            auto_ack=False,
        )
        await manual_subscription.start()
        first_delivery = await asyncio.wait_for(manual_subscription.get_message(), timeout=5.0)
        await resumed.disconnect()

        replayed_again = self.persistent_client(client_id=client_id)
        await replayed_again.connect()
        await asyncio.sleep(0.2)
        final_subscription = replayed_again.subscribe(topic, qos=QoS.AT_LEAST_ONCE)
        await final_subscription.start()
        second_delivery = await asyncio.wait_for(final_subscription.get_message(), timeout=5.0)
        assert first_delivery.payload == b"ack-after-replay"
        assert second_delivery.payload == first_delivery.payload
        await final_subscription.stop()
        await replayed_again.disconnect()

    async def test_persistent_session_replay_respects_subscription_buffer(
        self,
        topic: str,
    ) -> None:
        if not self.supports_persistent_sessions:
            pytest.skip("Broker test configuration does not retain persistent sessions")
        client_id = f"zmqtt-session-backpressure-{uuid.uuid4().hex[:8]}"
        original = self.persistent_client(client_id=client_id)
        await original.connect()
        original_subscription = original.subscribe(topic, qos=QoS.AT_LEAST_ONCE)
        await original_subscription.start()
        await original.disconnect()

        async with MQTTClient(self.host, self.port, version=self.version) as publisher:
            await publisher.publish(topic, b"first", qos=QoS.AT_LEAST_ONCE)
            await publisher.publish(topic, b"second", qos=QoS.AT_LEAST_ONCE)

        resumed = self.persistent_client(client_id=client_id)
        await resumed.connect()
        await asyncio.sleep(0.2)
        replay_subscription = resumed.subscribe(
            topic,
            qos=QoS.AT_LEAST_ONCE,
            receive_buffer_size=1,
        )
        await replay_subscription.start()
        first = await asyncio.wait_for(replay_subscription.get_message(), timeout=5.0)
        second = await asyncio.wait_for(replay_subscription.get_message(), timeout=5.0)
        assert [first.payload, second.payload] == [b"first", b"second"]
        await replay_subscription.stop()
        await resumed.disconnect()

    @pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
    async def test_persistent_session_replay_drops_without_ack_after_timeout(
        self,
        topic: str,
        qos: QoS,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        if not self.supports_persistent_sessions:
            pytest.skip("Broker test configuration does not retain persistent sessions")
        client_id = f"zmqtt-session-timeout-{uuid.uuid4().hex[:8]}"
        original = self.persistent_client(client_id=client_id)
        await original.connect()
        original_subscription = original.subscribe(topic, qos=qos)
        await original_subscription.start()
        await original.disconnect()

        async with MQTTClient(self.host, self.port, version=self.version) as publisher:
            await publisher.publish(topic, b"expired", qos=qos)

        resumed = self.persistent_client(
            client_id=client_id,
            session_replay_timeout=0.1,
        )
        await resumed.connect()
        await asyncio.sleep(0.5)
        await resumed.disconnect()

        replayed_again = self.persistent_client(client_id=client_id)
        await replayed_again.connect()
        await asyncio.sleep(0.2)
        replay_subscription = replayed_again.subscribe(topic, qos=qos)
        await replay_subscription.start()
        message = await asyncio.wait_for(replay_subscription.get_message(), timeout=5.0)
        assert message.payload == b"expired"
        assert any("Dropped 1 persistent-session replay messages" in message for message in caplog.messages)
        await replay_subscription.stop()
        await replayed_again.disconnect()

    async def test_tcp_disconnect_wakes_subscription_when_reconnect_disabled(self, topic: str) -> None:
        client = MQTTClient(
            self.host,
            self.port,
            reconnect=ReconnectConfig(enabled=False),
            version=self.version,
        )

        async with client, client.subscribe(topic) as subscription:
            message_task = asyncio.create_task(anext(subscription))
            await asyncio.sleep(0)

            await self.force_tcp_disconnect(client)

            with pytest.raises(MQTTDisconnectedError):
                await asyncio.wait_for(message_task, timeout=5.0)
            with pytest.raises(MQTTDisconnectedError, match="No active connection information"):
                _ = client.connection_info

    async def test_on_connection_recovery_failed_called(self) -> None:
        callback_calls = 0

        async def on_connection_recovery_failed() -> None:
            nonlocal callback_calls
            callback_calls += 1
            with pytest.raises(MQTTDisconnectedError, match="No active connection information"):
                _ = client.connection_info

        client = MQTTClient(
            self.host,
            self.port,
            reconnect=ReconnectConfig(enabled=False),
            on_connection_recovery_failed=on_connection_recovery_failed,
            version=self.version,
        )

        async with client:
            await self.force_tcp_disconnect(client)
            await asyncio.sleep(0.5)

        assert callback_calls == 1

    async def test_last_will(self, topic: str) -> None:
        will_topic = f"{topic}/will"
        client_id = f"zmqtt-will-{uuid.uuid4().hex[:8]}"
        will_properties = WillProperties(content_type="text/plain") if self.version == "5.0" else None
        will = Will(
            topic=will_topic,
            payload=b"offline",
            qos=QoS.AT_LEAST_ONCE,
            retain=False,
            properties=will_properties,
        )
        victim = MQTTClient(
            self.host,
            self.port,
            client_id=client_id,
            reconnect=ReconnectConfig(enabled=False),
            version=self.version,
            will=will,
        )

        async with (
            MQTTClient(self.host, self.port, version=self.version) as observer,
            observer.subscribe(will_topic, qos=QoS.AT_LEAST_ONCE) as subscription,
            victim,
        ):
            await self.trigger_session_takeover(client_id=client_id)
            message = await asyncio.wait_for(subscription.get_message(), timeout=5.0)

        assert message.topic == will_topic
        assert message.payload == b"offline"
        assert message.qos is QoS.AT_LEAST_ONCE
        if self.version == "5.0":
            assert message.properties is not None
            assert message.properties.content_type == "text/plain"
        else:
            assert message.properties is None

    async def test_overlapping_wildcard_priority_routing(
        self,
        mqtt_client: MQTTClient,
        topic: str,
    ) -> None:
        async with mqtt_client.subscribe(f"{topic}/#", f"{topic}/exact") as sub:
            await mqtt_client.publish(f"{topic}/exact", b"hit-exact")
            msg1 = await asyncio.wait_for(sub.get_message(), timeout=5.0)
            assert msg1.payload == b"hit-exact"
            await self.handle_sub_duplicates(sub=sub, n_duplicates=1)

            await mqtt_client.publish(f"{topic}/other", b"hit-wildcard")
            msg2 = await asyncio.wait_for(sub.get_message(), timeout=5.0)
            assert msg2.payload == b"hit-wildcard"
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(sub.get_message(), timeout=0.2)

    async def test_overlapping_wildcard_plus_over_hash(
        self,
        mqtt_client: MQTTClient,
        topic: str,
    ) -> None:
        async with mqtt_client.subscribe(f"{topic}/+/c", f"{topic}/#") as sub:
            await mqtt_client.publish(f"{topic}/b/c", b"plus-wins")
            msg = await asyncio.wait_for(sub.get_message(), timeout=5.0)
            assert msg.payload == b"plus-wins"
            await self.handle_sub_duplicates(sub=sub, n_duplicates=1)

    async def test_overlapping_wildcard_three_way(
        self,
        mqtt_client: MQTTClient,
        topic: str,
    ) -> None:
        async with mqtt_client.subscribe(
            f"{topic}/b/c",
            f"{topic}/b/+",
            f"{topic}/#",
        ) as sub:
            await mqtt_client.publish(f"{topic}/b/c", b"exact-wins")
            msg = await asyncio.wait_for(sub.get_message(), timeout=5.0)
            assert msg.payload == b"exact-wins"
            await self.handle_sub_duplicates(sub=sub, n_duplicates=2)

    async def test_wildcard_hash_matches_bare_topic(
        self,
        mqtt_client: MQTTClient,
        topic: str,
    ) -> None:
        async with mqtt_client.subscribe(f"{topic}/+", f"{topic}/#") as sub:
            await mqtt_client.publish(topic, b"bare-topic")
            msg = await asyncio.wait_for(sub.get_message(), timeout=5.0)
            assert msg.payload == b"bare-topic"
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(sub.get_message(), timeout=0.2)

    async def test_manual_ack_qos1(self, mqtt_client: MQTTClient, topic: str) -> None:
        async with mqtt_client.subscribe(
            topic,
            qos=QoS.AT_LEAST_ONCE,
            auto_ack=False,
        ) as sub:
            await mqtt_client.publish(topic, b"ack-me", qos=QoS.AT_LEAST_ONCE)
            msg = await asyncio.wait_for(sub.get_message(), timeout=5.0)
            await msg.ack()

        assert msg.payload == b"ack-me"

    async def test_manual_ack_qos1_idempotent(
        self,
        mqtt_client: MQTTClient,
        topic: str,
    ) -> None:
        async with mqtt_client.subscribe(
            topic,
            qos=QoS.AT_LEAST_ONCE,
            auto_ack=False,
        ) as sub:
            await mqtt_client.publish(topic, b"ack-twice", qos=QoS.AT_LEAST_ONCE)
            msg = await asyncio.wait_for(sub.get_message(), timeout=5.0)
            await msg.ack()
            await msg.ack()

        assert msg.payload == b"ack-twice"

    async def test_manual_connect_disconnect(self) -> None:
        client_id = f"zmqtt-manual-{uuid.uuid4().hex[:8]}"
        client = create_client(
            self.host,
            self.port,
            client_id=client_id,
            version=self.version,
        )
        with pytest.raises(MQTTDisconnectedError, match="No active connection information"):
            _ = client.connection_info
        for connection_id in (1, 2):
            await client.connect()
            info = client.connection_info
            assert info.connection_id == connection_id
            assert info.return_code == 0
            assert not info.session_present
            assert info.effective_client_id == client_id
            assert info.effective_keepalive == 60
            assert info.effective_session_expiry_interval == (0 if self.version == "5.0" else None)
            if self.version == "3.1.1":
                assert info.properties is None
            with pytest.raises(FrozenInstanceError):
                info.connection_id = 100  # type: ignore[misc]
            with pytest.raises(AttributeError):
                client.connection_info = info  # type: ignore[misc]
            rtt = await client.ping()
            assert rtt >= 0
            await client.disconnect()
            with pytest.raises(MQTTDisconnectedError, match="No active connection information"):
                _ = client.connection_info
            assert info.connection_id == connection_id

    async def test_connection_info_reconnect_after_failed_attempt(self, topic: str) -> None:
        attempts = 0
        retry_entered = asyncio.Event()
        allow_retry = asyncio.Event()
        client_id = f"zmqtt-info-{uuid.uuid4().hex[:8]}"

        async def factory(host: str, port: int, tls: ssl.SSLContext | bool | None) -> Transport:  # noqa: ARG001
            nonlocal attempts
            attempts += 1
            with pytest.raises(MQTTDisconnectedError, match="No active connection information"):
                _ = client.connection_info
            if attempts == 2:
                retry_entered.set()
                await allow_retry.wait()
                msg = "temporary connection failure"
                raise OSError(msg)
            return await open_tcp(host, port)

        client = MQTTClient(
            self.host,
            self.port,
            version=self.version,
            client_id=client_id,
            transport_factory=factory,
            reconnect=ReconnectConfig(initial_delay=0, max_attempts=2),
        )
        async with client, client.subscribe(topic) as subscription:
            old = client.connection_info
            await self.force_tcp_disconnect(client)
            await asyncio.wait_for(retry_entered.wait(), timeout=5)
            with pytest.raises(MQTTDisconnectedError, match="No active connection information"):
                _ = client.connection_info
            allow_retry.set()

            # A delivered message proves both handshake and subscription recovery.
            async with MQTTClient(self.host, self.port, version=self.version) as publisher:
                for _ in range(50):
                    await publisher.publish(topic, b"reconnected")
                    try:
                        message = await asyncio.wait_for(subscription.get_message(), timeout=0.1)
                    except asyncio.TimeoutError:
                        continue
                    break
                else:
                    pytest.fail("Subscription did not recover within 5 s")
            assert message.payload == b"reconnected"
            new = client.connection_info
            assert new.connection_id == 2
            assert new is not old
            assert old.connection_id == 1
            assert new.effective_client_id == old.effective_client_id == client_id
            assert attempts == 3

    async def test_context_manager_manual_pub_sub(self, topic: str) -> None:
        async with MQTTClient(
            self.host,
            self.port,
            client_id=f"zmqtt-manual-ps-{uuid.uuid4().hex[:8]}",
            version=self.version,
        ) as client:
            sub = client.subscribe(topic, qos=QoS.AT_LEAST_ONCE)
            await sub.start()
            try:
                await client.publish(topic, b"manual-pubsub", qos=QoS.AT_LEAST_ONCE)
                msg = await asyncio.wait_for(sub.get_message(), timeout=5.0)
            finally:
                await sub.stop()

        assert msg.payload == b"manual-pubsub"

    async def test_manual_ack_qos2(self, mqtt_client: MQTTClient, topic: str) -> None:
        async with mqtt_client.subscribe(
            topic,
            qos=QoS.EXACTLY_ONCE,
            auto_ack=False,
        ) as sub:
            await mqtt_client.publish(topic, b"ack-qos2", qos=QoS.EXACTLY_ONCE)
            msg = await asyncio.wait_for(sub.get_message(), timeout=5.0)
            await msg.ack()

        assert msg.payload == b"ack-qos2"

    async def test_request_response(self, topic: str) -> None:
        if self.version != "5.0":
            return

        async with (
            MQTTClient(self.host, self.port, version=self.version) as requester,
            MQTTClient(self.host, self.port, version=self.version) as responder,
            responder.subscribe(topic) as req_sub,
        ):

            async def respond() -> None:
                msg = await asyncio.wait_for(req_sub.get_message(), timeout=5.0)
                assert msg.properties is not None
                assert msg.properties.response_topic is not None
                await responder.publish(
                    msg.properties.response_topic,
                    b"pong",
                    properties=PublishProperties(
                        correlation_data=msg.properties.correlation_data,
                    ),
                )

            responder_task = asyncio.create_task(respond())
            reply = await requester.request(topic, b"ping", timeout=5.0)
            await responder_task

        assert reply.payload == b"pong"
        assert reply.properties is not None
        assert reply.properties.correlation_data is not None

    async def test_concurrent_requests_share_response_topic(self, topic: str) -> None:
        if self.version != "5.0":
            return

        response_topic = topic + "/responses"
        async with (
            MQTTClient(self.host, self.port, version=self.version) as requester,
            MQTTClient(self.host, self.port, version=self.version) as responder,
            responder.subscribe(topic) as req_sub,
        ):

            async def respond_in_reverse_order() -> None:
                requests = [await asyncio.wait_for(req_sub.get_message(), timeout=5.0) for _ in range(2)]
                await responder.publish(response_topic, b"no-correlation")
                await responder.publish(
                    response_topic,
                    b"unknown-correlation",
                    properties=PublishProperties(correlation_data=b"unknown"),
                )
                for msg in reversed(requests):
                    assert msg.properties is not None
                    assert msg.properties.response_topic == response_topic
                    await responder.publish(
                        response_topic,
                        b"reply-" + msg.payload,
                        properties=PublishProperties(
                            correlation_data=msg.properties.correlation_data,
                        ),
                    )

            responder_task = asyncio.create_task(respond_in_reverse_order())
            first, second = await asyncio.gather(
                requester.request(
                    topic,
                    b"first",
                    properties=PublishProperties(
                        response_topic=response_topic,
                        correlation_data=b"corr-first",
                    ),
                    timeout=5.0,
                ),
                requester.request(
                    topic,
                    b"second",
                    properties=PublishProperties(
                        response_topic=response_topic,
                        correlation_data=b"corr-second",
                    ),
                    timeout=5.0,
                ),
            )
            await responder_task

        assert first.payload == b"reply-first"
        assert second.payload == b"reply-second"

    @pytest.mark.parametrize("response_qos", tuple(QoS))
    async def test_matching_response_is_delivered_to_only_one_handler(
        self,
        topic: str,
        response_qos: QoS,
    ) -> None:
        if self.version != "5.0":
            return

        response_topic = topic + "/responses"
        correlation_data = b"request-correlation"
        async with (
            MQTTClient(self.host, self.port, version=self.version) as requester,
            MQTTClient(self.host, self.port, version=self.version) as responder,
            responder.subscribe(topic) as req_sub,
            requester.subscribe(response_topic, qos=response_qos) as response_sub,
        ):
            request_task = asyncio.create_task(
                requester.request(
                    topic,
                    b"request",
                    properties=PublishProperties(
                        response_topic=response_topic,
                        correlation_data=correlation_data,
                    ),
                    timeout=5.0,
                ),
            )
            request = await asyncio.wait_for(req_sub.get_message(), timeout=5.0)
            assert request.properties is not None
            await responder.publish(
                response_topic,
                b"response",
                properties=PublishProperties(
                    correlation_data=request.properties.correlation_data,
                ),
                qos=response_qos,
            )
            reply = await request_task
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(response_sub.get_message(), timeout=0.2)

        assert reply.payload == b"response"
        assert reply.qos is response_qos

    @pytest.mark.parametrize("response_qos", tuple(QoS))
    async def test_unmatched_response_is_delivered_to_subscription(
        self,
        topic: str,
        response_qos: QoS,
    ) -> None:
        if self.version != "5.0":
            return

        response_topic = topic + "/responses"
        async with (
            MQTTClient(self.host, self.port, version=self.version) as requester,
            MQTTClient(self.host, self.port, version=self.version) as responder,
            responder.subscribe(topic) as req_sub,
            requester.subscribe(response_topic, qos=response_qos) as response_sub,
        ):
            request_task = asyncio.create_task(
                requester.request(
                    topic,
                    b"request",
                    properties=PublishProperties(
                        response_topic=response_topic,
                        correlation_data=b"request-correlation",
                    ),
                    timeout=5.0,
                ),
            )
            request = await asyncio.wait_for(req_sub.get_message(), timeout=5.0)
            assert request.properties is not None
            await responder.publish(
                response_topic,
                b"unmatched",
                properties=PublishProperties(
                    correlation_data=b"unknown-correlation",
                ),
                qos=response_qos,
            )
            unmatched = await asyncio.wait_for(response_sub.get_message(), timeout=5.0)
            await responder.publish(
                response_topic,
                b"response",
                properties=PublishProperties(
                    correlation_data=request.properties.correlation_data,
                ),
                qos=response_qos,
            )
            reply = await request_task

        assert unmatched.payload == b"unmatched"
        assert unmatched.qos is response_qos
        assert reply.payload == b"response"

    async def test_request_survives_regular_subscription_stop(self, topic: str) -> None:
        if self.version != "5.0":
            return

        response_topic = topic + "/responses"
        async with (
            MQTTClient(self.host, self.port, version=self.version) as requester,
            MQTTClient(self.host, self.port, version=self.version) as responder,
            responder.subscribe(topic) as req_sub,
        ):
            response_sub = requester.subscribe(response_topic)
            await response_sub.start()
            request_task = asyncio.create_task(
                requester.request(
                    topic,
                    b"request",
                    properties=PublishProperties(
                        response_topic=response_topic,
                        correlation_data=b"request-correlation",
                    ),
                    timeout=5.0,
                ),
            )
            request = await asyncio.wait_for(req_sub.get_message(), timeout=5.0)
            assert request.properties is not None
            result = await response_sub.stop()
            await responder.publish(
                response_topic,
                b"response-after-stop",
                properties=PublishProperties(
                    correlation_data=request.properties.correlation_data,
                ),
            )
            reply = await request_task

        assert result is None
        assert reply.payload == b"response-after-stop"

    async def test_stop_reports_only_filters_sent_to_broker(self, topic: str) -> None:
        if self.version != "5.0":
            pytest.skip("request() requires MQTT 5.0")
        response_topic = f"{topic}/responses"
        other_filter = f"{topic}/other"
        async with (
            MQTTClient(self.host, self.port, version=self.version) as requester,
            MQTTClient(self.host, self.port, version=self.version) as responder,
            responder.subscribe(topic) as requests,
        ):
            response_sub = requester.subscribe(response_topic, other_filter)
            await response_sub.start()
            request_task = asyncio.create_task(
                requester.request(
                    topic,
                    b"request",
                    properties=PublishProperties(response_topic=response_topic),
                    timeout=5.0,
                ),
            )
            request = await asyncio.wait_for(requests.get_message(), timeout=5.0)
            assert request.properties is not None

            result = await response_sub.stop()
            await responder.publish(
                response_topic,
                b"reply",
                properties=PublishProperties(correlation_data=request.properties.correlation_data),
            )
            reply = await request_task

        assert result is not None
        assert result.topic_filters == (other_filter,)
        assert reply.payload == b"reply"

    async def test_request_backpressure_delays_publish(self, topic: str) -> None:
        if self.version != "5.0":
            return

        response_topic = topic + "/responses"
        async with (
            MQTTClient(self.host, self.port, version=self.version, max_pending_requests=1) as requester,
            MQTTClient(self.host, self.port, version=self.version) as responder,
            responder.subscribe(topic) as req_sub,
        ):
            first_task = asyncio.create_task(
                requester.request(
                    topic,
                    b"first",
                    properties=PublishProperties(
                        response_topic=response_topic,
                        correlation_data=b"corr-first",
                    ),
                    timeout=5.0,
                ),
            )
            first_request = await asyncio.wait_for(req_sub.get_message(), timeout=5.0)

            second_task = asyncio.create_task(
                requester.request(
                    topic,
                    b"second",
                    properties=PublishProperties(
                        response_topic=response_topic,
                        correlation_data=b"corr-second",
                    ),
                    timeout=5.0,
                ),
            )
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(req_sub.get_message(), timeout=0.2)

            assert first_request.properties is not None
            await responder.publish(
                response_topic,
                b"reply-first",
                properties=PublishProperties(
                    correlation_data=first_request.properties.correlation_data,
                ),
            )
            first_reply = await first_task

            second_request = await asyncio.wait_for(req_sub.get_message(), timeout=5.0)
            assert second_request.properties is not None
            await responder.publish(
                response_topic,
                b"reply-second",
                properties=PublishProperties(
                    correlation_data=second_request.properties.correlation_data,
                ),
            )
            second_reply = await second_task

        assert first_reply.payload == b"reply-first"
        assert second_reply.payload == b"reply-second"

    async def test_request_timeout(self, topic: str) -> None:
        if self.version != "5.0":
            return

        async with MQTTClient(self.host, self.port, version=self.version) as client:
            with pytest.raises(asyncio.TimeoutError):
                await client.request(topic + "/nobody-listening", b"ping", timeout=0.3)

    async def test_late_response_after_timeout_is_not_retained(self, topic: str) -> None:
        if self.version != "5.0":
            return

        response_topic = topic + "/late-response"
        request_seen = asyncio.Event()
        send_response = asyncio.Event()
        async with (
            MQTTClient(self.host, self.port, version=self.version) as requester,
            MQTTClient(self.host, self.port, version=self.version) as responder,
            requester.subscribe(response_topic) as response_sub,
            responder.subscribe(topic) as req_sub,
        ):

            async def respond_late() -> None:
                msg = await asyncio.wait_for(req_sub.get_message(), timeout=5.0)
                assert msg.properties is not None
                request_seen.set()
                await send_response.wait()
                await responder.publish(
                    response_topic,
                    b"late",
                    properties=PublishProperties(
                        correlation_data=msg.properties.correlation_data,
                    ),
                )

            responder_task = asyncio.create_task(respond_late())
            request_task = asyncio.create_task(
                requester.request(
                    topic,
                    b"request",
                    timeout=0.3,
                    properties=PublishProperties(response_topic=response_topic),
                ),
            )
            await asyncio.wait_for(request_seen.wait(), timeout=5.0)
            with pytest.raises(asyncio.TimeoutError):
                await request_task
            assert requester._request_dispatcher.pending_count == 0

            send_response.set()
            late = await asyncio.wait_for(response_sub.get_message(), timeout=5.0)
            await responder_task

        assert late.payload == b"late"
