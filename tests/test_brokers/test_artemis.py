import asyncio
import uuid

import pytest

from tests.test_brokers._base import BrokerTestBase
from zmqtt import MQTTClient, MQTTUnsubscribeError, QoS, ReconnectConfig, Subscription, Will, WillProperties


class BaseTestArtemis(BrokerTestBase):
    async def handle_sub_duplicates(
        self,
        *,
        sub: Subscription,
        n_duplicates: int,
    ) -> None:
        for _ in range(n_duplicates):
            await sub.get_message()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(sub.get_message(), timeout=0.2)

    async def test_message_ordering(
        self,
        mqtt_client: MQTTClient,  # noqa: ARG002
        topic: str,  # noqa: ARG002
    ) -> None:
        """Artemis has a race condition that occasionally delivers QoS 1 messages
        out of order, so this test flakes. Skip it while ordering is broken upstream.
        """
        pytest.skip("Artemis breaks message ordering: https://issues.apache.org/jira/browse/ARTEMIS-6191")

    async def test_last_will(self, topic: str) -> None:
        """
        Unlike other brokers, Artemis doen`t read other client_id takeover as unexpected reconnect
        so this test uses direct closing underlying tcp connection.
        Will properties aren`t supported by Artemis.
        Also Artemis steals will props for now. fix: https://github.com/apache/artemis/pull/6616
        """
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
            await self.force_tcp_disconnect(victim)
            message = await asyncio.wait_for(subscription.get_message(), timeout=5.0)

        assert message.topic == will_topic
        assert message.payload == b"offline"
        assert message.qos is QoS.AT_LEAST_ONCE

    async def test_subscription_identifier_overlapping(self, topic: str) -> None:
        """Artemis supports subscription identifiers (a lone identified
        subscription gets its echo just fine) — what it does NOT share with the
        other brokers is the two-copies-per-overlap delivery: for a ``$share``
        subscription plus its plain twin it sends ONE copy per message, to the
        plain subscription (tagged with its id). The shared twin starves, so
        waiting on it must time out.
        """
        if self.version != "5.0":  # 3.1.1 refuses the parameter — same as everywhere
            await super().test_subscription_identifier_overlapping(topic)
            return

        bare_topic = topic.lstrip("/")
        group = f"zmqtt-si-{uuid.uuid4().hex[:8]}"

        async with (
            MQTTClient(self.host, self.port, version=self.version) as client,
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

            plain_msgs = [await asyncio.wait_for(plain.get_message(), timeout=5.0) for _ in range(3)]
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(shared.get_message(), timeout=1.0)

        assert [m.payload for m in plain_msgs] == [b"m0", b"m1", b"m2"]
        assert {m.properties.subscription_identifier for m in plain_msgs if m.properties} == {2}


class TestArtemisV311(BaseTestArtemis):
    host = "127.0.0.1"
    port = 1883
    version = "3.1.1"


class TestArtemisV5(BaseTestArtemis):
    host = "127.0.0.1"
    port = 1883
    version = "5.0"

    @pytest.mark.xfail(reason="Confirmed Artemis bug")
    async def test_mixed_unsubscribe_reports_only_rejected_filter(self, mqtt_client: MQTTClient) -> None:
        suffix = uuid.uuid4().hex
        allowed_filter = f"zmqtt/unsuback/allowed/{suffix}"
        denied_filter = f"zmqtt/unsuback/denied/{suffix}"

        sub = mqtt_client.subscribe(allowed_filter, denied_filter)
        await sub.start()
        with pytest.raises(MQTTUnsubscribeError) as exc_info:
            await sub.stop()
        with pytest.raises(MQTTUnsubscribeError) as retry_exc_info:
            await sub.stop()
        await mqtt_client.publish(allowed_filter, b"must-not-arrive", qos=QoS.AT_LEAST_ONCE)
        await mqtt_client.publish(denied_filter, b"still-subscribed", qos=QoS.AT_LEAST_ONCE)
        message = await asyncio.wait_for(sub.get_message(), timeout=5.0)

        error = exc_info.value
        retry_error = retry_exc_info.value
        assert error.failures == {denied_filter: 0x80}
        assert error.topic_filters == (allowed_filter, denied_filter)
        assert error.reason_codes == (0x00, 0x80)
        assert retry_error.failures == {denied_filter: 0x80}
        assert retry_error.topic_filters == (denied_filter,)
        assert retry_error.reason_codes == (0x80,)
        assert message.topic == denied_filter
        assert message.payload == b"still-subscribed"
        with pytest.raises(asyncio.TimeoutError):  # trying to get msg from unsubscribed filter
            await asyncio.wait_for(sub.get_message(), timeout=0.5)
