import asyncio
import re
import uuid
from collections.abc import AsyncGenerator

import pytest

from tests.test_brokers._base import BrokerTestBase
from zmqtt import MQTTUnsubscribeError, Subscription
from zmqtt._internal._compat import ExceptionGroup
from zmqtt._internal.types.qos import QoS
from zmqtt.client import MQTTClient
from zmqtt.errors import MQTTPublishError


class BaseTestMosquitto(BrokerTestBase):
    denied_topic = "zmqtt/e2e/denied"
    username = "zmqtt-mosquitto"
    password = "zmqtt-mosquitto"  # noqa: S105

    @pytest.fixture
    async def mqtt_client(self) -> AsyncGenerator[MQTTClient]:
        async with MQTTClient(
            self.host,
            self.port,
            client_id=f"zmqtt-test-{uuid.uuid4().hex[:8]}",
            username=self.username,
            password=self.password,
            version=self.version,
        ) as client:
            yield client

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


class TestMosquittoV311(BaseTestMosquitto):
    host = "127.0.0.1"
    port = 1884
    version = "3.1.1"

    @pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
    async def test_publish_in_denied_topic_mqtt311_does_not_raise(
        self,
        mqtt_client: MQTTClient,
        qos: QoS,
        topic: str,
    ) -> None:
        await mqtt_client.publish(self.denied_topic, b"denied", qos=qos)

        async with mqtt_client.subscribe(topic) as sub:
            await mqtt_client.publish(topic, b"payload-qos0")
            msg = await sub.get_message()

        assert msg.topic == topic
        assert msg.payload == b"payload-qos0"


class TestMosquittoV5(BaseTestMosquitto):
    host = "127.0.0.1"
    port = 1884
    version = "5.0"

    @pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
    async def test_publish_in_denied_topic_raises_error(
        self,
        mqtt_client: MQTTClient,
        qos: QoS,
    ) -> None:
        with pytest.raises(
            MQTTPublishError, match=re.escape("Broker rejected publish (0x87 Not authorized)")
        ) as exc_info:
            await mqtt_client.publish(self.denied_topic, b"denied", qos=qos)

        assert exc_info.value.reason_code == 0x87
        assert exc_info.value.reason_name == "Not authorized"

    @pytest.mark.parametrize("qos", [QoS.AT_LEAST_ONCE, QoS.EXACTLY_ONCE])
    async def test_publish_in_denied_topic_remains_connection_usable(
        self,
        mqtt_client: MQTTClient,
        qos: QoS,
        topic: str,
    ) -> None:
        with pytest.raises(MQTTPublishError, match=re.escape("Broker rejected publish (0x87 Not authorized)")):
            await mqtt_client.publish(self.denied_topic, b"denied", qos=qos)

        async with mqtt_client.subscribe(topic) as sub:
            await mqtt_client.publish(topic, b"payload-qos0")
            msg = await sub.get_message()

        assert msg.topic == topic
        assert msg.payload == b"payload-qos0"

    async def test_rejected_unsubscribe_raises_error(self, mqtt_client: MQTTClient) -> None:
        denied_filter = f"zmqtt/unsuback/denied/{uuid.uuid4().hex}"

        sub = mqtt_client.subscribe(denied_filter)
        await sub.start()
        with pytest.raises(MQTTUnsubscribeError) as exc_info:
            await sub.stop()

        error = exc_info.value
        assert error.failures == {denied_filter: 0x87}
        assert error.topic_filters == (denied_filter,)
        assert error.reason_codes == (0x87,)
        assert error.reason_string is None

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
        assert error.failures == {denied_filter: 0x87}
        assert error.topic_filters == (allowed_filter, denied_filter)
        assert error.reason_codes == (0x00, 0x87)
        assert retry_error.failures == {denied_filter: 0x87}
        assert retry_error.topic_filters == (denied_filter,)
        assert retry_error.reason_codes == (0x87,)
        assert message.topic == denied_filter
        assert message.payload == b"still-subscribed"
        with pytest.raises(asyncio.TimeoutError):  # trying to get msg from unsubscribed filter
            await asyncio.wait_for(sub.get_message(), timeout=0.5)

    async def test_reconnect_restores_only_filter_rejected_by_unsuback(self, mqtt_client: MQTTClient) -> None:
        """After reconnect, only the filter rejected by UNSUBACK receives messages."""

        suffix = uuid.uuid4().hex
        allowed_filter = f"zmqtt/unsuback/allowed/{suffix}"
        denied_filter = f"zmqtt/unsuback/denied/{suffix}"
        sub = mqtt_client.subscribe(allowed_filter, denied_filter)

        # Act
        await sub.start()
        with pytest.raises(MQTTUnsubscribeError) as exc_info:
            await sub.stop()

        await mqtt_client.publish(denied_filter, b"reconnect-ready", retain=True)
        await asyncio.wait_for(sub.get_message(), timeout=5.0)
        await self.force_tcp_disconnect(mqtt_client)
        reconnect_message = await asyncio.wait_for(sub.get_message(), timeout=5.0)

        await mqtt_client.publish(allowed_filter, b"must-not-return")
        await mqtt_client.publish(denied_filter, b"still-subscribed-after-reconnect")
        message = await asyncio.wait_for(sub.get_message(), timeout=5.0)

        # Assert
        assert exc_info.value.failures == {denied_filter: 0x87}
        assert reconnect_message.payload == b"reconnect-ready"
        assert message.topic == denied_filter
        assert message.payload == b"still-subscribed-after-reconnect"

    async def test_rejected_subscription_cleanup_preserves_body_error(self, mqtt_client: MQTTClient) -> None:
        denied_filter = f"zmqtt/unsuback/denied/{uuid.uuid4().hex}"
        body_error = RuntimeError("application failed")

        with pytest.raises(ExceptionGroup) as exc_info:
            async with mqtt_client.subscribe(denied_filter):
                raise body_error

        errors = exc_info.value.exceptions
        assert errors[0] is body_error
        cleanup_error = errors[1]
        assert isinstance(cleanup_error, MQTTUnsubscribeError)
        assert cleanup_error.failures == {denied_filter: 0x87}
