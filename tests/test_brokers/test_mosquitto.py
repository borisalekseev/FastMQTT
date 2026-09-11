import asyncio
import logging
import re
import uuid
from collections.abc import AsyncGenerator

import pytest

from tests.test_brokers._base import BrokerTestBase
from zmqtt import PublishProperties, Subscription
from zmqtt._internal.types.qos import QoS
from zmqtt.client import MQTTClient
from zmqtt.errors import MQTTPublishError


class BaseTestMosquitto(BrokerTestBase):
    denied_topic = "zmqtt/e2e/denied"
    # Dynamic Security grants this client the ACLs the rejection tests rely on.
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

    async def test_stop_on_rejecting_broker_reports_no_reason_codes(
        self,
        mqtt_client: MQTTClient,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        denied_filter = f"zmqtt/unsuback/denied/{uuid.uuid4().hex}"
        sub = mqtt_client.subscribe(denied_filter)
        await sub.start()

        with caplog.at_level(logging.WARNING, logger="zmqtt.protocol"):
            result = await sub.stop()

        assert result is not None
        assert result.reason_codes == ()
        assert "Broker rejected unsubscribe" not in caplog.text


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

    async def test_stop_reports_rejected_filter(
        self, mqtt_client: MQTTClient, caplog: pytest.LogCaptureFixture
    ) -> None:
        suffix = uuid.uuid4().hex
        allowed_filter = f"zmqtt/unsuback/allowed/{suffix}"
        denied_filter = f"zmqtt/unsuback/denied/{suffix}"
        sub = mqtt_client.subscribe(allowed_filter, denied_filter)
        await sub.start()

        with caplog.at_level(logging.WARNING, logger="zmqtt.protocol"):
            result = await sub.stop()

        assert result is not None
        assert result.topic_filters == (allowed_filter, denied_filter)
        assert result.reason_codes == (0x00, 0x87)
        assert result.failures == {denied_filter: 0x87}
        assert result.reason_string is None  # this broker sends no diagnostic
        assert f"{denied_filter!r} (0x87 Not authorized)" in caplog.text
        # assert it stop's delivering
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(sub.get_message(), timeout=0.5)

    async def test_rejected_unsubscribe_keeps_body_exception(
        self,
        mqtt_client: MQTTClient,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        denied_filter = f"zmqtt/unsuback/denied/{uuid.uuid4().hex}"
        body_error = RuntimeError("body failed")

        with caplog.at_level(logging.WARNING, logger="zmqtt.protocol"), pytest.raises(RuntimeError) as exc_info:
            async with mqtt_client.subscribe(denied_filter):
                raise body_error

        assert exc_info.value is body_error
        assert f"{denied_filter!r} (0x87 Not authorized)" in caplog.text

    async def test_rejected_response_topic_release_is_logged(
        self,
        mqtt_client: MQTTClient,
        topic: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        response_topic = f"zmqtt/unsuback/denied/{uuid.uuid4().hex}"
        async with (
            MQTTClient(self.host, self.port, version=self.version) as responder,
            responder.subscribe(topic) as requests,
        ):
            request_task = asyncio.create_task(
                mqtt_client.request(
                    topic,
                    b"request",
                    properties=PublishProperties(response_topic=response_topic),
                    timeout=5.0,
                ),
            )
            request = await asyncio.wait_for(requests.get_message(), timeout=5.0)
            assert request.properties is not None

            with caplog.at_level(logging.WARNING, logger="zmqtt.protocol"):
                await responder.publish(
                    response_topic,
                    b"reply",
                    properties=PublishProperties(correlation_data=request.properties.correlation_data),
                )
                await request_task

        assert f"{response_topic!r} (0x87 Not authorized)" in caplog.text
