_REJECTION_THRESHOLD = 0x80


class MQTTError(Exception):
    """Base class for all zmqtt exceptions."""


class MQTTConnectError(MQTTError):
    """CONNACK returned a non-zero return code."""

    def __init__(self, return_code: int) -> None:
        self.return_code = return_code
        super().__init__(f"Connection refused: return code {return_code}")


class MQTTProtocolError(MQTTError):
    """Unexpected or malformed packet received."""


class MQTTDisconnectedError(MQTTError):
    """Connection lost unexpectedly."""


class MQTTTimeoutError(MQTTError):
    """An MQTT operation did not complete within the allotted time."""


class MQTTSubscribeError(MQTTError):
    """The broker rejected one or more filters in a SUBSCRIBE (SUBACK >= 0x80).

    Most commonly an authorization denial: without this error the subscription
    looks successful and silently never receives anything.
    """

    def __init__(self, failures: dict[str, int]) -> None:
        self.failures = failures
        rendered = ", ".join(f"{f!r} (0x{code:02X})" for f, code in failures.items())
        super().__init__(f"Broker rejected subscription: {rendered}")


class MQTTUnsubscribeError(MQTTError):
    """The broker rejected one or more filters in an UNSUBACK.

    ``topic_filters`` and ``reason_codes`` retain the complete acknowledgement
    in request order.  ``failures`` is a convenient summary of the rejected filters.
    """

    def __init__(
        self,
        topic_filters: tuple[str, ...],
        reason_codes: tuple[int, ...],
        reason_string: str | None,
    ) -> None:
        self.topic_filters = topic_filters
        self.reason_codes = reason_codes
        self.reason_string = reason_string
        self.failures = {
            topic_filter: reason_code
            for topic_filter, reason_code in zip(topic_filters, reason_codes, strict=False)
            if reason_code >= _REJECTION_THRESHOLD
        }
        rendered = ", ".join(f"{f!r} (0x{code:02X})" for f, code in self.failures.items())
        super().__init__(f"Broker rejected unsubscribe: {rendered}")


class MQTTPublishError(MQTTError):
    """The broker rejected a QoS 1/2 publish.

    *reason_name* is the spec's name for *reason_code* (``None`` for a code
    zmqtt does not recognize). *reason_string* is the broker's optional Reason
    String property.
    """

    def __init__(
        self,
        reason_code: int,
        reason_name: str | None,
        reason_string: str | None,
    ) -> None:
        self.reason_code = reason_code
        self.reason_name = reason_name
        self.reason_string = reason_string
        named = f"0x{reason_code:02X} {reason_name}" if reason_name else f"0x{reason_code:02X}"
        detail = f": {reason_string}" if reason_string else ""
        super().__init__(f"Broker rejected publish ({named}){detail}")


class MQTTInvalidTopicError(MQTTError):
    """Topic string or topic filter failed MQTT validation."""
