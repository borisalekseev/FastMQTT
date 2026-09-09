from zmqtt._internal.packets.properties import ConnAckProperties


class MQTTError(Exception):
    """Base class for all zmqtt exceptions."""


class MQTTConnectError(MQTTError):
    """CONNACK returned a non-zero return code."""

    def __init__(self, return_code: int, *, properties: ConnAckProperties | None = None) -> None:
        self.return_code = return_code
        self.properties = properties
        self.reason_string = properties.reason_string if properties is not None else None
        self.user_properties = properties.user_properties if properties is not None else ()
        self.server_reference = properties.server_reference if properties is not None else None
        super().__init__(f"Connection refused: return code {return_code}")


class MQTTProtocolError(MQTTError):
    """Unexpected or malformed packet received."""


class MQTTQoSExceededError(MQTTError):
    """A PUBLISH requested a QoS higher than the server's advertised Maximum QoS.

    Raised locally, before the packet is sent — the server would otherwise
    have to reject or drop it (MQTT 5.0 §3.2.2.3.4).
    """

    def __init__(self, requested: int, maximum: int) -> None:
        self.requested_qos = requested
        self.maximum_qos = maximum
        super().__init__(f"Requested QoS {requested} exceeds the server's Maximum QoS {maximum}")


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
