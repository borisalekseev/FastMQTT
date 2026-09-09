"""MQTT 5.0 properties: PropertyID enum, typed dataclasses, and wire encode/decode."""

import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import Any

from zmqtt._internal.packets._wire import (
    decode_bytes_field,
    decode_str,
    decode_varint,
    encode_bytes_field,
    encode_str,
    encode_varint,
)


class PropertyID(IntEnum):
    PAYLOAD_FORMAT_INDICATOR = 0x01
    MESSAGE_EXPIRY_INTERVAL = 0x02
    CONTENT_TYPE = 0x03
    RESPONSE_TOPIC = 0x08
    CORRELATION_DATA = 0x09
    SUBSCRIPTION_IDENTIFIER = 0x0B
    SESSION_EXPIRY_INTERVAL = 0x11
    ASSIGNED_CLIENT_IDENTIFIER = 0x12
    SERVER_KEEP_ALIVE = 0x13
    AUTHENTICATION_METHOD = 0x15
    AUTHENTICATION_DATA = 0x16
    REQUEST_PROBLEM_INFORMATION = 0x17
    WILL_DELAY_INTERVAL = 0x18
    REQUEST_RESPONSE_INFORMATION = 0x19
    RESPONSE_INFORMATION = 0x1A
    SERVER_REFERENCE = 0x1C
    REASON_STRING = 0x1F
    RECEIVE_MAXIMUM = 0x21
    TOPIC_ALIAS_MAXIMUM = 0x22
    TOPIC_ALIAS = 0x23
    MAXIMUM_QOS = 0x24
    RETAIN_AVAILABLE = 0x25
    USER_PROPERTY = 0x26
    MAXIMUM_PACKET_SIZE = 0x27
    WILDCARD_SUBSCRIPTION_AVAILABLE = 0x28
    SUBSCRIPTION_IDENTIFIER_AVAILABLE = 0x29
    SHARED_SUBSCRIPTION_AVAILABLE = 0x2A


# Wire type categorisation — drives the generic decoder.
_WIRE_BYTE = frozenset(
    {
        PropertyID.PAYLOAD_FORMAT_INDICATOR,
        PropertyID.REQUEST_PROBLEM_INFORMATION,
        PropertyID.REQUEST_RESPONSE_INFORMATION,
        PropertyID.MAXIMUM_QOS,
        PropertyID.RETAIN_AVAILABLE,
        PropertyID.WILDCARD_SUBSCRIPTION_AVAILABLE,
        PropertyID.SUBSCRIPTION_IDENTIFIER_AVAILABLE,
        PropertyID.SHARED_SUBSCRIPTION_AVAILABLE,
    },
)
_WIRE_UINT16 = frozenset(
    {
        PropertyID.SERVER_KEEP_ALIVE,
        PropertyID.RECEIVE_MAXIMUM,
        PropertyID.TOPIC_ALIAS_MAXIMUM,
        PropertyID.TOPIC_ALIAS,
    },
)
_WIRE_UINT32 = frozenset(
    {
        PropertyID.MESSAGE_EXPIRY_INTERVAL,
        PropertyID.SESSION_EXPIRY_INTERVAL,
        PropertyID.WILL_DELAY_INTERVAL,
        PropertyID.MAXIMUM_PACKET_SIZE,
    },
)
_WIRE_VARINT = frozenset({PropertyID.SUBSCRIPTION_IDENTIFIER})
_WIRE_STR = frozenset(
    {
        PropertyID.CONTENT_TYPE,
        PropertyID.RESPONSE_TOPIC,
        PropertyID.ASSIGNED_CLIENT_IDENTIFIER,
        PropertyID.AUTHENTICATION_METHOD,
        PropertyID.REASON_STRING,
        PropertyID.RESPONSE_INFORMATION,
        PropertyID.SERVER_REFERENCE,
    },
)
_WIRE_BYTES_DATA = frozenset(
    {
        PropertyID.CORRELATION_DATA,
        PropertyID.AUTHENTICATION_DATA,
    },
)
# USER_PROPERTY (0x26) is a repeatable string pair — handled separately.

_PID = PropertyID  # shorthand for inside this module


def encode_props(d: dict[PropertyID, Any]) -> bytes:
    """Encode a property dict to raw wire bytes (no length prefix)."""
    buf = bytearray()
    for pid, value in d.items():
        if pid in _WIRE_BYTE:
            buf += struct.pack("!BB", pid, int(value))
        elif pid in _WIRE_UINT16:
            buf += struct.pack("!BH", pid, value)
        elif pid in _WIRE_UINT32:
            buf += struct.pack("!BI", pid, value)
        elif pid in _WIRE_VARINT:
            buf += bytes([pid]) + encode_varint(value)
        elif pid in _WIRE_STR:
            buf += bytes([pid]) + encode_str(value)
        elif pid in _WIRE_BYTES_DATA:
            buf += bytes([pid]) + encode_bytes_field(value)
        elif pid == _PID.USER_PROPERTY:
            for k, v in value:
                buf += bytes([pid]) + encode_str(k) + encode_str(v)
    return bytes(buf)


def encode_props_block(d: dict[PropertyID, Any] | None) -> bytes:
    """Encode a properties block: varint length prefix + raw wire bytes."""
    if not d:
        return encode_varint(0)
    raw = encode_props(d)
    return encode_varint(len(raw)) + raw


def decode_props_block(
    buf: bytes | memoryview,
    offset: int,
) -> tuple[dict[PropertyID, Any], int]:
    """Decode a properties block.

    Returns (dict, bytes_consumed) where bytes_consumed includes the length prefix.
    """
    props_len, n = decode_varint(buf, offset)
    pos = offset + n
    end = pos + props_len
    d: dict[PropertyID, Any] = {}
    val: str | int | bytes
    while pos < end:
        try:
            pid = PropertyID(buf[pos])
        except ValueError as e:
            msg = f"Unknown property ID: {buf[pos]:#04x}"
            raise ValueError(msg) from e
        pos += 1
        if pid in _WIRE_BYTE:
            d[pid] = buf[pos]
            pos += 1
        elif pid in _WIRE_UINT16:
            d[pid] = struct.unpack_from("!H", buf, pos)[0]
            pos += 2
        elif pid in _WIRE_UINT32:
            d[pid] = struct.unpack_from("!I", buf, pos)[0]
            pos += 4
        elif pid in _WIRE_VARINT:
            val, vn = decode_varint(buf, pos)
            d[pid] = val
            pos += vn
        elif pid in _WIRE_STR:
            val, sn = decode_str(buf, pos)
            d[pid] = val
            pos += sn
        elif pid in _WIRE_BYTES_DATA:
            val, bn = decode_bytes_field(buf, pos)
            d[pid] = val
            pos += bn
        elif pid == _PID.USER_PROPERTY:
            k, kn = decode_str(buf, pos)
            pos += kn
            v, vn = decode_str(buf, pos)
            pos += vn
            pairs: list[tuple[str, str]] = list(d.get(pid, []))
            pairs.append((k, v))
            d[pid] = pairs
    return d, n + props_len


@dataclass(frozen=True, slots=True, kw_only=True)
class ConnectProperties:
    """MQTT 5.0 properties sent in the CONNECT packet.

    Attributes:
        session_expiry_interval: Seconds before the broker discards the session
            after disconnect. ``0`` means discard immediately; ``0xFFFFFFFF``
            means never expire.
        receive_maximum: Maximum number of QoS 1/2 messages the client will
            process concurrently.
        maximum_packet_size: Largest packet size (bytes) the client will accept.
        topic_alias_maximum: Number of topic aliases the client supports. ``0``
            disables topic aliases.
        request_response_information: Ask the broker to include response
            information in CONNACK.
        request_problem_information: Ask the broker to include reason strings and
            user properties on errors.
        authentication_method: Method name for enhanced authentication.
        authentication_data: Initial data for enhanced authentication.
        user_properties: Arbitrary key-value pairs forwarded to the broker.
    """

    session_expiry_interval: int | None = None
    receive_maximum: int | None = None
    maximum_packet_size: int | None = None
    topic_alias_maximum: int | None = None
    request_response_information: bool | None = None
    request_problem_information: bool | None = None
    authentication_method: str | None = None
    authentication_data: bytes | None = None
    user_properties: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class WillProperties:
    will_delay_interval: int | None = None
    payload_format_indicator: int | None = None
    message_expiry_interval: int | None = None
    content_type: str | None = None
    response_topic: str | None = None
    correlation_data: bytes | None = None
    user_properties: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class ConnAckProperties:
    session_expiry_interval: int | None = None
    receive_maximum: int | None = None
    maximum_qos: int | None = None
    retain_available: bool | None = None
    maximum_packet_size: int | None = None
    assigned_client_identifier: str | None = None
    topic_alias_maximum: int | None = None
    reason_string: str | None = None
    wildcard_subscription_available: bool | None = None
    subscription_identifier_available: bool | None = None
    shared_subscription_available: bool | None = None
    server_keep_alive: int | None = None
    response_information: str | None = None
    server_reference: str | None = None
    authentication_method: str | None = None
    authentication_data: bytes | None = None
    user_properties: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class PublishProperties:
    """MQTT 5.0 properties attached to a PUBLISH packet.

    Attributes:
        payload_format_indicator: ``0`` for unspecified binary, ``1`` for UTF-8
            encoded character data.
        message_expiry_interval: Seconds the broker retains the message. Omit to
            keep indefinitely.
        topic_alias: Topic alias integer (1..65535). Outgoing aliases must not
            exceed the server's Topic Alias Maximum for the current connection.
        response_topic: Topic the receiver should use when replying
            (request/response pattern).
        correlation_data: Opaque bytes the sender uses to match a response to a
            request.
        subscription_identifier: Identifier of the subscription that caused this
            message to be delivered.
        content_type: MIME type describing the payload content.
        user_properties: Arbitrary key-value pairs forwarded with the message.
    """

    payload_format_indicator: int | None = None
    message_expiry_interval: int | None = None
    topic_alias: int | None = None
    response_topic: str | None = None
    correlation_data: bytes | None = None
    subscription_identifier: int | None = None
    content_type: str | None = None
    user_properties: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class PubAckProperties:
    """Properties for PUBACK, PUBREC, PUBREL, and PUBCOMP."""

    reason_string: str | None = None
    user_properties: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class SubscribeProperties:
    subscription_identifier: int | None = None
    user_properties: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class SubAckProperties:
    reason_string: str | None = None
    user_properties: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class UnsubscribeProperties:
    user_properties: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class UnsubAckProperties:
    reason_string: str | None = None
    user_properties: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class DisconnectProperties:
    session_expiry_interval: int | None = None
    reason_string: str | None = None
    server_reference: str | None = None
    user_properties: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class AuthProperties:
    """MQTT 5.0 properties sent in an AUTH packet.

    Attributes:
        authentication_method: Method name for the enhanced authentication
            exchange.
        authentication_data: Method-specific data for this step of the
            authentication exchange.
        reason_string: Human-readable reason included for diagnostic purposes.
        user_properties: Arbitrary key-value pairs forwarded with the packet.
    """

    authentication_method: str | None = None
    authentication_data: bytes | None = None
    reason_string: str | None = None
    user_properties: tuple[tuple[str, str], ...] = ()


_UP = _PID.USER_PROPERTY  # shorthand
AnyProperties = (
    ConnectProperties
    | WillProperties
    | ConnAckProperties
    | PublishProperties
    | PubAckProperties
    | SubscribeProperties
    | SubAckProperties
    | UnsubscribeProperties
    | UnsubAckProperties
    | DisconnectProperties
    | AuthProperties
)


def _up(props: AnyProperties) -> dict[PropertyID, Any]:
    """Build a dict with user_properties if non-empty."""
    return {_UP: list(props.user_properties)} if props.user_properties else {}


def encode_connect_properties(props: ConnectProperties | None) -> bytes:
    if props is None:
        return encode_varint(0)
    d: dict[PropertyID, Any] = {
        pid: val
        for pid, val in [
            (_PID.SESSION_EXPIRY_INTERVAL, props.session_expiry_interval),
            (_PID.RECEIVE_MAXIMUM, props.receive_maximum),
            (_PID.MAXIMUM_PACKET_SIZE, props.maximum_packet_size),
            (_PID.TOPIC_ALIAS_MAXIMUM, props.topic_alias_maximum),
            (_PID.REQUEST_RESPONSE_INFORMATION, props.request_response_information),
            (_PID.REQUEST_PROBLEM_INFORMATION, props.request_problem_information),
            (_PID.AUTHENTICATION_METHOD, props.authentication_method),
            (_PID.AUTHENTICATION_DATA, props.authentication_data),
        ]
        if val is not None
    }
    d.update(_up(props))
    return encode_props_block(d)


def encode_will_properties(props: WillProperties | None) -> bytes:
    if props is None:
        return encode_varint(0)
    d: dict[PropertyID, Any] = {
        pid: val
        for pid, val in [
            (_PID.WILL_DELAY_INTERVAL, props.will_delay_interval),
            (_PID.PAYLOAD_FORMAT_INDICATOR, props.payload_format_indicator),
            (_PID.MESSAGE_EXPIRY_INTERVAL, props.message_expiry_interval),
            (_PID.CONTENT_TYPE, props.content_type),
            (_PID.RESPONSE_TOPIC, props.response_topic),
            (_PID.CORRELATION_DATA, props.correlation_data),
        ]
        if val is not None
    }
    d.update(_up(props))
    return encode_props_block(d)


def encode_connack_properties(props: ConnAckProperties | None) -> bytes:
    if props is None:
        return encode_varint(0)
    d: dict[PropertyID, Any] = {
        pid: val
        for pid, val in [
            (_PID.SESSION_EXPIRY_INTERVAL, props.session_expiry_interval),
            (_PID.RECEIVE_MAXIMUM, props.receive_maximum),
            (_PID.MAXIMUM_QOS, props.maximum_qos),
            (_PID.RETAIN_AVAILABLE, props.retain_available),
            (_PID.MAXIMUM_PACKET_SIZE, props.maximum_packet_size),
            (_PID.ASSIGNED_CLIENT_IDENTIFIER, props.assigned_client_identifier),
            (_PID.TOPIC_ALIAS_MAXIMUM, props.topic_alias_maximum),
            (_PID.REASON_STRING, props.reason_string),
            (
                _PID.WILDCARD_SUBSCRIPTION_AVAILABLE,
                props.wildcard_subscription_available,
            ),
            (
                _PID.SUBSCRIPTION_IDENTIFIER_AVAILABLE,
                props.subscription_identifier_available,
            ),
            (_PID.SHARED_SUBSCRIPTION_AVAILABLE, props.shared_subscription_available),
            (_PID.SERVER_KEEP_ALIVE, props.server_keep_alive),
            (_PID.RESPONSE_INFORMATION, props.response_information),
            (_PID.SERVER_REFERENCE, props.server_reference),
            (_PID.AUTHENTICATION_METHOD, props.authentication_method),
            (_PID.AUTHENTICATION_DATA, props.authentication_data),
        ]
        if val is not None
    }
    d.update(_up(props))
    return encode_props_block(d)


def encode_publish_properties(props: PublishProperties | None) -> bytes:
    if props is None:
        return encode_varint(0)
    d: dict[PropertyID, Any] = {
        pid: val
        for pid, val in [
            (_PID.PAYLOAD_FORMAT_INDICATOR, props.payload_format_indicator),
            (_PID.MESSAGE_EXPIRY_INTERVAL, props.message_expiry_interval),
            (_PID.TOPIC_ALIAS, props.topic_alias),
            (_PID.RESPONSE_TOPIC, props.response_topic),
            (_PID.CORRELATION_DATA, props.correlation_data),
            (_PID.SUBSCRIPTION_IDENTIFIER, props.subscription_identifier),
            (_PID.CONTENT_TYPE, props.content_type),
        ]
        if val is not None
    }
    d.update(_up(props))
    return encode_props_block(d)


def encode_puback_properties(props: PubAckProperties | None) -> bytes:
    if props is None:
        return encode_varint(0)
    d: dict[PropertyID, Any] = {}
    if props.reason_string is not None:
        d[_PID.REASON_STRING] = props.reason_string
    d.update(_up(props))
    return encode_props_block(d)


def encode_subscribe_properties(props: SubscribeProperties | None) -> bytes:
    if props is None:
        return encode_varint(0)
    d: dict[PropertyID, Any] = {}
    if props.subscription_identifier is not None:
        d[_PID.SUBSCRIPTION_IDENTIFIER] = props.subscription_identifier
    d.update(_up(props))
    return encode_props_block(d)


def encode_suback_properties(props: SubAckProperties | None) -> bytes:
    if props is None:
        return encode_varint(0)
    d: dict[PropertyID, Any] = {}
    if props.reason_string is not None:
        d[_PID.REASON_STRING] = props.reason_string
    d.update(_up(props))
    return encode_props_block(d)


def encode_unsubscribe_properties(props: UnsubscribeProperties | None) -> bytes:
    if props is None:
        return encode_varint(0)
    return encode_props_block(_up(props))


def encode_unsuback_properties(props: UnsubAckProperties | None) -> bytes:
    if props is None:
        return encode_varint(0)
    d: dict[PropertyID, Any] = {}
    if props.reason_string is not None:
        d[_PID.REASON_STRING] = props.reason_string
    d.update(_up(props))
    return encode_props_block(d)


def encode_disconnect_properties(props: DisconnectProperties | None) -> bytes:
    if props is None:
        return encode_varint(0)
    d: dict[PropertyID, Any] = {
        pid: val
        for pid, val in [
            (_PID.SESSION_EXPIRY_INTERVAL, props.session_expiry_interval),
            (_PID.REASON_STRING, props.reason_string),
            (_PID.SERVER_REFERENCE, props.server_reference),
        ]
        if val is not None
    }
    d.update(_up(props))
    return encode_props_block(d)


def encode_auth_properties(props: AuthProperties | None) -> bytes:
    if props is None:
        return encode_varint(0)
    d: dict[PropertyID, Any] = {
        pid: val
        for pid, val in [
            (_PID.AUTHENTICATION_METHOD, props.authentication_method),
            (_PID.AUTHENTICATION_DATA, props.authentication_data),
            (_PID.REASON_STRING, props.reason_string),
        ]
        if val is not None
    }
    d.update(_up(props))
    return encode_props_block(d)


def _up_tuple(d: dict[PropertyID, Any]) -> tuple[tuple[str, str], ...]:
    return tuple((k, v) for k, v in d.get(_UP, []))


def decode_connect_properties(
    buf: bytes | memoryview,
    offset: int,
) -> tuple[ConnectProperties | None, int]:
    d, n = decode_props_block(buf, offset)
    if not d:
        return None, n
    return ConnectProperties(
        session_expiry_interval=d.get(_PID.SESSION_EXPIRY_INTERVAL),
        receive_maximum=d.get(_PID.RECEIVE_MAXIMUM),
        maximum_packet_size=d.get(_PID.MAXIMUM_PACKET_SIZE),
        topic_alias_maximum=d.get(_PID.TOPIC_ALIAS_MAXIMUM),
        request_response_information=bool(d[_PID.REQUEST_RESPONSE_INFORMATION])
        if _PID.REQUEST_RESPONSE_INFORMATION in d
        else None,
        request_problem_information=bool(d[_PID.REQUEST_PROBLEM_INFORMATION])
        if _PID.REQUEST_PROBLEM_INFORMATION in d
        else None,
        authentication_method=d.get(_PID.AUTHENTICATION_METHOD),
        authentication_data=d.get(_PID.AUTHENTICATION_DATA),
        user_properties=_up_tuple(d),
    ), n


def decode_will_properties(
    buf: bytes | memoryview,
    offset: int,
) -> tuple[WillProperties | None, int]:
    d, n = decode_props_block(buf, offset)
    if not d:
        return None, n
    return WillProperties(
        will_delay_interval=d.get(_PID.WILL_DELAY_INTERVAL),
        payload_format_indicator=d.get(_PID.PAYLOAD_FORMAT_INDICATOR),
        message_expiry_interval=d.get(_PID.MESSAGE_EXPIRY_INTERVAL),
        content_type=d.get(_PID.CONTENT_TYPE),
        response_topic=d.get(_PID.RESPONSE_TOPIC),
        correlation_data=d.get(_PID.CORRELATION_DATA),
        user_properties=_up_tuple(d),
    ), n


def decode_connack_properties(
    buf: bytes | memoryview,
    offset: int,
) -> tuple[ConnAckProperties | None, int]:
    d, n = decode_props_block(buf, offset)
    if not d:
        return None, n
    return ConnAckProperties(
        session_expiry_interval=d.get(_PID.SESSION_EXPIRY_INTERVAL),
        receive_maximum=d.get(_PID.RECEIVE_MAXIMUM),
        maximum_qos=d.get(_PID.MAXIMUM_QOS),
        retain_available=bool(d[_PID.RETAIN_AVAILABLE]) if _PID.RETAIN_AVAILABLE in d else None,
        maximum_packet_size=d.get(_PID.MAXIMUM_PACKET_SIZE),
        assigned_client_identifier=d.get(_PID.ASSIGNED_CLIENT_IDENTIFIER),
        topic_alias_maximum=d.get(_PID.TOPIC_ALIAS_MAXIMUM),
        reason_string=d.get(_PID.REASON_STRING),
        wildcard_subscription_available=bool(d[_PID.WILDCARD_SUBSCRIPTION_AVAILABLE])
        if _PID.WILDCARD_SUBSCRIPTION_AVAILABLE in d
        else None,
        subscription_identifier_available=bool(
            d[_PID.SUBSCRIPTION_IDENTIFIER_AVAILABLE],
        )
        if _PID.SUBSCRIPTION_IDENTIFIER_AVAILABLE in d
        else None,
        shared_subscription_available=bool(d[_PID.SHARED_SUBSCRIPTION_AVAILABLE])
        if _PID.SHARED_SUBSCRIPTION_AVAILABLE in d
        else None,
        server_keep_alive=d.get(_PID.SERVER_KEEP_ALIVE),
        response_information=d.get(_PID.RESPONSE_INFORMATION),
        server_reference=d.get(_PID.SERVER_REFERENCE),
        authentication_method=d.get(_PID.AUTHENTICATION_METHOD),
        authentication_data=d.get(_PID.AUTHENTICATION_DATA),
        user_properties=_up_tuple(d),
    ), n


def decode_publish_properties(
    buf: bytes | memoryview,
    offset: int,
) -> tuple[PublishProperties | None, int]:
    d, n = decode_props_block(buf, offset)
    if not d:
        return None, n
    return PublishProperties(
        payload_format_indicator=d.get(_PID.PAYLOAD_FORMAT_INDICATOR),
        message_expiry_interval=d.get(_PID.MESSAGE_EXPIRY_INTERVAL),
        topic_alias=d.get(_PID.TOPIC_ALIAS),
        response_topic=d.get(_PID.RESPONSE_TOPIC),
        correlation_data=d.get(_PID.CORRELATION_DATA),
        subscription_identifier=d.get(_PID.SUBSCRIPTION_IDENTIFIER),
        content_type=d.get(_PID.CONTENT_TYPE),
        user_properties=_up_tuple(d),
    ), n


def decode_puback_properties(
    buf: bytes | memoryview,
    offset: int,
) -> tuple[PubAckProperties | None, int]:
    d, n = decode_props_block(buf, offset)
    if not d:
        return None, n
    return PubAckProperties(
        reason_string=d.get(_PID.REASON_STRING),
        user_properties=_up_tuple(d),
    ), n


def decode_subscribe_properties(
    buf: bytes | memoryview,
    offset: int,
) -> tuple[SubscribeProperties | None, int]:
    d, n = decode_props_block(buf, offset)
    if not d:
        return None, n
    return SubscribeProperties(
        subscription_identifier=d.get(_PID.SUBSCRIPTION_IDENTIFIER),
        user_properties=_up_tuple(d),
    ), n


def decode_suback_properties(
    buf: bytes | memoryview,
    offset: int,
) -> tuple[SubAckProperties | None, int]:
    d, n = decode_props_block(buf, offset)
    if not d:
        return None, n
    return SubAckProperties(
        reason_string=d.get(_PID.REASON_STRING),
        user_properties=_up_tuple(d),
    ), n


def decode_unsubscribe_properties(
    buf: bytes | memoryview,
    offset: int,
) -> tuple[UnsubscribeProperties | None, int]:
    d, n = decode_props_block(buf, offset)
    if not d:
        return None, n
    return UnsubscribeProperties(user_properties=_up_tuple(d)), n


def decode_unsuback_properties(
    buf: bytes | memoryview,
    offset: int,
) -> tuple[UnsubAckProperties | None, int]:
    d, n = decode_props_block(buf, offset)
    if not d:
        return None, n
    return UnsubAckProperties(
        reason_string=d.get(_PID.REASON_STRING),
        user_properties=_up_tuple(d),
    ), n


def decode_disconnect_properties(
    buf: bytes | memoryview,
    offset: int,
) -> tuple[DisconnectProperties | None, int]:
    d, n = decode_props_block(buf, offset)
    if not d:
        return None, n
    return DisconnectProperties(
        session_expiry_interval=d.get(_PID.SESSION_EXPIRY_INTERVAL),
        reason_string=d.get(_PID.REASON_STRING),
        server_reference=d.get(_PID.SERVER_REFERENCE),
        user_properties=_up_tuple(d),
    ), n


def decode_auth_properties(
    buf: bytes | memoryview,
    offset: int,
) -> tuple[AuthProperties | None, int]:
    d, n = decode_props_block(buf, offset)
    if not d:
        return None, n
    return AuthProperties(
        authentication_method=d.get(_PID.AUTHENTICATION_METHOD),
        authentication_data=d.get(_PID.AUTHENTICATION_DATA),
        reason_string=d.get(_PID.REASON_STRING),
        user_properties=_up_tuple(d),
    ), n
