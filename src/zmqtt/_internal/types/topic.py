from zmqtt.errors import MQTTInvalidTopicError

# Alias values travel as a two-byte integer; 0 is a protocol error (§3.3.2.3.4).
_MAX_ALIAS = 65535


def validate_publish(topic: str, *, topic_alias: int | None = None) -> None:
    """Validate an outgoing PUBLISH topic (MQTT 5 §3.3.2.3.4 aware).

    An empty Topic Name is legal only when a Topic Alias accompanies it;
    the alias value itself is validated against 1..65535.
    """
    if not topic:
        if topic_alias is not None:
            if not 1 <= topic_alias <= _MAX_ALIAS:
                msg = f"Topic Alias must be in 1..{_MAX_ALIAS}, got {topic_alias}"
                raise MQTTInvalidTopicError(msg)
            return
        msg = "Topic must not be empty"
        raise MQTTInvalidTopicError(msg)
    if "#" in topic or "+" in topic:
        msg = f"Wildcards not allowed in publish topic: {topic!r}"
        raise MQTTInvalidTopicError(msg)
    if "$" in topic[1:]:
        msg = f"'$' is only valid as the first character of a topic: {topic!r}"
        raise MQTTInvalidTopicError(msg)


def validate_subscribe_topic(topic: str) -> None:
    if not topic:
        msg = "Topic filter must not be empty"
        raise MQTTInvalidTopicError(msg)
    if "$" in topic[1:]:
        msg = f"'$' is only valid as the first character of a topic filter: {topic!r}"
        raise MQTTInvalidTopicError(msg)
    if "#" in topic:
        idx = topic.index("#")
        if idx != len(topic) - 1:
            msg = f"'#' must be the last character in a topic filter: {topic!r}"
            raise MQTTInvalidTopicError(msg)
        if idx > 0 and topic[idx - 1] != "/":
            msg = f"'#' must be preceded by '/' in a topic filter: {topic!r}"
            raise MQTTInvalidTopicError(msg)
    for level in topic.split("/"):
        if "+" in level and level != "+":
            msg = f"'+' must occupy an entire topic level in filter: {topic!r}"
            raise MQTTInvalidTopicError(msg)


def validate_response_topic(topic: str) -> None:
    validate_publish(topic)
