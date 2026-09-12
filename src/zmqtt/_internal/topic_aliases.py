"""Connection-scoped Topic Alias state (MQTT 5.0 §3.3.2.3.4).

Two independent mappings live per network connection:

- Outgoing: the client registers alias→topic and may then publish with an
  empty Topic Name, sending only the alias. The limit is the server's
  Topic Alias Maximum from CONNACK (absent ⇒ 0 ⇒ aliases not allowed).
- Incoming: the client advertises its own Topic Alias Maximum in CONNECT;
  0 (the default) tells the server not to use aliases. Incoming PUBLISHes
  carrying an alias are resolved to the full topic before routing.

Both mappings are cleared on every new network connection, resumed
sessions included — aliases are strictly connection-scoped state.
"""

from __future__ import annotations

# Alias values travel as a two-byte integer; 0 is a protocol error (§3.3.2.3.4).
MAX_ALIAS = 65535
_MIN_ALIAS = 1


class TopicAliasError(Exception):
    """Base for local Topic Alias violations."""


class OutgoingAliasLimitError(TopicAliasError):
    """The server's Topic Alias Maximum does not allow this alias."""


class UnknownIncomingAliasError(TopicAliasError):
    """An alias-only PUBLISH referenced an alias this connection never set."""


def validate_alias_range(alias: int) -> None:
    """Alias 0 is a protocol error; values fit a two-byte integer."""
    if not _MIN_ALIAS <= alias <= MAX_ALIAS:
        msg = f"Topic Alias must be in {_MIN_ALIAS}..{MAX_ALIAS}, got {alias}"
        raise TopicAliasError(msg)


class TopicAliasTable:
    """One direction of the connection-scoped alias mapping."""

    def __init__(self, limit: int = 0) -> None:
        self._limit = limit
        self._aliases: dict[int, str] = {}

    @property
    def limit(self) -> int:
        return self._limit

    def clear(self) -> None:
        self._aliases.clear()

    def set(self, alias: int, topic: str) -> None:
        """Register or re-bind *alias* to *topic* (full Topic Name)."""
        validate_alias_range(alias)
        if alias > self._limit:
            msg = f"Topic Alias {alias} exceeds the peer's Topic Alias Maximum {self._limit}"
            raise OutgoingAliasLimitError(msg)
        self._aliases[alias] = topic

    def get(self, alias: int) -> str | None:
        """Resolve *alias* to its full topic, or None if never set."""
        return self._aliases.get(alias)


def resolve_incoming(topic: str, alias: int | None, table: TopicAliasTable) -> str:
    """Apply §3.3.2.3.4 to one incoming PUBLISH; return the full topic.

    - alias set + full topic: (re)bind and return the topic as-is.
    - alias set + empty topic: resolve through the table; an unknown alias
      is a protocol error (the sender must establish it first).
    - alias absent: topic must be non-empty (regular publish).
    """
    if alias is None:
        if not topic:
            msg = "Empty Topic Name without a Topic Alias"
            raise TopicAliasError(msg)
        return topic
    validate_alias_range(alias)
    if alias > table.limit:
        msg = f"Peer sent Topic Alias {alias} above our advertised Topic Alias Maximum {table.limit}"
        raise TopicAliasError(msg)
    if topic:
        table.set(alias, topic)
        return topic
    resolved = table.get(alias)
    if resolved is None:
        msg = f"Alias-only PUBLISH for unknown Topic Alias {alias}"
        raise UnknownIncomingAliasError(msg)
    return resolved
