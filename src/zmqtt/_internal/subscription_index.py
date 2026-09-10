import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field

from zmqtt._internal.topic_matching import _segment_rank, _topic_matches
from zmqtt._internal.types.message import Message


@dataclass(slots=True, kw_only=True)
class SubscriptionEntry:
    queue: asyncio.Queue[Message]
    departed: asyncio.Event = field(default_factory=asyncio.Event)  # also wakes a delivery blocked on a full queue
    auto_ack: bool = True
    actual_filter: str = ""  # filter with broker-stripped subscription decorators removed
    subscription_identifier: int | None = None  # v5; echoed by the broker on PUBLISH


@dataclass(slots=True)
class _Node:
    children: dict[str, "_Node"] = field(default_factory=dict)
    wildcard_children: dict[str, "_Node"] = field(default_factory=dict)
    entries: list[tuple[str, SubscriptionEntry]] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class SubscriptionClaim:
    """What one subscription held at the instant it was read.

    Derived from the index rather than mirrored on the Subscription, so it cannot
    drift out of step with what the broker was actually told.
    """

    filters: tuple[str, ...] = ()
    departed: bool = False


@dataclass(frozen=True, slots=True)
class SubscriptionSelection:
    recipient: tuple[str, SubscriptionEntry] | None
    identifier_missing: bool = False
    tied_filters: tuple[str, ...] = ()


class SubscriptionIndex:
    def __init__(self) -> None:
        self._root = _Node()
        self._entries: dict[str, SubscriptionEntry] = {}
        self._by_identifier: dict[int, dict[str, SubscriptionEntry]] = {}
        self._response_observers: set[str] = set()

    def add(self, filter_: str, entry: SubscriptionEntry) -> None:
        if filter_ in self._entries:
            self.remove(filter_)

        self._entries[filter_] = entry
        self._link(filter_, entry)

    def _link(self, filter_: str, entry: SubscriptionEntry) -> None:
        tree_filter = entry.actual_filter or filter_
        node = self._root
        for part in tree_filter.split("/"):
            mapping = node.wildcard_children if part in {"+", "#"} else node.children
            node = mapping.setdefault(part, _Node())

        node.entries.append((filter_, entry))
        identifier = entry.subscription_identifier
        if identifier is not None:
            self._by_identifier.setdefault(identifier, {})[filter_] = entry

    def contains(self, filter_: str) -> bool:
        return filter_ in self._entries

    def owned_by(self, queue: "asyncio.Queue[Message]") -> list[tuple[str, SubscriptionEntry]]:
        """Entries fed by ``queue``, in subscribe order.

        Each Subscription creates exactly one queue, so the queue identifies its
        owner and no subscription needs to remember what it holds.
        """
        return [(f, e) for f, e in self._entries.items() if e.queue is queue]

    def is_departed(self, filter_: str) -> bool:
        """The owner asked to leave and the broker has not released the filter yet."""
        entry = self._entries.get(filter_)
        return entry is not None and entry.departed.is_set()

    def mark_departed(self, filter_: str) -> bool:
        """Give up the consumer, leaving the filter registered and routable.

        Delivery still fills the buffer but stops blocking on it: with nobody
        draining, a blocked put would hold the read loop for every subscription.
        """
        entry = self._entries.get(filter_)
        if entry is None or entry.departed.is_set():
            return False

        entry.departed.set()
        return True

    def add_response_observer(self, topic: str) -> bool:
        """Register an exact response-topic observer.

        Returns ``True`` only for the first observer on this connection.
        """
        if topic in self._response_observers:
            return False
        self._response_observers.add(topic)
        return True

    def remove_response_observer(self, topic: str) -> bool:
        if topic not in self._response_observers:
            return False
        self._response_observers.remove(topic)
        return True

    def has_response_observer(self, topic: str) -> bool:
        return topic in self._response_observers

    def get(self, filter_: str, default: SubscriptionEntry | None = None) -> SubscriptionEntry | None:
        return self._entries.get(filter_, default)

    def remove(self, filter_: str) -> SubscriptionEntry | None:
        entry = self._entries.pop(filter_, None)
        if entry is None:
            return None

        self._unlink(filter_, entry)
        return entry

    def _unlink(self, filter_: str, entry: SubscriptionEntry) -> None:
        entry.departed.set()
        tree_filter = entry.actual_filter or filter_
        self._remove_entry(tree_filter.split("/"), filter_, entry, self._root)

        identifier = entry.subscription_identifier
        if identifier is not None:
            identified = self._by_identifier.get(identifier)
            if identified is not None:
                identified.pop(filter_, None)
                if not identified:
                    self._by_identifier.pop(identifier)

    def clear(self) -> None:
        self._root = _Node()
        self._entries.clear()
        self._by_identifier.clear()
        self._response_observers.clear()

    def add_many(self, entries: Mapping[str, SubscriptionEntry]) -> None:
        for filter_, entry in entries.items():
            self.add(filter_, entry)

    def match(self, topic: str) -> list[tuple[str, SubscriptionEntry]]:
        matches: list[tuple[str, SubscriptionEntry]] = []
        self._collect(
            node=self._root,
            parts=topic.split("/"),
            collect_to=matches,
        )
        filtered = [item for item in matches if _topic_matches(self._actual_filter(*item), topic)]
        return sorted(filtered, key=lambda item: self._specificity(self._actual_filter(*item)))

    def best(self, topic: str) -> tuple[str, SubscriptionEntry] | None:
        return self.select(topic).recipient

    def by_identifier(self, identifier: int) -> list[tuple[str, SubscriptionEntry]]:
        return list(self._by_identifier.get(identifier, {}).items())

    def select(self, topic: str) -> SubscriptionSelection:
        return self._select_from(self.match(topic))

    def select_by_identifier(self, topic: str, identifier: int) -> SubscriptionSelection:
        identified = self.by_identifier(identifier)
        if identified:
            matching = [item for item in identified if _topic_matches(self._actual_filter(*item), topic)]
            return self._select_from(matching or identified)

        fallback = self.select(topic)
        return SubscriptionSelection(
            recipient=fallback.recipient,
            identifier_missing=True,
            tied_filters=fallback.tied_filters,
        )

    def _select_from(
        self,
        candidates: list[tuple[str, SubscriptionEntry]],
    ) -> SubscriptionSelection:
        if not candidates:
            return SubscriptionSelection(recipient=None)

        best_key = min(self._specificity(self._actual_filter(*item)) for item in candidates)
        winners = [item for item in candidates if self._specificity(self._actual_filter(*item)) == best_key]
        tied_filters = tuple(filter_ for filter_, _ in winners) if len(winners) > 1 else ()
        return SubscriptionSelection(
            recipient=winners[0],
            tied_filters=tied_filters,
        )

    def _remove_entry(
        self,
        parts: list[str],
        filter_: str,
        entry: SubscriptionEntry,
        node: _Node,
    ) -> None:
        if not parts:
            node.entries = [item for item in node.entries if not (item[0] == filter_ and item[1] is entry)]
            return

        part = parts[0]
        mapping = node.wildcard_children if part in {"#", "+"} else node.children

        child = mapping.get(part)
        if child is None:
            return

        self._remove_entry(parts[1:], filter_, entry, child)

        if not child.children and not child.wildcard_children and not child.entries:
            mapping.pop(part)

    def _collect(
        self,
        node: _Node,
        parts: list[str],
        collect_to: list[tuple[str, SubscriptionEntry]],
        idx: int = 0,
    ) -> None:
        if "#" in node.wildcard_children:
            collect_to.extend(node.wildcard_children["#"].entries)
        if idx == len(parts):
            collect_to.extend(node.entries)
            return

        part = parts[idx]
        if part in node.children:
            self._collect(
                node=node.children[part],
                parts=parts,
                idx=idx + 1,
                collect_to=collect_to,
            )
        if "+" in node.wildcard_children:
            self._collect(
                node=node.wildcard_children["+"],
                parts=parts,
                idx=idx + 1,
                collect_to=collect_to,
            )

    def _actual_filter(self, filter_: str, entry: SubscriptionEntry) -> str:
        return entry.actual_filter or filter_

    def _specificity(self, filter_: str) -> tuple[int, ...]:
        return tuple(_segment_rank(segment) for segment in filter_.split("/"))
