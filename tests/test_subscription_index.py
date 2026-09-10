"""Unit tests for SubscriptionIndex entry lifecycle."""

import asyncio
from collections.abc import Callable

import pytest

from zmqtt._internal.subscription_index import SubscriptionEntry, SubscriptionIndex
from zmqtt._internal.types.message import Message


def _index() -> tuple[SubscriptionIndex, SubscriptionEntry]:
    index = SubscriptionIndex()
    entry = SubscriptionEntry(queue=asyncio.Queue(1))
    index.add("a", entry)

    return index, entry


def test_a_fresh_entry_blocks_on_a_full_queue() -> None:
    _, entry = _index()

    assert not entry.departed.is_set()


@pytest.mark.parametrize("depart", [SubscriptionIndex.mark_departed, SubscriptionIndex.remove])
def test_departing_releases_a_suspended_delivery(depart: Callable[[SubscriptionIndex, str], object]) -> None:
    """Either way of giving up a consumer wakes a delivery blocked on its queue."""
    index, entry = _index()

    depart(index, "a")

    assert entry.departed.is_set()


def test_a_departed_entry_stays_registered_and_routable() -> None:
    """A refused unsubscribe keeps delivering; it only stops applying backpressure."""
    index, _ = _index()

    index.mark_departed("a")

    assert index.contains("a")
    assert index.is_departed("a")
    assert index.match("a") != []


def test_a_removed_entry_is_no_longer_routed() -> None:
    index, _ = _index()

    index.remove("a")

    assert not index.contains("a")
    assert index.match("a") == []


def test_owned_by_identifies_entries_by_their_queue() -> None:
    index = SubscriptionIndex()
    mine: asyncio.Queue[Message] = asyncio.Queue()
    theirs: asyncio.Queue[Message] = asyncio.Queue()
    index.add("a", SubscriptionEntry(queue=mine))
    index.add("b", SubscriptionEntry(queue=theirs))
    index.add("c", SubscriptionEntry(queue=mine))

    owned = index.owned_by(mine)

    assert [f for f, _ in owned] == ["a", "c"]
    assert [f for f, _ in index.owned_by(theirs)] == ["b"]
