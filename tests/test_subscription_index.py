"""Unit tests for SubscriptionIndex entry lifecycle."""

import asyncio

from zmqtt._internal.subscription_index import EntryState, SubscriptionEntry, SubscriptionIndex


def _index() -> tuple[SubscriptionIndex, SubscriptionEntry]:
    index = SubscriptionIndex()
    entry = SubscriptionEntry(queue=asyncio.Queue(1))
    index.add("a", entry)

    return index, entry


def test_a_fresh_entry_blocks_on_a_full_queue() -> None:
    _, entry = _index()

    assert not entry.detached.is_set()


def test_draining_releases_a_suspended_delivery() -> None:
    index, entry = _index()

    index.start_draining("a")

    assert entry.state is EntryState.DRAINING
    assert entry.detached.is_set()


def test_a_refused_unsubscribe_restores_blocking_delivery() -> None:
    index, entry = _index()
    index.start_draining("a")

    index.stop_draining("a")

    assert entry.state is EntryState.OWNED
    assert not entry.detached.is_set()


def test_releasing_keeps_the_registration_but_drops_the_consumer() -> None:
    index, entry = _index()

    index.release("a")

    assert index.contains("a")
    assert not index.has_consumer("a")
    assert entry.state is EntryState.RELEASED
    assert entry.detached.is_set()


def test_removing_releases_a_suspended_delivery() -> None:
    index, entry = _index()

    index.remove("a")

    assert not index.contains("a")
    assert entry.detached.is_set()


def test_a_released_entry_is_no_longer_routed() -> None:
    index, _ = _index()

    index.release("a")

    assert index.match("a") == []
