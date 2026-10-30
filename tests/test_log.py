from consensus.log import LogEntry, RaftLog
from consensus.state_machine import Put


def entry(term: int, value: str = "") -> LogEntry:
    return LogEntry(term, Put("k", value))


def test_empty_log_has_sentinel_at_index_zero() -> None:
    log = RaftLog()
    assert log.last_index == 0
    assert log.last_term == 0
    assert log.matches(0, 0)


def test_append_is_one_indexed() -> None:
    log = RaftLog()
    assert log.append(entry(1, "a")) == 1
    assert log.append(entry(2, "b")) == 2
    assert log[1] == entry(1, "a")
    assert log.term_at(2) == 2
    assert log.last_term == 2


def test_matches_rejects_missing_index_and_wrong_term() -> None:
    log = RaftLog([entry(1), entry(1)])
    assert log.matches(2, 1)
    assert not log.matches(3, 1)
    assert not log.matches(2, 2)


def test_merge_appends_new_entries() -> None:
    log = RaftLog([entry(1, "a")])
    log.merge(1, [entry(1, "b"), entry(1, "c")])
    assert log.entries_from(1) == (entry(1, "a"), entry(1, "b"), entry(1, "c"))


def test_merge_of_entries_already_held_is_a_no_op() -> None:
    log = RaftLog([entry(1, "a"), entry(1, "b"), entry(1, "c")])
    log.merge(0, [entry(1, "a")])
    assert log.last_index == 3


def test_merge_truncates_from_the_first_conflict() -> None:
    log = RaftLog([entry(1, "a"), entry(2, "stale"), entry(2, "stale too")])
    log.merge(1, [entry(3, "b")])
    assert log.entries_from(1) == (entry(1, "a"), entry(3, "b"))
