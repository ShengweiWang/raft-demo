"""The checker must catch each kind of violation. These tests put nodes into
states that a correct Raft never reaches, then run one check."""

import pytest

from consensus.invariants import SafetyViolation
from consensus.log import LogEntry, RaftLog
from consensus.node import RaftNode, Role
from consensus.sim import Cluster
from consensus.state_machine import Put


def entry(term: int, value: str) -> LogEntry:
    return LogEntry(term, Put("k", value))


def set_state(
    node: RaftNode,
    *,
    term: int,
    log: list[LogEntry] | None = None,
    commit: int = 0,
    role: Role = Role.FOLLOWER,
) -> None:
    node.state.current_term = term
    for e in log or []:
        node.log.append(e)
    node.commit_index = commit
    node.role = role


@pytest.fixture
def cluster() -> Cluster:
    return Cluster(3)


def test_consistent_state_passes(cluster: Cluster) -> None:
    n0, n1, _ = cluster.nodes.values()
    set_state(n0, term=1, log=[entry(1, "a")], commit=1, role=Role.LEADER)
    set_state(n1, term=1, log=[entry(1, "a")], commit=1)
    cluster.checker.check()


def test_two_leaders_in_one_term(cluster: Cluster) -> None:
    n0, n1, _ = cluster.nodes.values()
    set_state(n0, term=1, role=Role.LEADER)
    set_state(n1, term=1, role=Role.LEADER)
    with pytest.raises(SafetyViolation, match="two leaders in term 1"):
        cluster.checker.check()


def test_two_leaders_in_one_term_at_different_moments(cluster: Cluster) -> None:
    n0, n1, _ = cluster.nodes.values()
    set_state(n0, term=1, role=Role.LEADER)
    cluster.checker.check()
    n0.role = Role.FOLLOWER
    set_state(n1, term=1, role=Role.LEADER)
    with pytest.raises(SafetyViolation, match="two leaders in term 1"):
        cluster.checker.check()


def test_nodes_commit_different_entries_at_one_index(cluster: Cluster) -> None:
    n0, n1, _ = cluster.nodes.values()
    set_state(n0, term=1, log=[entry(1, "a")], commit=1)
    set_state(n1, term=2, log=[entry(2, "b")], commit=1)
    with pytest.raises(SafetyViolation, match="was committed there before"):
        cluster.checker.check()


def test_committed_entry_later_overwritten(cluster: Cluster) -> None:
    n0 = cluster.nodes["n0"]
    set_state(n0, term=1, log=[entry(1, "a")], commit=1)
    cluster.checker.check()
    n0.state.log = RaftLog([entry(2, "b")])
    with pytest.raises(SafetyViolation, match="was committed there before"):
        cluster.checker.check()


def test_leader_missing_a_committed_entry(cluster: Cluster) -> None:
    n0, n1, _ = cluster.nodes.values()
    set_state(n0, term=1, log=[entry(1, "a")], commit=1)
    cluster.checker.check()
    set_state(n1, term=2, role=Role.LEADER)
    with pytest.raises(SafetyViolation, match="lacks committed entry"):
        cluster.checker.check()


def test_stale_leader_from_an_older_term_may_lack_a_committed_entry(cluster: Cluster) -> None:
    n0, n1, _ = cluster.nodes.values()
    set_state(n0, term=2, log=[entry(2, "a")], commit=1)
    set_state(n1, term=1, role=Role.LEADER)
    cluster.checker.check()


def test_same_index_and_term_with_different_history(cluster: Cluster) -> None:
    n0, n1, _ = cluster.nodes.values()
    set_state(n0, term=2, log=[entry(1, "a"), entry(2, "x")])
    set_state(n1, term=2, log=[entry(1, "b"), entry(2, "x")])
    with pytest.raises(SafetyViolation, match="logs differ before it"):
        cluster.checker.check()


def test_commit_index_going_backwards(cluster: Cluster) -> None:
    n0 = cluster.nodes["n0"]
    set_state(n0, term=1, log=[entry(1, "a")], commit=1)
    cluster.checker.check()
    n0.commit_index = 0
    with pytest.raises(SafetyViolation, match="commit index went back"):
        cluster.checker.check()


def test_term_going_backwards(cluster: Cluster) -> None:
    n0 = cluster.nodes["n0"]
    set_state(n0, term=2)
    cluster.checker.check()
    n0.state.current_term = 1
    with pytest.raises(SafetyViolation, match="term went back"):
        cluster.checker.check()


def test_applying_an_uncommitted_entry(cluster: Cluster) -> None:
    n0 = cluster.nodes["n0"]
    set_state(n0, term=1, log=[entry(1, "a")], commit=0)
    n0.last_applied = 1
    with pytest.raises(SafetyViolation, match="before committing it"):
        cluster.checker.check()


def test_restart_may_reset_commit_index(cluster: Cluster) -> None:
    n0 = cluster.nodes["n0"]
    set_state(n0, term=1, log=[entry(1, "a")], commit=1)
    cluster.checker.check()
    cluster.crash("n0")
    cluster.restart("n0")
    assert cluster.nodes["n0"].commit_index == 0
    cluster.checker.check()
