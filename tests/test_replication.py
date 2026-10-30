import pytest

from consensus.sim import Cluster
from consensus.state_machine import Delete, Put

SEEDS = range(10)


@pytest.mark.parametrize("seed", SEEDS)
def test_committed_write_is_applied_on_every_node(seed: int) -> None:
    cluster = Cluster(3, seed=seed)
    index = cluster.submit(Put("x", "1"))
    cluster.wait_for_applied(index)
    for node in cluster.nodes.values():
        assert node.state_machine.data == {"x": "1"}


@pytest.mark.parametrize("seed", SEEDS)
def test_every_node_ends_with_the_same_log_and_state(seed: int) -> None:
    cluster = Cluster(5, seed=seed)
    last = 0
    for i in range(30):
        last = cluster.submit(Put(f"k{i % 7}", str(i)))
        cluster.run(1)  # Interleave writes with message delivery.
    cluster.wait_for_applied(last)

    expected = {f"k{k}": str(max(i for i in range(30) if i % 7 == k)) for k in range(7)}
    logs = {node.log.entries_from(1) for node in cluster.nodes.values()}
    assert len(logs) == 1
    for node in cluster.nodes.values():
        assert node.state_machine.data == expected


def test_delete_is_replicated() -> None:
    cluster = Cluster(3)
    cluster.submit(Put("x", "1"))
    index = cluster.submit(Delete("x"))
    cluster.wait_for_applied(index)
    for node in cluster.nodes.values():
        assert node.state_machine.data == {}


def test_follower_rejects_proposals() -> None:
    cluster = Cluster(3)
    leader = cluster.wait_for_leader()
    follower = next(n for n in cluster.nodes.values() if n is not leader)
    assert follower.propose(Put("x", "1")) is None


def test_single_node_commits_without_any_messages() -> None:
    cluster = Cluster(1)
    leader = cluster.wait_for_leader()
    index = leader.propose(Put("x", "1"))
    assert index is not None
    assert leader.commit_index == index
    assert leader.state_machine.data == {"x": "1"}


def test_entry_is_not_committed_before_a_majority_has_it() -> None:
    cluster = Cluster(3)
    leader = cluster.wait_for_leader()
    cluster.wait_for_applied(leader.log.last_index)  # Settle the leader's no-op.
    index = leader.propose(Put("x", "1"))
    assert index is not None
    assert leader.commit_index < index  # Nothing delivered yet.
    cluster.wait_for_applied(index)
    assert leader.commit_index >= index
