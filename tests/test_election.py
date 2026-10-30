import pytest

from consensus.node import Role
from consensus.sim import Cluster

SEEDS = range(20)


@pytest.mark.parametrize("size", [1, 3, 5])
@pytest.mark.parametrize("seed", SEEDS)
def test_elects_exactly_one_leader_that_everyone_follows(size: int, seed: int) -> None:
    cluster = Cluster(size, seed=seed)
    leader = cluster.wait_for_leader()
    cluster.run(50)  # Let heartbeats reach every follower.

    nodes = list(cluster.nodes.values())
    assert [n.id for n in nodes if n.role is Role.LEADER] == [leader.id]
    assert all(n.term == leader.term for n in nodes)
    assert all(n.leader_id == leader.id for n in nodes)


@pytest.mark.parametrize("seed", SEEDS)
def test_leader_is_stable_without_faults(seed: int) -> None:
    cluster = Cluster(5, seed=seed)
    leader = cluster.wait_for_leader()
    term = leader.term
    cluster.run(1000)
    assert cluster.leader() is leader
    assert leader.term == term
