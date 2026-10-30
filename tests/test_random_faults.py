"""Failure mode 12: everything at once, at random.

Each seed runs a five-node cluster through a random sequence of crashes,
restarts, partitions and heals, on a network that also loses and duplicates
messages, while writes keep arriving. The invariant checker runs after every
tick, so any safety violation fails with the seed and tick that caused it.
Then every fault is cleared and the cluster must recover.
"""

import random

import pytest

from consensus.sim import Cluster
from consensus.state_machine import Put

SEEDS = range(100)
ROUNDS = 40


def inject_random_fault(cluster: Cluster, rng: random.Random) -> None:
    ids = list(cluster.nodes)
    live = [node_id for node_id in ids if node_id not in cluster.crashed]
    roll = rng.random()
    if roll < 0.2 and live:
        cluster.crash(rng.choice(live))
    elif roll < 0.4 and cluster.crashed:
        cluster.restart(rng.choice(sorted(cluster.crashed)))
    elif roll < 0.55:
        rng.shuffle(ids)
        cut = rng.randint(1, len(ids) - 1)
        cluster.network.partition(ids[:cut], ids[cut:])
    elif roll < 0.7:
        cluster.network.heal()


@pytest.mark.parametrize("seed", SEEDS)
def test_random_faults_never_break_safety_and_the_cluster_recovers(seed: int) -> None:
    rng = random.Random(f"faults-{seed}")
    cluster = Cluster(5, seed=seed)
    cluster.network.drop_rate = 0.1
    cluster.network.duplicate_rate = 0.1

    for round_ in range(ROUNDS):
        inject_random_fault(cluster, rng)
        # Clients may be talking to any node that believes it leads, including
        # a stale leader cut off in a minority. Its writes can never commit and
        # must later be overwritten, which is the riskiest path to exercise.
        for node in cluster.live_nodes():
            node.propose(Put(f"k{rng.randrange(5)}", f"{round_}"))
        cluster.run(rng.randint(1, 30))

    # Clear every fault. Safety held throughout; now liveness must return.
    cluster.network.heal()
    cluster.network.drop_rate = 0.0
    cluster.network.duplicate_rate = 0.0
    for node_id in sorted(cluster.crashed):
        cluster.restart(node_id)
    cluster.write(Put("final", "1"))
    cluster.wait_for_convergence()

    states = [node.state_machine.data for node in cluster.nodes.values()]
    assert all(state == states[0] for state in states)
    assert states[0]["final"] == "1"
