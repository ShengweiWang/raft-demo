import random

from consensus.messages import Envelope, RequestVoteReply
from consensus.sim import Network

MESSAGE = RequestVoteReply(term=1, vote_granted=True)


def make_network() -> Network:
    return Network(random.Random(0), min_delay=1, max_delay=1)


def deliver(network: Network, src: str, dst: str) -> list[Envelope]:
    network.send(0, Envelope(src, dst, MESSAGE))
    return network.due(1)


def test_partition_blocks_both_directions_until_healed() -> None:
    network = make_network()
    network.partition({"n0"}, {"n1", "n2"})
    assert deliver(network, "n0", "n1") == []
    assert deliver(network, "n1", "n0") == []
    assert len(deliver(network, "n1", "n2")) == 1
    network.heal()
    assert len(deliver(network, "n0", "n1")) == 1


def test_partition_cuts_off_messages_already_in_flight() -> None:
    network = make_network()
    network.send(0, Envelope("n0", "n1", MESSAGE))
    network.partition({"n0"}, {"n1"})
    assert network.due(1) == []


def test_node_left_out_of_every_group_is_isolated() -> None:
    network = make_network()
    network.partition({"n0", "n1"})
    assert deliver(network, "n2", "n0") == []
    assert len(deliver(network, "n0", "n1")) == 1


def test_drop_and_duplicate() -> None:
    network = make_network()
    network.drop_rate = 1.0
    assert deliver(network, "n0", "n1") == []
    network.drop_rate = 0.0
    network.duplicate_rate = 1.0
    assert len(deliver(network, "n0", "n1")) == 2
