"""Unit tests that drive a single node directly, without the simulator."""

import random

from consensus.messages import AppendEntries, RequestVote, RequestVoteReply
from consensus.node import RaftNode, Role


def make_node() -> RaftNode:
    return RaftNode("n0", ["n0", "n1", "n2"], rng=random.Random(0))


def replies(node: RaftNode) -> list[object]:
    return [envelope.message for envelope in node.take_outbox()]


def test_grants_one_vote_per_term() -> None:
    node = make_node()
    node.receive("n1", RequestVote(term=1, candidate_id="n1", last_log_index=0, last_log_term=0))
    node.receive("n2", RequestVote(term=1, candidate_id="n2", last_log_index=0, last_log_term=0))
    assert replies(node) == [RequestVoteReply(1, True), RequestVoteReply(1, False)]


def test_repeated_request_from_same_candidate_is_granted_again() -> None:
    node = make_node()
    request = RequestVote(term=1, candidate_id="n1", last_log_index=0, last_log_term=0)
    node.receive("n1", request)
    node.receive("n1", request)
    assert replies(node) == [RequestVoteReply(1, True), RequestVoteReply(1, True)]


def test_becomes_candidate_exactly_at_election_timeout() -> None:
    node = make_node()
    for _ in range(node.election_timeout - 1):
        node.tick()
    role_before = node.role
    node.tick()
    assert role_before is Role.FOLLOWER
    assert node.role is Role.CANDIDATE
    assert node.term == 1
    assert {envelope.dst for envelope in node.take_outbox()} == {"n1", "n2"}


def test_heartbeat_from_leader_resets_election_timer() -> None:
    node = make_node()
    heartbeat = AppendEntries(
        term=1, leader_id="n1", prev_log_index=0, prev_log_term=0, entries=(), leader_commit=0
    )
    for _ in range(5 * node.config.election_timeout_max):
        node.tick()
        node.receive("n1", heartbeat)
    assert node.role is Role.FOLLOWER
    assert node.leader_id == "n1"
