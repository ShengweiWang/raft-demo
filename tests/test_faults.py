"""One test (or a few) per failure mode, numbered as in the README.

Every cluster test also runs the invariant checker after every tick, so besides
what each test asserts, no safety property may break at any moment.
"""

import pytest

from consensus.log import LogEntry, RaftLog
from consensus.messages import (
    AppendEntries,
    AppendEntriesReply,
    Message,
    NodeId,
    RequestVote,
    RequestVoteReply,
)
from consensus.node import PersistentState, RaftNode, Role
from consensus.sim import Cluster
from consensus.state_machine import Put

SEEDS = range(10)


def entry(term: int, value: str) -> LogEntry:
    return LogEntry(term, Put("k", value))


def sent(node: RaftNode) -> list[Message]:
    return [envelope.message for envelope in node.take_outbox()]


def make_leader(node: RaftNode, voters: list[NodeId]) -> None:
    """Drive a single node through an election by handing it the given votes."""
    while node.role is not Role.CANDIDATE:
        node.tick()
    for voter in voters:
        node.receive(voter, RequestVoteReply(node.term, vote_granted=True))
    assert is_leader(node)
    node.take_outbox()


def role(node: RaftNode) -> Role:
    # Read through a function so mypy doesn't assume a role checked earlier in a
    # test is still the same after the node has received more messages.
    return node.role


def is_leader(node: RaftNode) -> bool:
    return role(node) is Role.LEADER


def data(cluster: Cluster) -> list[dict[str, str]]:
    return [node.state_machine.data for node in cluster.live_nodes()]


# 1. Delayed and reordered messages


def test_late_append_entries_does_not_undo_a_newer_one() -> None:
    follower = RaftNode("n1", ["n0", "n1", "n2"])
    entries = (entry(1, "a"), entry(1, "b"), entry(1, "c"))
    newer = AppendEntries(1, "n0", 0, 0, entries, leader_commit=0)
    older = AppendEntries(1, "n0", 0, 0, entries[:1], leader_commit=0)

    follower.receive("n0", newer)
    follower.receive("n0", older)

    assert follower.log.entries_from(1) == entries
    assert sent(follower) == [
        AppendEntriesReply(1, success=True, match_index=3),
        AppendEntriesReply(1, success=True, match_index=1),
    ]


def test_late_reply_does_not_move_the_leader_backwards() -> None:
    leader = RaftNode("n0", ["n0", "n1", "n2"])
    make_leader(leader, ["n1"])
    leader.propose(Put("x", "1"))  # Index 2, after the no-op.

    leader.receive("n1", AppendEntriesReply(leader.term, success=True, match_index=2))
    leader.receive("n1", AppendEntriesReply(leader.term, success=True, match_index=1))

    assert leader.match_index["n1"] == 2
    assert leader.next_index["n1"] == 3


@pytest.mark.parametrize("seed", SEEDS)
def test_long_delays_still_converge(seed: int) -> None:
    cluster = Cluster(5, seed=seed)
    cluster.network.max_delay = 8  # Close to the election timeout: heavy reordering.
    for i in range(10):
        cluster.write(Put(f"k{i}", str(i)))
    cluster.wait_for_convergence()
    assert all(d == {f"k{i}": str(i) for i in range(10)} for d in data(cluster))


# 2. Lost messages


@pytest.mark.parametrize("seed", SEEDS)
def test_lost_messages_slow_progress_but_never_break_it(seed: int) -> None:
    cluster = Cluster(5, seed=seed)
    cluster.network.drop_rate = 0.3
    for i in range(10):
        cluster.write(Put(f"k{i}", str(i)))
    cluster.network.drop_rate = 0.0
    cluster.wait_for_convergence()
    assert all(d == {f"k{i}": str(i) for i in range(10)} for d in data(cluster))


# 3. Duplicated messages


def test_duplicate_vote_is_counted_once() -> None:
    candidate = RaftNode("n0", ["n0", "n1", "n2", "n3", "n4"])
    while candidate.role is not Role.CANDIDATE:
        candidate.tick()
    vote = RequestVoteReply(candidate.term, vote_granted=True)

    candidate.receive("n1", vote)
    candidate.receive("n1", vote)
    assert candidate.role is Role.CANDIDATE  # Two distinct votes (n0, n1) of three needed.

    candidate.receive("n2", vote)
    assert is_leader(candidate)


@pytest.mark.parametrize("seed", SEEDS)
def test_duplicated_messages_change_nothing(seed: int) -> None:
    cluster = Cluster(5, seed=seed)
    cluster.network.duplicate_rate = 0.5
    for i in range(10):
        cluster.write(Put(f"k{i}", str(i)))
    cluster.wait_for_convergence()
    assert all(d == {f"k{i}": str(i) for i in range(10)} for d in data(cluster))


# 4. Competing candidates (split vote)


@pytest.mark.parametrize("seed", SEEDS)
def test_split_vote_elects_no_one_and_retries_in_higher_terms(seed: int) -> None:
    # Four nodes split into two pairs: any candidate gets at most 2 of the 3
    # votes it needs. A partition makes the split certain rather than lucky.
    cluster = Cluster(4, seed=seed)
    cluster.network.partition({"n0", "n2"}, {"n1", "n3"})
    cluster.run(200)
    assert cluster.leader() is None
    assert max(node.term for node in cluster.nodes.values()) >= 3  # Kept retrying.

    cluster.network.heal()
    cluster.wait_for_leader()
    assert sum(node.role is Role.LEADER for node in cluster.nodes.values()) == 1


# 5. Leader crash


@pytest.mark.parametrize("seed", SEEDS)
def test_leader_crash_elects_a_new_leader_and_keeps_committed_writes(seed: int) -> None:
    cluster = Cluster(5, seed=seed)
    cluster.write(Put("before", "1"))
    old = cluster.wait_for_convergence()

    cluster.crash(old.id)
    new = cluster.wait_for_leader()
    assert new is not old
    assert new.term > old.term

    cluster.write(Put("after", "1"))
    cluster.wait_for_convergence()
    assert all(d == {"before": "1", "after": "1"} for d in data(cluster))


@pytest.mark.parametrize("seed", SEEDS)
def test_write_the_crashed_leader_never_sent_is_lost(seed: int) -> None:
    cluster = Cluster(5, seed=seed)
    old = cluster.wait_for_convergence()
    old.propose(Put("lost", "1"))
    cluster.crash(old.id)  # Before its next tick, so the entry never leaves it.

    cluster.write(Put("kept", "1"))
    cluster.restart(old.id)
    cluster.wait_for_convergence()
    assert all(d == {"kept": "1"} for d in data(cluster))


# 6. Follower crash and restart


@pytest.mark.parametrize("seed", SEEDS)
def test_restarted_follower_catches_up_by_replaying_the_log(seed: int) -> None:
    cluster = Cluster(3, seed=seed)
    cluster.write(Put("x", "1"))
    leader = cluster.wait_for_convergence()
    follower = next(n for n in cluster.nodes.values() if n is not leader)
    term = follower.term

    cluster.crash(follower.id)
    cluster.write(Put("y", "2"))  # Two of three nodes are still a majority.
    cluster.restart(follower.id)

    restarted = cluster.nodes[follower.id]
    assert restarted.term == term  # Kept on disk.
    assert restarted.state_machine.data == {}  # Not kept.
    cluster.wait_for_convergence()
    assert restarted.state_machine.data == {"x": "1", "y": "2"}


def test_restart_does_not_allow_a_second_vote_in_the_same_term() -> None:
    state = PersistentState()
    node = RaftNode("n0", ["n0", "n1", "n2"], state=state)
    node.receive("n1", RequestVote(term=1, candidate_id="n1", last_log_index=0, last_log_term=0))
    assert sent(node) == [RequestVoteReply(1, True)]

    restarted = RaftNode("n0", ["n0", "n1", "n2"], state=state)
    request = RequestVote(term=1, candidate_id="n2", last_log_index=0, last_log_term=0)
    restarted.receive("n2", request)
    assert sent(restarted) == [RequestVoteReply(1, False)]


# 7. Stale leader cut off in a minority


@pytest.mark.parametrize("seed", SEEDS)
def test_stale_leader_cannot_commit_and_steps_down_when_healed(seed: int) -> None:
    cluster = Cluster(5, seed=seed)
    old = cluster.wait_for_convergence()
    partner, *majority = [node_id for node_id in cluster.nodes if node_id != old.id]
    cluster.network.partition({old.id, partner}, majority)

    stale_index = old.propose(Put("stale", "1"))
    assert stale_index is not None
    cluster.run_until(lambda: cluster.leader() not in (None, old))
    cluster.write(Put("fresh", "1"))
    cluster.run(100)
    assert old.role is Role.LEADER  # It still believes it leads...
    assert old.commit_index < stale_index  # ...but cannot commit anything.

    cluster.network.heal()
    cluster.wait_for_convergence()
    assert role(old) is Role.FOLLOWER
    assert all(d == {"fresh": "1"} for d in data(cluster))


# 8. Majority lost


@pytest.mark.parametrize("seed", SEEDS)
def test_without_a_majority_nothing_commits_until_healed(seed: int) -> None:
    cluster = Cluster(5, seed=seed)
    cluster.write(Put("x", "1"))
    old = cluster.wait_for_convergence()
    committed = old.commit_index
    cluster.network.partition({"n0", "n1"}, {"n2", "n3"}, {"n4"})

    old.propose(Put("blocked", "1"))
    cluster.run(300)
    assert max(node.commit_index for node in cluster.nodes.values()) == committed
    assert all(n.term == old.term for n in cluster.nodes.values() if n.role is Role.LEADER)

    cluster.network.heal()
    cluster.write(Put("y", "2"))
    cluster.wait_for_convergence()
    # "blocked" was never committed, so it may or may not survive: if a node
    # holding it wins the next election, it commits along with that leader's
    # no-op. Either outcome is correct; only committed writes are guaranteed.
    for d in data(cluster):
        assert d.items() >= {"x": "1", "y": "2"}.items()


# 9. A follower's log diverged over several terms


@pytest.mark.parametrize("seed", SEEDS)
def test_diverged_follower_log_is_repaired(seed: int) -> None:
    cluster = Cluster(3, seed=seed)
    n0, n1, n2 = cluster.nodes.values()
    shared = [entry(1, "a"), entry(1, "b"), entry(2, "c")]
    n0.state.current_term = 2
    n0.state.log = RaftLog([*shared, entry(2, "x"), entry(2, "y")])
    for node in (n1, n2):
        node.state.current_term = 3
        node.state.log = RaftLog([*shared, entry(3, "d"), entry(3, "e"), entry(3, "f")])
    n1.election_elapsed = n1.election_timeout - 1  # n1 campaigns first.

    leader = cluster.wait_for_convergence()
    assert leader is n1
    assert n0.log.entries_from(1)[:6] == (*shared, entry(3, "d"), entry(3, "e"), entry(3, "f"))


def test_follower_commits_only_what_the_message_proved() -> None:
    # The leader's log is a, x, y with all three committed. The follower holds
    # a, x and a stale z from a deposed leader at index 3. A leader that sends
    # entries in batches may send just x (this one always sends everything, so
    # it can't produce this message, but the rule must not depend on that).
    # The message proves the logs match up to index 2 only, so the follower
    # must not commit index 3: its z there is not the leader's y.
    follower = RaftNode("n1", ["n0", "n1", "n2"])
    follower.state.current_term = 3
    follower.state.log = RaftLog([entry(1, "a"), entry(2, "x"), entry(2, "z")])
    batch = AppendEntries(3, "n0", 1, 1, (entry(2, "x"),), leader_commit=3)

    follower.receive("n0", batch)

    assert follower.commit_index == 2


# 10. An older-term entry reaches a majority but isn't safe yet


def test_leader_does_not_commit_old_term_entry_by_counting() -> None:
    # S1 from the README's timeline, at step 3: it wins term 4 holding entry A
    # from term 2 at index 2, then learns that S2 and S3 also hold A.
    ids = ["S1", "S2", "S3", "S4", "S5"]
    a = entry(2, "A")
    s1 = RaftNode("S1", ids, state=PersistentState(3, None, RaftLog([entry(1, "x"), a])))
    make_leader(s1, ["S2", "S3"])
    assert s1.term == 4
    assert s1.log.last_index == 3  # Its no-op from term 4.

    for peer in ("S2", "S3"):
        s1.receive(peer, AppendEntriesReply(4, success=True, match_index=2))
    assert s1.commit_index == 0  # A is on a majority, but is from term 2.

    for peer in ("S2", "S3"):
        s1.receive(peer, AppendEntriesReply(4, success=True, match_index=3))
    assert s1.commit_index == 3  # The no-op commits, and A with it.


def test_voters_refuse_the_older_log_once_the_no_op_reaches_them() -> None:
    # Step 4 of the timeline: S5 (last entry from term 3) asks S2 for a vote.
    ids = ["S1", "S2", "S3", "S4", "S5"]
    request = RequestVote(term=5, candidate_id="S5", last_log_index=2, last_log_term=3)
    before = [entry(1, "x"), entry(2, "A")]
    after = [*before, LogEntry(4, Put("k", "no-op"))]

    s2 = RaftNode("S2", ids, state=PersistentState(4, None, RaftLog(before)))
    s2.receive("S5", request)
    assert sent(s2) == [RequestVoteReply(5, True)]  # Without the no-op, S5 wins.

    s2 = RaftNode("S2", ids, state=PersistentState(4, None, RaftLog(after)))
    s2.receive("S5", request)
    assert sent(s2) == [RequestVoteReply(5, False)]  # With it, S5 cannot.


# 11. A node rejoins after a long isolation


@pytest.mark.parametrize("seed", SEEDS)
@pytest.mark.parametrize("writes_while_away", [False, True])
def test_rejoining_node_forces_an_election(seed: int, writes_while_away: bool) -> None:
    cluster = Cluster(5, seed=seed)
    old = cluster.wait_for_convergence()
    loner = next(node_id for node_id in cluster.nodes if node_id != old.id)
    cluster.network.partition([node_id for node_id in cluster.nodes if node_id != loner])

    if writes_while_away:
        cluster.write(Put("x", "1"))
    cluster.run(200)
    assert cluster.leader() is old  # The majority is undisturbed...
    assert cluster.nodes[loner].term >= old.term + 3  # ...while the loner's term climbs.

    term_before = old.term  # Read now: old.term moves once it hears the higher term.
    cluster.network.heal()
    # The loner's higher term forces a new election. Without writes its log is
    # still up to date, so the logs already match: wait for the election itself.
    cluster.run_until(
        lambda: (
            max((n.term for n in cluster.nodes.values() if is_leader(n)), default=0) > term_before
        )
    )
    new = cluster.wait_for_convergence()
    if writes_while_away:
        assert new.id != loner  # A node missing committed entries can't win.
