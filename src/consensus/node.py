"""A single Raft node: leader election and log replication."""

from __future__ import annotations

import random
from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import Enum

from consensus.log import LogEntry, RaftLog
from consensus.messages import (
    AppendEntries,
    AppendEntriesReply,
    Envelope,
    Message,
    NodeId,
    RequestVote,
    RequestVoteReply,
)
from consensus.state_machine import Command, KVStore, Noop


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass(frozen=True)
class Config:
    """Timing, in ticks. Heartbeats must arrive well within the election timeout."""

    election_timeout_min: int = 10
    election_timeout_max: int = 20
    heartbeat_interval: int = 3


@dataclass
class PersistentState:
    """State that survives a crash. Everything else on the node is lost."""

    current_term: int = 0
    voted_for: NodeId | None = None
    log: RaftLog = field(default_factory=RaftLog)


class RaftNode:
    """One Raft participant.

    The node does no I/O and has no real clock: time moves only when `tick` is
    called, messages arrive through `receive`, and outgoing messages are queued
    in `outbox` for the caller to deliver. That makes it deterministic and easy
    to test.
    """

    def __init__(
        self,
        node_id: NodeId,
        peers: Iterable[NodeId],
        *,
        state: PersistentState | None = None,
        rng: random.Random | None = None,
        config: Config | None = None,
    ) -> None:
        self.id = node_id
        self.peers = tuple(p for p in peers if p != node_id)
        self.state = state or PersistentState()
        self.config = config or Config()
        self.rng = rng or random.Random()
        self.state_machine = KVStore()
        self.outbox: list[Envelope] = []

        self.role = Role.FOLLOWER
        self.leader_id: NodeId | None = None
        self.commit_index = 0
        self.last_applied = 0

        self.votes: set[NodeId] = set()  # Candidate only.
        self.next_index: dict[NodeId, int] = {}  # Leader only.
        self.match_index: dict[NodeId, int] = {}  # Leader only.

        self.election_elapsed = 0
        self.election_timeout = self._random_election_timeout()
        self.heartbeat_elapsed = 0

    @property
    def term(self) -> int:
        return self.state.current_term

    @property
    def log(self) -> RaftLog:
        return self.state.log

    @property
    def quorum(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    # Public interface

    def tick(self) -> None:
        if self.role is Role.LEADER:
            self.heartbeat_elapsed += 1
            if self.heartbeat_elapsed >= self.config.heartbeat_interval:
                self._broadcast_append_entries()
        else:
            self.election_elapsed += 1
            if self.election_elapsed >= self.election_timeout:
                self._start_election()

    def propose(self, command: Command) -> int | None:
        """Append a command if we are leader and return its log index, else None.

        The index is not committed yet: the entry can still be lost if
        leadership changes before a majority stores it.
        """
        if self.role is not Role.LEADER:
            return None
        index = self.log.append(LogEntry(self.term, command))
        self._broadcast_append_entries()
        self._advance_commit_index()  # A single-node cluster commits immediately.
        return index

    def receive(self, src: NodeId, message: Message) -> None:
        # A newer term from anyone means we are out of date, whatever our role.
        if message.term > self.term:
            self._become_follower(message.term)

        if isinstance(message, RequestVote):
            self._on_request_vote(src, message)
        elif isinstance(message, RequestVoteReply):
            self._on_request_vote_reply(src, message)
        elif isinstance(message, AppendEntries):
            self._on_append_entries(src, message)
        else:
            self._on_append_entries_reply(src, message)

    def take_outbox(self) -> list[Envelope]:
        out, self.outbox = self.outbox, []
        return out

    # Elections

    def _start_election(self) -> None:
        self.role = Role.CANDIDATE
        self.state.current_term += 1
        self.state.voted_for = self.id
        self.leader_id = None
        self.votes = set()
        self._reset_election_timer()

        request = RequestVote(self.term, self.id, self.log.last_index, self.log.last_term)
        for peer in self.peers:
            self._send(peer, request)
        self._count_vote(self.id)

    def _on_request_vote(self, src: NodeId, m: RequestVote) -> None:
        # Only vote for a candidate whose log is at least as up to date as ours:
        # a newer last term wins, and for equal terms the longer log wins (tuples
        # compare exactly that way). A committed entry is on a majority and a
        # winner needs a majority of votes, so some voter holds the entry and
        # refuses any candidate without it. A leader never lacks a committed entry.
        log_ok = (m.last_log_term, m.last_log_index) >= (self.log.last_term, self.log.last_index)
        not_voted_for_other = self.state.voted_for in (None, m.candidate_id)
        grant = m.term == self.term and not_voted_for_other and log_ok
        if grant:
            self.state.voted_for = m.candidate_id
            self._reset_election_timer()
        self._send(src, RequestVoteReply(self.term, grant))

    def _on_request_vote_reply(self, src: NodeId, m: RequestVoteReply) -> None:
        if self.role is Role.CANDIDATE and m.term == self.term and m.vote_granted:
            self._count_vote(src)

    def _count_vote(self, voter: NodeId) -> None:
        self.votes.add(voter)
        if len(self.votes) >= self.quorum:
            self._become_leader()

    def _become_leader(self) -> None:
        self.role = Role.LEADER
        self.leader_id = self.id
        self.next_index = {peer: self.log.last_index + 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}
        # A leader only commits entries from its own term (see
        # _advance_commit_index), so append a no-op now: once it reaches a
        # majority, any leftover entries from earlier terms commit with it.
        self.propose(Noop())

    def _become_follower(self, term: int) -> None:
        if term > self.term:
            self.state.current_term = term
            self.state.voted_for = None
            self.leader_id = None
        self.role = Role.FOLLOWER

    # Replication

    def _broadcast_append_entries(self) -> None:
        for peer in self.peers:
            self._send_append_entries(peer)
        self.heartbeat_elapsed = 0

    def _send_append_entries(self, peer: NodeId) -> None:
        # Sends every entry the peer may be missing; an empty list is a heartbeat.
        prev_index = self.next_index[peer] - 1
        message = AppendEntries(
            term=self.term,
            leader_id=self.id,
            prev_log_index=prev_index,
            prev_log_term=self.log.term_at(prev_index),
            entries=self.log.entries_from(prev_index + 1),
            leader_commit=self.commit_index,
        )
        self._send(peer, message)

    def _on_append_entries(self, src: NodeId, m: AppendEntries) -> None:
        if m.term < self.term:
            self._send(src, AppendEntriesReply(self.term, success=False, match_index=0))
            return

        assert self.role is not Role.LEADER, f"two leaders in term {self.term}"
        self._become_follower(m.term)
        self.leader_id = m.leader_id
        self._reset_election_timer()

        if not self.log.matches(m.prev_log_index, m.prev_log_term):
            self._send(src, AppendEntriesReply(self.term, success=False, match_index=0))
            return

        self.log.merge(m.prev_log_index, m.entries)
        match_index = m.prev_log_index + len(m.entries)

        # Only commit up to what this message proved we share with the leader;
        # anything after that may be a stale entry the leader hasn't replaced yet.
        self.commit_index = max(self.commit_index, min(m.leader_commit, match_index))
        self._apply_committed()
        self._send(src, AppendEntriesReply(self.term, success=True, match_index=match_index))

    def _on_append_entries_reply(self, src: NodeId, m: AppendEntriesReply) -> None:
        if self.role is not Role.LEADER or m.term != self.term:
            return
        if m.success:
            # max(): replies can arrive out of order.
            self.match_index[src] = max(self.match_index[src], m.match_index)
            self.next_index[src] = self.match_index[src] + 1
            self._advance_commit_index()
        else:
            # The follower's log didn't match at prev_index: step back and retry.
            self.next_index[src] = max(1, self.next_index[src] - 1)
            self._send_append_entries(src)

    def _advance_commit_index(self) -> None:
        # The highest index a majority holds: sort every node's match index (ours
        # is our whole log) and take the quorum-th largest.
        matched = sorted([self.log.last_index, *self.match_index.values()], reverse=True)
        majority_index = matched[self.quorum - 1]
        # Only an entry from our own term is safe to commit by counting. An older
        # entry can sit on a majority and still be overwritten: a candidate whose
        # last entry has a newer term than the holders' wins their votes and then
        # replaces it. Our term is the newest in the cluster, so once a majority
        # holds one of our entries, no node lacking it can collect a majority of
        # votes. Older entries commit along with it, since they come earlier.
        if majority_index > self.commit_index and self.log.term_at(majority_index) == self.term:
            self.commit_index = majority_index
            self._apply_committed()

    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            self.state_machine.apply(self.log[self.last_applied].command)

    # Helpers

    def _send(self, dst: NodeId, message: Message) -> None:
        self.outbox.append(Envelope(self.id, dst, message))

    def _reset_election_timer(self) -> None:
        self.election_elapsed = 0
        self.election_timeout = self._random_election_timeout()

    def _random_election_timeout(self) -> int:
        return self.rng.randint(self.config.election_timeout_min, self.config.election_timeout_max)
