"""Raft RPCs, modelled as one-way messages.

The paper describes request/response RPCs. Here each request and each reply is
an independent message, so the network can drop, delay or reorder either half.
"""

from __future__ import annotations

from dataclasses import dataclass

from consensus.log import LogEntry

NodeId = str


@dataclass(frozen=True)
class RequestVote:
    term: int
    candidate_id: NodeId
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class RequestVoteReply:
    term: int
    vote_granted: bool


@dataclass(frozen=True)
class AppendEntries:
    term: int
    leader_id: NodeId
    prev_log_index: int
    prev_log_term: int
    entries: tuple[LogEntry, ...]
    leader_commit: int


@dataclass(frozen=True)
class AppendEntriesReply:
    term: int
    success: bool
    # On success: the index up to which the follower's log now matches the leader's.
    match_index: int


Message = RequestVote | RequestVoteReply | AppendEntries | AppendEntriesReply


@dataclass(frozen=True)
class Envelope:
    src: NodeId
    dst: NodeId
    message: Message
