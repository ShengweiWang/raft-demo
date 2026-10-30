"""Raft's safety properties, checked after every simulated tick.

Each check must hold at every moment, under any mix of crashes, partitions and
lost, duplicated or reordered messages. Faults may stop the cluster from making
progress; they must never make one of these false.
"""

from __future__ import annotations

from itertools import combinations
from typing import TYPE_CHECKING

from consensus.log import LogEntry
from consensus.messages import NodeId
from consensus.node import RaftNode, Role

if TYPE_CHECKING:
    from consensus.sim import Cluster


class SafetyViolation(AssertionError):
    pass


class InvariantChecker:
    def __init__(self, cluster: Cluster) -> None:
        self.cluster = cluster
        self.leader_of_term: dict[int, NodeId] = {}
        # Every entry any node has ever marked committed, in index order, and the
        # term that node was in when we first saw it. That term is never earlier
        # than the term the entry was really committed in.
        self.committed: list[LogEntry] = []
        self.committed_seen_in_term: list[int] = []
        # Per node: the node object last seen, and its term, commit index and
        # applied index then. A restart creates a new object, whose commit and
        # applied indexes legitimately start again from zero.
        self._progress: dict[NodeId, tuple[RaftNode, int, int, int]] = {}

    def check(self) -> None:
        nodes = list(self.cluster.nodes.values())
        for node in nodes:
            self._check_never_goes_backwards(node)
            self._check_one_leader_per_term(node)
            self._check_committed_entries_never_change(node)
        self._check_leaders_hold_committed_entries(nodes)
        self._check_matching_entries_mean_matching_history(nodes)

    def _fail(self, message: str) -> None:
        raise SafetyViolation(f"{message} (seed={self.cluster.seed}, tick={self.cluster.now})")

    def _check_never_goes_backwards(self, node: RaftNode) -> None:
        previous = self._progress.get(node.id)
        if previous is not None:
            previous_node, term, commit_index, last_applied = previous
            if node.term < term:
                self._fail(f"{node.id}'s term went back from {term} to {node.term}")
            if previous_node is node and node.commit_index < commit_index:
                self._fail(f"{node.id}'s commit index went back from {commit_index}")
            if previous_node is node and node.last_applied < last_applied:
                self._fail(f"{node.id}'s applied index went back from {last_applied}")
        if node.last_applied > node.commit_index:
            self._fail(f"{node.id} applied index {node.last_applied} before committing it")
        self._progress[node.id] = (node, node.term, node.commit_index, node.last_applied)

    def _check_one_leader_per_term(self, node: RaftNode) -> None:
        if node.role is not Role.LEADER:
            return
        leader = self.leader_of_term.setdefault(node.term, node.id)
        if leader != node.id:
            self._fail(f"two leaders in term {node.term}: {leader} and {node.id}")

    def _check_committed_entries_never_change(self, node: RaftNode) -> None:
        # Covers both "no two nodes commit different entries at the same index"
        # and "a committed entry is never removed": every node's committed prefix
        # must agree with everything ever committed anywhere. Since nodes apply
        # exactly their committed prefix in order, their state machines agree too.
        prefix = node.log.entries_from(1)[: node.commit_index]
        known = min(len(prefix), len(self.committed))
        if prefix[:known] != tuple(self.committed[:known]):
            index = next(i for i in range(known) if prefix[i] != self.committed[i]) + 1
            self._fail(
                f"{node.id} committed {prefix[index - 1]} at index {index}, "
                f"but {self.committed[index - 1]} was committed there before"
            )
        for entry in prefix[known:]:
            self.committed.append(entry)
            self.committed_seen_in_term.append(node.term)

    def _check_leaders_hold_committed_entries(self, nodes: list[RaftNode]) -> None:
        # A leader from the commit's term onwards must hold the entry. A stale
        # leader from an older term may legitimately lack it.
        for node in nodes:
            if node.role is not Role.LEADER:
                continue
            for index, entry in enumerate(self.committed, start=1):
                if node.term < self.committed_seen_in_term[index - 1]:
                    continue
                if index > node.log.last_index or node.log[index] != entry:
                    self._fail(
                        f"leader {node.id} of term {node.term} lacks committed "
                        f"entry {entry} at index {index}"
                    )

    def _check_matching_entries_mean_matching_history(self, nodes: list[RaftNode]) -> None:
        # If two logs have an entry with the same index and term, they are
        # identical up to and including that index. Checking the highest such
        # index for each pair is enough, since it covers every index below.
        logs = {node.id: node.log.entries_from(1) for node in nodes}
        for (a, log_a), (b, log_b) in combinations(logs.items(), 2):
            for index in range(min(len(log_a), len(log_b)), 0, -1):
                if log_a[index - 1].term == log_b[index - 1].term:
                    if log_a[:index] != log_b[:index]:
                        self._fail(
                            f"{a} and {b} agree on the term at index {index} "
                            "but their logs differ before it"
                        )
                    break
