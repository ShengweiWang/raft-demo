"""A deterministic, single-threaded simulation of a Raft cluster.

All randomness (election timeouts, message delays, faults) comes from one seeded RNG,
so any run, including a failing one, can be replayed exactly from its seed.
"""

from __future__ import annotations

import heapq
import random
from collections.abc import Callable, Iterable

from consensus.invariants import InvariantChecker
from consensus.messages import Envelope, NodeId
from consensus.node import Config, RaftNode, Role
from consensus.state_machine import Command


class Network:
    """Delivers each message after a random delay, so messages can be reordered.

    It can also drop or duplicate messages at random, and be split into
    partitions whose members cannot reach each other.
    """

    def __init__(self, rng: random.Random, min_delay: int = 1, max_delay: int = 3) -> None:
        self._rng = rng
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.drop_rate = 0.0
        self.duplicate_rate = 0.0
        # Which side of a partition each node is on; empty means fully connected.
        self._side: dict[NodeId, int] = {}
        # (deliver_at, sequence number, envelope); the sequence number breaks
        # ties deterministically and keeps envelopes from being compared.
        self._in_flight: list[tuple[int, int, Envelope]] = []
        self._sequence = 0

    def partition(self, *groups: Iterable[NodeId]) -> None:
        """Only nodes in the same group can reach each other. A node left out of
        every group is cut off from everyone."""
        self._side = {node_id: side for side, group in enumerate(groups) for node_id in group}

    def heal(self) -> None:
        self._side = {}

    def can_reach(self, src: NodeId, dst: NodeId) -> bool:
        if not self._side:
            return True
        side = self._side.get(src)
        return side is not None and side == self._side.get(dst)

    def send(self, now: int, envelope: Envelope) -> None:
        # Only draw from the RNG for faults that are switched on, so turning
        # faults off leaves every other random choice, and so each seed, unchanged.
        if self.drop_rate and self._rng.random() < self.drop_rate:
            return
        copies = 2 if self.duplicate_rate and self._rng.random() < self.duplicate_rate else 1
        for _ in range(copies):
            deliver_at = now + self._rng.randint(self.min_delay, self.max_delay)
            heapq.heappush(self._in_flight, (deliver_at, self._sequence, envelope))
            self._sequence += 1

    def due(self, now: int) -> list[Envelope]:
        # Reachability is checked on delivery, not on send, so a partition also
        # cuts off messages that were already on their way.
        envelopes = []
        while self._in_flight and self._in_flight[0][0] <= now:
            envelope = heapq.heappop(self._in_flight)[2]
            if self.can_reach(envelope.src, envelope.dst):
                envelopes.append(envelope)
        return envelopes


class Cluster:
    def __init__(self, size: int = 3, *, seed: int = 0, config: Config | None = None) -> None:
        self.seed = seed
        rng = random.Random(seed)
        ids = [f"n{i}" for i in range(size)]
        self.nodes: dict[NodeId, RaftNode] = {
            node_id: RaftNode(node_id, ids, rng=random.Random(rng.getrandbits(64)), config=config)
            for node_id in ids
        }
        self.network = Network(random.Random(rng.getrandbits(64)))
        self.crashed: set[NodeId] = set()
        self.now = 0
        # Raises SafetyViolation the moment any safety property breaks.
        self.checker = InvariantChecker(self)
        # Called after every step; the demo uses this to narrate a run.
        self.observers: list[Callable[[], None]] = []

    def live_nodes(self) -> list[RaftNode]:
        return [node for node_id, node in self.nodes.items() if node_id not in self.crashed]

    def step(self) -> None:
        """Advance time by one tick: tick every live node, then deliver due messages.

        Messages addressed to a crashed node are lost. Messages it sent before
        crashing are still in flight and may yet arrive.
        """
        self.now += 1
        for node in self.live_nodes():
            node.tick()
        for envelope in self.network.due(self.now):
            if envelope.dst not in self.crashed:
                self.nodes[envelope.dst].receive(envelope.src, envelope.message)
        for node in self.live_nodes():
            for envelope in node.take_outbox():
                self.network.send(self.now, envelope)
        self.checker.check()
        for observer in self.observers:
            observer()

    def crash(self, node_id: NodeId) -> None:
        self.crashed.add(node_id)

    def restart(self, node_id: NodeId) -> None:
        """Bring a node back with only what it had on disk: term, vote and log."""
        old = self.nodes[node_id]
        self.nodes[node_id] = RaftNode(
            node_id, (node_id, *old.peers), state=old.state, rng=old.rng, config=old.config
        )
        self.crashed.discard(node_id)

    def run(self, ticks: int) -> None:
        for _ in range(ticks):
            self.step()

    def run_until(self, condition: Callable[[], bool], max_ticks: int = 1000) -> None:
        for _ in range(max_ticks):
            if condition():
                return
            self.step()
        if not condition():
            raise TimeoutError(f"condition not met within {max_ticks} ticks (seed={self.seed})")

    def leader(self) -> RaftNode | None:
        """The live leader of the highest term, if any. A deposed leader from an
        older term may still believe it leads; it is not the cluster's leader."""
        leaders = [node for node in self.live_nodes() if node.role is Role.LEADER]
        return max(leaders, key=lambda node: node.term, default=None)

    def wait_for_leader(self) -> RaftNode:
        self.run_until(lambda: self.leader() is not None)
        leader = self.leader()
        assert leader is not None
        return leader

    def submit(self, command: Command) -> int:
        """Propose a command through the current leader and return its log index."""
        index = self.wait_for_leader().propose(command)
        assert index is not None
        return index

    def wait_for_applied(self, index: int) -> None:
        self.run_until(lambda: all(n.last_applied >= index for n in self.live_nodes()))
