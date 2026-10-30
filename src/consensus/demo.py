"""A narrated run of a simulated cluster: election, replication, leader crash,
re-election and recovery.

    python -m consensus.demo [--seed N] [--size N] [--step]

Events are inferred by comparing each node's state before and after every tick,
so the node itself carries no tracing code.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass

from consensus.log import LogEntry
from consensus.messages import NodeId
from consensus.node import Role
from consensus.sim import Cluster
from consensus.state_machine import Command, Delete, Noop, Put

_COLOR = sys.stdout.isatty() and "NO_COLOR" not in os.environ


def _style(text: str, code: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _COLOR else text


def _bold(text: str) -> str:
    return _style(text, "1")


def _dim(text: str) -> str:
    return _style(text, "2")


def _format_command(command: Command) -> str:
    match command:
        case Put(key, value):
            return f"{key}={value}"
        case Delete(key):
            return f"del {key}"
        case Noop():
            return "noop"


def _format_entries(entries: tuple[LogEntry, ...]) -> str:
    return " ".join(f"{e.term}:{_format_command(e.command)}" for e in entries)


@dataclass(frozen=True)
class NodeView:
    """What we compare between ticks to spot events."""

    role: Role
    term: int
    voted_for: NodeId | None
    log: tuple[LogEntry, ...]
    commit_index: int
    crashed: bool


def _view(cluster: Cluster) -> dict[NodeId, NodeView]:
    return {
        node_id: NodeView(
            role=node.role,
            term=node.term,
            voted_for=node.state.voted_for,
            log=node.log.entries_from(1),
            commit_index=node.commit_index,
            crashed=node_id in cluster.crashed,
        )
        for node_id, node in cluster.nodes.items()
    }


def _events(cluster: Cluster, node_id: NodeId, before: NodeView, after: NodeView) -> list[str]:
    if after.crashed:
        return [_style("crashed", "31")] if not before.crashed else []
    if before.crashed:
        return [
            f"restarts from disk: term {after.term}, {len(after.log)} log entries, "
            "nothing applied yet"
        ]

    events = []
    if after.role is Role.CANDIDATE and after.term > before.term:
        events.append(f"election timeout → candidate for term {after.term}, asks for votes")
    elif after.term > before.term and after.voted_for is None:
        events.append(f"sees newer term {after.term}")

    if after.role is Role.LEADER and before.role is not Role.LEADER:
        voters = ", ".join(sorted(cluster.nodes[node_id].votes))
        events.append(_style(f"wins term {after.term} with votes from {voters}", "1;32"))
    elif after.role is Role.FOLLOWER and before.role is not Role.FOLLOWER:
        events.append(f"steps down: {before.role.value} → follower")

    if after.voted_for not in (None, node_id) and (
        after.voted_for != before.voted_for or after.term != before.term
    ):
        events.append(f"votes for {after.voted_for} in term {after.term}")

    common = 0
    while common < min(len(before.log), len(after.log)) and before.log[common] == after.log[common]:
        common += 1
    if common < len(before.log):
        removed = _format_entries(before.log[common:])
        events.append(_style(f"log: drops uncommitted [{removed}]", "33"))
    if common < len(after.log):
        start, end = common + 1, len(after.log)
        where = f"index {start}" if start == end else f"indexes {start}-{end}"
        events.append(f"log: stores [{_format_entries(after.log[common:])}] at {where}")

    if after.commit_index > before.commit_index:
        source = "on a majority" if after.role is Role.LEADER else "learned from leader"
        events.append(f"commits through index {after.commit_index} ({source}), applies it")
    return events


def _table(cluster: Cluster) -> str:
    log_header = "log  (term:command, │ = commit point)"
    rows = [f"  {'node':<5} {'role':<10} {'term':>4}  {log_header:<44} kv"]
    for node_id, node in cluster.nodes.items():
        entries = node.log.entries_from(1)
        committed = _format_entries(entries[: node.commit_index])
        pending = _format_entries(entries[node.commit_index :])
        log = " ".join(part for part in (committed, "│", pending) if part)
        kv = " ".join(f"{k}={v}" for k, v in sorted(node.state_machine.data.items()))

        role = "crashed" if node_id in cluster.crashed else node.role.value
        row = f"  {node_id:<5} {role:<10} {node.term:>4}  {log:<44} {kv}"
        if node_id in cluster.crashed:
            row = _style(row, "2;31")
        elif node.role is Role.LEADER:
            row = _style(row, "1;32")
        elif node.role is Role.CANDIDATE:
            row = _style(row, "33")
        rows.append(row)
    return "\n".join(rows)


def _state_line(cluster: Cluster, node_id: NodeId) -> str:
    node = cluster.nodes[node_id]
    if node_id in cluster.crashed:
        return _style(f"{node_id:<4} crashed", "2;31")
    last = f"{node.log.last_index}@t{node.log.last_term}" if node.log.last_index else "-"
    if node.role is Role.LEADER:
        timer = f"heartbeat {node.heartbeat_elapsed}/{node.config.heartbeat_interval}"
    else:
        timer = f"election timer {node.election_elapsed}/{node.election_timeout}"
    line = (
        f"{node_id:<4} {node.role.value:<9} term {node.term:<3} "
        f"voted {node.state.voted_for or '-':<3} last entry {last:<6} "
        f"commit {node.commit_index:<3} {timer}"
    )
    if node.role is Role.LEADER:
        return _style(line, "1;32")
    if node.role is Role.CANDIDATE:
        return _style(line, "33")
    return line


class Narrator:
    """Prints every state change the cluster makes, one line per event.

    In step mode it also prints each node's key state after every tick that
    changed something, and waits for Enter before continuing.
    """

    def __init__(self, cluster: Cluster, *, step: bool = False) -> None:
        self.cluster = cluster
        self.step = step
        self.pause = step and sys.stdin.isatty()
        self._before = _view(cluster)
        self._quiet_ticks = 0
        cluster.observers.append(self.report)

    def report(self) -> None:
        after = _view(self.cluster)
        lines = [
            f"{node_id:<4} {event}"
            for node_id in self.cluster.nodes
            for event in _events(self.cluster, node_id, self._before[node_id], after[node_id])
        ]
        self._before = after
        if not lines:
            self._quiet_ticks += 1
            return

        if self.step:
            if self._quiet_ticks:
                ticks = "tick" if self._quiet_ticks == 1 else "ticks"
                print(_dim(f"  ... {self._quiet_ticks} quiet {ticks} (timers running, heartbeats)"))
            print(_bold(f"  tick {self.cluster.now}"))
            for line in lines:
                print(f"    {line}")
            print(_dim("    state:"))
            for node_id in self.cluster.nodes:
                print(f"      {_state_line(self.cluster, node_id)}")
            self._wait_for_enter()
        else:
            for line in lines:
                print(f"  {_dim(f't={self.cluster.now:>4}')}  {line}")
        self._quiet_ticks = 0

    def _wait_for_enter(self) -> None:
        if not self.pause:
            return
        try:
            answer = input(_dim("    [Enter] next tick, [c] run to end, [q] quit ")).strip()
        except EOFError:
            answer = "c"
        if answer == "q":
            raise SystemExit(0)
        if answer == "c":
            self.pause = False
        print()


def _phase(title: str, explanation: str) -> None:
    print(f"\n{_bold(title)}\n{_dim(explanation)}\n")


def _show(cluster: Cluster) -> None:
    print(f"\n{_table(cluster)}")


def run(seed: int, size: int, *, step: bool = False) -> None:
    cluster = Cluster(size, seed=seed)
    Narrator(cluster, step=step)
    print(_dim(f"{size} nodes, seed {seed}. Time is in simulated ticks."))

    _phase(
        "1. Election",
        "Every node starts as a follower in term 0. The first whose randomized "
        "timeout fires\nbecomes a candidate; a majority of votes makes it leader, "
        "and it appends a no-op.",
    )
    first_leader = cluster.wait_for_leader()
    cluster.wait_for_applied(first_leader.log.last_index)
    _show(cluster)

    _phase(
        "2. Replication",
        "Writes go to the leader, which sends them to followers. An entry commits "
        "once a majority\nstores it; followers learn the commit point from the next "
        "AppendEntries.",
    )
    writes: tuple[Command, ...] = (Put("x", "1"), Put("y", "2"))
    for command in writes:
        cluster.wait_for_applied(cluster.submit(command))
    _show(cluster)

    _phase(
        f"3. Leader {first_leader.id} crashes",
        "Followers stop hearing heartbeats, time out, and elect a new leader in a "
        "higher term.\nOnly a node whose log is at least as up to date as the "
        "voter's can get its vote.",
    )
    cluster.crash(first_leader.id)
    new_leader = cluster.wait_for_leader()
    cluster.wait_for_applied(new_leader.log.last_index)
    _show(cluster)

    _phase(
        "4. Writes continue",
        f"The surviving majority keeps committing without {first_leader.id}.",
    )
    writes = (Put("x", "3"), Delete("y"))
    for command in writes:
        cluster.wait_for_applied(cluster.submit(command))
    _show(cluster)

    _phase(
        f"5. {first_leader.id} restarts",
        "It returns with only its term, vote and log from disk. A heartbeat from "
        "the new leader\nteaches it the new term; it receives the entries it "
        "missed and replays the log.",
    )
    cluster.restart(first_leader.id)
    cluster.wait_for_applied(new_leader.log.last_index)
    _show(cluster)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--size", type=int, default=3)
    parser.add_argument(
        "--step",
        action="store_true",
        help="show every node's state after each eventful tick and pause for Enter",
    )
    args = parser.parse_args(argv)
    run(args.seed, args.size, step=args.step)


if __name__ == "__main__":
    main()
