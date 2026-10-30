"""The replicated log."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from consensus.state_machine import Command


@dataclass(frozen=True)
class LogEntry:
    term: int
    command: Command


class RaftLog:
    """A 1-indexed log, as in the paper.

    Index 0 is a virtual sentinel with term 0, so the consistency check for the
    very first entry (prev_log_index=0, prev_log_term=0) needs no special case.
    """

    def __init__(self, entries: Iterable[LogEntry] = ()) -> None:
        self._entries = list(entries)

    def __len__(self) -> int:
        return len(self._entries)

    def __getitem__(self, index: int) -> LogEntry:
        if not 1 <= index <= self.last_index:
            raise IndexError(f"log index {index} out of range 1..{self.last_index}")
        return self._entries[index - 1]

    @property
    def last_index(self) -> int:
        return len(self._entries)

    @property
    def last_term(self) -> int:
        return self.term_at(self.last_index)

    def term_at(self, index: int) -> int:
        return 0 if index == 0 else self[index].term

    def entries_from(self, index: int) -> tuple[LogEntry, ...]:
        return tuple(self._entries[index - 1 :])

    def append(self, entry: LogEntry) -> int:
        """Append an entry and return its index."""
        self._entries.append(entry)
        return self.last_index

    def matches(self, index: int, term: int) -> bool:
        """The AppendEntries consistency check: do we hold an entry at index with this term?"""
        return index <= self.last_index and self.term_at(index) == term

    def merge(self, prev_index: int, entries: Sequence[LogEntry]) -> None:
        """Place entries after prev_index, which must already have passed `matches`.

        Entries we already hold with the same term are kept, and the log is only
        truncated at the first real conflict. Truncating unconditionally at
        prev_index + 1 would be wrong: a delayed, shorter AppendEntries arriving
        after a newer one would erase entries we may already have acknowledged.
        """
        for offset, entry in enumerate(entries):
            index = prev_index + 1 + offset
            if index <= self.last_index:
                if self.term_at(index) == entry.term:
                    continue
                del self._entries[index - 1 :]
            self._entries.append(entry)
