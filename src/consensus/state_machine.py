"""The replicated state machine: a key-value store driven by log commands."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Put:
    key: str
    value: str


@dataclass(frozen=True)
class Delete:
    key: str


@dataclass(frozen=True)
class Noop:
    """Appended by a new leader so it can commit entries from earlier terms."""


Command = Put | Delete | Noop


class KVStore:
    def __init__(self) -> None:
        self.data: dict[str, str] = {}

    def apply(self, command: Command) -> None:
        match command:
            case Put(key, value):
                self.data[key] = value
            case Delete(key):
                self.data.pop(key, None)
            case Noop():
                pass
