# Consensus

A toy implementation of a consensus algorithm, used to replicate state across a
small cluster of nodes, with tests that exercise its failure modes.

## Quick start

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'

pytest          # run tests
ruff check .    # lint
mypy            # type-check
```

## Design

<!-- Algorithm choice, how nodes and the network are modelled, and why. -->

## Failure modes tested

<!-- Which failures the tests cover (crashes, partitions, message loss/reordering, ...) and why they matter. -->

## Trade-offs and limitations

<!-- What was deliberately left out, and what would change for a real system. -->

## Working with AI

<!-- What was delegated, how it was verified, and where the tools got it wrong. -->
