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

python -m consensus.demo           # narrated run: election, crash, re-election, recovery
python -m consensus.demo --step    # one tick at a time, with every node's state
```

## Design

<!-- Algorithm choice, how nodes and the network are modelled, and why. -->

## Failure modes

Two kinds of promise matter, and faults affect them differently:

- **Safety**: nothing bad ever happens. Two nodes never disagree about a
  committed command. This must hold under *every* failure below.
- **Liveness**: something good eventually happens. A leader is elected and
  writes commit. Faults may pause this (e.g. while a majority is unreachable);
  it must resume once the fault clears.

### What must always hold

Every simulated test runs an invariant checker (`consensus/invariants.py`) after
every tick. It fails the test immediately, naming the seed and tick, if any of
these is ever false:

| Invariant | Meaning |
|---|---|
| One leader per term | No two nodes are ever leader in the same term, even at different moments. |
| Committed entries never change | Once any node commits an entry at an index, no node ever commits a different entry there, and it is never removed. Since nodes apply exactly their committed entries in order, every state machine sees the same commands. |
| Leaders hold every committed entry | A leader of the term an entry was committed in, or any later term, has that entry in its log. |
| Same index and term means same history | If two logs have an entry with the same index and term, they are identical up to that point. |
| Progress never goes backwards | A node's term never decreases. Its commit and applied indexes never decrease, except that a restart resets them to zero (they are rebuilt, not lost). It never applies an entry before committing it. |

### Failure modes and what to expect

Status: **Tested** means a test asserts the behaviour today; **Planned** means
it is not covered yet.

#### 1. Delayed and reordered messages
- **Meaning:** messages take 1–3 ticks, so a later message can arrive first,
  e.g. an old AppendEntries after a newer one.
- **Expect:** logs still converge. A late message never removes entries a newer
  one added (the follower only truncates at a real conflict), and a late reply
  never moves the leader's view of a follower backwards.
- **Status:** Tested. `test_late_append_entries_does_not_undo_a_newer_one`,
  `test_late_reply_does_not_move_the_leader_backwards`, and
  `test_long_delays_still_converge` (delays up to 8 ticks, close to the
  election timeout). Every other cluster test also runs with random delays.

#### 2. Lost messages
- **Meaning:** any message, request or reply, can vanish.
- **Expect:** slower progress, never wrong results. A lost AppendEntries or its
  reply is resent on the next heartbeat, because the leader keeps sending
  everything a follower hasn't confirmed. A lost vote may stall an election; the
  candidate times out and retries in a higher term.
- **Status:** Tested. `test_lost_messages_slow_progress_but_never_break_it`
  (30% of messages dropped).

#### 3. Duplicated messages
- **Meaning:** the same message is delivered twice.
- **Expect:** no effect. Votes are counted as a set of voters, a node re-grants
  its vote to the same candidate in the same term, re-sent log entries are
  recognised as already present, and confirmations only ever move forward.
- **Status:** Tested. `test_duplicate_vote_is_counted_once`,
  `test_repeated_request_from_same_candidate_is_granted_again`, and
  `test_duplicated_messages_change_nothing` (half of all messages delivered twice).

#### 4. Competing candidates (split vote)
- **Meaning:** two or more nodes time out together and split the votes, so no
  one reaches a majority.
- **Expect:** no leader for that term. Each candidate's own timer fires again at
  a random time, so one usually gets ahead and wins a later term. Never two
  leaders in one term.
- **Status:** Tested. `test_split_vote_elects_no_one_and_retries_in_higher_terms`
  forces a 2–2 split in a four-node cluster with a partition, so the split is
  certain rather than lucky. `test_grants_one_vote_per_term` covers the vote
  itself.

#### 5. Leader crash
- **Meaning:** the leader stops; its messages stop.
- **Expect:** followers stop hearing heartbeats, time out, and elect a new
  leader in a higher term. Every committed write survives, because only a node
  holding all committed entries can collect a majority of votes. Writes the old
  leader accepted but had not committed may be lost; their clients were never
  told they succeeded.
- **Status:** Tested. `test_leader_crash_elects_a_new_leader_and_keeps_committed_writes`
  and `test_write_the_crashed_leader_never_sent_is_lost`.

#### 6. Follower crash and restart
- **Meaning:** a node stops and later comes back with only what was on disk:
  its term, its vote and its log. Its commit index and state machine are gone.
- **Expect:** it rejoins as a follower, learns the commit index from the
  leader's next message, and rebuilds its state by replaying the log. Because
  its vote survived, it cannot vote for a second candidate in the same term.
- **Status:** Tested. `test_restarted_follower_catches_up_by_replaying_the_log`
  and `test_restart_does_not_allow_a_second_vote_in_the_same_term`.

#### 7. Stale leader cut off in a minority
- **Meaning:** a partition leaves the leader with fewer than half the nodes.
- **Expect:** the old leader keeps accepting writes but can never commit them,
  since it can't reach a majority. The majority side elects a new leader and
  keeps committing. When the partition heals, the old leader sees the higher
  term and steps down, and its uncommitted entries are replaced by the new
  leader's. Its clients never see those writes applied.
- **Status:** Tested. `test_stale_leader_cannot_commit_and_steps_down_when_healed`.

#### 8. Majority lost
- **Meaning:** a partition or crashes leave no side with a majority.
- **Expect:** no leader can be elected and nothing commits. This is the
  deliberate choice: stop rather than risk disagreement. Once a majority can
  talk again, it elects a leader and continues with no committed entry lost.
  A write that was accepted but not committed may or may not survive: if a node
  holding it wins the next election, it commits along with that leader's no-op.
- **Status:** Tested. `test_without_a_majority_nothing_commits_until_healed`.

#### 9. A follower's log diverged over several terms
- **Meaning:** a follower holds entries, from leaders that later lost power, at
  indexes where the current leader has different entries.
- **Expect:** the leader's consistency check fails, and it steps back one entry
  per round trip until the logs agree. The follower then drops its conflicting
  entries and takes the leader's. Only uncommitted entries can conflict, so
  nothing committed is ever removed. A follower also never commits past what
  the leader's message proved it shares: entries beyond that point may be stale.
- **Status:** Tested. `test_diverged_follower_log_is_repaired` (a follower three
  entries off, across terms 2 and 3), `test_follower_commits_only_what_the_message_proved`,
  and `test_merge_truncates_from_the_first_conflict`.

#### 10. An older-term entry reaches a majority but isn't safe yet
- **Meaning:** a leader finishes copying an entry left over from an earlier
  term onto a majority. With five nodes S1–S5:
  1. S1 leads term 2, writes entry A at index 2 on S1 and S2, then crashes.
  2. S5 wins term 3 (S3 and S4 vote for it), writes entry B at index 2 only
     on itself, then crashes.
  3. S1 wins term 4 and copies A to S3. A is now on a majority.
  4. If S1 counted that as committed and then crashed, S5 could win term 5:
     its last entry (term 3) is newer than S2, S3 and S4's (term 2 or older),
     so they vote for it. S5 would then overwrite A with B everywhere.
- **Expect:** a leader only commits an entry by counting copies if the entry is
  from its own term. Older entries commit together with a newer one built on
  top of them. That's why a new leader immediately appends a no-op: once the
  no-op is on a majority, S2 and S3 end in term 4 and refuse S5's vote request.
- **Status:** Tested. `test_leader_does_not_commit_old_term_entry_by_counting`
  replays step 3 on S1, and `test_voters_refuse_the_older_log_once_the_no_op_reaches_them`
  replays step 4 on S2, with and without the no-op.

#### 11. A node rejoins after a long isolation
- **Meaning:** a cut-off node keeps timing out, raising its term each time
  (e.g. to 10 while the cluster is at 4).
- **Expect:** when it rejoins, its higher term forces the healthy leader to
  step down, causing an unnecessary election. It wins only if its log is still
  as up to date as the others; otherwise an up-to-date node wins. Safety holds;
  availability briefly suffers. This is a known limitation: the usual fix is a
  "pre-vote" round in which a node asks whether it *would* win before raising
  its term.
- **Status:** Tested, to document the behaviour rather than fix it.
  `test_rejoining_node_forces_an_election`, with and without writes while the
  node was away.

#### 12. All of the above at random
- **Meaning:** random crashes, restarts, partitions, losses and duplicates while
  clients keep writing, across many seeds.
- **Expect:** the invariant checker never fires. After all faults are healed,
  the cluster elects one leader and every node ends with the same log and the
  same state within a bounded number of ticks. A failing seed can be replayed
  exactly by re-running the test for that seed.
- **Status:** Tested. `test_random_faults_never_break_safety_and_the_cluster_recovers`,
  100 seeds of 40 rounds each. Clients write to every node that believes it
  leads, including stale ones, so conflicting entries really get overwritten.
  In a typical seed the cluster goes through about 27 terms, commits about 24
  entries, and spends about 90 ticks with no majority available.

### Do the tests catch bugs?

To check, each of these bugs was put into the code one at a time and the suite
was run:

| Bug | Caught by the whole suite | Caught by the random test alone |
|---|---|---|
| Commit without a majority | yes (many tests) | yes |
| Vote without checking how up to date the candidate's log is | yes (many tests) | yes |
| Commit an older-term entry by counting copies | yes (`test_leader_does_not_commit_old_term_entry_by_counting`) | no |
| Forget the vote on restart | yes (`test_restart_does_not_allow_a_second_vote_in_the_same_term`) | no |
| Truncate the log without a real conflict | yes (`test_late_append_entries_does_not_undo_a_newer_one`) | no |
| Follower commits past what the message proved | yes (`test_follower_commits_only_what_the_message_proved`) | no |
| Late reply moves the leader's view backwards | yes (`test_late_reply_does_not_move_the_leader_backwards`) | no |

Random faults reliably find blunt bugs but rarely produce the exact schedule a
subtle one needs, which is why each subtle rule also has a targeted test. Two
notes: this leader always sends its whole log, so the "commits past what the
message proved" case can only be built by hand (it arises with batched sends).
And moving the leader's view backwards turns out not to break safety here,
since the commit index can't go back; it only causes extra resends.

This exercise also found a gap in the invariant checker itself: it compared
each node's committed entries but not that the log still *contained* all of
them, so a log cut below its commit index went unnoticed. It now checks that.

### Not modelled

- Disk failure or corruption: persisted state is assumed durable as soon as it
  changes.
- Nodes that lie or misbehave on purpose; Raft assumes nodes fail only by stopping.
- Clock drift: there are no real clocks, only ticks, and every node's ticks run
  at the same rate. Raft's safety never depends on timing; only how quickly it
  elects a leader does.

## Trade-offs and limitations

<!-- What was deliberately left out, and what would change for a real system. -->

## Working with AI

<!-- What was delegated, how it was verified, and where the tools got it wrong. -->
