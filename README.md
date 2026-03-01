# Raft

Implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) in Go, from MIT 6.5840 (Distributed Systems).

## What's implemented

### Lab 3A: Leader Election
- Three state FSM: follower, candidate, leader
- Randomized election timeouts to prevent split votes
- RequestVote RPC with log freshness checks
- Automatic leader demotion when higher terms are discovered

### Lab 3B: Log Replication
- AppendEntries RPC with log synchronization
- Fast log conflict resolution using XTerm/XIndex (skips multiple entries per RPC instead of one at a time)
- Commit index advancement on majority replication
- Concurrent replication to all peers via goroutines

### Lab 3C: Persistence
- currentTerm, votedFor, and log persisted via labgob encoding
- State reloaded on crash recovery
- All RPC handlers persist after state changes

### Key Value Server (kvsrv)
- Linearizable KV store on top of Raft
- Exactly once semantics with per client duplicate detection

## Design

Uses Go channels for state machine transitions instead of polling. The ticker goroutine uses `select` for timeout based election triggering. No busy waiting.

```
             RequestVote RPC
Candidate ◄──────────────────► Peers
    │
    │ wins majority
    ▼
  Leader ──── AppendEntries ──► Followers
    │              RPC
    │ commit on
    │ majority ack
    ▼
  applyCh ──► Service/Tester
```

All 27 Raft tests pass including network partitions, packet loss, crashes, concurrent starts, and unreliable networks.

## Interesting bug: duplicate log entries after crash

A server S1 has logs `{1 102} {2 103}`. The leader sends new entries `{2 104} {2 105}`. S1 appends them:

```
S1: {1 102} {2 103} {2 104} {2 105}
```

S1 crashes. The logs are persisted, so S1 restarts with the same state. But the leader never got the AppendEntries response, so it never updated nextIndex for S1.

When S1 comes back, the leader resends `{2 104} {2 105}` starting at the old nextIndex. Without proper handling, S1 ends up with:

```
S1: {1 102} {2 103} {2 104} {2 105} {2 104} {2 105}   <- duplicated!
```

The fix: in AppendEntries, before appending, check if the entries already exist at the given index with matching terms. Only append entries that are actually new. The Raft paper says "if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it". The key insight is the other case: if an existing entry matches the new one (same index, same term), skip it.
