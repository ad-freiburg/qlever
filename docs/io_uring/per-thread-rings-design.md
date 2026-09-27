# Design: per-thread io_uring rings

## Goal

Lift the "single-threaded use only" restriction on the io_uring batch
lookup path so that concurrent query/export threads can issue vocabulary
reads without serializing on one shared ring or one pooled manager.
Each thread owns its ring exclusively for the whole time it uses it.
No mutex protects a ring on the I/O path.

## Paper reference

Jasny, El-Hindi, Ziegler, Leis, Binnig, "io_uring for High-Performance
DBMSs: When and How to Use It", PVLDB 19(1), 2026
(preprint arXiv:2512.04859). Section 3.4.3 covers multi-threaded io_uring
use: instead of sharing one ring across threads, each thread drives its
own exclusively owned ring, which removes cross-thread synchronization
from submission and completion reaping. The thesis reviews this paper in
`document/chapters/05B-iouring-batch-lookup.tex`, section "io_uring for
database systems".

## Current state on master

- `src/util/IoUringManager.h:124`: `SyncIoPolicy` is documented as
  "Single-threaded use only."
- `src/util/IoUringManager.h:162-166`: `IoUringPolicy` (the persistent
  io_uring manager behind `BatchManager<IoUringPolicy>`) is documented
  as "Single-threaded use only." Its bookkeeping (`ring_`,
  `numInFlightReadRequests_`, `numInFlightReadRequestsPerBatch_`,
  `inFlightReadsByRequestId_`, `nextRequestIdToAssign_`) has no
  synchronization, so concurrent `addBatch`/`wait` calls on one policy
  object are data races.
- `src/index/vocabulary/VocabularyOnDisk.h:42-45`: `VocabularyOnDisk`
  holds a pool of `NUM_VOCAB_BATCH_IO_MANAGERS`
  (`src/global/Constants.h:380`, currently 8) `BatchManagerBase`
  managers behind a `ThreadSafeQueue`.
- `src/index/vocabulary/VocabularyOnDisk.cpp:280-297`: `open()` builds
  the pool via `makeBatchManager`; `lookupBatch` pops one manager, runs
  both read phases (`readOffsetPairs`, `readStrings`) through it, and
  returns it via a cleanup guard.

The pool gives transient exclusive ownership (pop, use, push), but the
ring still migrates across threads on every checkout, the queue itself
is a contention point under export fan-out, and nothing binds a ring to
the thread that drives it. That is the gap this design closes.

## Design

1. **Exclusive ownership model.** A ring is owned by exactly one thread
   at a time, and the owner is fixed for the ring's lifetime slice: the
   thread that created (or checked out) the ring is the only thread that
   ever calls `addBatch`/`wait` on it. Ownership transfer happens only
   through explicit handoff (pool push/pop while the ring is idle, with
   zero in-flight requests), never while I/O is outstanding. No lock is
   held during submission or completion reaping.
2. **Ring-per-thread lifecycle.** Each worker thread lazily creates its
   own `BatchManager` (io_uring policy when available, sync fallback
   otherwise) on first vocabulary batch lookup and destroys it at thread
   teardown, draining in-flight batches first (the existing destructor
   drain in `IoUringManager.cpp` already covers this). A bounded pool
   remains as the fallback for threads that cannot hold a ring
   (short-lived threads), sized by the existing
   `NUM_VOCAB_BATCH_IO_MANAGERS` constant. Ring size stays configurable
   through the existing `ringSize` parameter.
3. **How VocabularyOnDisk shards lookups across rings.** `lookupBatch`
   resolves the calling thread's owned ring (thread-local, created on
   demand) instead of popping from the shared queue; the queue remains
   only for threads without an owned ring. Both read phases of one
   `lookupBatch` call (offsets phase, then string phase) use the same
   thread-owned ring, preserving the current two-phase ordering. Batch
   handles stay per-ring (they already are: `nextBatchHandle_` lives in
   `BatchManager`), so no cross-ring handle namespace is introduced.
4. **Failure semantics stay local.** `makeBatchManager` keeps its
   probe-once fallback: if `io_uring_queue_init` fails, the thread's
   owned ring is a sync manager, and other threads are unaffected. An
   I/O error on one thread's ring throws to that thread's caller only
   and never poisons another thread's ring.
5. **No shared mutable state on the I/O path.** The per-ring counters,
   per-batch map, and request-id map remain plain (non-atomic) members
   because they are only touched by the owning thread. Thread safety
   follows from ownership, not from locking.

## Acceptance

- [x] Thread-safety unit tests: concurrent `lookupBatch` calls from N
  threads against a fixture vocabulary return byte-identical results to
  the serial path (including the sync-fallback policy), plus a stress
  test that migrates load across threads. Implemented in
  `test/index/vocabulary/VocabularyOnDiskTest.cpp`
  (`LookupBatchConcurrentMatchesSerial`,
  `LookupBatchConcurrentSyncFallbackMatchesSerial`,
  `LookupBatchOversubscribedThreadsMatchesSerial`).
  Still open: a ThreadSanitizer run with zero reports.
- [ ] Ural Wikidata-truthy scaling benchmark: export workload over the
  Wikidata truthy index (`/local/data-ssd/stoetzem/wikidata`) scales
  lookup throughput with thread count up to the pool/thread count,
  with no regression at one thread versus the current master behavior.

## Implementation

`VocabularyOnDisk::lookupBatch` (`src/index/vocabulary/VocabularyOnDisk.cpp`)
resolves the calling thread's owned ring via `threadLocalManager()` and runs
both read phases through it, instead of popping from the shared queue. The
owned ring is created on first use (per-thread probe-once backend selection,
so a failed `io_uring_queue_init` degrades only that thread) and destroyed at
thread teardown (draining in-flight batches first). Ownership is bounded by
`NUM_VOCAB_BATCH_IO_MANAGERS` via a compare-exchange claim on a per-vocabulary
budget; excess threads fall back to the shared `ioManagers_` pool with the
previous pop-use-push discipline. Thread-local rings are keyed by the
vocabulary's budget (`weak_ptr`, pruned when expired), so vocabulary
destruction, moves, and address reuse can neither leak nor collide entries.
`open()` gained a defaulted `preferIoUring` parameter (`false` forces the sync
fallback, used by the tests). Policy documentation in
`src/util/IoUringManager.h` now states thread-confinement instead of
"single-threaded use only".

## Dependency note

This design is the prerequisite for any SQPoll work: a kernel poll
thread per ring only makes sense once each ring has a single owning
thread with a steady submission stream. No SQPoll flag, kernel thread
tuning, or wake/sleep policy is part of this change.

## Out of scope

- SQPoll mode (`IORING_SETUP_SQPOLL`) and any poll-thread tuning.
- Registered buffers / fixed files.
- Changing the two-phase lookup shape (offsets, then strings) or the
  on-disk vocabulary layout.
- Cross-ring batch handles or work stealing between rings.
- Any change to the SPARQL-level result contract.
