# io_uring runtime tuning parameters: design plan

## Goal

This document plans runtime tuning parameters for the io_uring vocabulary
lookup path. Operators can tune the ring size and the batch window without
rebuilding QLever. The defaults preserve current behavior. This PR contains
the design only. The implementation follows in a later PR.

## Paper reference

Jasny et al. study io_uring parameter tuning for storage engines. Their YCSB
results show that submission batching amortizes syscall cost. A batch of 16
cuts the per-operation cost from about 1340 to about 300 CPU cycles. Larger
batches flatten near 250 cycles per operation. Their runtime adapts the batch
size to load. It defers submissions when many I/Os are in flight. It flushes
earlier when few requests are pending. These findings are reviewed in the
thesis in `document/chapters/05B-iouring-batch-lookup.tex` (YCSB tuning
section, Figure `fig:iouring-batch-cycles`). The review motivates exposing
the ring size and the batch window as runtime knobs.

## Current state

All paths below are on `origin/master` at the time of writing.

- `src/util/IoUringManager.h:87`: `BatchManager` takes
  `unsigned ringSize = 256`. The fixed default of 256 applies to every
  manager that is built without an explicit argument.
- `src/util/IoUringManager.h:133`: `SyncIoPolicy` accepts the same default
  and ignores it. The constructor exists only to satisfy the policy concept.
- `src/util/IoUringManager.h:215`: `IoUringPolicy` takes an explicit
  `ringSize`. The comment at line 214 states that the value must be positive
  and that a power of two is preferred because liburing rounds up.
- `src/util/IoUringManager.h:247`: `makeBatchManager` takes
  `unsigned ringSize = 256`. It forwards the value to the selected policy.
- `src/index/vocabulary/VocabularyOnDisk.cpp:296`: `VocabularyOnDisk::open`
  calls `makeBatchManager(preferIoUring)` without an argument. Every pooled
  manager therefore uses the fixed default of 256.
- `src/global/Constants.h:380`: `NUM_VOCAB_BATCH_IO_MANAGERS` is 8. The pool
  holds 8 managers. Each manager owns one ring.
- No `RuntimeParameters` knob controls the ring size or the batch window. No
  batch window limit exists in the lookup path today.

## Design

1. Add a `SizeT` knob `iouring-ring-size` with default 256. It sets the ring
   size that `makeBatchManager` forwards to the policy constructor.
2. Add a `SizeT` knob `vocab-batch-window` with default 0, where 0 means no
   cap. It limits how many reads one `addBatch` submission carries in the
   `lookupBatch` path. A nonzero value splits larger batches into windows.
3. Declare both knobs next to `constructDeduplication_` in
   `src/global/RuntimeParameters.h:244`. Register both in the constructor in
   `src/global/RuntimeParameters.cpp:17`. Follow the existing `add(...)`
   pattern used by all current knobs.
4. Constrain both knobs with `setParameterConstraint`, following the
   `mustBeStrictlyPositive` example in `RuntimeParameters.cpp:81-96`. The
   ring size constraint additionally rejects values above 4096. Powers of
   two are preferred but not required, since liburing rounds up.
5. Plumb the values in `VocabularyOnDisk::open`
   (`VocabularyOnDisk.cpp:279`). The function reads both knobs from the
   global runtime parameters once. It passes the ring size to
   `makeBatchManager`. It stores the batch window for `lookupBatch`.
6. Keep the fallback behavior unchanged. When io_uring setup fails at
   runtime, `makeBatchManager` still falls back to `SyncIoPolicy`. The ring
   size argument stays ignored on that path.

### Defaults and valid ranges

| Knob | Default | Valid range | Notes |
|------|---------|-------------|-------|
| `iouring-ring-size` | 256 | 1 to 4096 | Power of two preferred. liburing rounds up. Matches the current fixed default. |
| `vocab-batch-window` | 0 (no cap) | 0 to 1M | 0 preserves current behavior. Positive values split large batches into windows. |

## Acceptance

- Unit tests construct the new knobs from strings. Value 0 and negative
  values are rejected for `iouring-ring-size`. Value 0 is accepted for
  `vocab-batch-window`. Out-of-range values are rejected with a readable
  message. Defaults equal the table above.
- A sensitivity run on Ural uses the Wikidata truthy index at
  `/local/data-ssd/stoetzem/wikidata`. It varies `iouring-ring-size` over
  64, 256, and 1024. It varies `vocab-batch-window` over 0, 16, and 64. It
  reports the resolution-phase time per batch. The driver aborts when the
  serving manifest or the query suite is missing.
- The design PR itself is docs only. It adds no behavior change.

## Note on the stale branch

The branch `origin/marvin-io-uring-param-tuning` explored this direction
earlier. This design supersedes that exploration. The stale branch is
referenced here for traceability. It is NOT closed or deleted by this PR.

## Out of scope

- SQPoll mode, registered buffers, and deferred task reaping stay unchanged.
  They are separate tuning dimensions with their own tradeoffs.
- The pool size `NUM_VOCAB_BATCH_IO_MANAGERS` stays a compile-time constant.
- No change to the synchronous `pread` fallback path beyond keeping it.
- No implementation code. This PR adds only this design document.
