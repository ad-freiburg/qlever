# Adaptive io_uring batch sizing: design plan

## Goal

QLever's `IoUringPolicy` currently submits vocabulary reads against a fixed
ring window (default 256) and drains completions only when that window is
full, so small batches pay one syscall per flush while large batches can
stall behind a full ring. This plan adds a ratio controller that adapts the
effective batch target to the ratio of outstanding I/Os to still-pending
reads, which keeps device utilization high without stalling when little work
remains. The default behavior stays identical to the fixed window until the
controller is explicitly enabled.

## Paper reference

Jasny et al., `io_uring for High-Performance DBMSs: When and How to Use It`
(`jasny2026iouring`), as surveyed in the thesis in
`document/chapters/05B-iouring-batch-lookup.tex`, `\section{Related work}`
(`Section::related-work`), subsection `io_uring for database systems`,
paragraph `Batched read submission.` under `Using io_uring to Batch Reads.`
(`Paragraph::using-iouring-batch-reads`). The surveyed element is the
runtime that adapts the read batch size to the ratio of outstanding I/Os to
waiting fibers: it defers submissions when many I/Os are already in flight
(to increase amortization) and flushes earlier when few are pending (to keep
the CPU busy).

## Current state on master

All references are on `origin/master`:

- `src/util/IoUringManager.h:87`: `BatchManager` defaults to
  `ringSize = 256`.
- `src/util/IoUringManager.h:133`: `SyncIoPolicy` accepts and ignores the
  same default of 256, which keeps both policies constructible alike.
- `src/util/IoUringManager.h:215`: `IoUringPolicy` takes `ringSize` once at
  construction and never adjusts it afterwards.
- `src/util/IoUringManager.h:247`: `makeBatchManager` defaults to
  `ringSize = 256` for both backends.
- `src/util/IoUringManager.cpp:115-122`: `addBatch` uses a fixed ring-full
  check (`numInFlightReadRequests_ >= ringSize_`) that submits and drains
  only when the window is full.
- `src/util/IoUringManager.cpp:147`: `addBatch` flushes the remaining
  prepared SQEs unconditionally at the end of the batch.

There is no feedback from completion rate or remaining work into submission
timing. The window is fixed for the lifetime of the policy.

## Design

Numbered implementation steps for the ratio controller:

1. Add an `AdaptiveBatchController` value type in `src/util/` that stores a
   minimum batch size, a maximum batch size (defaulting to the ring size),
   and the defer/flush ratio thresholds. It exposes one pure function
   `shouldFlush(outstanding, pending)` that returns true when the pending
   reads are few relative to the outstanding I/Os (flush early to keep the
   device busy) and false when many I/Os are already in flight (defer to
   increase amortization).
2. Give `IoUringPolicy` an optional controller member that is disabled by
   default. When disabled, `addBatch` keeps the exact fixed-window behavior
   of step `src/util/IoUringManager.cpp:115-122`, so existing callers see no
   change.
3. When enabled, replace the fixed ring-full check with the controller
   decision: inside the per-read loop of `addBatch`, compute
   `outstanding = numInFlightReadRequests_` and
   `pending = remaining reads of the current batch`, and call
   `shouldFlush(outstanding, pending)` to decide whether to
   `io_uring_submit` now or keep preparing SQEs. The ring-full condition
   remains as a hard safety bound that always submits and drains.
4. Clamp the deferred group to the configured maximum batch size so a very
   large batch still submits incrementally and never exceeds the ring.
   Clamp the minimum batch size to at least one so a nearly finished batch
   always flushes instead of waiting for work that will never arrive.
5. Expose the controller through the existing construction path: extend
   `makeBatchManager` and the runtime parameters with an opt-in setting
   (e.g. `adaptive-batch` bounds) that constructs the policy with the
   controller enabled. The default stays disabled with `ringSize = 256`.
6. Add unit tests for the controller (threshold boundaries, clamping, and
   the disabled-by-default path) next to the existing `IoUringPolicy`
   tests, and gate the rollout on the benchmark in Acceptance below.

## Acceptance

- Unit tests: the new controller tests and the existing `IoUringPolicy`
  tests pass, including a test that the disabled controller reproduces the
  fixed-window submission sequence exactly.
- Benchmark gating: a Wikidata-truthy benchmark on Ural
  (`/local/data-ssd/stoetzem/wikidata`) compares the controller against the
  fixed window on a vocabulary-heavy `CONSTRUCT` export. The change is
  accepted only if it does not regress export resolution time and shows the
  expected amortization effect (fewer submits per read at high queue depth,
  no added stall at low queue depth). The benchmark driver must point at
  the Wikidata truthy serving manifest and a Wikidata query suite, and it
  must abort when either is missing.

## Relation to open PR #55

PR #55 (`marvin-io-uring-sliding-window`, `io_uring: sliding-window batch
submit (kill one-in/one-out)`) changes submission pacing *within* a fixed
window by keeping the ring full as completions arrive. This plan is
complementary, not a duplicate: the ratio controller decides *how large*
each submitted group should be based on outstanding versus pending work,
while the sliding window decides *when* to top the ring back up. The two
compose (controller sets the group target, sliding window maintains it),
and either is useful without the other.

## Out of scope

- Porting the paper's fiber runtime or cooperative scheduler to QLever.
  QLever's export path stays single-threaded; `pending` here means
  not-yet-submitted reads of the current batch, not waiting fibers.
- SQPoll, IOPoll, registered buffers, and NVMe passthrough tuning.
- Network I/O or any non-vocabulary use of io_uring.
- Changing the default `ringSize = 256` or enabling the controller by
  default. Both stay as they are until the benchmark in Acceptance says
  otherwise.
