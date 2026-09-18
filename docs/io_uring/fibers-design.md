# Fiber cooperative scheduling over io_uring: design plan

Status: design proposal. This document merges no runtime code. It records
the goal, the paper mechanism it ports, the current blocking behavior with
file and line anchors, and the numbered design steps a later implementation
PR must follow.

## Goal

The export-path vocabulary lookup currently blocks the calling thread
while it waits for io_uring completions. The goal is to replace that
blocking wait with cooperative fiber scheduling, so that one thread runs
several lookup batches concurrently and useful batch work fills the time
that completions spend in flight.

## Paper reference

Jasny, El-Hindi, Ziegler, Leis, and Binnig, "io_uring for High-Performance
DBMSs: When and How to Use It", Proceedings of the VLDB Endowment 19(1),
2026 (preprint arXiv:2512.04859). The thesis cites it as
`jasny2026iouring` (`document/references/references.bib:1049`).

The ported mechanism is the paper's asynchronous transaction execution,
reviewed in the thesis in `document/chapters/05B-iouring-batch-lookup.tex`,
subsubsection "Using io_uring in the Storage Engine"
(`\label{Subsection::using-iouring-storage-engine}`), paragraphs
"Fibers and cooperative scheduling"
(`\label{Paragraph::fibers-cooperative-scheduling}`) and
"Asynchronous transaction execution". Each transaction runs as a fiber
that issues its I/O requests and yields on page faults, which lets the
io_uring-based runtime schedule other fibers and keeps the CPU active.
With up to 128 fibers the paper's buffer manager rises from about 19 k
tx/s to about 183 k tx/s (thesis Figure `Figure::ycsb-progression`,
`io_uring+Fibers` bar), which is the largest single throughput jump in
the paper's YCSB progression. The thesis fibers primer in Appendix D.2,
"Fibers and cooperative scheduling" (`\label{Appendix:fibers}`) in
`document/chapters/99-backmatter.tex`, defines the underlying concepts
(stackful coroutines, user-level context switch cost, cooperative
against preemptive scheduling).

## Current state

The blocking behavior lives in the io_uring read policy:

- `src/util/IoUringManager.cpp:151-159`: `IoUringPolicy::wait`
  drains completions in a loop until the batch handle disappears. The
  loop makes no progress on any other batch while it runs.
- `src/util/IoUringManager.cpp:162-168`: `drainOneCqe` calls
  `io_uring_wait_cqe` (`IoUringManager.cpp:165`), which parks the whole
  OS thread until at least one completion queue entry arrives.
- `src/util/IoUringManager.cpp:115-122`: the `addBatch` ring-full path
  also blocks in `drainOneCqe` until submission slots free up.
- `src/util/IoUringManager.h:105-106`: `BatchManager::wait` documents
  the contract as "Block until every read in `handle` has completed."

Every wait therefore donates the thread to the kernel instead of to
other ready lookup batches.

## Design

1. **One fiber per lookup batch.** Each `BatchManager::addBatch` call
   mints a `BatchHandle` and the batch body (submit reads, wait for
   this handle's completions, hand strings to the formatter) runs as
   its own fiber on the calling thread. Batch identity stays the
   existing `BatchHandle`; the scheduler keys ready and waiting sets
   by that handle.
2. **Yield on pending CQE instead of blocking wait.** `wait(handle)`
   becomes a loop over non-blocking completion reaps: it reaps every
   available CQE, attributes each to its batch exactly as `drainOneCqe`
   does today, and when the awaited handle still has completions
   outstanding, the fiber yields to the scheduler rather than entering
   `io_uring_wait_cqe`. The scheduler resumes the fiber once a reaped
   CQE belongs to its handle. The blocking `io_uring_wait_cqe` call
   remains only as the last-resort park when no fiber on the thread is
   runnable and completions are still outstanding.
3. **Scheduler integration with the adaptive-batching ratio
   controller.** Submission timing keeps the paper's feedback rule
   (thesis Section `Subsection::using-iouring-storage-engine`: the
   runtime adapts batch size to the ratio of outstanding I/Os to
   waiting fibers; it defers submissions while many I/Os are in flight
   and flushes early when few are pending). The scheduler exposes both
   counts, namely the number of completions still outstanding and the
   number of fibers currently waiting, so the submission path can apply
   that rule without new bookkeeping.
4. **Stackful fibers (Boost.Fiber over Boost.Context), not stackless
   C++20 coroutines.** The lookup path suspends inside nested calls
   (batch assembly, vocabulary resolution helpers, completion
   attribution), and a stackful fiber suspends from any nesting depth
   with the whole call chain intact. A stackless coroutine suspends
   only at marked points in one function frame, so it would force a
   rewrite of every nested helper into coroutine plumbing. This repeats
   the paper's own justification: its transactions block inside nested
   B-tree code, which is why fibers carry them (thesis Appendix D.2).
   The per-switch cost stays at tens of cycles with no kernel entry,
   against thousands of cycles for a thread switch.

## Acceptance

- Correctness tests run on every implementation PR: yield and resume
  ordering tests prove that a fiber resumes only after at least one of
  its own completions was reaped, that completions are attributed to
  the batch handle recorded at submission, that no wakeup is lost when
  a completion lands between the last reap and the yield, and that a
  fiber that never yields cannot starve its thread siblings.
- Performance acceptance is a benchmark on Ural against the Wikidata
  truthy index (`/local/data-ssd/stoetzem/wikidata`, basename
  `wikidata`) with the Wikidata query suite. The benchmark driver must
  point at the Wikidata truthy serving manifest and the Wikidata query
  suite, and it must abort when either is missing. No DBLP numbers are
  used for this feature.

## Dependency note

This design builds on two earlier io_uring stages. Per-thread rings
give each worker its own submission and completion queues, so a fiber
that yields never blocks fibers on other threads. Adaptive batching
provides the outstanding-I/O to waiting-fiber ratio controller that
step 3 integrates with. Both stages land first; this design assumes
their interfaces and does not re-specify them.

## Out of scope

The implementation itself, including fiber stack sizing, work
stealing or cross-thread fiber migration, integration with kernel-side
polling modes (IOPoll, SQPoll), registered buffers, the network
shuffle path from the paper, any change to the synchronous `pread`
fallback, and any thesis prose edits.
