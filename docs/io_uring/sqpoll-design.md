# SQPoll kernel poll thread: design plan

## Goal

Remove the `io_uring_enter` syscall from the submission path of QLever's
vocabulary batch lookup. A dedicated kernel thread polls the submission
queue and issues I/O on behalf of the application thread. The export path
then pays no syscall per submitted batch while the poller stays awake.

## Paper reference

Jasny et al. (`jasny2026iouring`) describe SQPoll as one of the two ways
to issue I/O in the kernel. The thesis reviews this paper in
`document/chapters/05B-iouring-batch-lookup.tex` (Section on the Jasny et
al. review, around line 367). The SQPoll trade-off is summarized at line
549: each submission saves about one syscall, but a sleeping poller costs
about 30 microseconds to wake. SQPoll therefore wins only when submissions
arrive steadily enough to keep the thread awake. The thesis design sketch
lives in `Subsection::sqpoll` ("Kernel-side submission polling (SQPoll)",
line 2376).

## Current state

`src/util/IoUringManager.cpp:67` creates the ring with no setup flags:

```cpp
int ret = io_uring_queue_init(ringSize_, &ring_, /*flags=*/0);
```

No `IORING_SETUP_*` flag is set anywhere in `src/` or `test/` on
`origin/master`. Every `io_uring_submit()` therefore enters the kernel on
the application thread. This document is the first step toward changing
that for the submission path.

## Design

1. Replace `io_uring_queue_init` with `io_uring_queue_init_params` in
   `IoUringPolicy::IoUringPolicy`, so the ring accepts a `struct
   io_uring_params` with explicit setup flags.
2. Set `IORING_SETUP_SQPOLL` and pin the kernel poller with `SQ_AFF` by
   assigning `sq_thread_cpu` to a fixed core. Set `sq_thread_idle` to
   2000 ms, so the poller stays awake across the gaps within one query
   but sleeps between queries.
3. Implement fallback when the kernel denies SQPoll. If the params call
   fails with `-EPERM` (missing `CAP_SYS_NICE`) or `-EINVAL` (kernel
   without SQPoll support), retry with `flags = 0` and log the downgrade.
   If the plain init also fails, keep the existing `SyncIoPolicy`
   fallback unchanged.
4. Evaluate `IORING_SETUP_DEFER_TASKRUN` as part of this work. Measure
   completion latency with the flag on and off on the Wikidata-truthy
   benchmark before deciding. Do not enable it unconditionally.
5. Evaluate `IORING_SETUP_SINGLE_ISSUER` as part of this work. It is only
   valid if exactly one thread ever submits to a ring. Confirm that
   property holds after per-thread rings land, then measure before
   deciding.

## Acceptance

- Feature-detection unit tests cover the new setup path. The tests
  attempt an SQPoll ring with a short idle timeout and skip cleanly when
  the kernel denies it. They assert the fallback order: SQPoll first,
  plain ring second, `SyncIoPolicy` last.
- A Ural benchmark on the Wikidata truthy index (`/local/data-ssd/
  stoetzem/wikidata`) compares SQPoll on against SQPoll off for the
  vocabulary batch lookup. It reports wall time and per-batch submission
  cost. The benchmark aborts when the serving manifest is missing.

## Dependency note

This work requires per-thread rings first (`feat/iouring-per-thread-rings`).
`SINGLE_ISSUER` is unsound on a shared ring, and one SQPoll thread per
shared ring would serialize all submitters. Land per-thread rings, then
rebase this work on top.

The stale branch `origin/marvin-io-uring-sqpoll` is superseded by this
PR. It remains referenced for its history. Do not close or delete it in
this PR.

## Out of scope

IOPoll stays rejected. It requires `O_DIRECT` on pollable files, which
the compressed vocabulary reads do not satisfy. This PR evaluates SQPoll
only and changes nothing about completion polling.

## Implementation status

Implemented in `src/util/IoUringManager.{h,cpp}`: `IoUringSetupOptions`
(defaults preserve the plain ring), `IoUringPolicy(unsigned, const
IoUringSetupOptions&)` via `io_uring_queue_init_params` with
`IORING_SETUP_SQPOLL | IORING_SETUP_SQ_AFF` and `sq_thread_idle` from the
options, fallback to a plain ring on `-EPERM`/`-EINVAL`, and
`IoUringPolicy::sqPollAvailable()` as the feature probe.
`IORING_SETUP_DEFER_TASKRUN` and `IORING_SETUP_SINGLE_ISSUER` are plumbed
as opt-in flags, default off, pending the Wikidata-truthy benchmark.
Production wiring into the vocabulary lookup pool follows after
per-thread rings land.

## Follow-up implementation (pool wiring)

Wired in `src/index/vocabulary/VocabularyOnDisk.{h,cpp}` behind the new
`iouring-sqpoll` runtime knob (default off): `open()` reads the knob once
and builds every pooled manager with `useSqPoll` set accordingly. Each
pooled manager owns its ring exclusively while checked out (pop, both read
phases, push back while idle), so one SQPoll thread per pooled ring cannot
serialize concurrent submitters. `SINGLE_ISSUER` stays off because rings
migrate across threads over their lifetime until per-thread rings land.
`DEFER_TASKRUN` stays off pending measurement. The same-binary SQPoll
on/off benchmark on the Wikidata truthy index toggles only this knob.
