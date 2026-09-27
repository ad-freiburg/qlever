# NVMe passthrough reads via `IORING_OP_URING_CMD`

## Goal

Replace the generic block-layer read path in `IoUringPolicy` with native NVMe
commands submitted through io_uring (`IORING_OP_URING_CMD`) on devices that
support it. The passthrough path must bypass the generic storage stack for
each read while keeping the existing plain-read path as the fallback for
devices and kernels that do not support passthrough.

## Paper reference

Jasny et al. describe this step in the subsubsection "Tuning io_uring for
the Storage Engine" (thesis: `document/chapters/05B-iouring-batch-lookup.tex`,
paragraph "NVMe passthrough.", lines 926-929). The io_uring
`OP_URING_CMD` opcode issues native NVMe commands through the kernel to
device queues and bypasses the generic storage stack. In the paper's
buffer-managed storage engine, passthrough adds 20% throughput (to
300 k tx/s, the +Passthru leg), and the subsequent IOPoll leg adds 21%
(to 376 k tx/s, the +IOPoll leg). This design covers the passthrough leg
only; IOPoll stays out of scope.

## Current state

`IoUringPolicy::addBatch` in `src/util/IoUringManager.cpp` prepares one
block-layer read SQE per request with `io_uring_prep_read` at
`src/util/IoUringManager.cpp:131`. The SQE is tagged with a request id at
`src/util/IoUringManager.cpp:139-141`, and the batch is flushed with
`io_uring_submit` at `src/util/IoUringManager.cpp:147`. Completions are
drained per batch in `IoUringPolicy::wait` at
`src/util/IoUringManager.cpp:151-159`. Every read therefore traverses the
generic filesystem and block stack before reaching the device.

## Design

1. Add a `submitPassthroughRead` helper next to the `io_uring_prep_read`
   call site (`src/util/IoUringManager.cpp:131`) that prepares an
   `IORING_OP_URING_CMD` SQE carrying a native NVMe read command (namespace,
   starting LBA, block count) for the requested file offset and length.
2. Detect passthrough capability once per device at ring setup (NVMe
   character device present and kernel supports `IORING_OP_URING_CMD` for
   it). Store the result as a per-fd flag on the policy object.
3. Route each request in `addBatch` through the passthrough helper when the
   flag is set, and through the existing `io_uring_prep_read` path
   otherwise. The fallback is per device, not per request, so mixed fleets
   keep working without configuration.
4. Keep completion handling unchanged: the passthrough SQE carries the same
   `user_data` request id scheme (`src/util/IoUringManager.cpp:139-141`),
   so `drainOneCqe` and `wait` need no changes.
5. Interact with fixed (registered) buffers by requiring the registered
   buffer set for the passthrough path: the NVMe command targets the
   already-pinned target buffer, which preserves the zero-copy data path
   and keeps buffer registration as the single pinning point.
6. Gate the feature behind a runtime parameter (default off) so operators
   enable passthrough explicitly per deployment after validating their
   device and kernel combination.

## Acceptance

- Unit tests cover capability detection and the fallback: a device without
  passthrough support takes the plain-read path, and a failed capability
  probe disables passthrough without failing the batch.
- A benchmark on Ural against the Wikidata truthy index on NVMe storage
  compares passthrough reads against the plain-read path on the same
  hardware and reports throughput and per-I/O CPU cost.

## Dependency note

This design requires PR #83 (fixed buffers / `O_DIRECT`) first, because
the passthrough path targets pinned buffers and direct device access.
PR #93 (`SEND_ZC`) is socket zero-copy for the network path and is
unrelated to this storage change; it must not be confused with the
registered-buffer interaction described above.

## Out of scope

- IOPoll completion polling (the paper's +IOPoll leg).
- SQPoll submission polling and multishot operations.
- Changes to the filesystem layout or to non-NVMe devices.
- Write-path passthrough; reads only.
