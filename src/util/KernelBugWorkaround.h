// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_KERNELBUGWORKAROUND_H
#define QLEVER_SRC_UTIL_KERNELBUGWORKAROUND_H

#include <absl/functional/any_invocable.h>

#include <cstddef>
#include <string_view>

// A workaround for a bug of the Linux kernels 7.0 and 7.1 (up to 7.1.8) on
// arm64: when `madvise(MADV_DONTNEED)` frees a page table that has become
// empty, the kernel flushes the wrong address from the TLB, so other CPUs keep
// using the freed page table. This causes random stalls, crashes, and corrupt
// data. `jemalloc` triggers the bug when it returns freed memory to the kernel.
// The bug is fixed by
// https://github.com/torvalds/linux/commit/478a1c3abebfc717db0d1281a9cdd7befafee542
// (in 7.1.9, 7.2, and later, and in backports of the distributions).
namespace ad_utility::kernelBugWorkaround {

// The environment variable that disables the workaround (if set to any value).
// Only set it on a kernel that contains the fix.
inline constexpr const char* disableEnvVar =
    "QLEVER_DISABLE_KERNEL_BUG_WORKAROUND";

// Return true if the kernel with the given `machine` (as from `uname -m`) and
// `release` (as from `uname -r`) may have the bug. The release alone doesn't
// tell whether a distribution backported the fix, so return true for all
// arm64 kernels based on 7.0, 7.1 up to 7.1.8, and the 7.2 release candidates
// before the fix.
bool mayHaveBug(std::string_view machine, std::string_view release);

// The signature of the control interface `mallctl` of `jemalloc`. An empty
// `MallctlFunction` means that QLever doesn't use `jemalloc`.
using MallctlFunction = absl::AnyInvocable<int(
    const char* name, void* oldp, size_t* oldlenp, void* newp, size_t newlen)>;

// The outcome of `applyIfNeeded`.
enum class Status { NotNeeded, DisabledByUser, NoJemalloc, Applied };

// If the kernel may have the bug and the workaround is not disabled, make
// `jemalloc` never return freed memory to the kernel (by setting its
// `dirty_decay_ms` to -1 for all arenas, also for those that are created
// later), and log a warning. Call this early in `main`. The memory of the
// process then stays at its peak. On other operating systems than Linux, do
// nothing and return `Status::NotNeeded`.
Status applyIfNeeded();

// The same, but with the `machine` and `release` of the kernel, the presence
// of the environment variable `disableEnvVar`, and the `mallctl` function of
// `jemalloc` given explicitly (for testing). If `mallctlFunction` is empty,
// return `Status::NoJemalloc` instead of `Status::Applied`.
Status applyIfNeeded(std::string_view machine, std::string_view release,
                     bool disabledByUser, MallctlFunction mallctlFunction);

}  // namespace ad_utility::kernelBugWorkaround

#endif  // QLEVER_SRC_UTIL_KERNELBUGWORKAROUND_H
