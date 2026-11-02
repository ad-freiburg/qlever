// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PAGECACHEFIRSTPOLICY_H
#define QLEVER_SRC_UTIL_PAGECACHEFIRSTPOLICY_H

#include <cstdint>
#include <type_traits>
#include <vector>

#include "backports/algorithm.h"
#include "backports/span.h"
#include "util/IoUringManagerConcepts.h"

namespace ad_utility {

// Serve the reads of a batch that are completely in the page cache with
// non-blocking `preadv2(RWF_NOWAIT)` calls, one call per run of adjacent file
// ranges (split into chunks of at most `IOV_MAX` reads), and return the
// positions of the other reads in ascending order. The caller issues those
// through its regular path, which also reports errors. A call that fails with
// `EOPNOTSUPP` or `ENOSYS` switches the fast path off for the whole process
// (see `pageCacheFastPathIsSupported`).
// Precondition: the three spans have the same length.
std::vector<size_t> readPageCacheHits(int fd, ql::span<const size_t> numBytes,
                                      ql::span<const uint64_t> offsets,
                                      ql::span<char*> buffers);

// Return whether `readPageCacheHits` may use `preadv2(RWF_NOWAIT)`: false if it
// is not compiled in, or once a call has failed with `EOPNOTSUPP` or `ENOSYS`.
// That latch is process-wide.
bool pageCacheFastPathIsSupported() noexcept;

namespace detail {
// Owning copies of the reads of a batch at the indices `positions` into the
// three equally long spans, in the order of `positions`, as the `numBytes`,
// `offsets` and `buffers` arguments of `Policy::addBatch`.
struct SelectedReads {
  std::vector<size_t> numBytes_;
  std::vector<uint64_t> offsets_;
  std::vector<char*> buffers_;

  SelectedReads(ql::span<const size_t> numBytes,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                ql::span<const size_t> positions)
      : numBytes_{selectAt(numBytes, positions)},
        offsets_{selectAt(offsets, positions)},
        buffers_{selectAt(buffers, positions)} {}

 private:
  // The elements of `values` at `positions`, copied into a vector.
  template <typename T>
  static std::vector<std::remove_const_t<T>> selectAt(
      ql::span<T> values, ql::span<const size_t> positions) {
    return ::ranges::to<std::vector<std::remove_const_t<T>>>(
        positions |
        ql::views::transform([values](size_t i) { return values[i]; }));
  }
};
}  // namespace detail

// A `ReadPolicy` that serves the reads of a batch that are in the page cache
// with `readPageCacheHits` and passes only the others on to `Policy`.
template <typename Policy>
class PageCacheFirstPolicy {
  static_assert(
      ReadPolicyConcept<Policy>,
      "PageCacheFirstPolicy's Policy must satisfy the ReadPolicyConcept.");

 public:
  using BatchHandle = typename Policy::BatchHandle;

  // `ringSize` is forwarded to the constructor of the wrapped `Policy`.
  explicit PageCacheFirstPolicy(unsigned ringSize) : policy_(ringSize) {}

  PageCacheFirstPolicy(const PageCacheFirstPolicy&) = delete;
  PageCacheFirstPolicy& operator=(const PageCacheFirstPolicy&) = delete;

  // Switch the fast path on (the default) or off for the batches that are added
  // afterwards.
  void setPageCacheFastPath(bool enabled) noexcept { enabled_ = enabled; }

  // Serve the reads of the batch that are in the page cache and pass the others
  // on to the decorated policy. The arguments have the contract of
  // `readPageCacheHits` and `Policy::addBatch`.
  void addBatch(int fd, ql::span<const size_t> numBytes,
                ql::span<const uint64_t> offsets, ql::span<char*> buffers,
                BatchHandle handle) {
    if (!enabled_ || !pageCacheFastPathIsSupported()) {
      policy_.addBatch(fd, numBytes, offsets, buffers, handle);
      return;
    }
    const std::vector<size_t> notServed =
        readPageCacheHits(fd, numBytes, offsets, buffers);
    if (notServed.size() == numBytes.size()) {
      // Nothing was served, so the batch is passed on unchanged.
      policy_.addBatch(fd, numBytes, offsets, buffers, handle);
      return;
    }
    detail::SelectedReads missed{numBytes, offsets, buffers, notServed};
    policy_.addBatch(fd, missed.numBytes_, missed.offsets_, missed.buffers_,
                     handle);
  }

  // Block until every read of the batch `handle` has completed.
  void wait(BatchHandle handle) { policy_.wait(handle); }

 private:
  Policy policy_;
  bool enabled_ = true;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_PAGECACHEFIRSTPOLICY_H
