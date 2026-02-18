// Copyright 2024, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_UTIL_IOURINGHELPERS_H
#define QLEVER_SRC_UTIL_IOURINGHELPERS_H

#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <stdexcept>
#include <vector>

#ifdef QLEVER_HAS_IO_URING
#include <liburing.h>
#endif

#include "backports/span.h"

namespace ad_utility {

// Reads multiple regions from a file descriptor into memory.
// For each i in [0, n): reads `sizes[i]` bytes from file offset
// `fileOffsets[i]` into `targetPointers[i]`.
// If io_uring is available, uses async I/O. Otherwise falls back to pread.
// Internally sorts reads by file offset for sequential I/O.
inline void readBatch(int fd, ql::span<const size_t> sizes,
                      ql::span<const uint64_t> fileOffsets,
                      ql::span<char*> targetPointers) {
  const size_t n = sizes.size();
  if (n == 0) {
    return;
  }

  // Build a permutation sorted by file offset for sequential I/O.
  std::vector<size_t> perm(n);
  std::iota(perm.begin(), perm.end(), size_t{0});
  std::sort(perm.begin(), perm.end(), [&](size_t a, size_t b) {
    return fileOffsets[a] < fileOffsets[b];
  });

  // Synchronous fallback: loop with pread in sorted offset order.
  auto readSynchronously = [&]() {
    for (size_t pi = 0; pi < n; ++pi) {
      size_t i = perm[pi];
      size_t bytesRead = 0;
      while (bytesRead < sizes[i]) {
        ssize_t ret =
            pread(fd, targetPointers[i] + bytesRead, sizes[i] - bytesRead,
                  static_cast<off_t>(fileOffsets[i]) +
                      static_cast<off_t>(bytesRead));
        if (ret < 0) {
          throw std::runtime_error("pread failed during batch read");
        }
        bytesRead += static_cast<size_t>(ret);
      }
    }
  };

#ifdef QLEVER_HAS_IO_URING
  [&]() {
    struct io_uring ring;
    int ret = io_uring_queue_init(static_cast<unsigned>(n), &ring, 0);
    if (ret < 0) {
      readSynchronously();
      return;
    }

    // Submit read SQEs in sorted order.
    for (size_t pi = 0; pi < n; ++pi) {
      size_t i = perm[pi];
      struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
      if (!sqe) {
        io_uring_submit(&ring);
        sqe = io_uring_get_sqe(&ring);
      }
      if (!sqe) {
        io_uring_queue_exit(&ring);
        readSynchronously();
        return;
      }
      io_uring_prep_read(sqe, fd, targetPointers[i],
                         static_cast<unsigned>(sizes[i]),
                         static_cast<__u64>(fileOffsets[i]));
      io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(i));
    }

    ret = io_uring_submit(&ring);
    if (ret < 0) {
      io_uring_queue_exit(&ring);
      readSynchronously();
      return;
    }

    // Reap all CQEs.
    for (size_t completed = 0; completed < n; ++completed) {
      struct io_uring_cqe* cqe;
      ret = io_uring_wait_cqe(&ring, &cqe);
      if (ret < 0) {
        io_uring_queue_exit(&ring);
        throw std::runtime_error(
            "io_uring_wait_cqe failed during batch read");
      }
      io_uring_cqe_seen(&ring, cqe);
    }

    io_uring_queue_exit(&ring);
  }();
#else
  readSynchronously();
#endif
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_IOURINGHELPERS_H
