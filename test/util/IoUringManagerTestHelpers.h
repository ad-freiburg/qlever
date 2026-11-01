// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_IOURINGMANAGERTESTHELPERS_H
#define QLEVER_TEST_UTIL_IOURINGMANAGERTESTHELPERS_H

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <string>
#include <utility>
#include <vector>

#include "GTestHelpers.h"
#include "backports/algorithm.h"
#include "backports/span.h"
#include "util/File.h"

namespace ioUringManagerTestHelpers {

// Writes `content` to a temporary file and keeps it open for reading.
// `fd()` exposes the file descriptor; the file is removed from disk on
// destruction. Use `makeTempFile` below to get the file and its fd in one step.
class TempFile {
 public:
  explicit TempFile(std::string_view content)
      : path_{absl::StrCat(gtestCurrentTestName(), ".tmp")} {
    // Open for reading and writing (`"w+b"`): the tests read from this file's
    // `fd()` via `pread`/io_uring.
    readFile_ = ad_utility::File{path_, "w+b"};
    readFile_.write(content.data(), content.size());
    // Flush the libc stream buffer so the written bytes reach the kernel and
    // are visible to the reads the tests issue on `fd()`.
    readFile_.flush();
  }
  // Movable (so a factory can return it by value). The moved-from object's
  // `path_` is cleared so that only the live object removes the file.
  TempFile(TempFile&& other) noexcept
      : path_{std::move(other.path_)}, readFile_{std::move(other.readFile_)} {
    other.path_.clear();
  }
  // Close the descriptor and remove the file from disk.
  ~TempFile() {
    if (!path_.empty()) {
      readFile_.close();
      std::remove(path_.c_str());
    }
  }
  int fd() const { return readFile_.fd(); }

 private:
  std::string path_;
  ad_utility::File readFile_;
};

// Create a temporary file holding `content` and return it together with its
// file descriptor. The returned `tmp` must be kept alive for as long as `fd` is
// used.
std::pair<TempFile, int> makeTempFile(std::string_view content) {
  TempFile tmp{content};
  int fd = tmp.fd();
  return {std::move(tmp), fd};
}

// Test helper that accumulates a batch of read requests and owns their target
// buffers. After the batch has completed, `result()` returns the bytes that
// each read produced, in request order.
class ReadBatchForTesting {
 public:
  // Add a read of `numBytes` bytes at `offset`; returns the read's index.
  size_t add(uint64_t offset, size_t numBytes) {
    offsets_.push_back(offset);
    numBytes_.push_back(numBytes);
    targetBuffers_.emplace_back(numBytes, '\0');
    return targetBuffers_.size() - 1;
  }

  // Add several reads at once from a range of `(offset, numBytes)` pairs.
  void add(ql::span<const std::pair<uint64_t, size_t>> reads) {
    for (const auto& [offset, numBytes] : reads) {
      add(offset, numBytes);
    }
  }

  void add(std::initializer_list<std::pair<uint64_t, size_t>> reads) {
    add(ql::span<const std::pair<uint64_t, size_t>>{reads.begin(),
                                                    reads.size()});
  }

  // Submit all accumulated reads to `manager` for file `fd`; returns the
  // handle.
  template <typename Manager>
  typename Manager::BatchHandle submitTo(Manager& manager, int fd) {
    std::vector<char*> pointers = bufferPointers();
    return manager.addBatch(fd, numBytes_, offsets_, pointers);
  }

  // The bytes read by each read, in request order (valid once the batch has
  // completed).
  const std::vector<std::string>& result() const { return targetBuffers_; }

  // The file offset of read `i`.
  uint64_t offset(size_t i) const { return offsets_[i]; }

  // Like `submitTo`, but directly to a `ReadPolicy`.
  template <typename Policy>
  void submitToPolicy(Policy& policy, int fd,
                      typename Policy::BatchHandle handle) {
    std::vector<char*> pointers = bufferPointers();
    policy.addBatch(fd, numBytes_, offsets_, pointers, handle);
  }

 private:
  // The buffer pointers, built when the batch is submitted, after all buffers
  // have been added, so the addresses are stable (no further `add` will
  // reallocate `targetBuffers_`). `addBatch` copies each address into its read
  // request, so the vector need not outlive the call.
  std::vector<char*> bufferPointers() {
    return ::ranges::to_vector(targetBuffers_ |
                               ql::views::transform([](std::string& buffer) {
                                 return buffer.data();
                               }));
  }

  std::vector<size_t> numBytes_;
  std::vector<uint64_t> offsets_;
  std::vector<std::string> targetBuffers_;
};

}  // namespace ioUringManagerTestHelpers

#endif  // QLEVER_TEST_UTIL_IOURINGMANAGERTESTHELPERS_H
