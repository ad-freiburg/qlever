// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "util/IoUringManager.h"

namespace {

// Helper: write `content` to a temporary file and return the path to the
// temporary file.
class TempFile {
 public:
  explicit TempFile(const std::string& content)
      : path_("IoUringManagerTest.tmp") {
    FILE* f = std::fopen(path_.c_str(), "wb");
    EXPECT_NE(f, nullptr);
    std::fwrite(content.data(), 1, content.size(), f);
    std::fclose(f);
  }
  ~TempFile() { std::remove(path_.c_str()); }
  const std::string& path() const { return path_; }

 private:
  std::string path_;
};

// Open a `TempFile` and return its `fd` (caller must fclose after he is done
// reading).
static FILE* openFile(const TempFile& tmp) {
  FILE* f = std::fopen(tmp.path().c_str(), "rb");
  EXPECT_NE(f, nullptr);
  return f;
}

// Typed test fixture: runs all tests against both `IoUringManager` and
// `SyncIoManager` when io_uring is present. Runs tests only against
// `SyncIoManager` when io_uring is present. Runs tests only against
// `SyncIoManager` when io_uring is not present.
template <typename T>
class IoUringManagerTest : public ::testing::Test {};

#ifdef QLEVER_HAS_IO_URING
using ManagerTypes =
    ::testing::Types<ad_utility::IoUringManager, ad_utility::SyncIoManager>;
#else
using ManagerTypes = ::testing::Types<ad_utility::SyncIoManager>;
#endif

TYPED_TEST_SUITE(IoUringManagerTest, ManagerTypes);

// The basic happy path: a single batch of reads is submitted and waited on.
// Each read result lands with the correct bytes in its own target buffer.
// The non-sequential offsets in `fileOffsets` (8, 0, 12) also cover
// order-independence of reads within a batch.
TYPED_TEST(IoUringManagerTest, SingleBatch) {
  std::string fileContent = "AAAABBBBCCCCDDDD";
  TempFile tmp(fileContent);
  FILE* file = openFile(tmp);
  int fd = fileno(file);

  std::vector<size_t> numBytesToRead{4, 4, 4};
  std::vector<uint64_t> fileOffsets{8, 0, 12};
  std::vector<char> targetBuffersForBatch0(4);
  std::vector<char> targetBuffersForBatch1(4);
  std::vector<char> targetBuffersForBatch2(4);
  std::vector<char*> ptrsToTargetBuffers{targetBuffersForBatch0.data(),
                                         targetBuffersForBatch1.data(),
                                         targetBuffersForBatch2.data()};

  TypeParam IOManager(64);
  auto batchHandle =
      IOManager.addBatch(fd, numBytesToRead, fileOffsets, ptrsToTargetBuffers);
  IOManager.wait(batchHandle);
  std::fclose(file);

  EXPECT_EQ(std::string(targetBuffersForBatch0.data(), 4), "CCCC");
  EXPECT_EQ(std::string(targetBuffersForBatch1.data(), 4), "AAAA");
  EXPECT_EQ(std::string(targetBuffersForBatch2.data(), 4), "DDDD");
}

// Edge case: a batch with no reads is valid and a no-op. `addBatch()` with
// empty spans returns a handle that `wait()` must accept without blocking or
// throwing. Note the file descriptor is `-1`: with zero read requests the file
// descriptor is never touched, so an empty batch
TYPED_TEST(IoUringManagerTest, EmptyBatch) {
  TypeParam IoManager(64);
  auto batchHandle = IoManager.addBatch(-1, {}, {}, {});
  // Should not block or throw.
  IoManager.wait(batchHandle);
}

// MultipleBatchesSequential: 3 batches submitted and waited in order.
TYPED_TEST(IoUringManagerTest, MultipleBatchesSequential) {
  std::string content = "AAAABBBBCCCCDDDDEEEEFFFFGGGG";
  TempFile tmp(content);
  FILE* f = openFile(tmp);
  int fd = fileno(f);

  TypeParam IOManager(64);

  auto makeAndWait = [&](uint64_t fileOffset, size_t numBytesToRead,
                         const std::string& expected) {
    std::vector<char> targetBuffer(numBytesToRead);
    std::vector<size_t> sizes{numBytesToRead};
    std::vector<uint64_t> fileOffsets{fileOffset};
    std::vector<char*> bufferPointers{targetBuffer.data()};
    auto batchHandle =
        IOManager.addBatch(fd, sizes, fileOffsets, bufferPointers);
    IOManager.wait(batchHandle);
    EXPECT_EQ(std::string(targetBuffer.data(), numBytesToRead), expected);
  };

  makeAndWait(0, 4, "AAAA");
  makeAndWait(4, 4, "BBBB");
  makeAndWait(8, 4, "CCCC");

  std::fclose(f);
}

// Verify that batch handles can be waited on out of submission order: batches A
// and B are submitted, then their batch handles are `wait()`ed in reverse
// (batch B's handle before batch A's handle). Each batch handle must still
// resolve its own batch's read correctly, so the wait order is independent of
// the submission order.
TYPED_TEST(IoUringManagerTest, WaitOutOfOrder) {
  std::string content = "AAAABBBB";
  TempFile tmp(content);
  FILE* file = openFile(tmp);
  int fd = fileno(file);

  TypeParam IOManager(64);

  std::vector<char> targetBuffersA(4);
  std::vector<char> targetBuffersB(4);
  std::vector<size_t> numBytesToReadA{4};
  std::vector<size_t> numBytesToReadB{4};
  std::vector<uint64_t> fileOffsetsA{0};
  std::vector<uint64_t> fileOffsetsB{4};
  std::vector<char*> ptrsToTargetBuffersA{targetBuffersA.data()};
  std::vector<char*> ptrsToTargetBuffersB{targetBuffersB.data()};

  auto batchHandleA = IOManager.addBatch(fd, numBytesToReadA, fileOffsetsA,
                                         ptrsToTargetBuffersA);
  auto batchHandleB = IOManager.addBatch(fd, numBytesToReadB, fileOffsetsB,
                                         ptrsToTargetBuffersB);

  IOManager.wait(batchHandleB);
  IOManager.wait(batchHandleA);
  std::fclose(file);

  EXPECT_EQ(std::string(targetBuffersA.data(), 4), "AAAA");
  EXPECT_EQ(std::string(targetBuffersB.data(), 4), "BBBB");
}

// A single `addBatch` call requesting more reads (400) than the submission ring
// buffer can hold at once (64) forces the manager to submit the SQEs in
// successive rounds: it fills the ring buffer with as many SQEs as fit, reaps
// their CQEs, then refills with the next round of until all 400 reads complete.
// Verify every read still lands in the correct buffer. Each 4-byte chunk holds
// a distinct repeated character, so a misrouted read is detected as a mismatch.
TYPED_TEST(IoUringManagerTest, BatchLargerThanRing) {
  constexpr size_t N = 400;
  constexpr size_t CHUNK = 4;

  // Builds a file of N 4-byte chunks. Chunk `i` stores the number `i`. Because
  // every chunk's content equals it's position, a read that lands in the wrong
  // target buffer yields the wrong number as a result and is detected.
  std::string content(N * CHUNK, '\0');
  for (size_t i = 0; i < N; ++i) {
    char c = static_cast<char>('A' + (i % 26));
    std::memset(&content[i * CHUNK], c, CHUNK);
  }
  TempFile tmp(content);
  FILE* f = openFile(tmp);
  int fd = fileno(f);

  std::vector<size_t> numBytesToRead(N, CHUNK);
  std::vector<uint64_t> fileOffsets(N);
  std::vector<std::vector<char>> targetBuffers(N, std::vector<char>(CHUNK));
  std::vector<char*> ptrsToTargetBuffers(N);
  for (size_t i = 0; i < N; ++i) {
    fileOffsets[i] = static_cast<uint64_t>(i * CHUNK);
    ptrsToTargetBuffers[i] = targetBuffers[i].data();
  }

  TypeParam ioManager(64);
  auto batchHandle =
      ioManager.addBatch(fd, numBytesToRead, fileOffsets, ptrsToTargetBuffers);
  ioManager.wait(batchHandle);
  std::fclose(f);

  for (size_t i = 0; i < N; ++i) {
    char expected = static_cast<char>('A' + (i % 26));
    for (size_t j = 0; j < CHUNK; ++j) {
      ASSERT_EQ(targetBuffers[i][j], expected) << "mismatch at chunk " << i;
    }
  }
}

// Verify that many independent `addBatch` calls can be outstanding (submitted
// to the kernel but not yet waited on) at once, and that the manager tracks
// each batch's completion correctly. M batches of one read each are submitted
// before any `wait`. Since the kernel posts completions in arbitrary order,
// the manager must map each one back to its issuing batch.
TYPED_TEST(IoUringManagerTest, MultipleSmallBatchesPipelined) {
  // A file of M 4-byte chunks; chunk i is filled with the character 'A'+i.
  // Since M <= 26, the chunks have distinct contents. Thus, a read whose data
  // lands in the wrong buffer can be detected as a mismatch below.
  constexpr size_t M = 20;
  std::string content(M * 4, '\0');
  for (size_t i = 0; i < M; ++i) {
    std::memset(&content[i * 4], static_cast<char>('A' + i), 4);
  }
  TempFile tmp(content);
  FILE* file = openFile(tmp);
  int fd = fileno(file);

  TypeParam IOManager(64);

  // One target buffer and one handle per batch, so every batch's result and
  // completion can be checked independently.
  std::vector<std::vector<char>> targetBuffersPerBatch(M, std::vector<char>(4));
  std::vector<typename TypeParam::BatchHandle> batchHandles(M);

  // Submit all M batches (one read each) before waiting on any of them, so they
  // are outstanding concurrently. Batch i reads chunk i (offset i*4) into
  // targetBuffersPerBatch[i].
  for (size_t i = 0; i < M; ++i) {
    std::vector<size_t> numBytesToRead{4};
    std::vector<uint64_t> fileOffsets{static_cast<uint64_t>(i * 4)};
    std::vector<char*> ptrsToTargetBuffers{targetBuffersPerBatch[i].data()};
    batchHandles[i] = IOManager.addBatch(fd, numBytesToRead, fileOffsets,
                                         ptrsToTargetBuffers);
  }
  // Only now wait on each handle. Each wait must block until that batch's own
  // read has completed, regardless of the order the kernel posted completions.
  for (size_t i = 0; i < M; ++i) {
    IOManager.wait(batchHandles[i]);
  }
  std::fclose(file);

  // Each batch's read must have landed in its own buffer:
  // targetBuffersPerBatch[i] holds 'A'+i. A completion routed to the wrong
  // buffer shows up as a mismatch here.
  for (size_t i = 0; i < M; ++i) {
    char expected = static_cast<char>('A' + i);
    EXPECT_EQ(targetBuffersPerBatch[i][0], expected)
        << "mismatch at batch " << i;
  }
}

// Reading from an invalid fd (-1) must throw std::runtime_error.
// `SyncIoManager` throws in `addBatch` (it reads immediately); `IoUringManager`
// throws in wait (the error surfaces as a completion). Both calls sit in one
// EXPECT_THROW block so the test passes regardless of which one throws.
TYPED_TEST(IoUringManagerTest, InvalidFdThrows) {
  TypeParam IOManager(64);
  std::vector<char> targetBuffers(4);
  std::vector<size_t> numBytesToRead{4};
  std::vector<uint64_t> fileOffsets{0};
  std::vector<char*> ptrsToTargetBuffers{targetBuffers.data()};
  EXPECT_THROW(
      {
        auto batchHandle = IOManager.addBatch(-1, numBytesToRead, fileOffsets,
                                              ptrsToTargetBuffers);
        IOManager.wait(batchHandle);
      },
      std::runtime_error);
}

// Request more bytes than the file contains, i.e. read past EOF. A read that
// cannot be fully satisfied is a short read, which both managers must report as
// an error (`std::runtime_error`). `SyncIoManager` throws in `addBatch`,
// `IoUringManager` in `wait`, so both calls sit in one `EXPECT_THROW` block.
TYPED_TEST(IoUringManagerTest, ReadPastEofThrows) {
  std::string content = "AAAABBBB";  // 8 bytes
  TempFile tmp(content);
  FILE* file = openFile(tmp);
  int fd = fileno(file);

  TypeParam ioManager(64);
  std::vector<char> targetBuffer(16, '\0');
  std::vector<size_t> numBytesToRead{16};  // request more than the 8 available
  std::vector<uint64_t> fileOffsets{0};
  std::vector<char*> ptrsToTargetBuffers{targetBuffer.data()};

  EXPECT_THROW(
      {
        auto batchHandle = ioManager.addBatch(fd, numBytesToRead, fileOffsets,
                                              ptrsToTargetBuffers);
        ioManager.wait(batchHandle);
      },
      std::runtime_error);
  std::fclose(file);
}

// TODO<ms2144>: add test cases that test "larger than trivial reads". All
// reads tested currently are 4 bytes. A multi-KB read (bigger than a page)
// (TODO: what is a page in this context?) would give real confidence (TODO:
// why?) that offsets/length are handled at scale, and is the realistic
// vocabulary lookup size (TODO: is that true?)
TYPED_TEST(IoUringManagerTest, LargeReads) {
  std::string content = "TODO";  // TODO bytes
  TempFile tmp(content);
  FILE* file = openFile(tmp);
  int fd = fileno(file);

  TypeParam ioManager(64);
  std::vector<char> targetBuffer(16, '\0');
  std::vector<size_t> numBytesToRead{9999};  // TODO
  std::vector<uint64_t> fileOffsets{0};
  std::vector<char*> ptrsToTargetBuffers{targetBuffer.data()};
}

// TODO<ms2144>: heterogeneous read sizes within a batch. No test issues reads
// where the numBytesTOread differs between the individual calls.

// TODO<ms2144>: zero-length reads interleaved with non-zero reads in one batch.

// TODO<ms2144>: wait() edge cases: wait() on an already completed and erased
// handle (double wait) -> should be a no-op, wait() on a never issued/bogus
// handle: currently a silent no-op (erase of absent key), Is that intended?

// TODO<ms2144>: destructor with un-waited, in-flight batches. Nothing tests
// dropping a manager (or never calling wait) while completions are outstanding
// ~IoUringManager only calls io_uring_queue_exit. Whether that's clean with
// pending CQEs is unverified.

}  // namespace
