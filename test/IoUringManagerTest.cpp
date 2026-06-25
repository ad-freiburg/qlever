// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "index/vocabulary/VocabularyTypes.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/GTestHelpers.h"
#include "util/IoUringManager.h"

namespace {

// Writes `content` to a temporary file (named after the currently running
// gtest, so different test cases never collide) and keeps it open for reading.
// `fd()` exposes the file descriptor; the file is removed from disk on
// destruction. Use `makeTempFile` below to get the file and its fd in one step.
class TempFile {
 public:
  explicit TempFile(std::string_view content)
      : path_(absl::StrCat(gtestCurrentTestName(), ".tmp")) {
    ad_utility::File{path_, "wb"}.write(content.data(), content.size());
    readFile_ = ad_utility::File{path_, "rb"};
  }
  // Movable (so a factory can return it by value). The moved-from object's
  // `path_` is cleared so that only the live object removes the file.
  TempFile(TempFile&& other) noexcept
      : path_(std::exchange(other.path_, {})),
        readFile_(std::move(other.readFile_)) {}
  ~TempFile() {
    if (!path_.empty()) {
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
// buffers, so the parallel numBytes / offsets / buffer-pointer spans that
// `addBatch` expects are built with a single `add(offset, numBytes)` call per
// read. After the batch has completed, `result(i)` returns the bytes that read
// `i` produced.
class ReadBatchForTesting {
 public:
  // Add a read of `numBytes` bytes at `offset`; returns the read's index.
  size_t add(uint64_t offset, size_t numBytes) {
    offsets_.push_back(offset);
    numBytes_.push_back(numBytes);
    targetBuffers_.emplace_back(numBytes);
    pointers_.push_back(targetBuffers_.back().data());
    return targetBuffers_.size() - 1;
  }

  // Submit all accumulated reads to `manager` for file `fd`; returns the
  // handle.
  template <typename Manager>
  typename Manager::BatchHandle submitTo(Manager& manager, int fd) {
    return manager.addBatch(fd, numBytes_, offsets_, pointers_);
  }

  // The bytes read by read `i` (valid once the batch has completed).
  std::string_view result(size_t i) const {
    return {targetBuffers_.at(i).data(), targetBuffers_.at(i).size()};
  }

 private:
  std::vector<size_t> numBytes_;
  std::vector<uint64_t> offsets_;
  std::vector<std::vector<char>> targetBuffers_;
  std::vector<char*> pointers_;
};

// Typed test fixture: each `TypeParam` is a `BatchManager` instantiated with a
// concrete I/O policy. When io_uring is present the tests run against both the
// `IoUringPolicy` and the `SyncIoPolicy` backends. If io_uring is not present,
// the tests run against `SyncIoPolicy` only.
template <typename T>
class IoUringManagerTest : public ::testing::Test {};

#ifdef QLEVER_HAS_IO_URING
using ManagerTypes =
    ::testing::Types<ad_utility::BatchManager<ad_utility::IoUringPolicy>,
                     ad_utility::BatchManager<ad_utility::SyncIoPolicy>>;
#else
using ManagerTypes =
    ::testing::Types<ad_utility::BatchManager<ad_utility::SyncIoPolicy>>;
#endif

TYPED_TEST_SUITE(IoUringManagerTest, ManagerTypes);

// The basic happy path: a single batch of reads is submitted and waited on.
// Each read result lands with the correct bytes in its own target buffer.
// The non-sequential offsets in `fileOffsets` (8, 0, 12) also cover
// order-independence of reads within a batch.
TYPED_TEST(IoUringManagerTest, SingleBatch) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  ReadBatchForTesting batch;
  batch.add(8, 4);
  batch.add(0, 4);
  batch.add(12, 4);

  TypeParam manager(64);
  manager.wait(batch.submitTo(manager, fd));

  EXPECT_EQ(batch.result(0), "CCCC");
  EXPECT_EQ(batch.result(1), "AAAA");
  EXPECT_EQ(batch.result(2), "DDDD");
}

// Throw for an invalid file descriptor, irrespective of the fact that the batch
// is empty.
TYPED_TEST(IoUringManagerTest, EmptyBatch) {
  TypeParam manager(64);
  ReadBatchForTesting batch;  // no reads
  EXPECT_THROW(manager.wait(batch.submitTo(manager, -1)),
               ad_utility::Exception);
}

// MultipleBatchesSequential: 3 batches submitted and waited in order.
TYPED_TEST(IoUringManagerTest, MultipleBatchesSequential) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDDEEEEFFFFGGGG");

  TypeParam manager(64);

  auto makeAndWait = [&](uint64_t offset, size_t numBytes,
                         std::string_view expected) {
    ReadBatchForTesting batch;
    batch.add(offset, numBytes);
    manager.wait(batch.submitTo(manager, fd));
    EXPECT_EQ(batch.result(0), expected);
  };

  makeAndWait(0, 4, "AAAA");
  makeAndWait(4, 4, "BBBB");
  makeAndWait(8, 4, "CCCC");
}

// Verify that batch handles can be waited on out of submission order: batches A
// and B are submitted, then their batch handles are `wait()`ed in reverse
// (batch B's handle before batch A's handle). Each batch handle must still
// resolve its own batch's read correctly, so the wait order is independent of
// the submission order.
TYPED_TEST(IoUringManagerTest, WaitOutOfOrder) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");

  TypeParam manager(64);

  ReadBatchForTesting batchA;
  batchA.add(0, 4);
  ReadBatchForTesting batchB;
  batchB.add(4, 4);

  auto handleA = batchA.submitTo(manager, fd);
  auto handleB = batchB.submitTo(manager, fd);

  manager.wait(handleB);
  manager.wait(handleA);

  EXPECT_EQ(batchA.result(0), "AAAA");
  EXPECT_EQ(batchB.result(0), "BBBB");
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
  auto [tmp, fd] = makeTempFile(content);

  ReadBatchForTesting batch;
  for (size_t i = 0; i < N; ++i) {
    batch.add(static_cast<uint64_t>(i * CHUNK), CHUNK);
  }

  TypeParam manager(64);
  manager.wait(batch.submitTo(manager, fd));

  for (size_t i = 0; i < N; ++i) {
    std::string expected(CHUNK, static_cast<char>('A' + (i % 26)));
    ASSERT_EQ(batch.result(i), expected) << "mismatch at chunk " << i;
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
  auto [tmp, fd] = makeTempFile(content);

  TypeParam manager(64);

  // One batch (with one read each) and one handle per batch, so every batch's
  // result and completion can be checked independently.
  std::vector<ReadBatchForTesting> batches(M);
  std::vector<typename TypeParam::BatchHandle> batchHandles(M);

  // Submit all M batches (one read each) before waiting on any of them, so they
  // are outstanding concurrently. Batch i reads chunk i (offset i*4).
  for (size_t i = 0; i < M; ++i) {
    batches[i].add(static_cast<uint64_t>(i * 4), 4);
    batchHandles[i] = batches[i].submitTo(manager, fd);
  }
  // Only now wait on each handle. Each wait must block until that batch's own
  // read has completed, regardless of the order the kernel posted completions.
  for (size_t i = 0; i < M; ++i) {
    manager.wait(batchHandles[i]);
  }

  // Each batch's read must have landed in its own buffer: batch i holds 'A'+i.
  // A completion routed to the wrong buffer shows up as a mismatch here.
  for (size_t i = 0; i < M; ++i) {
    std::string expected(4, static_cast<char>('A' + i));
    EXPECT_EQ(batches[i].result(0), expected) << "mismatch at batch " << i;
  }
}

// Reading from an invalid fd (-1) must throw std::runtime_error.
// `SyncIoPolicy` throws in `addBatch` (it reads immediately); `IoUringPolicy`
// throws in `wait` (the error surfaces as a completion). Both calls sit in one
// EXPECT_THROW block so the test passes regardless of which one throws.
TYPED_TEST(IoUringManagerTest, InvalidFdThrows) {
  TypeParam manager(64);
  ReadBatchForTesting batch;
  batch.add(0, 4);
  EXPECT_THROW(manager.wait(batch.submitTo(manager, -1)), std::runtime_error);
}

// Request more bytes than the file contains, i.e. read past EOF. A read that
// cannot be fully satisfied is a short read, which both policies must report as
// an error (`std::runtime_error`). `SyncIoPolicy` throws in `addBatch`,
// `IoUringPolicy` in `wait`, so both calls sit in one `EXPECT_THROW` block.
TYPED_TEST(IoUringManagerTest, ReadPastEofThrows) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");  // 8 bytes

  TypeParam manager(64);
  ReadBatchForTesting batch;
  batch.add(0, 16);  // request more than the 8 available

  EXPECT_THROW(manager.wait(batch.submitTo(manager, fd)),
               ad_utility::Exception);
}

// TODO: fix
/*
TYPED_TEST(IoUringManagerTest, LargeReads) {
  std::string content = "TODO";  // TODO bytes
  auto [tmp, fd] = makeTempFile(content);

  TypeParam ioManager(64);
  ReadBatchForTesting batch;
  batch.add(0, 9999)
  std::vector<char> targetBuffer(16, '\0');
  std::vector<size_t> numBytesToRead{9999};  // TODO
  std::vector<uint64_t> fileOffsets{0};
  std::vector<char*> ptrsToTargetBuffers{targetBuffer.data()};
}
*/

// TODO<ms2144>: heterogeneous read sizes within a batch. No test issues reads
// where the numBytesTOread differs between the individual calls.

// TODO<ms2144>: zero-length reads interleaved with non-zero reads in one batch.

// TODO<ms2144>: wait() edge cases: wait() on an already completed and erased
// handle (double wait) -> should be a no-op, wait() on a never issued/bogus
// handle: currently a silent no-op (erase of absent key), Is that intended?

// TODO<ms2144>: destructor with un-waited, in-flight batches. Nothing tests
// dropping a manager (or never calling wait) while completions are outstanding.
// ~IoUringPolicy only calls io_uring_queue_exit. Whether that's clean with
// pending CQEs is unverified.

// `asResult` exposes the span over the filled views, and the returned aliasing
// shared_ptr keeps the backing buffer/views alive after the original owning
// shared_ptr is dropped (the whole point of the aliasing shared_ptr).
TEST(LookupDataCommonBase, AsResultExposesViewsAndKeepsDataAlive) {
  auto data = std::make_shared<VocabBatchLookupData>();
  data->buffer() = {'f', 'o', 'o', 'b', 'a', 'r'};
  data->views().emplace_back(data->buffer().data(), 3);      // "foo"
  data->views().emplace_back(data->buffer().data() + 3, 3);  // "bar"

  VocabBatchLookupResult result = VocabBatchLookupData::asResult(data);

  ASSERT_EQ(result->size(), 2u);
  EXPECT_EQ((*result)[0], "foo");
  EXPECT_EQ((*result)[1], "bar");

  // Drop our reference; the aliasing shared_ptr must keep the data alive.
  data.reset();
  EXPECT_EQ((*result)[0], "foo");
  EXPECT_EQ((*result)[1], "bar");
}

// An empty lookup result is valid: no views, empty span.
TEST(LookupDataCommonBase, AsResultEmpty) {
  auto data = std::make_shared<VocabBatchLookupData>();
  VocabBatchLookupResult result = VocabBatchLookupData::asResult(data);
  EXPECT_TRUE(result->empty());
}

// Tests for `PmrVocabBatchLookupData`: the `monotonic_buffer_resource` backing
// used when words are produced incrementally with sizes not known up front
// (e.g. decompressing one word at a time in `CompressedVocabulary`). Each word
// gets a pointer-stable allocation, so appending a later (differently sized)
// word never invalidates an earlier `string_view`, unlike the single growing
// buffer of `VocabBatchLookupData`, which would reallocate and leave the
// already-recorded views dangling.
TEST(LookupDataCommonBase, PmrAsResultPointerStableAcrossAppends) {
  auto data = std::make_shared<PmrVocabBatchLookupData>();
  data->buffer() = std::make_unique<ql::pmr::monotonic_buffer_resource>();
  auto* resource = data->buffer().get();

  // Allocate each word separately from the monotonic resource and record a view
  // into it. Because the allocations are pointer-stable, the first view stays
  // valid after the second word is appended.
  auto appendWord = [&](std::string_view word) {
    char* p = static_cast<char*>(resource->allocate(word.size()));
    std::memcpy(p, word.data(), word.size());
    data->views().emplace_back(p, word.size());
  };
  appendWord("foo");
  std::string_view firstView = data->views().front();
  appendWord("barbaz");
  // Appending the second word did not invalidate the first view.
  EXPECT_EQ(firstView, "foo");

  VocabBatchLookupResult result = PmrVocabBatchLookupData::asResult(data);
  ASSERT_EQ(result->size(), 2u);
  EXPECT_EQ((*result)[0], "foo");
  EXPECT_EQ((*result)[1], "barbaz");

  // The aliasing shared_ptr keeps the resource (and thus its allocations)
  // alive.
  data.reset();
  EXPECT_EQ((*result)[0], "foo");
  EXPECT_EQ((*result)[1], "barbaz");
}

// An empty pmr lookup result is valid: no views, empty span (matches the
// `VocabBatchLookupData` `AsResultEmpty` case).
TEST(LookupDataCommonBase, PmrAsResultEmpty) {
  auto data = std::make_shared<PmrVocabBatchLookupData>();
  data->buffer() = std::make_unique<ql::pmr::monotonic_buffer_resource>();
  VocabBatchLookupResult result = PmrVocabBatchLookupData::asResult(data);
  EXPECT_TRUE(result->empty());
}

}  // namespace

namespace ad_utility {

// A read that is fully satisfied returns the requested bytes from the requested
// offset.
TEST(ReadFullyOrThrow, FullReadSucceeds) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");
  std::vector<char> targetBuffer(4);
  SyncIoPolicy::readFullyOrThrow(fd, targetBuffer.data(), 4, 4);
  EXPECT_EQ(std::string(targetBuffer.data(), 4), "BBBB");
}

// Requesting more bytes than the file contains (read past EOF) is a short read
// and must throw.
TEST(ReadFullyOrThrow, ShortReadThrows) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");  // 8 bytes
  std::vector<char> targetBuffer(16);
  EXPECT_THROW(SyncIoPolicy::readFullyOrThrow(fd, targetBuffer.data(), 16, 0),
               ad_utility::Exception);
}

// Reading from an invalid file descriptor must throw.
TEST(ReadFullyOrThrow, InvalidFdThrows) {
  std::vector<char> targetBuffer(4);
  EXPECT_THROW(SyncIoPolicy::readFullyOrThrow(-1, targetBuffer.data(), 4, 0),
               ad_utility::Exception);
}

}  // namespace ad_utility
