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
#include <initializer_list>
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

using namespace ::testing;

// Writes `content` to a temporary file (named after the currently running
// gtest, so different test cases never collide) and keeps it open for reading.
// `fd()` exposes the file descriptor; the file is removed from disk on
// destruction. Use `makeTempFile` below to get the file and its fd in one step.
class TempFile {
 public:
  explicit TempFile(std::string_view content) {
    auto path = absl::StrCat(gtestCurrentTestName(), ".tmp");
    ad_utility::File{path, "wb"}.write(content.data(), content.size());
  }
  // Movable (so a factory can return it by value). The moved-from object's
  // `path_` is cleared so that only the live object removes the file.
  TempFile(TempFile&& other) noexcept : readFile_(std::move(other.readFile_)) {}
  ~TempFile() {}
  int fd() const { return readFile_.fd(); }

 private:
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

  // Same, but from a brace list, e.g. `batch.add({{8, 4}, {0, 4}, {12, 4}})`.
  void add(std::initializer_list<std::pair<uint64_t, size_t>> reads) {
    for (const auto& [offset, numBytes] : reads) {
      add(offset, numBytes);
    }
  }

  // Submit all accumulated reads to `manager` for file `fd`; returns the
  // handle.
  template <typename Manager>
  typename Manager::BatchHandle submitTo(Manager& manager, int fd) {
    // Build the buffer pointers here, after all buffers have been added, so the
    // addresses are stable (no further `add` will reallocate `targetBuffers_`).
    // `addBatch` copies each address into its read request, so this temporary
    // vector need not outlive the call.
    std::vector<char*> pointers;
    pointers.reserve(targetBuffers_.size());
    for (auto& buffer : targetBuffers_) {
      pointers.push_back(buffer.data());
    }
    return manager.addBatch(fd, numBytes_, offsets_, pointers);
  }

  // The bytes read by each read, in request order (valid once the batch has
  // completed).
  const std::vector<std::string>& result() const { return targetBuffers_; }

 private:
  std::vector<size_t> numBytes_;
  std::vector<uint64_t> offsets_;
  std::vector<std::string> targetBuffers_;
};

// Test helper that builds the file content and the matching batch of reads
// together. `addRead(bytes)` appends `bytes` as the next region of the file,
// registers a read of that region, and remembers `bytes` as the expected
// result.
class SequentialReadScenarioForTesting {
 public:
  // Append `bytes` as the next region of the file and register a read for it.
  void addRead(std::string_view bytes) {
    batch_.add(content_.size(), bytes.size());  // offset = current end of file
    expected_.emplace_back(bytes);
    content_.append(bytes);
  }

  // The full file content to pass to `makeTempFile`.
  const std::string& content() const { return content_; }

  // Submit all reads to `manager` for file `fd`; returns the handle.
  template <typename Manager>
  typename Manager::BatchHandle submitTo(Manager& manager, int fd) {
    return batch_.submitTo(manager, fd);
  }

  // The bytes actually read (valid once the batch has completed).
  const std::vector<std::string>& results() const { return batch_.result(); }

  // The bytes that were written for each read, in request order.
  const std::vector<std::string>& expected() const { return expected_; }

 private:
  std::string content_;
  std::vector<std::string> expected_;
  ReadBatchForTesting batch_;
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
  batch.add({{8, 4}, {0, 4}, {12, 4}});

  TypeParam manager(64);
  manager.wait(batch.submitTo(manager, fd));

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("CCCC", "AAAA", "DDDD"));
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
    EXPECT_THAT(batch.result(), ::testing::ElementsAre(expected));
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

  EXPECT_THAT(batchA.result(), ::testing::ElementsAre("AAAA"));
  EXPECT_THAT(batchB.result(), ::testing::ElementsAre("BBBB"));
}

// A single `addBatch` call requesting more reads (400) than the submission ring
// buffer can hold at once (64) forces the manager to submit the SQEs in
// successive rounds: it fills the ring buffer with as many SQEs as fit, reaps
// their CQEs, then refills with the next round of SQEs until all 400 reads
// complete. Verify every read still lands in the correct buffer. Each 4-byte
// chunk holds a distinct repeated character, so a misrouted read is detected as
// a mismatch.
TYPED_TEST(IoUringManagerTest, BatchLargerThanRing) {
  constexpr size_t N = 400;
  constexpr size_t CHUNKSIZE = 4;

  // Each chunk holds a distinct repeated character, so a read landing in the
  // wrong buffer is detected as a mismatch. The chunk bytes are written to the
  // file and reused as the expected result, so the pattern isn't duplicated.
  SequentialReadScenarioForTesting scenario;
  for (size_t i = 0; i < N; ++i) {
    scenario.addRead(std::string(CHUNKSIZE, static_cast<char>('A' + (i % 26))));
  }

  auto [tmp, fd] = makeTempFile(scenario.content());
  TypeParam manager(64);
  manager.wait(scenario.submitTo(manager, fd));

  EXPECT_THAT(scenario.results(),
              ::testing::ElementsAreArray(scenario.expected()));
}

// Verify that many independent `addBatch` calls can be outstanding (submitted
// to the kernel but not yet waited on) at once, and that the manager tracks
// each batch's completion correctly. M batches of one read each are submitted
// before any `wait`. Since the kernel posts completions in arbitrary order,
// the manager must map each one back to its issuing batch.
TYPED_TEST(IoUringManagerTest, MultipleSmallBatchesPipelined) {
  // M 4-byte chunks; chunk i is filled with the character 'A'+i. Since M <= 26
  // the chunks have distinct contents, so a read whose data lands in the wrong
  // buffer is detected as a mismatch below.
  constexpr size_t M = 20;
  std::string fileContent;
  std::vector<std::string> expected;
  std::vector<ReadBatchForTesting> batches(M);
  for (size_t i = 0; i < M; ++i) {
    std::string chunk(4, static_cast<char>('A' + i));
    batches[i].add(fileContent.size(), chunk.size());
    fileContent.append(chunk);
    expected.push_back(std::move(chunk));
  }
  auto [tmp, fd] = makeTempFile(fileContent);

  TypeParam manager(64);

  // Submit all M batches (one read each) before waiting on any of them, so they
  // are outstanding concurrently.
  std::vector<typename TypeParam::BatchHandle> batchHandles(M);
  for (size_t i = 0; i < M; ++i) {
    batchHandles[i] = batches[i].submitTo(manager, fd);
  }
  // Only now wait on each handle. Each wait must block until that batch's own
  // read has completed, regardless of the order the kernel posted completions.
  for (size_t i = 0; i < M; ++i) {
    manager.wait(batchHandles[i]);
  }

  // Each batch's read must have landed in its own buffer; a completion routed
  // to the wrong buffer shows up as a mismatch here.
  for (size_t i = 0; i < M; ++i) {
    EXPECT_THAT(batches[i].result(), ::testing::ElementsAre(expected[i]))
        << "mismatch at batch " << i;
  }
}

// Request more bytes than the file contains, i.e. read past EOF. A read that
// cannot be fully satisfied is a short read, which both policies must report as
// an error (`std::runtime_error`). `SyncIoPolicy` throws in `addBatch`,
// `IoUringPolicy` in `wait`.
TYPED_TEST(IoUringManagerTest, ReadPastEofThrows) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");  // 8 bytes

  TypeParam manager(64);
  ReadBatchForTesting batch;
  batch.add(0, 16);  // request more than the 8 available

  AD_EXPECT_THROW_WITH_MESSAGE(manager.wait(batch.submitTo(manager, fd)),
                               HasSubstr("read fewer bytes than requested"));
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
  AD_EXPECT_THROW_WITH_MESSAGE(
      SyncIoPolicy::readFullyOrThrow(fd, targetBuffer.data(), 16, 0),
      HasSubstr("read fewer bytes than requested"));
}

}  // namespace ad_utility
