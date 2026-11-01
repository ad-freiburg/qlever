// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <climits>
#include <cstdio>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "index/vocabulary/VocabularyTypes.h"
#include "util/Exception.h"
#include "util/FakeIoUring.h"
#include "util/File.h"
#include "util/GTestHelpers.h"
#include "util/IoUringManager.h"
#include "util/IoUringManagerTestHelpers.h"
#include "util/Log.h"
#include "util/PageCacheReadTestHelpers.h"
#include "util/Views.h"

namespace {

using namespace ::testing;
using namespace ioUringManagerTestHelpers;

// Test helper that builds the file content and the matching batch of reads
// together. `addRead(bytes)` appends `bytes` as the next region of the file,
// registers a read of that region, and remembers `bytes` as the expected
// result.
class SequentialReadScenarioForTesting {
 private:
  std::string content_;
  std::vector<std::string> expected_;
  ReadBatchForTesting batch_;

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

  // The bytes that we expect to be read, in request order.
  const std::vector<std::string>& expected() const { return expected_; }
};

#ifdef QLEVER_HAS_IO_URING
// Return `true` iff io_uring actually works at runtime. Even when compiled
// in, the `io_uring_setup` syscall can be blocked (for example, by the
// default seccomp profile of Docker, which also applies to the `RUN` steps
// of the CI image build that execute this test suite). The result is probed
// once and cached.
bool ioUringAvailableAtRuntime() {
  static const bool available = []() {
    bool preferIoUring = true;
    [[maybe_unused]] auto manager = ad_utility::makeBatchManager(preferIoUring);
    return preferIoUring;
  }();
  return available;
}
#endif

// Typed test fixture: each `TypeParam` is a `BatchManager` instantiated with a
// concrete I/O policy, with and without the page-cache fast path
// (`PageCacheFirstPolicy`). When io_uring is present the tests run against both
// the `IoUringPolicy` and the `SyncIoPolicy` backends. If io_uring is not
// present (at compile time, or blocked at runtime, see
// `ioUringAvailableAtRuntime`), the tests run against `SyncIoPolicy` only.
template <typename T>
class IoUringManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
#ifdef QLEVER_HAS_IO_URING
    if constexpr (std::is_same_v<
                      T, ad_utility::BatchManager<ad_utility::IoUringPolicy>> ||
                  std::is_same_v<T, ad_utility::BatchManager<
                                        ad_utility::PageCacheFirstPolicy<
                                            ad_utility::IoUringPolicy>>>) {
      if (!ioUringAvailableAtRuntime()) {
        GTEST_SKIP() << "io_uring is compiled in, but not available at "
                        "runtime (e.g. blocked by seccomp inside Docker)";
      }
    }
#endif
  }
};

#ifdef QLEVER_HAS_IO_URING
using ManagerTypes = ::testing::Types<
    ad_utility::BatchManager<ad_utility::IoUringPolicy>,
    ad_utility::BatchManager<ad_utility::SyncIoPolicy>,
    ad_utility::BatchManager<
        ad_utility::PageCacheFirstPolicy<ad_utility::IoUringPolicy>>,
    ad_utility::BatchManager<
        ad_utility::PageCacheFirstPolicy<ad_utility::SyncIoPolicy>>>;
#else
using ManagerTypes =
    ::testing::Types<ad_utility::BatchManager<ad_utility::SyncIoPolicy>,
                     ad_utility::BatchManager<ad_utility::PageCacheFirstPolicy<
                         ad_utility::SyncIoPolicy>>>;
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

// `addBatch` requires the per-request spans (byte counts, offsets, target
// buffers) to all have the same length. Passing spans of differing lengths is
// a precondition violation and must throw before any I/O is issued.
TYPED_TEST(IoUringManagerTest, mismatchedSpanLengthsThrow) {
  auto [tmp, fd] = makeTempFile("DOESNT_MATTER");

  TypeParam manager(64);

  std::string buffer(4, '\0');
  std::vector<size_t> numBytes{4};            // length 1
  std::vector<uint64_t> fileOffsets{0, 4};    // length 2 -> mismatch
  std::vector<char*> buffers{buffer.data()};  // length 1

  AD_EXPECT_THROW_WITH_MESSAGE(
      std::ignore = manager.addBatch(fd, numBytes, fileOffsets, buffers),
      HasSubstr("spans must have same length"));
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

// Batch handles can be waited on out of submission order: batches A and B
// are added one after the other (each `addBatch` completes its batch before
// returning), then their handles are `wait()`ed in reverse order. Each
// handle must still resolve its own batch's reads correctly.
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

// A batch much larger than the ring completes with every read in its own
// buffer. Ring 4 is smaller than a `REAP_WAVE`, so the ring-full wait is
// clamped to the outstanding reads; ring 8 is exactly one wave; with ring 256,
// more than 64 CQEs can be ready at once, so a wave is reaped in several
// chunks.
TYPED_TEST(IoUringManagerTest, BatchMuchLargerThanRing) {
  const std::pair<unsigned, size_t> ringSizeAndNumReads[] = {
      {4, 80}, {8, 80}, {256, 600}};
  for (auto [ringSize, numReads] : ringSizeAndNumReads) {
    SequentialReadScenarioForTesting scenario;
    for (size_t i = 0; i < numReads; ++i) {
      // Distinct chunks, so that a completion in the wrong buffer is detected.
      scenario.addRead(absl::StrFormat("%04zu", i));
    }
    auto [tmp, fd] = makeTempFile(scenario.content());
    TypeParam manager(ringSize);
    // All reads are page-cache hits; send them through the decorated policy.
    manager.setPageCacheFastPath(false);
    manager.wait(scenario.submitTo(manager, fd));
    EXPECT_THAT(scenario.results(),
                ::testing::ElementsAreArray(scenario.expected()))
        << "ring size " << ringSize;
  }
}

// Many small batches (1, 3 or 5 reads each), each completed by its own
// `addBatch` and waited on in a different order than they were added, all
// see the correct bytes. Batch sizes that are not multiples of the reap
// wave (1, 3, 5, ...) cover partial waves within a batch.
TYPED_TEST(IoUringManagerTest, ManySmallBatchesCompletedByAddBatch) {
  constexpr size_t M = 12;
  std::string fileContent;
  std::vector<std::vector<std::string>> expected(M);
  std::vector<ReadBatchForTesting> batches(M);
  size_t numChunks = 0;
  for (size_t i = 0; i < M; ++i) {
    const size_t numReads = 2 * (i % 3) + 1;
    for (size_t j = 0; j < numReads; ++j) {
      // Distinct chunks, so that a completion in the wrong buffer is detected.
      std::string chunk = absl::StrFormat("%03zu", numChunks++);
      batches[i].add(fileContent.size(), chunk.size());
      fileContent.append(chunk);
      expected[i].push_back(std::move(chunk));
    }
  }
  auto [tmp, fd] = makeTempFile(fileContent);

  TypeParam manager(64);
  // All reads are page-cache hits; send them through the decorated policy.
  manager.setPageCacheFastPath(false);
  std::vector<typename TypeParam::BatchHandle> handles;
  for (auto& batch : batches) {
    handles.push_back(batch.submitTo(manager, fd));
  }
  // Wait on the last batch first, then on the rest in submission order.
  manager.wait(handles.back());
  for (size_t i = 0; i + 1 < M; ++i) {
    manager.wait(handles[i]);
  }
  for (size_t i = 0; i < M; ++i) {
    EXPECT_THAT(batches[i].result(), ::testing::ElementsAreArray(expected[i]))
        << "mismatch at batch " << i;
  }
}

// A failed read in one batch does not affect a good batch that was added
// before it and is waited on after it.
TYPED_TEST(IoUringManagerTest, ErrorInOneBatchKeepsOtherBatchesConsistent) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCC");  // 12 bytes

  TypeParam manager(64);
  // The reads are page-cache hits; send them through the decorated policy so
  // that `good` and `bad` share the ring.
  manager.setPageCacheFastPath(false);
  ReadBatchForTesting good;
  good.add({{0, 4}, {4, 4}, {8, 4}});
  ReadBatchForTesting bad;
  bad.add(8, 16);  // past EOF: short read

  // Both policies read in `addBatch`, so the throw happens on submission.
  auto goodHandle = good.submitTo(manager, fd);
  AD_EXPECT_THROW_WITH_MESSAGE(manager.wait(bad.submitTo(manager, fd)),
                               HasSubstr("read fewer bytes than requested"));
  manager.wait(goodHandle);
  EXPECT_THAT(good.result(), ::testing::ElementsAre("AAAA", "BBBB", "CCCC"));
}

// Many independent batches of one read each, each added and only then
// waited on, each see their read in their own buffer. (Every `addBatch`
// already completes its batch; the `wait`s below are no-ops that must
// still resolve each handle to its own batch.)
TYPED_TEST(IoUringManagerTest, MultipleSmallBatches) {
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

  // Add all M batches (one read each); each completes inside `addBatch`.
  std::vector<typename TypeParam::BatchHandle> batchHandles(M);
  for (size_t i = 0; i < M; ++i) {
    batchHandles[i] = batches[i].submitTo(manager, fd);
  }
  // Wait on each handle, in submission order.
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
// an error (`std::runtime_error`). Both policies throw in `addBatch`.
TYPED_TEST(IoUringManagerTest, ReadPastEofThrows) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");  // 8 bytes

  TypeParam manager(64);
  ReadBatchForTesting batch;
  batch.add(0, 16);  // request more than the 8 available

  AD_EXPECT_THROW_WITH_MESSAGE(manager.wait(batch.submitTo(manager, fd)),
                               HasSubstr("read fewer bytes than requested"));
}

// Two reads, each larger than a memory page (4 KiB on Linux), exercise
// multi-page `pread`/SQEs -- including one at a large non-zero offset, which is
// the realistic size for reading a compressed vocabulary block. Each half holds
// a distinct non-repeating pattern (stepped by a prime so it does not align to
// a power-of-two boundary), so a truncated or misaligned read is detected, not
// just a wrong length.
TYPED_TEST(IoUringManagerTest, LargeReads) {
  constexpr size_t kHalf = 32 * 1024;  // 32 KiB, well past a 4 KiB page
  std::string first(kHalf, '\0');
  std::string second(kHalf, '\0');
  for (size_t i = 0; i < kHalf; ++i) {
    first[i] = static_cast<char>(i % 251);
    second[i] = static_cast<char>((i + 100) % 251);
  }

  SequentialReadScenarioForTesting scenario;
  scenario.addRead(first);   // read at offset 0
  scenario.addRead(second);  // read at offset 32 KiB
  auto [tmp, fd] = makeTempFile(scenario.content());

  TypeParam manager(64);
  manager.wait(scenario.submitTo(manager, fd));

  EXPECT_THAT(scenario.results(),
              ::testing::ElementsAreArray(scenario.expected()));
}

// Heterogeneous read sizes within a batch.
TYPED_TEST(IoUringManagerTest, differingReadSizes) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  ReadBatchForTesting batch;
  batch.add({{8, 1}, {0, 2}, {12, 3}});

  TypeParam manager(64);
  manager.wait(batch.submitTo(manager, fd));

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("C", "AA", "DDD"));
}

// Zero-length reads interleaved with non-zero reads in one batch.
TYPED_TEST(IoUringManagerTest, zeroLengthReadsWithNonZeroLengthReads) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  ReadBatchForTesting batch;
  batch.add({{8, 1}, {0, 0}, {12, 3}});

  TypeParam manager(64);
  manager.wait(batch.submitTo(manager, fd));

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("C", "", "DDD"));
}

// Dropping a `SyncIoPolicy`-backed manager with reads submitted but never
// waited: the synchronous policy performs all reads eagerly in `submitTo`, so
// by the time the manager is destroyed nothing is outstanding, the destructor
// has nothing to drain, and it logs no warning. This is the counterpart to the
// io_uring-specific `dropManagerWithoutWait` test below.
TEST(IoUringManagerDrop, dropSyncManagerHasNothingOutstanding) {
  using Manager = ad_utility::BatchManager<ad_utility::SyncIoPolicy>;
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  // Capture the log so we can assert that the destructor stays silent.
  auto [restoreLog, logStream] = setGlobalLoggingStreamToStringStream();

  ReadBatchForTesting batch;
  batch.add({{8, 4}, {0, 4}, {12, 4}});
  {
    Manager manager(64);
    batch.submitTo(manager, fd);  // reads happen synchronously here
    // `manager` is destroyed here; nothing is outstanding, so no warning.
  }

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("CCCC", "AAAA", "DDDD"));
  EXPECT_THAT(logStream.str(), ::testing::IsEmpty());
}

#ifdef QLEVER_HAS_IO_URING
// `addBatch` reads the whole batch, so a failed read is thrown by the
// `addBatch` of its own batch, and the next batch is not affected.
TEST(IoUringPolicyErrors, failureIsThrownByItsOwnBatch) {
  if (!ioUringAvailableAtRuntime()) {
    GTEST_SKIP() << "io_uring is compiled in, but not available at runtime "
                    "(e.g. blocked by seccomp inside Docker)";
  }
  using Manager = ad_utility::BatchManager<ad_utility::IoUringPolicy>;
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCC");  // 12 bytes
  Manager manager(64);
  ReadBatchForTesting bad;
  bad.add(8, 16);  // past EOF: short read
  AD_EXPECT_THROW_WITH_MESSAGE(bad.submitTo(manager, fd),
                               HasSubstr("read fewer bytes than requested"));
  ReadBatchForTesting good;
  good.add({{0, 4}, {4, 4}, {8, 4}});
  manager.wait(good.submitTo(manager, fd));
  EXPECT_THAT(good.result(), ::testing::ElementsAre("AAAA", "BBBB", "CCCC"));
}

// Drop the manager without waiting for its batch. `IoUringPolicy::addBatch`
// returns only once the kernel holds none of the batch's reads, so there is
// nothing left to drain: the results are complete and nothing is logged. This
// is specific to the io_uring backend, so the test is not part of the typed
// suite.
TEST(IoUringManagerDrop, dropManagerWithoutWait) {
  if (!ioUringAvailableAtRuntime()) {
    GTEST_SKIP() << "io_uring is compiled in, but not available at runtime "
                    "(e.g. blocked by seccomp inside Docker)";
  }
  using Manager = ad_utility::BatchManager<ad_utility::IoUringPolicy>;
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");
  auto [restoreLog, logStream] = setGlobalLoggingStreamToStringStream();

  ReadBatchForTesting batch;
  batch.add({{8, 4}, {0, 4}, {12, 4}});
  {
    Manager manager(64);
    batch.submitTo(manager, fd);  // never waited for
    EXPECT_THAT(batch.result(), ::testing::ElementsAre("CCCC", "AAAA", "DDDD"));
  }
  EXPECT_THAT(logStream.str(), ::testing::IsEmpty());
}

// `LiburingRing::cancelQueued` rewrites the queued SQEs in the real ring: once
// submitted, they complete as no-ops and never write into their buffers.
TEST(LiburingRing, cancelledReadsCompleteAsNoOps) {
  if (!ioUringAvailableAtRuntime()) {
    GTEST_SKIP() << "io_uring is compiled in, but not available at runtime "
                    "(e.g. blocked by seccomp inside Docker)";
  }
  using Completion = ad_utility::IoUringInterface::Completion;
  // The policy's real no-op id, so that a collision with an id that liburing
  // reserves would show up here.
  constexpr uint64_t noOp = ad_utility::IoUringPolicy::noOpRequestId();
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCC");
  auto ring = ad_utility::makeLiburingRing(8);
  constexpr size_t NUMBER_OF_READS = 3;
  std::vector<std::string> buffers(NUMBER_OF_READS, std::string(4, '-'));
  for (size_t i = 0; i < NUMBER_OF_READS; ++i) {
    ASSERT_TRUE(ring->prepareRead(fd, buffers[i].data(), 4, 4 * i, i));
  }
  EXPECT_THAT(ring->cancelQueued(noOp), ElementsAre(0, 1, 2));
  // No-ops that are cancelled again are not reported again.
  EXPECT_THAT(ring->cancelQueued(noOp), IsEmpty());
  ASSERT_EQ(ring->submit(), static_cast<int>(NUMBER_OF_READS));
  ASSERT_EQ(ring->waitForCompletions(NUMBER_OF_READS), 0);
  std::array<Completion, 8> completions{};
  ASSERT_EQ(ring->popCompletions(completions), NUMBER_OF_READS);
  // Every cancelled read completes as a no-op with result 0.
  // A vector, not a `take` view: gtest would print the view with range-v3's
  // `operator<<`, which GCC 11 fails to compile for this element type.
  const std::vector<Completion> reaped(completions.begin(),
                                       completions.begin() + NUMBER_OF_READS);
  EXPECT_THAT(reaped, ::testing::Each(::testing::AllOf(
                          AD_FIELD(Completion, requestId, ::testing::Eq(noOp)),
                          AD_FIELD(Completion, result, ::testing::Eq(0)))));
  EXPECT_THAT(buffers, Each(std::string(4, '-')));
}

// A model-based test of `IoUringPolicy` on `FakeIoUring`, which takes only
// some of the submitted SQEs, fails submits with `EINTR` and with bursts of
// `EAGAIN` (some longer than the policy retries), interrupts waits, completes
// requests in any order, and makes reads short or fail. For many random
// seeds, ring sizes and batches it checks:
// - after every `addBatch`, the ring holds no read of the batch, so the
//   caller may free the buffers;
// - `addBatch` throws iff a read of the batch was faulty or its reads were
//   cancelled, and otherwise every buffer holds the bytes of its file range.
TEST(IoUringPolicyModel, randomFaultsNeverLeaveReadsInTheRing) {
  using ad_utility::testing::FakeIoUring;
  std::string file;
  for (size_t i = 0; i < 64; ++i) {
    file += absl::StrFormat("%04zu", i);
  }
  FakeIoUring::FaultCounts total;
  for (uint32_t seed = 0; seed < 300; ++seed) {
    SCOPED_TRACE(absl::StrCat("seed ", seed));
    std::mt19937 random{seed};
    auto pick = [&random](std::initializer_list<double> values) {
      return *(values.begin() + std::uniform_int_distribution<size_t>{
                                    0, values.size() - 1}(random));
    };
    FakeIoUring::Faults faults;
    faults.partialSubmit = pick({0, 0.3, 0.9});
    faults.submitInterrupted = pick({0, 0.1});
    faults.submitOutOfMemory = pick({0, 0.05, 0.3});
    faults.maxEagainBurst = static_cast<size_t>(pick({1, 4, 40}));
    faults.waitInterrupted = pick({0, 0.1});
    faults.shortRead = pick({0, 0.01, 0.1});
    faults.readError = pick({0, 0.01});
    const auto ringSize = static_cast<unsigned>(pick({1, 2, 3, 4, 8, 16}));
    auto fakeRing = std::make_unique<FakeIoUring>(file, ringSize, faults, seed);
    FakeIoUring& fake = *fakeRing;
    ad_utility::IoUringPolicy policy{ringSize, std::move(fakeRing)};

    for (size_t batchIndex = 0; batchIndex < 10; ++batchIndex) {
      ReadBatchForTesting batch;
      const size_t numReads =
          std::uniform_int_distribution<size_t>{0, 40}(random);
      for (size_t i = 0; i < numReads; ++i) {
        const size_t numBytes =
            std::uniform_int_distribution<size_t>{1, 16}(random);
        const size_t offset = std::uniform_int_distribution<size_t>{
            0, file.size() - numBytes}(random);
        batch.add(offset, numBytes);
      }
      fake.resetObservations();
      bool threw = false;
      try {
        batch.submitToPolicy(policy, /*fd=*/-1, batchIndex);
      } catch (const std::exception& e) {
        threw = true;
        EXPECT_THAT(e.what(), HasSubstr("IoUringPolicy"));
      }
      ASSERT_EQ(fake.numReadsInRing(), 0u);
      EXPECT_EQ(threw, fake.injectedReadFault() || fake.cancelled());
      if (!threw) {
        std::vector<std::string> expected;
        expected.reserve(numReads);
        for (size_t i = 0; i < numReads; ++i) {
          expected.push_back(
              file.substr(batch.offset(i), batch.result()[i].size()));
        }
        EXPECT_THAT(batch.result(), ::testing::ElementsAreArray(expected));
      }
    }
    const auto& counts = fake.faultCounts();
    total.partialSubmits += counts.partialSubmits;
    total.interruptedSubmits += counts.interruptedSubmits;
    total.outOfMemorySubmits += counts.outOfMemorySubmits;
    total.interruptedWaits += counts.interruptedWaits;
    total.faultyReads += counts.faultyReads;
    total.cancelledReads += counts.cancelledReads;
  }
  // Every kind of fault occurred.
  EXPECT_GT(total.partialSubmits, 0u);
  EXPECT_GT(total.interruptedSubmits, 0u);
  EXPECT_GT(total.outOfMemorySubmits, 0u);
  EXPECT_GT(total.interruptedWaits, 0u);
  EXPECT_GT(total.faultyReads, 0u);
  EXPECT_GT(total.cancelledReads, 0u);
}

// `IoUringPolicy::addBatch` rejects spans of different lengths.
TEST(IoUringPolicyModel, spansOfDifferentLengthsThrow) {
  using ad_utility::testing::FakeIoUring;
  ad_utility::IoUringPolicy policy{
      8, std::make_unique<FakeIoUring>("AAAA", 8, FakeIoUring::Faults{}, 0)};
  std::vector<size_t> numBytes{4, 4};
  std::vector<uint64_t> offsets{0};
  std::string buffer(8, '-');
  std::vector<char*> buffers{buffer.data(), buffer.data() + 4};
  AD_EXPECT_THROW_WITH_MESSAGE(
      policy.addBatch(/*fd=*/-1, numBytes, offsets, buffers, 0),
      HasSubstr("offsets.size() == numBytesToRead.size()"));
}

// A failed wait, and a submit error other than `EINTR` and `EAGAIN` while
// the kernel holds reads of the batch, mean that the ring is broken while
// the kernel may hold reads. Throwing could let the caller free buffers
// that the kernel still writes into, so the process terminates. A fatal
// submit error before the kernel took any read of the batch is different:
// nothing was handed to the kernel, so the batch simply fails.
TEST(IoUringPolicyModel, aBrokenRingTerminates) {
  using ad_utility::testing::FakeIoUring;
  // Avoid warnings and crashes when running all tests at once.
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  auto readWith = [](FakeIoUring::Faults faults) {
    ad_utility::IoUringPolicy policy{
        8, std::make_unique<FakeIoUring>("AAAABBBB", 8, faults, 0)};
    ReadBatchForTesting batch;
    batch.add({{0, 4}, {4, 4}});
    batch.submitToPolicy(policy, /*fd=*/-1, 0);
  };
  FakeIoUring::Faults brokenSubmit;
  brokenSubmit.brokenSubmitErrno = EBADF;
  // The very first submit already fails, so the kernel holds no read of
  // the batch: `addBatch` throws instead of terminating.
  AD_EXPECT_THROW_WITH_MESSAGE(
      readWith(brokenSubmit),
      HasSubstr("io_uring_submit failed in IoUringPolicy"));
  FakeIoUring::Faults brokenWait;
  brokenWait.brokenWaitErrno = EBADF;
  // Keep both reads in the kernel, so that the policy has to wait.
  brokenWait.completeOnSubmit = false;
  EXPECT_DEATH_IF_SUPPORTED(readWith(brokenWait),
                            "io_uring_wait_cqes failed in IoUringPolicy");
}

// `IoUringPolicy` needs a ring size > 0 and a ring.
TEST(IoUringPolicyModel, invalidConstructionThrows) {
  using ad_utility::testing::FakeIoUring;
  auto makeFake = [] {
    return std::make_unique<FakeIoUring>("AAAA", 8, FakeIoUring::Faults{}, 0);
  };
  AD_EXPECT_THROW_WITH_MESSAGE(ad_utility::IoUringPolicy(0, makeFake()),
                               HasSubstr("ringSize_ > 0"));
  AD_EXPECT_THROW_WITH_MESSAGE(ad_utility::IoUringPolicy(8, nullptr),
                               HasSubstr("ring_ != nullptr"));
}
#endif

// Wait on `BatchHandle` for which the read call has already been reaped from
// the completion queue.
TYPED_TEST(IoUringManagerTest, waitOnNonExistingBatch) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  ReadBatchForTesting batch;
  batch.add({{8, 4}, {0, 4}, {12, 4}});

  TypeParam manager(64);
  auto realHandle = batch.submitTo(manager, fd);

  manager.wait(realHandle);
  // wait again on the same, already waited on handle.
  manager.wait(realHandle);

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("CCCC", "AAAA", "DDDD"));
}

// Wait on `BatchHandle` for which no actual batch of read calls has been
// issued.
TYPED_TEST(IoUringManagerTest, fakeHandle) {
  // create a handle for which no request has been queued.
  auto fakeHandle = 999;

  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  ReadBatchForTesting batch;
  batch.add({{8, 4}, {0, 4}, {12, 4}});

  TypeParam manager(64);
  auto realHandle = batch.submitTo(manager, fd);

  // waiting on fake handle should never block.
  manager.wait(fakeHandle);
  // Wait on the real handle so the reads actually complete before we check
  // them.
  manager.wait(realHandle);

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("CCCC", "AAAA", "DDDD"));
}

// Check that the `manager` (as returned by `makeBatchManager`, see the tests
// below) performs correct reads via the type-erased `BatchManagerBase`
// interface.
void expectManagerWorks(ad_utility::BatchManagerBase& manager) {
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCC");
  ReadBatchForTesting batch;
  batch.add({{4, 4}, {0, 4}});
  manager.wait(batch.submitTo(manager, fd));
  EXPECT_THAT(batch.result(), ::testing::ElementsAre("BBBB", "AAAA"));
}

// True if `manager` is a `BatchManager` for `Policy`, decorated with
// `PageCacheFirstPolicy` exactly where the page-cache fast path is supported.
template <typename Policy>
bool usesPolicy(ad_utility::BatchManagerBase& manager) {
  if (ad_utility::pageCacheFastPathIsSupported()) {
    return dynamic_cast<ad_utility::BatchManager<
               ad_utility::PageCacheFirstPolicy<Policy>>*>(&manager) != nullptr;
  }
  return dynamic_cast<ad_utility::BatchManager<Policy>*>(&manager) != nullptr;
}

// With `preferIoUring == false`, `makeBatchManager` must return a
// `SyncIoPolicy`-backed manager without probing io_uring, and leave the flag
// `false`.
TEST(MakeBatchManager, syncBackendWhenIoUringNotPreferred) {
  bool preferIoUring = false;
  auto manager = ad_utility::makeBatchManager(preferIoUring);
  ASSERT_NE(manager, nullptr);
  EXPECT_FALSE(preferIoUring);
  EXPECT_TRUE(usesPolicy<ad_utility::SyncIoPolicy>(*manager));
  expectManagerWorks(*manager);
}

// With `preferIoUring == true`, the backend depends on the runtime
// environment: if io_uring is compiled in and its setup succeeds, an
// `IoUringPolicy`-backed manager is returned and the flag stays `true`.
// Otherwise (io_uring not compiled in, or its setup fails at runtime, e.g.
// blocked by seccomp inside Docker), the flag is set to `false` and a
// `SyncIoPolicy`-backed manager is returned. Either way, the returned
// manager must work.
TEST(MakeBatchManager, backendMatchesFlagWhenIoUringPreferred) {
  bool preferIoUring = true;
  auto manager = ad_utility::makeBatchManager(preferIoUring);
  ASSERT_NE(manager, nullptr);
#ifdef QLEVER_HAS_IO_URING
  if (preferIoUring) {
    EXPECT_TRUE(usesPolicy<ad_utility::IoUringPolicy>(*manager));
  } else {
    EXPECT_TRUE(usesPolicy<ad_utility::SyncIoPolicy>(*manager));
  }
#else
  EXPECT_FALSE(preferIoUring);
  EXPECT_TRUE(usesPolicy<ad_utility::SyncIoPolicy>(*manager));
#endif
  expectManagerWorks(*manager);
}

// The manager from `makeBatchManager` works with the page-cache fast path
// switched on and off.
TEST(MakeBatchManager, worksWithTheFastPathOnAndOff) {
  bool preferIoUring = false;
  auto manager = ad_utility::makeBatchManager(preferIoUring);
  expectManagerWorks(*manager);
  manager->setPageCacheFastPath(false);
  expectManagerWorks(*manager);
}
}  // namespace
