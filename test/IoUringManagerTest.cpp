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
#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "index/vocabulary/VocabularyTypes.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/GTestHelpers.h"
#include "util/IoUringManager.h"
#include "util/Log.h"

namespace {

using namespace ::testing;

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
    // Build the buffer pointers here, after all buffers have been added, so the
    // addresses are stable (no further `add` will reallocate `targetBuffers_`).
    // `addBatch` copies each address into its read request, so this temporary
    // vector need not outlive the call.
    std::vector<char*> pointers = ::ranges::to_vector(
        targetBuffers_ | ql::views::transform([](std::string& buffer) {
          return buffer.data();
        }));
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
      HasSubstr("spans should have same length"));
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

// A read that is fully satisfied returns the requested bytes from the requested
// offset.
TEST(ReadFullyOrThrow, FullReadSucceeds) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");
  std::vector<char> targetBuffer(4);
  ad_utility::SyncIoPolicy::readFullyOrThrow(fd, targetBuffer.data(), 4, 4);
  EXPECT_EQ(std::string(targetBuffer.data(), 4), "BBBB");
}

// An invalid file descriptor makes the underlying `pread` call fail (return
// -1), which must cause a throw.
TEST(ReadFullyOrThrow, PreadFailureThrows) {
  std::vector<char> targetBuffer(4);
  constexpr int invalidFd = -1;
  AD_EXPECT_THROW_WITH_MESSAGE(ad_utility::SyncIoPolicy::readFullyOrThrow(
                                   invalidFd, targetBuffer.data(), 4, 0),
                               HasSubstr("pread failed"));
}

// Requesting more bytes than the file contains (read past EOF) is a short read
// and must throw.
TEST(ReadFullyOrThrow, ShortReadThrows) {
  auto [tmp, fd] = makeTempFile("AAAABBBB");  // 8 bytes
  std::vector<char> targetBuffer(16);
  AD_EXPECT_THROW_WITH_MESSAGE(ad_utility::SyncIoPolicy::readFullyOrThrow(
                                   fd, targetBuffer.data(), 16, 0),
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
// by the time the manager is destroyed nothing is in flight, the destructor has
// nothing to drain, and it logs no warning. This is the counterpart to the
// io_uring-specific `dropRunningManager` test below.
TEST(IoUringManagerDrop, dropSyncManagerHasNothingInFlight) {
  using Manager = ad_utility::BatchManager<ad_utility::SyncIoPolicy>;
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  // Capture the log so we can assert that the destructor stays silent.
  std::ostringstream logStream;
  auto restoreLog = setGlobalLoggingStreamForTesting(&logStream);

  ReadBatchForTesting batch;
  batch.add({{8, 4}, {0, 4}, {12, 4}});
  {
    Manager manager(64);
    batch.submitTo(manager, fd);  // reads happen synchronously here
    // `manager` is destroyed here; nothing is in flight, so no warning.
  }

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("CCCC", "AAAA", "DDDD"));
  EXPECT_THAT(logStream.str(), ::testing::IsEmpty());
}

#ifdef QLEVER_HAS_IO_URING
// Drop the manager while reads are still in flight (submitted but never
// waited). `IoUringPolicy`'s destructor drains the outstanding completions
// (and logs a warning) before tearing down the ring, so the kernel is done
// writing into the target buffers. `batch` is declared before `manager`, so its
// buffers outlive the destructor's drain. After the manager is destroyed every
// read has completed, so the results are correct. This draining is specific to
// the asynchronous io_uring backend, so the test is not part of the typed
// suite.
TEST(IoUringManagerDrop, dropRunningManager) {
  using Manager = ad_utility::BatchManager<ad_utility::IoUringPolicy>;
  auto [tmp, fd] = makeTempFile("AAAABBBBCCCCDDDD");

  // Redirect the global logging stream so we can assert on the destructor's
  // warning.
  std::ostringstream logStream;
  auto restoreLog = setGlobalLoggingStreamForTesting(&logStream);

  ReadBatchForTesting batch;
  batch.add({{8, 4}, {0, 4}, {12, 4}});
  {
    Manager manager(64);
    batch.submitTo(manager, fd);  // submit, but never wait
    // `manager` is destroyed here; its destructor drains the in-flight reads.
  }

  EXPECT_THAT(batch.result(), ::testing::ElementsAre("CCCC", "AAAA", "DDDD"));
  EXPECT_THAT(logStream.str(), ::testing::HasSubstr("still in flight"));
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
}  // namespace
