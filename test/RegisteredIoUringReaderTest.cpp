// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <fcntl.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <string>
#include <type_traits>
#include <vector>

#include "./util/FileTestHelpers.h"
#include "util/Exception.h"
#include "util/RegisteredIoUringReader.h"

using namespace ad_utility::export_prototypes;

namespace {

// Deterministic content byte for offset `i`, so tests can verify reads.
char contentByte(size_t i) { return static_cast<char>((i * 31 + 7) % 256); }

// Create a temp file with `size` bytes of deterministic content. Returns the
// path together with a cleanup guard that deletes the file.
auto makeTempFileWithContent(size_t size) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  std::vector<char> content(size);
  for (size_t i = 0; i < size; ++i) {
    content[i] = contentByte(i);
  }
  int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
  AD_CORRECTNESS_CHECK(fd >= 0);
  AD_CORRECTNESS_CHECK(::write(fd, content.data(), content.size()) ==
                       static_cast<ssize_t>(content.size()));
  AD_CORRECTNESS_CHECK(::close(fd) == 0);
  return std::make_pair(std::move(path), std::move(cleanup));
}

// Create a sparse temp file of `totalSize` bytes where only
// `[filledOffset, filledOffset + filledSize)` holds deterministic content and
// the rest are holes (reads return zeros without a short read).
auto makeSparseTempFile(size_t totalSize, size_t filledOffset,
                        size_t filledSize) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
  AD_CORRECTNESS_CHECK(fd >= 0);
  AD_CORRECTNESS_CHECK(::ftruncate(fd, static_cast<off_t>(totalSize)) == 0);
  std::vector<char> content(filledSize);
  for (size_t i = 0; i < filledSize; ++i) {
    content[i] = contentByte(filledOffset + i);
  }
  AD_CORRECTNESS_CHECK(::pwrite(fd, content.data(), content.size(),
                                static_cast<off_t>(filledOffset)) ==
                       static_cast<ssize_t>(content.size()));
  AD_CORRECTNESS_CHECK(::close(fd) == 0);
  return std::make_pair(std::move(path), std::move(cleanup));
}

// Check that `dest` holds the deterministic content for file range
// `[offset, offset + dest.size())`.
void expectContent(ql::span<const char> dest, size_t offset) {
  for (size_t i = 0; i < dest.size(); ++i) {
    EXPECT_EQ(dest[i], contentByte(offset + i)) << "mismatch at byte " << i;
  }
}

}  // namespace

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, BlockAlignmentHelpers) {
  EXPECT_TRUE(isBlockAligned(0));
  EXPECT_TRUE(isBlockAligned(kDirectIoBlockSize));
  EXPECT_TRUE(isBlockAligned(2 * kDirectIoBlockSize));
  EXPECT_FALSE(isBlockAligned(1));
  EXPECT_FALSE(isBlockAligned(kDirectIoBlockSize - 1));
  EXPECT_FALSE(isBlockAligned(kDirectIoBlockSize + 1));
  EXPECT_FALSE(isBlockAligned(5000));

  PinnedArena arena{1};
  EXPECT_TRUE(isPointerAligned(arena.data()));
  EXPECT_FALSE(isPointerAligned(arena.data() + 1));
  EXPECT_TRUE(isPointerAligned(arena.data(), 16));
  EXPECT_TRUE(isPointerAligned(nullptr));
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, PinnedArenaDefaults) {
  PinnedArena arena;
  EXPECT_EQ(arena.numSlots(), 0u);
  EXPECT_EQ(arena.slotSize(), 0u);
  EXPECT_EQ(arena.totalBytes(), 0u);
  EXPECT_EQ(arena.data(), nullptr);
  EXPECT_TRUE(arena.iovecs().empty());
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, PinnedArenaAllocationAndAlignment) {
  PinnedArena arena{4};
  EXPECT_EQ(arena.numSlots(), 4u);
  EXPECT_EQ(arena.slotSize(), kDirectIoBlockSize);
  EXPECT_EQ(arena.totalBytes(), 4 * kDirectIoBlockSize);
  EXPECT_TRUE(isPointerAligned(arena.data()));

  // Slots are contiguous and each slot start is block aligned.
  for (size_t i = 0; i < arena.numSlots(); ++i) {
    auto slot = arena.getSlotSpan(i);
    EXPECT_EQ(slot.size(), kDirectIoBlockSize);
    EXPECT_EQ(slot.data(), arena.data() + i * kDirectIoBlockSize);
    EXPECT_TRUE(isPointerAligned(slot.data()));
  }

  // The iovecs describe exactly the arena slots.
  auto iovecs = arena.iovecs();
  EXPECT_EQ(iovecs.size(), 4u);
  for (size_t i = 0; i < iovecs.size(); ++i) {
    EXPECT_EQ(iovecs[i].iov_base, arena.data() + i * kDirectIoBlockSize);
    EXPECT_EQ(iovecs[i].iov_len, kDirectIoBlockSize);
  }

  // Non-default slot sizes (still multiples of 4KB) work as well.
  PinnedArena wide{2, 2 * kDirectIoBlockSize};
  EXPECT_EQ(wide.slotSize(), 2 * kDirectIoBlockSize);
  EXPECT_EQ(wide.totalBytes(), 4 * kDirectIoBlockSize);
  EXPECT_TRUE(isPointerAligned(wide.data()));
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, PinnedArenaSlotReadWrite) {
  PinnedArena arena{2};
  auto first = arena.getSlotSpan(0);
  auto second = arena.getSlotSpan(1);
  // Slots do not overlap.
  EXPECT_NE(first.data(), second.data());
  for (size_t i = 0; i < first.size(); ++i) {
    first[i] = contentByte(i);
    second[i] = contentByte(first.size() + i);
  }
  expectContent(arena.getSlotSpan(0), 0);
  expectContent(arena.getSlotSpan(1), first.size());

  // The const overload exposes the same memory read-only.
  const PinnedArena& constArena = arena;
  expectContent(constArena.getSlotSpan(0), 0);
  static_assert(std::is_same_v<decltype(constArena.getSlotSpan(0)),
                               ql::span<const char>>);
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, PinnedArenaContractViolations) {
  EXPECT_THROW(PinnedArena{0}, ad_utility::Exception);
  EXPECT_THROW(PinnedArena{1, 0}, ad_utility::Exception);
  EXPECT_THROW(PinnedArena{1, 100}, ad_utility::Exception);
  EXPECT_THROW(PinnedArena{1, kDirectIoBlockSize + 1}, ad_utility::Exception);

  // Slot indices run out: index `numSlots` is already invalid.
  PinnedArena arena{2};
  EXPECT_THROW(arena.getSlotSpan(2), ad_utility::Exception);
  const PinnedArena& constArena = arena;
  EXPECT_THROW(constArena.getSlotSpan(2), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, PinnedArenaMoveSemantics) {
  PinnedArena movedFrom{2};
  char* raw = movedFrom.data();
  PinnedArena movedTo{std::move(movedFrom)};
  EXPECT_EQ(movedFrom.data(), nullptr);
  EXPECT_EQ(movedFrom.numSlots(), 0u);
  EXPECT_EQ(movedTo.data(), raw);
  EXPECT_EQ(movedTo.numSlots(), 2u);
  // The moved-to arena is fully usable.
  auto slot = movedTo.getSlotSpan(1);
  slot[0] = 'x';
  EXPECT_EQ(movedTo.getSlotSpan(1)[0], 'x');

  // Move assignment also releases the previously held buffer.
  PinnedArena other{1};
  char* otherRaw = other.data();
  other = std::move(movedTo);
  EXPECT_EQ(other.data(), raw);
  EXPECT_NE(other.data(), otherRaw);
  EXPECT_EQ(movedTo.data(), nullptr);
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, BlockReadRequestValid) {
  PinnedArena arena{1};
  BlockReadRequest req{
      0, 0, 0, 0, static_cast<uint32_t>(kDirectIoBlockSize), arena.data()};
  EXPECT_EQ(req.fileIndex, 0u);
  EXPECT_EQ(req.fileOffset, 0u);
  EXPECT_EQ(req.bufferIndex, 0u);
  EXPECT_EQ(req.bufferOffset, 0u);
  EXPECT_EQ(req.numBytes, kDirectIoBlockSize);
  EXPECT_EQ(req.destination, arena.data());

  // Without the Direct I/O requirement, unaligned values are accepted.
  std::vector<char> buffer(100, 0);
  BlockReadRequest unaligned{5, 13, 2, 7, 100, buffer.data(), false};
  EXPECT_EQ(unaligned.fileIndex, 5u);
  EXPECT_EQ(unaligned.fileOffset, 13u);
  EXPECT_EQ(unaligned.numBytes, 100u);
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, BlockReadRequestContractViolations) {
  PinnedArena arena{1};
  const uint32_t numBytes = static_cast<uint32_t>(kDirectIoBlockSize);
  // Zero bytes or a null destination are rejected with or without alignment.
  EXPECT_THROW(BlockReadRequest(0, 0, 0, 0, 0, arena.data()),
               ad_utility::Exception);
  EXPECT_THROW(BlockReadRequest(0, 0, 0, 0, numBytes, nullptr),
               ad_utility::Exception);
  // Misaligned fields are rejected when Direct I/O alignment is required.
  EXPECT_THROW(BlockReadRequest(0, 1, 0, 0, numBytes, arena.data()),
               ad_utility::Exception);
  EXPECT_THROW(BlockReadRequest(0, 0, 0, 1, numBytes, arena.data()),
               ad_utility::Exception);
  EXPECT_THROW(BlockReadRequest(0, 0, 0, 0, 100, arena.data()),
               ad_utility::Exception);
  EXPECT_THROW(BlockReadRequest(0, 0, 0, 0, numBytes, arena.data() + 1),
               ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, ReaderConfigDefaults) {
  RegisteredReaderConfig config;
  EXPECT_EQ(config.ringEntries, 512u);
  EXPECT_TRUE(config.useDirectIo);
  EXPECT_TRUE(config.useRegisteredFiles);
  EXPECT_TRUE(config.useRegisteredBuffers);
  EXPECT_EQ(config.additionalFlags, 0u);
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, DirectIoFileDefaults) {
  DirectIoFile file;
  EXPECT_FALSE(file.isOpen());
  EXPECT_EQ(file.fd(), -1);
  EXPECT_EQ(file.size(), 0u);
  EXPECT_FALSE(file.isDirect());
  EXPECT_TRUE(file.path().empty());
  // Closing a never-opened file is a no-op.
  file.close();
  EXPECT_FALSE(file.isOpen());
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, DirectIoFileOpenNonexistentThrows) {
  auto [path, cleanup] = ad_utility::testing::filenameForTesting();
  EXPECT_THROW(DirectIoFile{path.string(), false}, ad_utility::Exception);
  EXPECT_THROW(DirectIoFile{path.string(), true}, ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, DirectIoFileOpenCloseAndMove) {
  auto [tmpFile, cleanup] = makeTempFileWithContent(100);
  DirectIoFile file{tmpFile.string(), false};
  EXPECT_TRUE(file.isOpen());
  EXPECT_GE(file.fd(), 0);
  EXPECT_EQ(file.size(), 100u);
  EXPECT_EQ(file.path(), tmpFile.string());
  EXPECT_FALSE(file.isDirect());

  file.close();
  EXPECT_FALSE(file.isOpen());
  EXPECT_EQ(file.fd(), -1);
  // Double close is safe.
  file.close();

  // Reopening works and picks up the new size.
  file.open(tmpFile.string(), false);
  EXPECT_TRUE(file.isOpen());
  EXPECT_EQ(file.size(), 100u);

  // The move constructor transfers ownership; the source is closed.
  DirectIoFile moved{std::move(file)};
  EXPECT_FALSE(file.isOpen());
  EXPECT_TRUE(moved.isOpen());
  EXPECT_EQ(moved.size(), 100u);
  EXPECT_EQ(moved.path(), tmpFile.string());

  // Move assignment into a live object closes the previous file first.
  auto [otherFile, otherCleanup] = makeTempFileWithContent(10);
  DirectIoFile other{otherFile.string(), false};
  int oldFd = other.fd();
  other = std::move(moved);
  EXPECT_NE(other.fd(), oldFd);
  EXPECT_FALSE(moved.isOpen());
  EXPECT_EQ(other.size(), 100u);
  EXPECT_EQ(other.path(), tmpFile.string());
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, DirectIoFileOpenReadWrite) {
  auto [tmpFile, cleanup] = makeTempFileWithContent(100);
  // Opening an existing file read-write (no `O_RDONLY`) works.
  DirectIoFile file{tmpFile.string(), false, false};
  EXPECT_TRUE(file.isOpen());
  EXPECT_EQ(file.size(), 100u);
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, DirectIoFileOpenWithDirectIoWhenSupported) {
  auto [tmpFile, cleanup] = makeTempFileWithContent(kDirectIoBlockSize);
  // `O_DIRECT` is a filesystem property: some filesystems reject it, in
  // which case there is nothing to assert about the flag.
  try {
    DirectIoFile file{tmpFile.string(), true};
    EXPECT_TRUE(file.isOpen());
    EXPECT_TRUE(file.isDirect());
  } catch (const ad_utility::Exception&) {
    GTEST_SKIP() << "filesystem does not support O_DIRECT";
  }
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, ReadSyncBuffered) {
  auto [tmpFile, cleanup] = makeTempFileWithContent(100);
  DirectIoFile file{tmpFile.string(), false};
  std::vector<char> buffer(100, 0);
  RegisteredIoUringReader::readSync(file.fd(), 0, {buffer.data(), 40}, false);
  expectContent(ql::span<const char>{buffer.data(), 40}, 0);
  // Nonzero offsets work as well.
  RegisteredIoUringReader::readSync(file.fd(), 40, {buffer.data(), 60}, false);
  expectContent(ql::span<const char>{buffer.data(), 60}, 40);
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, ReadSyncDirectAligned) {
  auto [tmpFile, cleanup] = makeTempFileWithContent(2 * kDirectIoBlockSize);
  DirectIoFile file{tmpFile.string(), false};
  PinnedArena arena{1};
  auto slot = arena.getSlotSpan(0);
  RegisteredIoUringReader::readSync(file.fd(), 0, slot, true);
  expectContent(slot, 0);
  RegisteredIoUringReader::readSync(file.fd(), kDirectIoBlockSize, slot, true);
  expectContent(slot, kDirectIoBlockSize);
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, ReadSyncErrors) {
  auto [tmpFile, cleanup] = makeTempFileWithContent(100);
  DirectIoFile file{tmpFile.string(), false};
  std::vector<char> buffer(100, 0);
  ql::span<char> dest{buffer.data(), buffer.size()};
  PinnedArena arena{1};
  auto slot = arena.getSlotSpan(0);

  // Invalid fd and empty destination.
  EXPECT_THROW(RegisteredIoUringReader::readSync(-1, 0, dest, false),
               ad_utility::Exception);
  EXPECT_THROW(
      RegisteredIoUringReader::readSync(file.fd(), 0, ql::span<char>{}, false),
      ad_utility::Exception);

  // Direct I/O alignment violations: offset, size, and pointer.
  EXPECT_THROW(RegisteredIoUringReader::readSync(file.fd(), 1, slot, true),
               ad_utility::Exception);
  ql::span<char> shortSpan{arena.data(), 100};
  EXPECT_THROW(RegisteredIoUringReader::readSync(file.fd(), 0, shortSpan, true),
               ad_utility::Exception);
  ql::span<char> unalignedSpan{arena.data() + 1, kDirectIoBlockSize};
  EXPECT_THROW(
      RegisteredIoUringReader::readSync(file.fd(), 0, unalignedSpan, true),
      ad_utility::Exception);

  // Short reads throw: at end-of-file nothing is returned, and a request
  // reaching past end-of-file is incomplete.
  EXPECT_THROW(RegisteredIoUringReader::readSync(file.fd(), 100, dest, false),
               ad_utility::Exception);
  EXPECT_THROW(RegisteredIoUringReader::readSync(file.fd(), 90, dest, false),
               ad_utility::Exception);

  // A failed `pread` (here: reading from a write-only fd) throws.
  int writeOnlyFd = ::open(tmpFile.string().c_str(), O_WRONLY);
  ASSERT_GE(writeOnlyFd, 0);
  EXPECT_THROW(RegisteredIoUringReader::readSync(writeOnlyFd, 0, dest, false),
               ad_utility::Exception);
  EXPECT_EQ(::close(writeOnlyFd), 0);
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, ReadSyncSparseHole) {
  // A hole reads as zeros and is not a short read.
  auto [tmpFile, cleanup] = makeSparseTempFile(
      2 * kDirectIoBlockSize, kDirectIoBlockSize, kDirectIoBlockSize);
  DirectIoFile file{tmpFile.string(), false};
  EXPECT_EQ(file.size(), 2 * kDirectIoBlockSize);
  std::vector<char> zeros(kDirectIoBlockSize, static_cast<char>(0xFF));
  RegisteredIoUringReader::readSync(file.fd(), 0, {zeros.data(), zeros.size()},
                                    false);
  EXPECT_TRUE(
      std::all_of(zeros.begin(), zeros.end(), [](char c) { return c == 0; }));
  // The written region next to the hole holds the expected content.
  RegisteredIoUringReader::readSync(file.fd(), kDirectIoBlockSize,
                                    {zeros.data(), zeros.size()}, false);
  expectContent(ql::span<const char>{zeros.data(), zeros.size()},
                kDirectIoBlockSize);
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, SubmitEmptyBatch) {
  RegisteredIoUringReader reader;
  EXPECT_EQ(reader.inFlightCount(), 0u);
  EXPECT_FALSE(reader.isFilesRegistered());
  EXPECT_FALSE(reader.isBuffersRegistered());
  EXPECT_EQ(reader.submitBatch({}), 0u);
  BatchResult empty = reader.waitBatch(0);
  EXPECT_EQ(empty.requestsCompleted, 0u);
  EXPECT_EQ(empty.totalBytesRead, 0u);
  EXPECT_TRUE(empty.success);
}

// Without liburing there is no ring, so waiting on an unknown batch id is a
// defined empty result. With a live ring this would block forever, hence the
// guard.
#ifndef QLEVER_HAS_LIBURING
TEST(RegisteredIoUringReader, WaitUnknownBatchIsEmpty) {
  RegisteredIoUringReader reader;
  BatchResult result = reader.waitBatch(42);
  EXPECT_EQ(result.requestsCompleted, 0u);
  EXPECT_EQ(result.totalBytesRead, 0u);
  EXPECT_TRUE(result.success);
}
#endif

// _____________________________________________________________________________
// End-to-end batch read through raw fds. This exercises the synchronous
// `pread` fallback when no ring is available and the unpinned ring path
// otherwise, with identical observable behavior.
TEST(RegisteredIoUringReader, SyncRoundtripRawFd) {
  auto [tmpFile, cleanup] = makeTempFileWithContent(200);
  DirectIoFile file{tmpFile.string(), false};

  RegisteredReaderConfig config;
  config.useDirectIo = false;
  RegisteredIoUringReader reader{config};

  std::vector<char> first(100, 0);
  std::vector<char> second(100, 0);
  // Raw fds are passed as `fileIndex` while no files are registered.
  std::vector<BlockReadRequest> requests{
      BlockReadRequest{static_cast<uint32_t>(file.fd()), 0, 0, 0, 100,
                       first.data(), false},
      BlockReadRequest{static_cast<uint32_t>(file.fd()), 100, 0, 0, 100,
                       second.data(), false}};
  auto batchId = reader.submitBatch(requests);
  EXPECT_NE(batchId, 0u);
  BatchResult result = reader.waitBatch(batchId);
  EXPECT_EQ(result.requestsCompleted, 2u);
  EXPECT_EQ(result.totalBytesRead, 200u);
  EXPECT_TRUE(result.success);
  EXPECT_EQ(reader.inFlightCount(), 0u);
  expectContent(ql::span<const char>{first.data(), first.size()}, 0);
  expectContent(ql::span<const char>{second.data(), second.size()}, 100);

  // Batches can be submitted and waited on in any order.
  std::vector<char> third(50, 0);
  std::vector<BlockReadRequest> single{BlockReadRequest{
      static_cast<uint32_t>(file.fd()), 50, 0, 0, 50, third.data(), false}};
  auto firstId = reader.submitBatch(single);
  auto secondId = reader.submitBatch(single);
  EXPECT_NE(firstId, secondId);
  BatchResult secondResult = reader.waitBatch(secondId);
  EXPECT_EQ(secondResult.requestsCompleted, 1u);
  EXPECT_EQ(secondResult.totalBytesRead, 50u);
  BatchResult firstResult = reader.waitBatch(firstId);
  EXPECT_EQ(firstResult.requestsCompleted, 1u);
  expectContent(ql::span<const char>{third.data(), third.size()}, 50);
}

// _____________________________________________________________________________
// Same as above, but with the default config (`useDirectIo == true`), so all
// requests use aligned offsets, sizes, and arena destinations.
TEST(RegisteredIoUringReader, SyncRoundtripDirectAligned) {
  auto [tmpFile, cleanup] = makeTempFileWithContent(2 * kDirectIoBlockSize);
  DirectIoFile file{tmpFile.string(), false};

  RegisteredIoUringReader reader;
  PinnedArena arena{2};
  const uint32_t numBytes = static_cast<uint32_t>(kDirectIoBlockSize);
  std::vector<BlockReadRequest> requests{
      BlockReadRequest{static_cast<uint32_t>(file.fd()), 0, 0, 0, numBytes,
                       arena.getSlotSpan(0).data()},
      BlockReadRequest{static_cast<uint32_t>(file.fd()), kDirectIoBlockSize, 0,
                       0, numBytes, arena.getSlotSpan(1).data()}};
  BatchResult result = reader.waitBatch(reader.submitBatch(requests));
  EXPECT_EQ(result.requestsCompleted, 2u);
  EXPECT_EQ(result.totalBytesRead, 2 * kDirectIoBlockSize);
  EXPECT_TRUE(result.success);
  expectContent(arena.getSlotSpan(0), 0);
  expectContent(arena.getSlotSpan(1), kDirectIoBlockSize);
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, ShortReadThrows) {
  auto [tmpFile, cleanup] = makeTempFileWithContent(100);
  DirectIoFile file{tmpFile.string(), false};

  RegisteredReaderConfig config;
  config.useDirectIo = false;
  RegisteredIoUringReader reader{config};
  std::vector<char> buffer(kDirectIoBlockSize, 0);
  std::vector<BlockReadRequest> requests{BlockReadRequest{
      static_cast<uint32_t>(file.fd()), 0, 0, 0,
      static_cast<uint32_t>(buffer.size()), buffer.data(), false}};
  // Without a ring the synchronous path throws in `submitBatch`, with a live
  // ring the completion throws in `waitBatch`; either way an exception
  // escapes.
  EXPECT_THROW(
      {
        auto batchId = reader.submitBatch(requests);
        (void)reader.waitBatch(batchId);
      },
      ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, RegisterEmptyThrows) {
  RegisteredIoUringReader reader;
  EXPECT_THROW(reader.registerFiles({}), ad_utility::Exception);
  EXPECT_THROW(reader.registerBuffers({}), ad_utility::Exception);
  EXPECT_FALSE(reader.isFilesRegistered());
  EXPECT_FALSE(reader.isBuffersRegistered());
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, UnregisterWithoutRegisterIsNoop) {
  RegisteredIoUringReader reader;
  reader.unregisterFiles();
  reader.unregisterBuffers();
  EXPECT_FALSE(reader.isFilesRegistered());
  EXPECT_FALSE(reader.isBuffersRegistered());
}

// _____________________________________________________________________________
// Roundtrip through registered files and buffers. When io_uring
// initialization failed (e.g. blocked by seccomp), `registerFiles` throws and
// there is nothing further to assert in this environment.
TEST(RegisteredIoUringReader, RegisteredFilesRoundtrip) {
  auto [tmpFile, cleanup] = makeTempFileWithContent(2 * kDirectIoBlockSize);
  DirectIoFile file{tmpFile.string(), false};

  RegisteredIoUringReader reader;
  try {
    const int fd = file.fd();
    reader.registerFiles(ql::span<const int>{&fd, 1});
  } catch (const ad_utility::Exception&) {
    // No live ring: the sync fallback below needs no registration.
    GTEST_SKIP() << "io_uring is not available, registration throws";
  }
  EXPECT_TRUE(reader.isFilesRegistered());

  PinnedArena arena{1};
  const uint32_t numBytes = static_cast<uint32_t>(kDirectIoBlockSize);
  std::vector<BlockReadRequest> requests{
      BlockReadRequest{0, 0, 0, 0, numBytes, arena.getSlotSpan(0).data()}};
  BatchResult result = reader.waitBatch(reader.submitBatch(requests));
  EXPECT_EQ(result.requestsCompleted, 1u);
  EXPECT_EQ(result.totalBytesRead, kDirectIoBlockSize);
  expectContent(arena.getSlotSpan(0), 0);

  reader.unregisterFiles();
  EXPECT_FALSE(reader.isFilesRegistered());
}

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, RegisteredBuffersRoundtrip) {
  auto [tmpFile, cleanup] = makeTempFileWithContent(kDirectIoBlockSize);
  DirectIoFile file{tmpFile.string(), false};

  RegisteredIoUringReader reader;
  PinnedArena arena{1};
  try {
    const int fd = file.fd();
    reader.registerFiles(ql::span<const int>{&fd, 1});
    reader.registerBuffers(arena.iovecs());
  } catch (const ad_utility::Exception&) {
    GTEST_SKIP() << "io_uring is not available, registration throws";
  }
  EXPECT_TRUE(reader.isFilesRegistered());
  EXPECT_TRUE(reader.isBuffersRegistered());

  // With registered buffers the destination must be the registered range.
  const uint32_t numBytes = static_cast<uint32_t>(kDirectIoBlockSize);
  std::vector<BlockReadRequest> requests{
      BlockReadRequest{0, 0, 0, 0, numBytes, arena.getSlotSpan(0).data()}};
  BatchResult result = reader.waitBatch(reader.submitBatch(requests));
  EXPECT_EQ(result.requestsCompleted, 1u);
  expectContent(arena.getSlotSpan(0), 0);

  reader.unregisterBuffers();
  EXPECT_FALSE(reader.isBuffersRegistered());
  reader.unregisterFiles();
  EXPECT_FALSE(reader.isFilesRegistered());
}

// _____________________________________________________________________________
// Registered fds are only resolved when `useRegisteredFiles` is set; with the
// flag off, `fileIndex` keeps meaning a raw fd.
TEST(RegisteredIoUringReader, RegisteredFilesIgnoredWhenFlagOff) {
  auto [tmpFile, cleanup] = makeTempFileWithContent(kDirectIoBlockSize);
  DirectIoFile file{tmpFile.string(), false};

  RegisteredReaderConfig config;
  config.useDirectIo = false;
  config.useRegisteredFiles = false;
  RegisteredIoUringReader reader{config};
  try {
    const int fd = file.fd();
    reader.registerFiles(ql::span<const int>{&fd, 1});
  } catch (const ad_utility::Exception&) {
    GTEST_SKIP() << "io_uring is not available, registration throws";
  }
  std::vector<char> buffer(kDirectIoBlockSize, 0);
  std::vector<BlockReadRequest> requests{BlockReadRequest{
      static_cast<uint32_t>(file.fd()), 0, 0, 0,
      static_cast<uint32_t>(kDirectIoBlockSize), buffer.data(), false}};
  BatchResult result = reader.waitBatch(reader.submitBatch(requests));
  EXPECT_EQ(result.requestsCompleted, 1u);
  expectContent(ql::span<const char>{buffer.data(), buffer.size()}, 0);
  reader.unregisterFiles();
}

// Only compiled with liburing: assert the error path when the ring failed to
// initialize (e.g. seccomp-blocked io_uring). Registration must throw and
// leave the reader unregistered rather than crash or half-register.
#ifdef QLEVER_HAS_LIBURING
TEST(RegisteredIoUringReader, RegisterThrowsWithoutLiveRing) {
  auto [tmpFile, cleanup] = makeTempFileWithContent(kDirectIoBlockSize);
  DirectIoFile file{tmpFile.string(), false};
  PinnedArena arena{1};

  RegisteredIoUringReader reader;
  bool registered = true;
  try {
    const int fd = file.fd();
    reader.registerFiles(ql::span<const int>{&fd, 1});
  } catch (const ad_utility::Exception&) {
    registered = false;
    EXPECT_FALSE(reader.isFilesRegistered());
  }
  if (registered) {
    EXPECT_TRUE(reader.isFilesRegistered());
    reader.unregisterFiles();
  }
  try {
    reader.registerBuffers(arena.iovecs());
  } catch (const ad_utility::Exception&) {
    registered = false;
    EXPECT_FALSE(reader.isBuffersRegistered());
  }
  if (registered) {
    EXPECT_TRUE(reader.isBuffersRegistered());
    reader.unregisterBuffers();
  }
}
#endif

// _____________________________________________________________________________
TEST(RegisteredIoUringReader, MoveSemantics) {
  RegisteredIoUringReader source;
  RegisteredIoUringReader movedTo{std::move(source)};
  // The moved-to reader is fully functional.
  EXPECT_EQ(movedTo.submitBatch({}), 0u);
  EXPECT_EQ(movedTo.inFlightCount(), 0u);

  auto [tmpFile, cleanup] = makeTempFileWithContent(100);
  DirectIoFile file{tmpFile.string(), false};
  RegisteredReaderConfig config;
  config.useDirectIo = false;
  RegisteredIoUringReader other{config};
  other = std::move(movedTo);
  std::vector<char> buffer(100, 0);
  std::vector<BlockReadRequest> requests{BlockReadRequest{
      static_cast<uint32_t>(file.fd()), 0, 0, 0, 100, buffer.data(), false}};
  BatchResult result = other.waitBatch(other.submitBatch(requests));
  EXPECT_EQ(result.requestsCompleted, 1u);
  expectContent(ql::span<const char>{buffer.data(), buffer.size()}, 0);
}
