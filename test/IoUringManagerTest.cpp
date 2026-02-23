// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.

#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "util/IoUringManager.h"

namespace {

// Helper: write `content` to a temporary file and return its path.
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

// Open a TempFile and return its fd (caller must fclose).
static FILE* openFile(const TempFile& tmp) {
  FILE* f = std::fopen(tmp.path().c_str(), "rb");
  EXPECT_NE(f, nullptr);
  return f;
}

// ---------------------------------------------------------------------------
// SingleBatch: addBatch + wait for one batch, verify data.
// ---------------------------------------------------------------------------
TEST(IoUringManager, SingleBatch) {
  std::string content = "AAAABBBBCCCCDDDD";
  TempFile tmp(content);
  FILE* f = openFile(tmp);
  int fd = fileno(f);

  std::vector<size_t> sizes{4, 4, 4};
  std::vector<uint64_t> offsets{8, 0, 12};
  std::vector<char> buf0(4), buf1(4), buf2(4);
  std::vector<char*> ptrs{buf0.data(), buf1.data(), buf2.data()};

  ad_utility::IoUringManager mgr(64);
  auto handle = mgr.addBatch(fd, sizes, offsets, ptrs);
  mgr.wait(handle);
  std::fclose(f);

  EXPECT_EQ(std::string(buf0.data(), 4), "CCCC");
  EXPECT_EQ(std::string(buf1.data(), 4), "AAAA");
  EXPECT_EQ(std::string(buf2.data(), 4), "DDDD");
}

// ---------------------------------------------------------------------------
// EmptyBatch: addBatch with 0 reads → wait is a no-op.
// ---------------------------------------------------------------------------
TEST(IoUringManager, EmptyBatch) {
  ad_utility::IoUringManager mgr(64);
  auto handle = mgr.addBatch(-1, {}, {}, {});
  // Should not block or throw.
  mgr.wait(handle);
}

// ---------------------------------------------------------------------------
// MultipleBatchesSequential: 3 batches submitted and waited in order.
// ---------------------------------------------------------------------------
TEST(IoUringManager, MultipleBatchesSequential) {
  std::string content = "AAAABBBBCCCCDDDDEEEEFFFFGGGG";
  TempFile tmp(content);
  FILE* f = openFile(tmp);
  int fd = fileno(f);

  ad_utility::IoUringManager mgr(64);

  auto makeAndWait = [&](uint64_t offset, size_t sz,
                         const std::string& expected) {
    std::vector<char> buf(sz);
    std::vector<size_t> sizes{sz};
    std::vector<uint64_t> offsets{offset};
    std::vector<char*> ptrs{buf.data()};
    auto h = mgr.addBatch(fd, sizes, offsets, ptrs);
    mgr.wait(h);
    EXPECT_EQ(std::string(buf.data(), sz), expected);
  };

  makeAndWait(0, 4, "AAAA");
  makeAndWait(4, 4, "BBBB");
  makeAndWait(8, 4, "CCCC");

  std::fclose(f);
}

// ---------------------------------------------------------------------------
// WaitOutOfOrder: submit batch A then B, wait(B) first, then wait(A).
// ---------------------------------------------------------------------------
TEST(IoUringManager, WaitOutOfOrder) {
  std::string content = "AAAABBBB";
  TempFile tmp(content);
  FILE* f = openFile(tmp);
  int fd = fileno(f);

  ad_utility::IoUringManager mgr(64);

  std::vector<char> bufA(4), bufB(4);
  std::vector<size_t> sizesA{4}, sizesB{4};
  std::vector<uint64_t> offsetsA{0}, offsetsB{4};
  std::vector<char*> ptrsA{bufA.data()}, ptrsB{bufB.data()};

  auto hA = mgr.addBatch(fd, sizesA, offsetsA, ptrsA);
  auto hB = mgr.addBatch(fd, sizesB, offsetsB, ptrsB);

  mgr.wait(hB);
  mgr.wait(hA);
  std::fclose(f);

  EXPECT_EQ(std::string(bufA.data(), 4), "AAAA");
  EXPECT_EQ(std::string(bufB.data(), 4), "BBBB");
}

// ---------------------------------------------------------------------------
// BatchLargerThanRing: batch with 400 reads, ring size 64 → drip-fed.
// ---------------------------------------------------------------------------
TEST(IoUringManager, BatchLargerThanRing) {
  constexpr size_t N = 400;
  constexpr size_t CHUNK = 4;

  // Build a file with N * CHUNK bytes, each chunk is its index repeated.
  std::string content(N * CHUNK, '\0');
  for (size_t i = 0; i < N; ++i) {
    char c = static_cast<char>('A' + (i % 26));
    std::memset(&content[i * CHUNK], c, CHUNK);
  }
  TempFile tmp(content);
  FILE* f = openFile(tmp);
  int fd = fileno(f);

  std::vector<size_t> sizes(N, CHUNK);
  std::vector<uint64_t> offsets(N);
  std::vector<std::vector<char>> bufs(N, std::vector<char>(CHUNK));
  std::vector<char*> ptrs(N);
  for (size_t i = 0; i < N; ++i) {
    offsets[i] = static_cast<uint64_t>(i * CHUNK);
    ptrs[i] = bufs[i].data();
  }

  ad_utility::IoUringManager mgr(64);
  auto h = mgr.addBatch(fd, sizes, offsets, ptrs);
  mgr.wait(h);
  std::fclose(f);

  for (size_t i = 0; i < N; ++i) {
    char expected = static_cast<char>('A' + (i % 26));
    for (size_t j = 0; j < CHUNK; ++j) {
      ASSERT_EQ(bufs[i][j], expected) << "mismatch at chunk " << i;
    }
  }
}

// ---------------------------------------------------------------------------
// MultipleSmallBatchesPipelined: submit many batches before waiting on any.
// ---------------------------------------------------------------------------
TEST(IoUringManager, MultipleSmallBatchesPipelined) {
  constexpr size_t M = 20;
  std::string content(M * 4, '\0');
  for (size_t i = 0; i < M; ++i) {
    std::memset(&content[i * 4], static_cast<char>('A' + i), 4);
  }
  TempFile tmp(content);
  FILE* f = openFile(tmp);
  int fd = fileno(f);

  ad_utility::IoUringManager mgr(64);

  std::vector<std::vector<char>> bufs(M, std::vector<char>(4));
  std::vector<ad_utility::IoUringManager::BatchHandle> handles(M);

  for (size_t i = 0; i < M; ++i) {
    std::vector<size_t> sizes{4};
    std::vector<uint64_t> offsets{static_cast<uint64_t>(i * 4)};
    std::vector<char*> ptrs{bufs[i].data()};
    handles[i] = mgr.addBatch(fd, sizes, offsets, ptrs);
  }
  for (size_t i = 0; i < M; ++i) {
    mgr.wait(handles[i]);
  }
  std::fclose(f);

  for (size_t i = 0; i < M; ++i) {
    char expected = static_cast<char>('A' + i);
    EXPECT_EQ(bufs[i][0], expected) << "mismatch at batch " << i;
  }
}

// ---------------------------------------------------------------------------
// InvalidFdThrows: addBatch with fd=-1, wait() → std::runtime_error.
// ---------------------------------------------------------------------------
TEST(IoUringManager, InvalidFdThrows) {
  ad_utility::IoUringManager mgr(64);
  std::vector<char> buf(4);
  std::vector<size_t> sizes{4};
  std::vector<uint64_t> offsets{0};
  std::vector<char*> ptrs{buf.data()};
  auto h = mgr.addBatch(-1, sizes, offsets, ptrs);
  EXPECT_THROW(mgr.wait(h), std::runtime_error);
}

}  // namespace
