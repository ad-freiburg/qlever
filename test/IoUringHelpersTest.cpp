// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.

#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "util/IoUringHelpers.h"

namespace {

// Helper: write `content` to a temporary file and return its path.
class TempFile {
 public:
  explicit TempFile(const std::string& content) : path_("IoUringHelpersTest.tmp") {
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

TEST(IoUringHelpers, ReadBatchMultipleRegions) {
  // Write known content: "AAAA BBBB CCCC DDDD" (20 bytes).
  std::string content = "AAAABBBBCCCCDDDD";
  TempFile tmp(content);

  FILE* f = std::fopen(tmp.path().c_str(), "rb");
  ASSERT_NE(f, nullptr);
  int fd = fileno(f);

  // Read three regions out of order: "CCCC" (offset 8), "AAAA" (offset 0),
  // "DDDD" (offset 12).
  std::vector<size_t> sizes{4, 4, 4};
  std::vector<uint64_t> offsets{8, 0, 12};
  std::vector<char> buf0(4), buf1(4), buf2(4);
  std::vector<char*> ptrs{buf0.data(), buf1.data(), buf2.data()};

  ad_utility::readBatch(fd, sizes, offsets, ptrs);
  std::fclose(f);

  EXPECT_EQ(std::string(buf0.data(), 4), "CCCC");
  EXPECT_EQ(std::string(buf1.data(), 4), "AAAA");
  EXPECT_EQ(std::string(buf2.data(), 4), "DDDD");
}

TEST(IoUringHelpers, ReadBatchEmpty) {
  // Empty batch should be a no-op.
  ad_utility::readBatch(-1, {}, {}, {});
}

}  // namespace
