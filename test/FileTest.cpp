// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#include <gtest/gtest.h>

#include <array>
#include <filesystem>
#include <thread>
#include <vector>

#include "backports/algorithm.h"
#include "util/File.h"
#include "util/GTestHelpers.h"

namespace ad_utility {
TEST(File, move) {
  std::string filename = "testFileMove.tmp";
  File file1(filename, "w");
  ASSERT_TRUE(file1.isOpen());
  file1.write("aaa", 3);
  EXPECT_EQ(file1.name(), "testFileMove.tmp");

  File file2;
  ASSERT_TRUE(file1.isOpen());
  ASSERT_FALSE(file2.isOpen());
  file2 = std::move(file1);
  ASSERT_FALSE(file1.isOpen());
  ASSERT_TRUE(file2.isOpen());

  file2.write("bbb", 3);
  File file3(std::move(file2));
  ASSERT_FALSE(file2.isOpen());
  ASSERT_TRUE(file3.isOpen());
  file3.write("ccc", 3);
  file3.close();

  File fileRead(filename, "r");
  ASSERT_TRUE(fileRead.isOpen());
  std::string s;
  s.resize(2);
  auto numBytes = fileRead.read(s.data(), 2);
  ASSERT_EQ(numBytes, 2u);
  ASSERT_EQ(s, "aa");

  File fileRead2;
  fileRead2 = std::move(fileRead);
  s.resize(5);
  numBytes = fileRead2.read(s.data(), 5);
  ASSERT_EQ(numBytes, 5u);
  ASSERT_EQ(s, "abbbc");

  File fileRead3{std::move(fileRead2)};
  s.resize(2);
  numBytes = fileRead3.read(s.data(), 2);
  ASSERT_EQ(numBytes, 2u);
  ASSERT_EQ(s, "cc");

  ASSERT_EQ(0u, fileRead3.read(s.data(), 9));
  ad_utility::deleteFile(filename);
}
}  // namespace ad_utility

// _____________________________________________________________________________
// Test that the positioned `write` puts the bytes at the given offset, without
// moving the file position, and that it extends the file when it writes past
// its end.
TEST(File, writeAtOffset) {
  std::string filename = "testFileWriteAtOffset.tmp";
  {
    ad_utility::File file{filename, "w+"};
    // The ranges are written out of order and leave a gap, which the file
    // system fills with zeros.
    EXPECT_EQ(file.write("world", 5, 8), 5);
    EXPECT_EQ(file.write("hello", 5, 0), 5);
    EXPECT_EQ(file.sizeOfFile(), 13);
  }
  {
    ad_utility::File file{filename, "r"};
    std::array<char, 13> buffer{};
    EXPECT_EQ(file.read(buffer.data(), buffer.size(), 0), 13);
    EXPECT_EQ(std::string(buffer.data(), 5), "hello");
    EXPECT_EQ(std::string(buffer.data() + 8, 5), "world");
    EXPECT_EQ(buffer[5], 0);
  }
  ad_utility::deleteFile(filename);
}

// Test that threads which write to ranges that do not overlap do not need any
// synchronization. This is what lets the index build write the blocks of its
// temporary files and of its permutations concurrently.
TEST(File, concurrentWritesAtDisjointOffsets) {
  std::string filename = "testFileConcurrentWrites.tmp";
  static constexpr size_t numThreads = 8;
  static constexpr size_t numBytesPerThread = 4096;
  {
    ad_utility::File file{filename, "w+"};
    std::vector<std::thread> threads;
    for (size_t threadIdx : ql::views::iota(size_t{0}, numThreads)) {
      threads.emplace_back([&file, threadIdx]() {
        std::vector<char> data(numBytesPerThread,
                               static_cast<char>('a' + threadIdx));
        auto offset = static_cast<off_t>(threadIdx * numBytesPerThread);
        EXPECT_EQ(file.write(data.data(), data.size(), offset),
                  static_cast<ssize_t>(numBytesPerThread));
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }
    EXPECT_EQ(file.sizeOfFile(), numThreads * numBytesPerThread);
  }
  // Every thread's range holds that thread's byte, so nothing was interleaved.
  ad_utility::File file{filename, "r"};
  for (size_t threadIdx : ql::views::iota(size_t{0}, numThreads)) {
    std::vector<char> buffer(numBytesPerThread);
    auto offset = static_cast<off_t>(threadIdx * numBytesPerThread);
    ASSERT_EQ(file.read(buffer.data(), buffer.size(), offset),
              static_cast<ssize_t>(numBytesPerThread));
    EXPECT_EQ(std::vector<char>(numBytesPerThread,
                                static_cast<char>('a' + threadIdx)),
              buffer)
        << "thread " << threadIdx;
  }
  file.close();
  ad_utility::deleteFile(filename);
}

TEST(File, getLastOffset) {
  ad_utility::File closedFile;
  ASSERT_FALSE(closedFile.isOpen());
  ASSERT_THROW(closedFile.getLastOffset(), ad_utility::Exception);

  // Write a few `off_t` values to a file and check that `getLastOffset`
  // returns the offset of the last value as well as the value itself.
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename]() { ad_utility::deleteFile(filename); };
  std::array<off_t, 3> offsets{42, 1337, 123456789};
  {
    ad_utility::File writer{filename, "w"};
    ASSERT_TRUE(writer.isOpen());
    writer.write(offsets.data(), offsets.size() * sizeof(off_t));
  }

  ad_utility::File reader{filename, "r"};
  ASSERT_TRUE(reader.isOpen());
  auto [lastOffsetOffset, lastOffset] = reader.getLastOffset();
  // The last `off_t` starts after the first two `off_t`s.
  ASSERT_EQ(lastOffsetOffset, 2 * sizeof(off_t));
  ASSERT_EQ(lastOffset, offsets.back());
  reader.close();
}

// _____________________________________________________________________________
TEST(File, duplicateForReadingClosedFileThrows) {
  // Duplicating a default-constructed (not open) file violates the contract.
  ad_utility::File closedFile;
  ASSERT_FALSE(closedFile.isOpen());
  AD_EXPECT_THROW_WITH_MESSAGE((void)closedFile.duplicateForReading(),
                               ::testing::HasSubstr("isOpen()"));
}

// _____________________________________________________________________________
TEST(File, duplicateForReadingIsIndependentOfFileName) {
  std::string_view testData = "0123456789";
  std::string filename = gtestCurrentTestName();
  std::string renamedFilename = filename + ".renamed";
  absl::Cleanup cleanup = [&filename, &renamedFilename]() {
    ad_utility::deleteFile(filename, false);
    ad_utility::deleteFile(renamedFilename, false);
  };
  {
    ad_utility::File writer{filename, "w"};
    ASSERT_TRUE(writer.isOpen());
    writer.write(testData.data(), testData.size());
  }

  ad_utility::File original{filename, "r"};
  ASSERT_TRUE(original.isOpen());

  // The duplicate refers to the same file and carries over the name.
  ad_utility::File duplicate = original.duplicateForReading();
  ASSERT_TRUE(duplicate.isOpen());
  EXPECT_EQ(duplicate.name(), original.name());
  EXPECT_NE(duplicate.fd(), original.fd());

  // Closing one file leaves the other fully functional (independent handles).
  original.close();
  ASSERT_FALSE(original.isOpen());
  ASSERT_TRUE(duplicate.isOpen());

  std::filesystem::rename(filename, renamedFilename);

  ad_utility::File duplicate2 = duplicate.duplicateForReading();
  ASSERT_TRUE(duplicate2.isOpen());
  std::string buffer;
  buffer.resize(testData.size());
  ASSERT_EQ(duplicate.read(buffer.data(), testData.size(), 0), testData.size());
  EXPECT_EQ(buffer, testData);
}

// _____________________________________________________________________________
TEST(File, makeFilestream) {
  std::string filename = "makeFilstreamTest.dat";
  ad_utility::makeOfstream(filename) << "helloAgain\n";
  std::string s;
  auto reader = ad_utility::makeIfstream(filename);
  ASSERT_TRUE(reader.is_open());
  ASSERT_TRUE(std::getline(reader, s));
  ASSERT_EQ("helloAgain", s);
  ASSERT_FALSE(std::getline(reader, s));

  // Throw on nonexisting file
  ASSERT_THROW(ad_utility::makeIfstream("nonExisting1620349.datxyz"),
               std::runtime_error);
}
