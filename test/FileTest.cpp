// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#include <gtest/gtest.h>

#include <array>
#include <filesystem>
#include <vector>

#include "util/File.h"
#include "util/GTestHelpers.h"
#include "util/Views.h"
#include "util/jthread.h"

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
// using or changing the file position, and that the ranges can be written in
// an arbitrary order.
TEST(File, writeAtOffset) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename]() { ad_utility::deleteFile(filename); };
  {
    ad_utility::File file{filename, "w+"};
    // The ranges are written out of order and leave a gap, which the file
    // system fills with zeros.
    file.write("world", 5, 8);
    file.write("hello", 5, 0);
    // The file position was never used nor changed by the writes above.
    EXPECT_EQ(file.tell(), 0);
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
}

// _____________________________________________________________________________
// Test that writing past the end of the file extends it, and that reading and
// writing at explicit offsets can be interleaved freely.
TEST(File, writeAtOffsetPastTheEndOfTheFile) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename]() { ad_utility::deleteFile(filename); };
  ad_utility::File file{filename, "w+"};
  file.write("abc", 3, 0);
  EXPECT_EQ(file.sizeOfFile(), 3);

  // Write beyond the current end of the file. The file grows, and the gap in
  // between reads as zeros.
  file.write("xyz", 3, 16);
  EXPECT_EQ(file.sizeOfFile(), 19);

  std::array<char, 19> buffer{};
  buffer.fill('u');
  ASSERT_EQ(file.read(buffer.data(), buffer.size(), 0), 19);
  EXPECT_EQ(std::string(buffer.data(), 3), "abc");
  EXPECT_EQ(std::string(buffer.data() + 16, 3), "xyz");
  EXPECT_EQ(std::string(buffer.data() + 3, 13), std::string(13, '\0'));

  // Overwrite a range in the middle, read it back, and then append again. The
  // interleaved reads and writes do not interfere with each other.
  file.write("MN", 2, 8);
  std::array<char, 2> small{};
  ASSERT_EQ(file.read(small.data(), small.size(), 8), 2);
  EXPECT_EQ(std::string(small.data(), 2), "MN");
  file.write("!", 1, 19);
  EXPECT_EQ(file.sizeOfFile(), 20);
}

// _____________________________________________________________________________
// Test that a positioned read which extends past the end of the file returns
// the number of bytes that were actually read (so far it never returned).
TEST(File, readAtOffsetPastTheEndOfTheFile) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename]() { ad_utility::deleteFile(filename); };
  ad_utility::File file{filename, "w+"};
  file.write("abcde", 5, 0);

  // A read that starts inside the file and ends behind it.
  std::array<char, 10> buffer{};
  EXPECT_EQ(file.read(buffer.data(), buffer.size(), 2), 3);
  EXPECT_EQ(std::string(buffer.data(), 3), "cde");

  // A read that starts behind the end of the file.
  EXPECT_EQ(file.read(buffer.data(), buffer.size(), 7), 0);
}

// _____________________________________________________________________________
// Test that writing 0 bytes is a no-op that does not throw, also when the
// offset is past the end of the file.
TEST(File, writeAtOffsetZeroBytes) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename]() { ad_utility::deleteFile(filename); };
  ad_utility::File file{filename, "w+"};
  file.write("abc", 3, 0);
  EXPECT_NO_THROW(file.write("xyz", 0, 0));
  EXPECT_NO_THROW(file.write("xyz", 0, 3));
  EXPECT_NO_THROW(file.write("xyz", 0, 100));
  EXPECT_NO_THROW(file.write(nullptr, 0, 1000));
  EXPECT_EQ(file.tell(), 0);
  EXPECT_EQ(file.sizeOfFile(), 3);
  std::array<char, 3> buffer{};
  ASSERT_EQ(file.read(buffer.data(), buffer.size(), 0), 3);
  EXPECT_EQ(std::string(buffer.data(), 3), "abc");
}

// _____________________________________________________________________________
// Test that a write which the operating system might not perform in a single
// step is looped until all the bytes have been written.
TEST(File, writeAtOffsetWritesEverything) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename]() { ad_utility::deleteFile(filename); };
  // A buffer that is large enough that a single `pwrite` is allowed to write
  // only a part of it, in which case the implementation has to loop.
  static constexpr size_t numBytes = 4 * 1024 * 1024;
  std::vector<char> data(numBytes);
  for (size_t i : ad_utility::integerRange(numBytes)) {
    data[i] = static_cast<char>(i % 251);
  }
  static constexpr off_t offset = 7;
  {
    ad_utility::File file{filename, "w+"};
    file.write(data.data(), data.size(), offset);
    EXPECT_EQ(file.sizeOfFile(), static_cast<off_t>(numBytes) + offset);
  }
  ad_utility::File file{filename, "r"};
  std::vector<char> buffer(numBytes);
  ASSERT_EQ(file.read(buffer.data(), buffer.size(), offset),
            static_cast<ssize_t>(numBytes));
  EXPECT_EQ(data, buffer);
}

// _____________________________________________________________________________
// Test that a failing write throws an exception that reports the error of
// `pwrite`.
TEST(File, writeAtOffsetReportsErrors) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename]() { ad_utility::deleteFile(filename); };
  {
    ad_utility::File writer{filename, "w"};
    writer.write("0123456789", 10);
  }
  // A file that was opened for reading cannot be written to.
  ad_utility::File file{filename, "r"};
  AD_EXPECT_THROW_WITH_MESSAGE(file.write("abc", 3, 0),
                               ::testing::HasSubstr("Bad file descriptor"));
}

// _____________________________________________________________________________
// Test that threads which write to ranges that do not overlap do not need any
// synchronization. The lock-free appends to a `CompressedBlockFile` rely on
// this (see #3589).
TEST(File, concurrentWritesAtDisjointOffsets) {
  std::string filename = gtestCurrentTestName();
  absl::Cleanup cleanup = [&filename]() { ad_utility::deleteFile(filename); };
  static constexpr size_t numThreads = 8;
  static constexpr size_t numBytesPerThread = 4096;
  {
    ad_utility::File file{filename, "w+"};
    {
      // The destructors of the `JThread`s join the threads.
      std::vector<ad_utility::JThread> threads;
      for (size_t threadIdx : ad_utility::integerRange(numThreads)) {
        threads.emplace_back([&file, threadIdx]() {
          std::vector<char> data(numBytesPerThread,
                                 static_cast<char>('a' + threadIdx));
          auto offset = static_cast<off_t>(threadIdx * numBytesPerThread);
          file.write(data.data(), data.size(), offset);
        });
      }
    }
    EXPECT_EQ(file.sizeOfFile(),
              static_cast<off_t>(numThreads * numBytesPerThread));
  }
  // Every thread's range holds that thread's byte, so nothing was interleaved.
  ad_utility::File file{filename, "r"};
  for (size_t threadIdx : ad_utility::integerRange(numThreads)) {
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
}

// _____________________________________________________________________________
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
