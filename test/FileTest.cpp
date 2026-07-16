// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#include <gtest/gtest.h>

#include <array>
#include <filesystem>

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
