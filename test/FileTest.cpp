// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#include <gtest/gtest.h>

#include "util/File.h"

namespace ad_utility {
TEST(File, move) {
  std::string filename = "testFileMove.tmp";
  File file1(filename, "w");
  ASSERT_TRUE(file1.isOpen());
  file1.write("aaa", 3);

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
