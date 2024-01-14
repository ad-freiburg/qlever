// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#include <gtest/gtest.h>

#include <cstdio>

#include "util/File.h"

using std::string;
using std::vector;

namespace ad_utility {
class FileTest : public ::testing::Test {
 protected:
  void SetUp() override {
    {
      File testFile1("_tmp_testFile1", "w");
      string content = "line1\nline2\n";
      testFile1.write(content.c_str(), content.size());
    }
    {
      File testFile2("_tmp_testFile2", "w");
      string content = "line1\nline2";
      testFile2.write(content.c_str(), content.size());
    }
    {
      File testFile3("_tmp_testFile3", "w");
      string content = "line1\n";
      testFile3.write(content.c_str(), content.size());
    }
    {
      File testFile3("_tmp_testFile4", "w");
      string content = "";
      testFile3.write(content.c_str(), content.size());
    }
    {
      File testFileBinary("_tmp_testFileBinary", "w");
      size_t a = 1;
      size_t b = 0;
      size_t c = 5000;
      off_t off = 3;
      testFileBinary.write(&a, sizeof(size_t));
      testFileBinary.write(&b, sizeof(size_t));
      testFileBinary.write(&c, sizeof(size_t));
      testFileBinary.write(&off, sizeof(off_t));
    }
  }

  void TearDown() override {
    remove("_tmp_testFile1");
    remove("_tmp_testFile2");
    remove("_tmp_testFile3");
    remove("_tmp_testFile4");
    remove("_tmp_testFileBinary");
  }
};

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
