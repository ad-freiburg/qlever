// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "parser/ParallelBuffer.h"

// ________________________________________________________
TEST(ParallelBuffer, ParallelFileBuffer) {
  std::string filename = "parallelBufferTest.first.dat";
  auto of = ad_utility::makeOfstream(filename);
  of << "abcdefghij";
  of.close();

  size_t blocksize = 4;
  ParallelFileBuffer buf(blocksize);
  buf.open(filename);
  std::vector<ParallelFileBuffer::BufferType> expected{
      {'a', 'b', 'c', 'd'}, {'e', 'f', 'g', 'h'}, {'i', 'j'}};

  std::vector<ParallelFileBuffer::BufferType> actual;
  while (auto block = buf.getNextBlock()) {
    actual.push_back(block.value());
  }

  ad_utility::deleteFile(filename);

  EXPECT_THAT(actual, ::testing::ElementsAreArray(expected));

  // Reading from an unopened buffer throws.
  ParallelFileBuffer buf2(blocksize);
  EXPECT_ANY_THROW(buf2.getNextBlock());
}

// ________________________________________________________
TEST(ParallelBuffer, ParallelBufferWithEndRegex) {
  std::string filename = "parallelBufferWithEndRegex.dat";
  auto of = ad_utility::makeOfstream(filename);
  of << "ab1cde23fgh";
  of.close();

  size_t blocksize = 5;
  {
    // We will always have blocks that end with a number that is followed by a
    // letter. The numbers must be at most 5 positions apart from each other.
    // Note: It is crucial that the regex contains exactly one capture group.
    // The end of the capture group determines the end of the block.
    ParallelBufferWithEndRegex buf(blocksize, "([0-9])[a-z]");
    buf.open(filename);
    std::vector<ParallelFileBuffer::BufferType> expected{
        {'a', 'b', '1'}, {'c', 'd', 'e', '2', '3'}, {'f', 'g', 'h'}};

    std::vector<ParallelFileBuffer::BufferType> actual;
    while (auto opt = buf.getNextBlock()) {
      actual.push_back(opt.value());
    }

    EXPECT_THAT(actual, ::testing::ElementsAreArray(expected));
  }
  {
    // The following regex is not found in the data, and the data is too large
    // for one block, so the parsing fails
    ParallelBufferWithEndRegex buf(blocksize, "([x-z])");
    buf.open(filename);
    AD_EXPECT_THROW_WITH_MESSAGE(
        buf.getNextBlock(),
        ::testing::ContainsRegex("which marks the end of a statement"));
  }
  {
    // The same example but with a larger blocksize, s.t. the complete input
    // fits into a single block. In this case it is no error that the regex can
    // never be found.
    ParallelBufferWithEndRegex buf(100, "([x-z])");
    buf.open(filename);
    std::vector<ParallelFileBuffer::BufferType> expected{
        {'a', 'b', '1', 'c', 'd', 'e', '2', '3', 'f', 'g', 'h'}};

    std::vector<ParallelFileBuffer::BufferType> actual;
    while (auto opt = buf.getNextBlock()) {
      actual.push_back(opt.value());
    }
    EXPECT_THAT(actual, ::testing::ElementsAreArray(expected));
  }

  ad_utility::deleteFile(filename);
  // Reading from an unopened buffer throws.
  ParallelBufferWithEndRegex buf2(blocksize, "ab");
  EXPECT_ANY_THROW(buf2.getNextBlock());
}

// ________________________________________________________
TEST(ParallelBuffer, ParallelBufferWithEndRegexLongLookahead) {
  std::string filename = "parallelBufferWithEndRegexLongLookahead.dat";
  auto of = ad_utility::makeOfstream(filename);
  of << "abcdef1";
  for (size_t i = 0; i < 2000; ++i) {
    of << 'x';
  }
  of.close();

  size_t blocksize = 2000;
  {
    // We will always have blocks that end with a number that is followed by a
    // letter. The numbers must be at most 5 positions apart from each other.
    // Note: It is crucial that the regex contains exactly one capture group.
    // The end of the capture group determines the end of the block.
    ParallelBufferWithEndRegex buf(blocksize, "([0-9])[a-z]");
    buf.open(filename);
    std::vector<ParallelFileBuffer::BufferType> expected{
        {'a', 'b', 'c', 'd', 'e', 'f', '1'}};
    expected.emplace_back(2000, 'x');

    std::vector<ParallelFileBuffer::BufferType> actual;
    while (auto opt = buf.getNextBlock()) {
      actual.push_back(opt.value());
    }
    EXPECT_THAT(actual, ::testing::ElementsAreArray(expected));
  }
  ad_utility::deleteFile(filename);
}
