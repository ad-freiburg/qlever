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
  ParallelFileBuffer buf(blocksize, filename);
  EXPECT_EQ(buf.getBlocksize(), blocksize);
  std::vector<ParallelFileBuffer::BufferType> expected{
      {'a', 'b', 'c', 'd'}, {'e', 'f', 'g', 'h'}, {'i', 'j'}};

  std::vector<ParallelFileBuffer::BufferType> actual;
  while (auto block = buf.getNextBlock()) {
    actual.push_back(block.value());
  }

  ad_utility::deleteFile(filename);

  EXPECT_THAT(actual, ::testing::ElementsAreArray(expected));
}

namespace {
// Scan `sv` from the back and return the position right after the last digit
// that is immediately followed by a lowercase letter, or `std::nullopt` if
// there is no such digit.
std::optional<size_t> findDigitFollowedByLetter(std::string_view sv) {
  for (size_t i = sv.size(); i-- > 1;) {
    if (sv[i] >= 'a' && sv[i] <= 'z' && sv[i - 1] >= '0' && sv[i - 1] <= '9') {
      return i;
    }
  }
  return std::nullopt;
}

// Scan `sv` from the back and return the position right after the last
// character in the range `x`-`z`, or `std::nullopt` if there is none.
std::optional<size_t> findXToZ(std::string_view sv) {
  size_t pos = sv.find_last_of("xyz");
  return pos == std::string_view::npos ? std::nullopt
                                       : std::optional<size_t>{pos + 1};
}
}  // namespace

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
    ParallelBufferWithEndRegex buf(
        std::make_unique<ParallelFileBuffer>(blocksize, filename),
        findDigitFollowedByLetter, "a digit followed by a letter");
    std::vector<ParallelFileBuffer::BufferType> expected{
        {'a', 'b', '1'}, {'c', 'd', 'e', '2', '3'}, {'f', 'g', 'h'}};

    std::vector<ParallelFileBuffer::BufferType> actual;
    while (auto opt = buf.getNextBlock()) {
      actual.push_back(opt.value());
    }

    EXPECT_THAT(actual, ::testing::ElementsAreArray(expected));
  }
  {
    // The following pattern is not found in the data, and the data is too large
    // for one block, so the parsing fails
    ParallelBufferWithEndRegex buf(
        std::make_unique<ParallelFileBuffer>(blocksize, filename), findXToZ,
        "a letter from x to z");
    AD_EXPECT_THROW_WITH_MESSAGE(
        buf.getNextBlock(),
        ::testing::ContainsRegex("which marks the end of a statement"));
  }
  {
    // The same example but with a larger blocksize, s.t. the complete input
    // fits into a single block. In this case it is no error that the pattern
    // can never be found.
    ParallelBufferWithEndRegex buf(
        std::make_unique<ParallelFileBuffer>(100, filename), findXToZ,
        "a letter from x to z");
    std::vector<ParallelFileBuffer::BufferType> expected{
        {'a', 'b', '1', 'c', 'd', 'e', '2', '3', 'f', 'g', 'h'}};

    std::vector<ParallelFileBuffer::BufferType> actual;
    while (auto opt = buf.getNextBlock()) {
      actual.push_back(opt.value());
    }
    EXPECT_THAT(actual, ::testing::ElementsAreArray(expected));
  }

  ad_utility::deleteFile(filename);
}

// ________________________________________________________
TEST(ParallelBuffer, ParallelBufferWithEndRegexLongLookahead) {
  std::string filename = "parallelBufferWithEndRegexLongLookahead.dat";
  size_t blocksize = 2000;
  auto of = ad_utility::makeOfstream(filename);
  of << "abcdef1";
  for (size_t i = 0; i < blocksize; ++i) {
    of << 'x';
  }
  of.close();

  {
    // We will always have blocks that end with a number that is followed by a
    // letter. Here the only such position is far from the end of the block, so
    // the manual scan has to look back across many bytes.
    ParallelBufferWithEndRegex buf(
        std::make_unique<ParallelFileBuffer>(blocksize, filename),
        findDigitFollowedByLetter, "a digit followed by a letter");
    std::vector<ParallelFileBuffer::BufferType> expected{
        {'a', 'b', 'c', 'd', 'e', 'f', '1'}};
    expected.emplace_back(blocksize, 'x');

    std::vector<ParallelFileBuffer::BufferType> actual;
    while (auto opt = buf.getNextBlock()) {
      actual.push_back(opt.value());
    }
    EXPECT_THAT(actual, ::testing::ElementsAreArray(expected));
  }
  ad_utility::deleteFile(filename);
}
