// Copyright 2023 - 2026 The QLever Authors, in particular:
//
// 2023 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "../util/GTestHelpers.h"
#include "parser/AsyncBlockSource.h"
#include "util/File.h"
#include "util/MemorySize/MemorySize.h"

namespace {

namespace qp = qlever::parser;
using namespace ad_utility::memory_literals;

// Synchronously drive an `AsyncBlockSource` until EOF and return all blocks
// in order. Throws if the source signals an error.
std::vector<qp::ByteBlock> drainAllBlocks(qp::AsyncBlockSource& source) {
  std::vector<qp::ByteBlock> result;
  while (auto opt = source.asyncGetNextBlock(boost::asio::use_future).get()) {
    result.push_back(std::move(*opt));
  }
  return result;
}

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
TEST(AsyncFileBlockSource, ReadsInBlocks) {
  std::string filename = gtestCurrentTestName();
  auto of = ad_utility::makeOfstream(filename);
  of << "abcdefghij";
  of.close();
  absl::Cleanup fileCleanup{[&] { ad_utility::deleteFile(filename); }};

  boost::asio::thread_pool pool{1};
  ad_utility::MemorySize blocksize = 4_B;
  qp::AsyncFileBlockSource buf(pool.get_executor(), blocksize, filename);
  EXPECT_EQ(buf.getBlocksize(), blocksize);
  std::vector<qp::ByteBlock> expected{
      {'a', 'b', 'c', 'd'}, {'e', 'f', 'g', 'h'}, {'i', 'j'}};

  auto actual = drainAllBlocks(buf);
  EXPECT_THAT(actual, ::testing::ElementsAreArray(expected));
}

// ________________________________________________________
TEST(AsyncStatementBoundaryBlockSource, CutsAtBoundary) {
  std::string filename = gtestCurrentTestName();
  auto of = ad_utility::makeOfstream(filename);
  of << "ab1cde23fgh";
  of.close();
  absl::Cleanup fileCleanup{[&] { ad_utility::deleteFile(filename); }};

  boost::asio::thread_pool pool{1};
  ad_utility::MemorySize blocksize = 5_B;
  {
    // Blocks always end with a number that is followed by a letter. The
    // `AsyncStatementBoundaryBlockSource` cuts after the last digit that
    // precedes a letter, as determined by `findDigitFollowedByLetter`.
    qp::AsyncStatementBoundaryBlockSource buf(
        pool.get_executor(),
        std::make_unique<qp::AsyncFileBlockSource>(pool.get_executor(),
                                                   blocksize, filename),
        findDigitFollowedByLetter, "a digit followed by a letter");
    std::vector<qp::ByteBlock> expected{
        {'a', 'b', '1'}, {'c', 'd', 'e', '2', '3'}, {'f', 'g', 'h'}};
    auto actual = drainAllBlocks(buf);
    EXPECT_THAT(actual, ::testing::ElementsAreArray(expected));
  }
  {
    // The following pattern is not found in the data, and the data is too
    // large for one block, so the parsing fails.
    qp::AsyncStatementBoundaryBlockSource buf(
        pool.get_executor(),
        std::make_unique<qp::AsyncFileBlockSource>(pool.get_executor(),
                                                   blocksize, filename),
        findXToZ, "a letter from x to z");
    AD_EXPECT_THROW_WITH_MESSAGE(
        drainAllBlocks(buf), ::testing::ContainsRegex("No statement boundary"));
  }
  {
    // The same example but with a larger blocksize, s.t. the complete input
    // fits into a single block. In this case it is no error that the pattern
    // can never be found.
    qp::AsyncStatementBoundaryBlockSource buf(
        pool.get_executor(),
        std::make_unique<qp::AsyncFileBlockSource>(pool.get_executor(), 100_B,
                                                   filename),
        findXToZ, "a letter from x to z");
    std::vector<qp::ByteBlock> expected{
        {'a', 'b', '1', 'c', 'd', 'e', '2', '3', 'f', 'g', 'h'}};
    auto actual = drainAllBlocks(buf);
    EXPECT_THAT(actual, ::testing::ElementsAreArray(expected));
  }
}

// ________________________________________________________
TEST(AsyncStatementBoundaryBlockSource, LongLookahead) {
  std::string filename = gtestCurrentTestName();
  auto of = ad_utility::makeOfstream(filename);
  of << "abcdef1";
  for (size_t i = 0; i < 2000; ++i) {
    of << 'x';
  }
  of.close();
  absl::Cleanup fileCleanup{[&] { ad_utility::deleteFile(filename); }};

  boost::asio::thread_pool pool{1};
  ad_utility::MemorySize blocksize = 2000_B;
  {
    // The only position that ends a statement is far from the end of the block,
    // so the manual scan has to look back across many bytes.
    qp::AsyncStatementBoundaryBlockSource buf(
        pool.get_executor(),
        std::make_unique<qp::AsyncFileBlockSource>(pool.get_executor(),
                                                   blocksize, filename),
        findDigitFollowedByLetter, "a digit followed by a letter");
    std::vector<qp::ByteBlock> expected{{'a', 'b', 'c', 'd', 'e', 'f', '1'}};
    expected.emplace_back(2000, 'x');
    auto actual = drainAllBlocks(buf);
    EXPECT_THAT(actual, ::testing::ElementsAreArray(expected));
  }
}

// ________________________________________________________
TEST(AsyncBlockSource, UseFutureToken) {
  std::string filename = gtestCurrentTestName();
  auto of = ad_utility::makeOfstream(filename);
  of << "ab1cd";
  of.close();
  absl::Cleanup fileCleanup{[&] { ad_utility::deleteFile(filename); }};

  boost::asio::thread_pool pool{1};
  qp::AsyncFileBlockSource buf(pool.get_executor(), 3_B, filename);

  // Retrieve blocks via `use_future` and verify success and EOF paths.
  EXPECT_THAT(buf.asyncGetNextBlock(boost::asio::use_future).get(),
              ::testing::Optional(::testing::ElementsAre('a', 'b', '1')));
  EXPECT_THAT(buf.asyncGetNextBlock(boost::asio::use_future).get(),
              ::testing::Optional(::testing::ElementsAre('c', 'd')));
  EXPECT_EQ(buf.asyncGetNextBlock(boost::asio::use_future).get(), std::nullopt);
}
