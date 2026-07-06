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
TEST(AsyncEndRegexBlockSource, CutsAtRegexBoundary) {
  std::string filename = gtestCurrentTestName();
  auto of = ad_utility::makeOfstream(filename);
  of << "ab1cde23fgh";
  of.close();
  absl::Cleanup fileCleanup{[&] { ad_utility::deleteFile(filename); }};

  boost::asio::thread_pool pool{1};
  ad_utility::MemorySize blocksize = 5_B;
  {
    // Blocks always end with a digit. The regex `([0-9])[a-z]` matches a
    // digit followed by a letter, the `AsyncEndRegexBlockSource` always cuts
    // after the first capture group in the regex, so after the number that
    // precedes a letter.
    qp::AsyncEndRegexBlockSource buf(
        std::make_unique<qp::AsyncFileBlockSource>(pool.get_executor(),
                                                   blocksize, filename),
        "([0-9])[a-z]");
    std::vector<qp::ByteBlock> expected{
        {'a', 'b', '1'}, {'c', 'd', 'e', '2', '3'}, {'f', 'g', 'h'}};
    auto actual = drainAllBlocks(buf);
    EXPECT_THAT(actual, ::testing::ElementsAreArray(expected));
  }
  {
    // The following regex is not found in the data, and the data is too
    // large for one block, so the parsing fails.
    qp::AsyncEndRegexBlockSource buf(
        std::make_unique<qp::AsyncFileBlockSource>(pool.get_executor(),
                                                   blocksize, filename),
        "([x-z])");
    AD_EXPECT_THROW_WITH_MESSAGE(
        drainAllBlocks(buf),
        ::testing::ContainsRegex("which marks the end of a statement"));
  }
  {
    // The same example but with a larger blocksize, s.t. the complete input
    // fits into a single block. In this case it is no error that the regex
    // can never be found.
    qp::AsyncEndRegexBlockSource buf(std::make_unique<qp::AsyncFileBlockSource>(
                                         pool.get_executor(), 100_B, filename),
                                     "([x-z])");
    std::vector<qp::ByteBlock> expected{
        {'a', 'b', '1', 'c', 'd', 'e', '2', '3', 'f', 'g', 'h'}};
    auto actual = drainAllBlocks(buf);
    EXPECT_THAT(actual, ::testing::ElementsAreArray(expected));
  }
}

// ________________________________________________________
TEST(AsyncEndRegexBlockSource, LongLookahead) {
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
    qp::AsyncEndRegexBlockSource buf(
        std::make_unique<qp::AsyncFileBlockSource>(pool.get_executor(),
                                                   blocksize, filename),
        "([0-9])[a-z]");
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
