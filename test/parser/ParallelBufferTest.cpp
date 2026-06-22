// Copyright 2023 - 2026 The QLever Authors, in particular:
//
// 2023 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/asio/thread_pool.hpp>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "../util/GTestHelpers.h"
#include "parser/AsyncBlockSource.h"
#include "util/File.h"

namespace {

namespace qp = qlever::parser;

// Synchronously drive an `AsyncBlockSource` until EOF and return all blocks
// in order. Throws if the source signals an error.
std::vector<qp::ByteBlock> drainAllBlocks(qp::AsyncBlockSource& source) {
  boost::asio::thread_pool pool{1};
  std::vector<qp::ByteBlock> result;
  while (true) {
    std::promise<std::pair<std::exception_ptr, std::optional<qp::ByteBlock>>>
        promise;
    auto future = promise.get_future();
    source.asyncGetNextBlock(
        pool.get_executor(),
        [&promise](std::exception_ptr ep,
                   std::optional<qp::ByteBlock> opt) mutable {
          promise.set_value({ep, std::move(opt)});
        });
    auto [ep, opt] = future.get();
    if (ep) std::rethrow_exception(ep);
    if (!opt) break;
    result.push_back(std::move(*opt));
  }
  return result;
}

}  // namespace

// ________________________________________________________
TEST(AsyncFileBlockSource, ReadsInBlocks) {
  std::string filename = "asyncFileBlockSourceTest.first.dat";
  auto of = ad_utility::makeOfstream(filename);
  of << "abcdefghij";
  of.close();

  size_t blocksize = 4;
  qp::AsyncFileBlockSource buf(blocksize, filename);
  EXPECT_EQ(buf.getBlocksize(), blocksize);
  std::vector<qp::ByteBlock> expected{
      {'a', 'b', 'c', 'd'}, {'e', 'f', 'g', 'h'}, {'i', 'j'}};

  auto actual = drainAllBlocks(buf);
  ad_utility::deleteFile(filename);
  EXPECT_THAT(actual, ::testing::ElementsAreArray(expected));
}

// ________________________________________________________
TEST(AsyncEndRegexBlockSource, CutsAtRegexBoundary) {
  std::string filename = "asyncEndRegexBlockSourceTest.dat";
  auto of = ad_utility::makeOfstream(filename);
  of << "ab1cde23fgh";
  of.close();

  size_t blocksize = 5;
  {
    // We will always have blocks that end with a number that is followed by
    // a letter. The numbers must be at most 5 positions apart from each
    // other. Note: It is crucial that the regex contains exactly one
    // capture group. The end of the capture group determines the end of
    // the block.
    qp::AsyncEndRegexBlockSource buf(
        std::make_unique<qp::AsyncFileBlockSource>(blocksize, filename),
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
        std::make_unique<qp::AsyncFileBlockSource>(blocksize, filename),
        "([x-z])");
    AD_EXPECT_THROW_WITH_MESSAGE(
        drainAllBlocks(buf),
        ::testing::ContainsRegex("which marks the end of a statement"));
  }
  {
    // The same example but with a larger blocksize, s.t. the complete input
    // fits into a single block. In this case it is no error that the regex
    // can never be found.
    qp::AsyncEndRegexBlockSource buf(
        std::make_unique<qp::AsyncFileBlockSource>(100, filename), "([x-z])");
    std::vector<qp::ByteBlock> expected{
        {'a', 'b', '1', 'c', 'd', 'e', '2', '3', 'f', 'g', 'h'}};
    auto actual = drainAllBlocks(buf);
    EXPECT_THAT(actual, ::testing::ElementsAreArray(expected));
  }

  ad_utility::deleteFile(filename);
}

// ________________________________________________________
TEST(AsyncEndRegexBlockSource, LongLookahead) {
  std::string filename = "asyncEndRegexBlockSourceLongLookahead.dat";
  auto of = ad_utility::makeOfstream(filename);
  of << "abcdef1";
  for (size_t i = 0; i < 2000; ++i) {
    of << 'x';
  }
  of.close();

  size_t blocksize = 2000;
  {
    qp::AsyncEndRegexBlockSource buf(
        std::make_unique<qp::AsyncFileBlockSource>(blocksize, filename),
        "([0-9])[a-z]");
    std::vector<qp::ByteBlock> expected{{'a', 'b', 'c', 'd', 'e', 'f', '1'}};
    expected.emplace_back(2000, 'x');
    auto actual = drainAllBlocks(buf);
    EXPECT_THAT(actual, ::testing::ElementsAreArray(expected));
  }
  ad_utility::deleteFile(filename);
}
