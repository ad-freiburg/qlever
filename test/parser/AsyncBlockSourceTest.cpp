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
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "../util/GTestHelpers.h"
#include "parser/AsyncBlockSource.h"
#include "util/Exception.h"
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

// Never find a boundary. Used to force `AsyncStatementBoundaryBlockSource`
// into its "no boundary found, peek at the next block" code path.
std::optional<size_t> findNever([[maybe_unused]] std::string_view sv) {
  return std::nullopt;
}

// A `BlockingBlockSource` whose `getNextBlockImpl` results are scripted in
// advance, one `Step` per call. A `Step` holding a `qp::ByteBlock` is
// returned as-is; a `Step` holding a `std::string` is thrown as a
// `std::runtime_error` with that message. This is used to exercise the
// error-forwarding paths (`if (ep) ...` checks and the `catch` clause in
// `BlockingBlockSource::asyncGetNextBlockImpl`) that a real `FileBlockSource`
// cannot trigger, because reading from a file never throws in practice.
class ScriptedBlockSource : public qp::BlockingBlockSource {
 public:
  using Step = std::variant<qp::ByteBlock, std::string>;

  ScriptedBlockSource(const boost::asio::any_io_executor& exec,
                      ad_utility::MemorySize blocksize, std::vector<Step> steps)
      : qp::BlockingBlockSource{exec, blocksize}, steps_{std::move(steps)} {}

 protected:
  std::optional<qp::ByteBlock> getNextBlockImpl() override {
    AD_CORRECTNESS_CHECK(nextStep_ < steps_.size());
    Step& step = steps_[nextStep_++];
    if (auto* message = std::get_if<std::string>(&step)) {
      throw std::runtime_error{*message};
    }
    return std::move(std::get<qp::ByteBlock>(step));
  }

 private:
  std::vector<Step> steps_;
  size_t nextStep_ = 0;
};

}  // namespace

// ________________________________________________________
TEST(FileBlockSource, ReadsInBlocks) {
  std::string filename = gtestCurrentTestName();
  auto of = ad_utility::makeOfstream(filename);
  of << "abcdefghij";
  of.close();
  absl::Cleanup fileCleanup{[&] { ad_utility::deleteFile(filename); }};

  boost::asio::thread_pool pool{1};
  ad_utility::MemorySize blocksize = 4_B;
  qp::FileBlockSource buf(pool.get_executor(), blocksize, filename);
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
        std::make_unique<qp::FileBlockSource>(pool.get_executor(), blocksize,
                                              filename),
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
        std::make_unique<qp::FileBlockSource>(pool.get_executor(), blocksize,
                                              filename),
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
        std::make_unique<qp::FileBlockSource>(pool.get_executor(), 100_B,
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
        std::make_unique<qp::FileBlockSource>(pool.get_executor(), blocksize,
                                              filename),
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
  qp::FileBlockSource buf(pool.get_executor(), 3_B, filename);

  // Retrieve blocks via `use_future` and verify success and EOF paths.
  EXPECT_THAT(buf.asyncGetNextBlock(boost::asio::use_future).get(),
              ::testing::Optional(::testing::ElementsAre('a', 'b', '1')));
  EXPECT_THAT(buf.asyncGetNextBlock(boost::asio::use_future).get(),
              ::testing::Optional(::testing::ElementsAre('c', 'd')));
  EXPECT_EQ(buf.asyncGetNextBlock(boost::asio::use_future).get(), std::nullopt);
}

// ________________________________________________________
TEST(BlockingBlockSource, ForwardsExceptionFromGetNextBlockImpl) {
  boost::asio::thread_pool pool{1};
  // The `catch (...)` clause in `BlockingBlockSource::asyncGetNextBlockImpl`
  // must turn this exception into an `exception_ptr` that is forwarded to the
  // handler, and from there all the way through `asyncGetNextBlock`'s
  // dispatch to the `use_future` completion token.
  ScriptedBlockSource buf(
      pool.get_executor(), 4_B,
      {ScriptedBlockSource::Step{std::string{"boom from getNextBlockImpl"}}});
  AD_EXPECT_THROW_WITH_MESSAGE(
      drainAllBlocks(buf), ::testing::HasSubstr("boom from getNextBlockImpl"));
}

// ________________________________________________________
TEST(AsyncStatementBoundaryBlockSource,
     ForwardsExceptionFromInitialInnerFetch) {
  boost::asio::thread_pool pool{1};
  // The very first `callAsyncGetNextBlockImpl` call on `inner_` (in
  // `asyncGetNextBlockImpl`) fails. This must be forwarded via the `if (ep)`
  // branch of `forwardErrors`, without ever calling `findEndPosition_`.
  auto inner = std::make_unique<ScriptedBlockSource>(
      pool.get_executor(), 4_B,
      std::vector<ScriptedBlockSource::Step>{
          ScriptedBlockSource::Step{std::string{"boom from initial fetch"}}});
  qp::AsyncStatementBoundaryBlockSource buf(
      pool.get_executor(), std::move(inner), findXToZ, "a letter from x to z");
  AD_EXPECT_THROW_WITH_MESSAGE(drainAllBlocks(buf),
                               ::testing::HasSubstr("boom from initial fetch"));
}

// ________________________________________________________
TEST(AsyncStatementBoundaryBlockSource, ForwardsExceptionFromPeek) {
  boost::asio::thread_pool pool{1};
  // The first block from `inner_` contains no boundary (because
  // `findNever` never finds one), so `handleMissingBoundary` peeks at a
  // second block from `inner_`, which fails. This must be forwarded via the
  // `if (ep)` branch of `forwardErrors` inside `handleMissingBoundary`, as a
  // plain forwarded exception rather than being turned into a "no statement
  // boundary" error.
  auto inner = std::make_unique<ScriptedBlockSource>(
      pool.get_executor(), 4_B,
      std::vector<ScriptedBlockSource::Step>{
          ScriptedBlockSource::Step{qp::ByteBlock{'a', 'b', 'c', 'd'}},
          ScriptedBlockSource::Step{std::string{"boom from peek"}}});
  qp::AsyncStatementBoundaryBlockSource buf(
      pool.get_executor(), std::move(inner), findNever, "never found");
  AD_EXPECT_THROW_WITH_MESSAGE(drainAllBlocks(buf),
                               ::testing::HasSubstr("boom from peek"));
}

// ________________________________________________________
TEST(AsyncStatementBoundaryBlockSource,
     ForwardsExceptionFromEndPositionFinder) {
  boost::asio::thread_pool pool{1};
  // `findEndPosition_` is user-supplied code and runs inline in the handler
  // chain of the inner source. An exception from it must be delivered via
  // the completion handler like any other error: exactly once (the handler
  // is one-shot, see `BlockingBlockSource::asyncGetNextBlockImpl`) and
  // without escaping into the inner source's executor.
  auto inner = std::make_unique<ScriptedBlockSource>(
      pool.get_executor(), 4_B,
      std::vector<ScriptedBlockSource::Step>{
          ScriptedBlockSource::Step{qp::ByteBlock{'a', 'b', 'c', 'd'}}});
  auto findThrows = [](std::string_view) -> std::optional<size_t> {
    throw std::runtime_error{"boom from findEndPosition"};
  };
  qp::AsyncStatementBoundaryBlockSource buf(
      pool.get_executor(), std::move(inner), findThrows, "throwing finder");
  AD_EXPECT_THROW_WITH_MESSAGE(
      drainAllBlocks(buf), ::testing::HasSubstr("boom from findEndPosition"));
}
