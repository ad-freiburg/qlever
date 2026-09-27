// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

#include "engine/RleVectorStream.h"
#include "global/Id.h"
#include "util/GTestHelpers.h"

using ql::engine::rle::RleVectorStream;
using ::testing::ElementsAre;
using ::testing::HasSubstr;
using Run = RleVectorStream::Run;

namespace {
constexpr uint32_t MAX_LENGTH = std::numeric_limits<uint32_t>::max();

// Matcher for a `Run` with the given value and length.
auto isRun(Id value, uint32_t length) {
  return ::testing::AllOf(AD_FIELD(Run, value_, value),
                          AD_FIELD(Run, length_, length));
}

// Return the runs of `stream` as a vector.
std::vector<Run> runsOf(const RleVectorStream& stream) {
  return {stream.runs().begin(), stream.runs().end()};
}

// Return the rows of `stream` as a vector.
std::vector<Id> materializeToVector(const RleVectorStream& stream) {
  std::vector<Id> rows(stream.totalRows());
  stream.materialize(rows);
  return rows;
}

// Return a column that consists of the given runs of integers.
std::vector<Id> makeColumn(
    const std::vector<std::pair<int64_t, size_t>>& runs) {
  std::vector<Id> column;
  for (auto [value, length] : runs) {
    column.insert(column.end(), length, Id::makeFromInt(value));
  }
  return column;
}
}  // namespace

// _____________________________________________________________________________
TEST(RleVectorStreamTest, AppendMergesEqualValues) {
  auto I = Id::makeFromInt;
  RleVectorStream stream;
  stream.append(I(1), 100);
  stream.append(I(2), 50);
  // The same value as the last run extends that run.
  stream.append(I(2), 50);
  stream.append(I(3), 200);

  EXPECT_EQ(stream.numRuns(), 3u);
  EXPECT_EQ(stream.totalRows(), 400u);
  EXPECT_THAT(runsOf(stream), ElementsAre(isRun(I(1), 100), isRun(I(2), 100),
                                          isRun(I(3), 200)));
  EXPECT_EQ(materializeToVector(stream),
            makeColumn({{1, 100}, {2, 100}, {3, 200}}));
}

// _____________________________________________________________________________
TEST(RleVectorStreamTest, AppendZeroRowsIsNoOp) {
  auto I = Id::makeFromInt;
  RleVectorStream stream;
  stream.append(I(7), 0);
  EXPECT_EQ(stream.numRuns(), 0u);
  EXPECT_EQ(stream.totalRows(), 0u);

  stream.append(I(7), 10);
  stream.append(I(7), 0);
  stream.append(I(8), 0);
  EXPECT_THAT(runsOf(stream), ElementsAre(isRun(I(7), 10)));
  EXPECT_EQ(stream.totalRows(), 10u);
}

// _____________________________________________________________________________
TEST(RleVectorStreamTest, AppendSaturatesRunLength) {
  auto I = Id::makeFromInt;
  RleVectorStream stream;
  // `(MAX_LENGTH - 5) + 15` rows of the same value do not fit into one run: the
  // first run gets `MAX_LENGTH` rows, a second run of the same value the
  // remaining 10.
  stream.append(I(9), MAX_LENGTH - 5);
  stream.append(I(9), 15);
  EXPECT_THAT(runsOf(stream),
              ElementsAre(isRun(I(9), MAX_LENGTH), isRun(I(9), 10)));
  EXPECT_EQ(stream.totalRows(), static_cast<size_t>(MAX_LENGTH) + 10u);
}

// _____________________________________________________________________________
TEST(RleVectorStreamTest, CopyAndMove) {
  auto I = Id::makeFromInt;
  RleVectorStream stream;
  stream.append(I(9), 5);
  stream.append(I(10), 3);
  auto expectTwoRuns = [&I](const RleVectorStream& s) {
    EXPECT_THAT(runsOf(s), ElementsAre(isRun(I(9), 5), isRun(I(10), 3)));
    EXPECT_EQ(s.totalRows(), 8u);
  };
  auto expectEmpty = [](const RleVectorStream& s) {
    EXPECT_TRUE(s.runs().empty());
    EXPECT_EQ(s.numRuns(), 0u);
    EXPECT_EQ(s.totalRows(), 0u);
  };

  RleVectorStream copy{stream};
  expectTwoRuns(copy);
  expectTwoRuns(stream);
  RleVectorStream copyAssigned;
  copyAssigned = stream;
  expectTwoRuns(copyAssigned);

  // A moved-from stream is empty, after move construction and after move
  // assignment.
  RleVectorStream moved{std::move(stream)};
  expectTwoRuns(moved);
  expectEmpty(stream);
  RleVectorStream moveAssigned;
  moveAssigned = std::move(moved);
  expectTwoRuns(moveAssigned);
  expectEmpty(moved);
}

// _____________________________________________________________________________
TEST(RleVectorStreamTest, MaterializeIntoTooSmallDestinationThrows) {
  RleVectorStream stream;
  stream.append(Id::makeFromInt(4), 3);
  std::vector<Id> dest(stream.totalRows() - 1);
  AD_EXPECT_THROW_WITH_MESSAGE(stream.materialize(dest),
                               HasSubstr("Destination too small"));
}

// _____________________________________________________________________________
TEST(RleVectorStreamTest, MaterializeExactlySizedDestination) {
  RleVectorStream stream;
  stream.append(Id::makeFromInt(4), 3);
  stream.append(Id::makeFromInt(5), 2);
  std::vector<Id> dest(stream.totalRows());
  stream.materialize(dest);
  EXPECT_EQ(dest, makeColumn({{4, 3}, {5, 2}}));
}

// _____________________________________________________________________________
TEST(RleVectorStreamTest, MaterializeLeavesTailUnchanged) {
  RleVectorStream stream;
  stream.append(Id::makeFromInt(4), 2);
  std::vector<Id> dest(4, Id::makeFromInt(-1));
  stream.materialize(dest);
  EXPECT_EQ(dest, makeColumn({{4, 2}, {-1, 2}}));

  // An empty stream writes nothing.
  RleVectorStream empty;
  std::vector<Id> noRows;
  empty.materialize(noRows);
  empty.materialize(dest);
  EXPECT_EQ(dest, makeColumn({{4, 2}, {-1, 2}}));
}

// _____________________________________________________________________________
TEST(RleVectorStreamTest, FromColumn) {
  auto I = Id::makeFromInt;
  std::vector<Id> column = makeColumn({{1, 3}, {2, 1}, {3, 2}, {1, 1}});

  auto stream = RleVectorStream::fromColumn(column, 4);
  ASSERT_TRUE(stream.has_value());
  EXPECT_THAT(runsOf(stream.value()),
              ElementsAre(isRun(I(1), 3), isRun(I(2), 1), isRun(I(3), 2),
                          isRun(I(1), 1)));
  EXPECT_EQ(stream->totalRows(), column.size());
  EXPECT_EQ(materializeToVector(stream.value()), column);

  // One run more than allowed.
  EXPECT_FALSE(RleVectorStream::fromColumn(column, 3).has_value());

  // An empty column has no runs, also with a limit of zero.
  auto empty = RleVectorStream::fromColumn({}, 0);
  ASSERT_TRUE(empty.has_value());
  EXPECT_EQ(empty->numRuns(), 0u);
  EXPECT_FALSE(RleVectorStream::fromColumn(column, 0).has_value());
}
