//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "engine/Result.h"
#include "util/IdTableHelpers.h"

using namespace std::chrono_literals;

namespace {
// Helper function to generate all possible splits of an IdTable in order to
// exhaustively test generator variants.
std::vector<cppcoro::generator<IdTable>> getAllSubSplits(
    const IdTable& idTable) {
  std::vector<cppcoro::generator<IdTable>> result;
  for (size_t i = 0; i < std::pow(idTable.size() - 1, 2); ++i) {
    std::vector<size_t> reverseIndex{};
    size_t copy = i;
    for (size_t index = 0; index < idTable.size(); ++index) {
      if (copy % 2 == 1) {
        reverseIndex.push_back(index);
      }
      copy /= 2;
    }
    result.push_back(
        [](auto split, IdTable clone) -> cppcoro::generator<IdTable> {
          IdTable subSplit{clone.numColumns(),
                           ad_utility::makeUnlimitedAllocator<IdTable>()};
          size_t splitIndex = 0;
          for (size_t i = 0; i < clone.size(); ++i) {
            subSplit.push_back(clone[i]);
            if (splitIndex < split.size() && split[splitIndex] == i) {
              co_yield subSplit;
              subSplit.clear();
              ++splitIndex;
            }
          }
          if (subSplit.size() > 0) {
            co_yield subSplit;
          }
        }(std::move(reverseIndex), idTable.clone()));
  }

  return result;
}

// _____________________________________________________________________________
void consumeGenerator(cppcoro::generator<IdTable>& generator) {
  for ([[maybe_unused]] IdTable& _ : generator) {
  }
}
}  // namespace

TEST(Result, verifyIdTableThrowsWhenActuallyLazy) {
  Result result1{
      []() -> cppcoro::generator<IdTable> { co_return; }(), {}, LocalVocab{}};
  EXPECT_FALSE(result1.isFullyMaterialized());
  EXPECT_THROW(result1.idTable(), ad_utility::Exception);

  Result result2{[]() -> cppcoro::generator<IdTable> { co_return; }(),
                 {},
                 result1.getSharedLocalVocab()};
  EXPECT_FALSE(result2.isFullyMaterialized());
  EXPECT_THROW(result2.idTable(), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(Result, verifyIdTableThrowsOnSecondAccess) {
  const Result result{
      []() -> cppcoro::generator<IdTable> { co_return; }(), {}, LocalVocab{}};
  // First access should work
  for ([[maybe_unused]] IdTable& _ : result.idTables()) {
    ADD_FAILURE() << "Generator is empty";
  }
  // Now it should throw
  EXPECT_THROW(result.idTables(), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(Result, verifyIdTablesThrowsWhenFullyMaterialized) {
  Result result1{
      IdTable{ad_utility::makeUnlimitedAllocator<IdTable>()}, {}, LocalVocab{}};
  EXPECT_TRUE(result1.isFullyMaterialized());
  EXPECT_THROW(result1.idTables(), ad_utility::Exception);

  Result result2{IdTable{ad_utility::makeUnlimitedAllocator<IdTable>()},
                 {},
                 result1.getSharedLocalVocab()};
  EXPECT_TRUE(result2.isFullyMaterialized());
  EXPECT_THROW(result2.idTables(), ad_utility::Exception);
}

// _____________________________________________________________________________
using CIs = std::vector<ColumnIndex>;
class ResultSortTestS : public testing::TestWithParam<CIs> {};
class ResultSortTestF : public testing::TestWithParam<CIs> {};

TEST_P(ResultSortTestS, verifyAssertSortOrderIsRespectedSucceedsWhenSorted) {
  if constexpr (!ad_utility::areExpensiveChecksEnabled) {
    GTEST_SKIP_("Expensive checks are disabled, skipping test.");
  }
  auto idTable = makeIdTableFromVector({{1, 6, 0}, {2, 5, 0}, {3, 4, 0}});

  for (auto& generator : getAllSubSplits(idTable)) {
    Result result{std::move(generator), GetParam(), LocalVocab{}};
    EXPECT_NO_THROW(consumeGenerator(result.idTables()));
  }

  EXPECT_NO_THROW((Result{std::move(idTable), GetParam(), LocalVocab{}}));
}

INSTANTIATE_TEST_SUITE_P(SuccessCases, ResultSortTestS,
                         testing::Values(CIs{}, CIs{0}, CIs{0, 1}, CIs{2, 0}));

// _____________________________________________________________________________
TEST_P(ResultSortTestF, verifyAssertSortOrderIsRespectedThrowsWhenNotSorted) {
  if constexpr (!ad_utility::areExpensiveChecksEnabled) {
    GTEST_SKIP_("Expensive checks are disabled, skipping test.");
  }
  auto idTable = makeIdTableFromVector({{1, 6, 0}, {2, 5, 0}, {3, 4, 0}});

  for (auto& generator : getAllSubSplits(idTable)) {
    Result result{std::move(generator), GetParam(), LocalVocab{}};
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        consumeGenerator(result.idTables()),
        ::testing::HasSubstr("compareRowsBySortColumns"),
        ad_utility::Exception);
  }

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      (Result{std::move(idTable), GetParam(), LocalVocab{}}),
      ::testing::HasSubstr("compareRowsBySortColumns"), ad_utility::Exception);
}

INSTANTIATE_TEST_SUITE_P(FailureCases, ResultSortTestF,
                         testing::Values(CIs{1}, CIs{1, 0}, CIs{2, 1}));

// _____________________________________________________________________________
TEST(Result,
     verifyAnErrorIsThrownIfSortedByHasHigherIndicesThanTheTableHasColumns) {
  auto idTable = makeIdTableFromVector({{1, 6, 0}, {2, 5, 0}, {3, 4, 0}});
  using ad_utility::Exception;
  auto matcher = ::testing::HasSubstr("colIndex < idTable.numColumns()");

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      (Result{idTable.clone(), {3}, LocalVocab{}}), matcher, Exception);

  for (auto& generator : getAllSubSplits(idTable)) {
    Result result{std::move(generator), {3}, LocalVocab{}};
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(consumeGenerator(result.idTables()),
                                          matcher, Exception);
  }

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      (Result{idTable.clone(), {2, 1337}, LocalVocab{}}), matcher, Exception);

  for (auto& generator : getAllSubSplits(idTable)) {
    Result result{std::move(generator), {2, 1337}, LocalVocab{}};
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(consumeGenerator(result.idTables()),
                                          matcher, Exception);
  }
}

// _____________________________________________________________________________
TEST(Result, verifyRunOnNewChunkComputedThrowsWithFullyMaterializedResult) {
  Result result{makeIdTableFromVector({{}}), {}, LocalVocab{}};

  EXPECT_THROW(
      result.runOnNewChunkComputed(
          [](const IdTable&, std::chrono::microseconds) {}, [](bool) {}),
      ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(Result, verifyRunOnNewChunkComputedFiresCorrectly) {
  auto idTable1 = makeIdTableFromVector({{1, 6, 0}, {2, 5, 0}});
  auto idTable2 = makeIdTableFromVector({{3, 4, 0}});
  auto idTable3 = makeIdTableFromVector({{1, 6, 0}, {2, 5, 0}, {3, 4, 0}});

  Result result{
      [](auto& t1, auto& t2, auto& t3) -> cppcoro::generator<IdTable> {
        std::this_thread::sleep_for(1ms);
        co_yield t1;
        std::this_thread::sleep_for(3ms);
        co_yield t2;
        std::this_thread::sleep_for(5ms);
        co_yield t3;
      }(idTable1, idTable2, idTable3),
      {},
      LocalVocab{}};
  uint32_t callCounter = 0;
  bool finishedConsuming = false;

  result.runOnNewChunkComputed(
      [&](const IdTable& idTable, std::chrono::microseconds duration) {
        ++callCounter;
        if (callCounter == 1) {
          EXPECT_EQ(&idTable1, &idTable);
          EXPECT_GE(duration, 1ms);
        } else if (callCounter == 2) {
          EXPECT_EQ(&idTable2, &idTable);
          EXPECT_GE(duration, 3ms);
        } else if (callCounter == 3) {
          EXPECT_EQ(&idTable3, &idTable);
          EXPECT_GE(duration, 5ms);
        }
      },
      [&](bool error) {
        EXPECT_FALSE(error);
        finishedConsuming = true;
      });

  consumeGenerator(result.idTables());

  EXPECT_EQ(callCounter, 3);
  EXPECT_TRUE(finishedConsuming);
}

// _____________________________________________________________________________
TEST(Result, verifyRunOnNewChunkCallsFinishOnError) {
  Result result{
      []() -> cppcoro::generator<IdTable> {
        throw std::runtime_error{"verifyRunOnNewChunkCallsFinishOnError"};
        co_return;
      }(),
      {},
      LocalVocab{}};
  uint32_t callCounterGenerator = 0;
  uint32_t callCounterFinished = 0;

  result.runOnNewChunkComputed(
      [&](const IdTable&, std::chrono::microseconds) {
        ++callCounterGenerator;
      },
      [&](bool error) {
        EXPECT_TRUE(error);
        ++callCounterFinished;
      });

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      consumeGenerator(result.idTables()),
      ::testing::HasSubstr("verifyRunOnNewChunkCallsFinishOnError"),
      std::runtime_error);

  EXPECT_EQ(callCounterGenerator, 0);
  EXPECT_EQ(callCounterFinished, 1);
}

// _____________________________________________________________________________
TEST(Result, verifyRunOnNewChunkCallsFinishOnPartialConsumption) {
  uint32_t callCounterGenerator = 0;
  uint32_t callCounterFinished = 0;

  {
    Result result{[](IdTable idTable) -> cppcoro::generator<IdTable> {
                    co_yield idTable;
                  }(makeIdTableFromVector({{}})),
                  {},
                  LocalVocab{}};

    result.runOnNewChunkComputed(
        [&](const IdTable&, std::chrono::microseconds) {
          ++callCounterGenerator;
        },
        [&](bool error) {
          EXPECT_FALSE(error);
          ++callCounterFinished;
        });

    result.idTables().begin();
  }

  EXPECT_EQ(callCounterGenerator, 1);
  EXPECT_EQ(callCounterFinished, 1);
}

// _____________________________________________________________________________
TEST(Result, verifyCacheDuringConsumptionThrowsWhenFullyMaterialized) {
  Result result{makeIdTableFromVector({{}}), {}, LocalVocab{}};
  EXPECT_THROW(
      result.cacheDuringConsumption(
          [](const std::optional<IdTable>&, const IdTable&) { return true; },
          [](Result) {}),
      ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(Result, verifyCacheDuringConsumptionRespectsPassedParameters) {
  auto idTable = makeIdTableFromVector({{0, 7}, {1, 6}, {2, 5}, {3, 4}});

  // Test positive case
  for (auto& generator : getAllSubSplits(idTable)) {
    Result result{std::move(generator), {0}, LocalVocab{}};
    result.cacheDuringConsumption(
        [predictedSize = 0](const std::optional<IdTable>& aggregator,
                            const IdTable& newTable) mutable {
          if (aggregator.has_value()) {
            EXPECT_EQ(aggregator.value().numColumns(), predictedSize);
          } else {
            EXPECT_EQ(predictedSize, 0);
          }
          predictedSize += newTable.numColumns();
          return true;
        },
        [&](Result aggregatedResult) {
          EXPECT_TRUE(aggregatedResult.isFullyMaterialized());
          EXPECT_EQ(aggregatedResult.idTable(), idTable);
          EXPECT_EQ(aggregatedResult.sortedBy(), std::vector<ColumnIndex>{0});
        });
  }

  // Test negative case
  for (auto& generator : getAllSubSplits(idTable)) {
    uint32_t callCounter = 0;
    Result result{std::move(generator), {}, LocalVocab{}};
    result.cacheDuringConsumption(
        [&](const std::optional<IdTable>& aggregator, const IdTable&) {
          EXPECT_FALSE(aggregator.has_value());
          ++callCounter;
          return false;
        },
        [&](Result) { ++callCounter; });
    EXPECT_EQ(callCounter, 0);
  }
}

// _____________________________________________________________________________
TEST(Result, verifyApplyLimitOffsetDoesCorrectlyApplyLimitAndOffset) {
  auto idTable =
      makeIdTableFromVector({{0, 9}, {1, 8}, {2, 7}, {3, 6}, {4, 5}});
  LimitOffsetClause limitOffset{2, 2};
  {
    uint32_t callCounter = 0;
    Result result{idTable.clone(), {}, LocalVocab{}};
    result.applyLimitOffset(
        limitOffset, [&](std::chrono::microseconds, const IdTable& innerTable) {
          // NOTE: duration can't be tested here, processors are too fast
          auto comparisonTable = makeIdTableFromVector({{2, 7}, {3, 6}});
          EXPECT_EQ(innerTable, comparisonTable);
          ++callCounter;
        });
    EXPECT_EQ(callCounter, 1);
  }

  for (auto& generator : getAllSubSplits(idTable)) {
    std::vector<size_t> colSizes{};
    uint32_t totalRows = 0;
    Result result{std::move(generator), {}, LocalVocab{}};
    result.applyLimitOffset(
        limitOffset, [&](std::chrono::microseconds, const IdTable& innerTable) {
          // NOTE: duration can't be tested here, processors are too fast
          for (const auto& row : innerTable) {
            ASSERT_EQ(row.size(), 2);
            // Make sure we never get values that were supposed to be filtered
            // out.
            EXPECT_NE(row[0].getVocabIndex().get(), 0);
            EXPECT_NE(row[0].getVocabIndex().get(), 1);
            EXPECT_NE(row[0].getVocabIndex().get(), 4);
            EXPECT_NE(row[1].getVocabIndex().get(), 9);
            EXPECT_NE(row[1].getVocabIndex().get(), 8);
            EXPECT_NE(row[1].getVocabIndex().get(), 5);
          }
          totalRows += innerTable.size();
          colSizes.push_back(innerTable.numColumns());
        });

    EXPECT_EQ(totalRows, 0);
    EXPECT_TRUE(colSizes.empty());

    consumeGenerator(result.idTables());

    EXPECT_EQ(totalRows, 2);
    EXPECT_THAT(colSizes, ::testing::Each(testing::Eq(2)));
  }
}

// _____________________________________________________________________________
TEST(Result, verifyApplyLimitOffsetHandlesZeroLimitCorrectly) {
  auto idTable = makeIdTableFromVector({{0, 7}, {1, 6}, {2, 5}, {3, 4}});
  LimitOffsetClause limitOffset{0, 1};
  {
    uint32_t callCounter = 0;
    Result result{idTable.clone(), {}, LocalVocab{}};
    result.applyLimitOffset(
        limitOffset, [&](std::chrono::microseconds, const IdTable& innerTable) {
          EXPECT_EQ(innerTable.numRows(), 0);
          ++callCounter;
        });
    EXPECT_EQ(callCounter, 1);
  }

  for (auto& generator : getAllSubSplits(idTable)) {
    uint32_t callCounter = 0;
    Result result{std::move(generator), {}, LocalVocab{}};
    result.applyLimitOffset(
        limitOffset,
        [&](std::chrono::microseconds, const IdTable&) { ++callCounter; });

    consumeGenerator(result.idTables());

    EXPECT_EQ(callCounter, 0);
  }
}

// _____________________________________________________________________________
using LIC = LimitOffsetClause;
class ResultLimitTestS : public testing::TestWithParam<LimitOffsetClause> {};
class ResultLimitTestF : public testing::TestWithParam<LimitOffsetClause> {};

TEST_P(ResultLimitTestS,
       verifyAssertThatLimitWasRespectedDoesNotThrowIfLimitWasRespected) {
  auto idTable = makeIdTableFromVector({{0, 7}, {1, 6}, {2, 5}, {3, 4}});
  {
    Result result{idTable.clone(), {}, LocalVocab{}};
    EXPECT_NO_THROW(result.assertThatLimitWasRespected(GetParam()));
  }

  for (auto& generator : getAllSubSplits(idTable)) {
    Result result{std::move(generator), {}, LocalVocab{}};
    result.assertThatLimitWasRespected(GetParam());
    EXPECT_NO_THROW(consumeGenerator(result.idTables()));
  }
}

INSTANTIATE_TEST_SUITE_P(SuccessCases, ResultLimitTestS,
                         testing::Values(LIC{}, LIC{4, 0}, LIC{4, 1337},
                                         LIC{42, 0}, LIC{42, 1337}));

// _____________________________________________________________________________
TEST_P(ResultLimitTestF,
       verifyAssertThatLimitWasRespectedDoesThrowIfLimitWasNotRespected) {
  auto idTable = makeIdTableFromVector({{0, 7}, {1, 6}, {2, 5}, {3, 4}});
  {
    Result result{idTable.clone(), {}, LocalVocab{}};
    EXPECT_THROW(result.assertThatLimitWasRespected(GetParam()),
                 ad_utility::Exception);
  }

  for (auto& generator : getAllSubSplits(idTable)) {
    Result result{std::move(generator), {}, LocalVocab{}};
    result.assertThatLimitWasRespected(GetParam());
    EXPECT_THROW(consumeGenerator(result.idTables()), ad_utility::Exception);
  }
}

INSTANTIATE_TEST_SUITE_P(FailureCases, ResultLimitTestF,
                         testing::Values(LIC{3, 0}, LIC{3, 1}, LIC{3, 2}));

// _____________________________________________________________________________
class ResultDefinednessTestS : public testing::TestWithParam<const IdTable*> {};
class ResultDefinednessTestF : public testing::TestWithParam<const IdTable*> {};

auto u = Id::makeUndefined();
auto correctTable1 = makeIdTableFromVector({{0, 7}, {1, 6}, {2, 5}, {3, 4}});
auto correctTable2 = makeIdTableFromVector({{0, u}, {1, 6}, {2, 5}, {3, 4}});
auto correctTable3 = makeIdTableFromVector({{0, 7}, {1, 6}, {2, 5}, {3, u}});
auto correctTable4 = makeIdTableFromVector({{0, u}, {1, u}, {2, u}, {3, u}});
auto wrongTable1 = makeIdTableFromVector({{u, 7}, {1, 6}, {2, 5}, {3, 4}});
auto wrongTable2 = makeIdTableFromVector({{u, 7}, {u, 6}, {u, 5}, {u, 4}});
auto wrongTable3 = makeIdTableFromVector({{0, 7}, {1, 6}, {2, 5}, {u, 4}});

TEST_P(ResultDefinednessTestS,
       verifyCheckDefinednessDoesNotThrowIfColumnIsDefined) {
  if constexpr (!ad_utility::areExpensiveChecksEnabled) {
    GTEST_SKIP_("Expensive checks are disabled, skipping test.");
  }
  VariableToColumnMap map{
      {Variable{"?a"}, {0, ColumnIndexAndTypeInfo::AlwaysDefined}},
      {Variable{"?b"}, {1, ColumnIndexAndTypeInfo::PossiblyUndefined}}};

  {
    Result result{GetParam()->clone(), {}, LocalVocab{}};
    EXPECT_NO_THROW(result.checkDefinedness(map));
  }
  for (auto& generator : getAllSubSplits(*GetParam())) {
    Result result{std::move(generator), {}, LocalVocab{}};
    result.checkDefinedness(map);
    EXPECT_NO_THROW(consumeGenerator(result.idTables()));
  }
}

INSTANTIATE_TEST_SUITE_P(SuccessCases, ResultDefinednessTestS,
                         testing::Values(&correctTable1, &correctTable2,
                                         &correctTable3, &correctTable4));

// _____________________________________________________________________________
TEST_P(ResultDefinednessTestF,
       verifyCheckDefinednessDoesThrowIfColumnIsNotDefinedWhenClaimingItIs) {
  if constexpr (!ad_utility::areExpensiveChecksEnabled) {
    GTEST_SKIP_("Expensive checks are disabled, skipping test.");
  }
  VariableToColumnMap map{
      {Variable{"?a"}, {0, ColumnIndexAndTypeInfo::AlwaysDefined}},
      {Variable{"?b"}, {1, ColumnIndexAndTypeInfo::PossiblyUndefined}}};

  {
    Result result{GetParam()->clone(), {}, LocalVocab{}};
    EXPECT_THROW(result.checkDefinedness(map), ad_utility::Exception);
  }
  for (auto& generator : getAllSubSplits(*GetParam())) {
    Result result{std::move(generator), {}, LocalVocab{}};
    result.checkDefinedness(map);
    EXPECT_THROW(consumeGenerator(result.idTables()), ad_utility::Exception);
  }
}

INSTANTIATE_TEST_SUITE_P(FailureCases, ResultDefinednessTestF,
                         testing::Values(&wrongTable1, &wrongTable2,
                                         &wrongTable3));
