//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "engine/Result.h"
#include "util/IdTableHelpers.h"

using namespace std::chrono_literals;
using ::testing::AnyOf;
using ::testing::Combine;
using ::testing::HasSubstr;
using ::testing::Not;
using ::testing::Values;

namespace {
using IdTableVocabPair = Result::IdTableVocabPair;

// Helper function to generate all possible splits of an IdTable in order to
// exhaustively test generator variants.
std::vector<Result::Generator> getAllSubSplits(const IdTable& idTable) {
  std::vector<Result::Generator> result;
  for (size_t i = 0; i < std::pow(idTable.size() - 1, 2); ++i) {
    std::vector<size_t> reverseIndex{};
    size_t copy = i;
    for (size_t index = 0; index < idTable.size(); ++index) {
      if (copy % 2 == 1) {
        reverseIndex.push_back(index);
      }
      copy /= 2;
    }
    result.push_back([](auto split, IdTable clone) -> Result::Generator {
      IdTable subSplit{clone.numColumns(),
                       ad_utility::makeUnlimitedAllocator<IdTable>()};
      size_t splitIndex = 0;
      for (size_t i = 0; i < clone.size(); ++i) {
        subSplit.push_back(clone[i]);
        if (splitIndex < split.size() && split[splitIndex] == i) {
          IdTableVocabPair pair{std::move(subSplit), LocalVocab{}};
          co_yield pair;
          // Move back if not moved out to reuse buffer.
          subSplit = std::move(pair.idTable_);
          subSplit.clear();
          ++splitIndex;
        }
      }
      if (subSplit.size() > 0) {
        co_yield {std::move(subSplit), LocalVocab{}};
      }
    }(std::move(reverseIndex), idTable.clone()));
  }

  return result;
}

// _____________________________________________________________________________
void consumeGenerator(Result::LazyResult generator) {
  for ([[maybe_unused]] IdTableVocabPair& _ : generator) {
  }
}
}  // namespace

TEST(Result, verifyIdTableThrowsWhenActuallyLazy) {
  Result result{[]() -> Result::Generator { co_return; }(), {}};
  EXPECT_FALSE(result.isFullyMaterialized());
  EXPECT_THROW(result.idTable(), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(Result, verifyIdTableThrowsOnSecondAccess) {
  const Result result{[]() -> Result::Generator { co_return; }(), {}};
  // First access should work
  for ([[maybe_unused]] IdTableVocabPair& _ : result.idTables()) {
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
class ResultSortTest : public testing::TestWithParam<std::tuple<bool, CIs>> {};

TEST_P(ResultSortTest, verifyAssertSortOrderIsRespectedSucceedsWhenSorted) {
  if constexpr (!ad_utility::areExpensiveChecksEnabled) {
    GTEST_SKIP_("Expensive checks are disabled, skipping test.");
  }
  auto idTable = makeIdTableFromVector({{1, 6, 0}, {2, 5, 0}, {3, 4, 0}});

  for (auto& generator : getAllSubSplits(idTable)) {
    Result result{std::move(generator), std::get<1>(GetParam())};
    if (std::get<0>(GetParam())) {
      EXPECT_NO_THROW(consumeGenerator(result.idTables()));
    } else {
      AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
          consumeGenerator(result.idTables()),
          HasSubstr("compareRowsBySortColumns"), ad_utility::Exception);
    }
  }

  if (std::get<0>(GetParam())) {
    EXPECT_NO_THROW(
        (Result{std::move(idTable), std::get<1>(GetParam()), LocalVocab{}}));
  } else {
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
        (Result{std::move(idTable), std::get<1>(GetParam()), LocalVocab{}}),
        HasSubstr("compareRowsBySortColumns"), ad_utility::Exception);
  }
}

INSTANTIATE_TEST_SUITE_P(SuccessCases, ResultSortTest,
                         Combine(Values(true),
                                 Values(CIs{}, CIs{0}, CIs{0, 1}, CIs{2, 0})));

INSTANTIATE_TEST_SUITE_P(FailureCases, ResultSortTest,
                         Combine(Values(false),
                                 Values(CIs{1}, CIs{1, 0}, CIs{2, 1})));

// _____________________________________________________________________________
TEST(Result,
     verifyAnErrorIsThrownIfSortedByHasHigherIndicesThanTheTableHasColumns) {
  auto idTable = makeIdTableFromVector({{1, 6, 0}, {2, 5, 0}, {3, 4, 0}});
  using ad_utility::Exception;
  auto matcher = HasSubstr("colIndex < idTable.numColumns()");

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      (Result{idTable.clone(), {3}, LocalVocab{}}), matcher, Exception);

  for (auto& generator : getAllSubSplits(idTable)) {
    Result result{std::move(generator), {3}};
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(consumeGenerator(result.idTables()),
                                          matcher, Exception);
  }

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      (Result{idTable.clone(), {2, 1337}, LocalVocab{}}), matcher, Exception);

  for (auto& generator : getAllSubSplits(idTable)) {
    Result result{std::move(generator), {2, 1337}};
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(consumeGenerator(result.idTables()),
                                          matcher, Exception);
  }
}

// _____________________________________________________________________________
TEST(Result, verifyRunOnNewChunkComputedThrowsWithFullyMaterializedResult) {
  Result result{makeIdTableFromVector({{}}), {}, LocalVocab{}};

  EXPECT_THROW(result.runOnNewChunkComputed(
                   [](const IdTableVocabPair&, std::chrono::microseconds) {},
                   [](bool) {}),
               ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(Result, verifyRunOnNewChunkComputedFiresCorrectly) {
  auto idTable1 = makeIdTableFromVector({{1, 6, 0}, {2, 5, 0}});
  auto idTable2 = makeIdTableFromVector({{3, 4, 0}});
  auto idTable3 = makeIdTableFromVector({{1, 6, 0}, {2, 5, 0}, {3, 4, 0}});

  Result result{
      [](auto& t1, auto& t2, auto& t3) -> Result::Generator {
        std::this_thread::sleep_for(1ms);
        LocalVocab localVocab{};
        localVocab.getIndexAndAddIfNotContained(LocalVocabEntry{
            ad_utility::triple_component::Literal::literalWithoutQuotes(
                "Test")});
        co_yield {t1.clone(), std::move(localVocab)};
        std::this_thread::sleep_for(3ms);
        co_yield {t2.clone(), LocalVocab{}};
        std::this_thread::sleep_for(5ms);
        co_yield {t3.clone(), LocalVocab{}};
      }(idTable1, idTable2, idTable3),
      {}};
  uint32_t callCounter = 0;
  bool finishedConsuming = false;

  result.runOnNewChunkComputed(
      [&](const IdTableVocabPair& pair, std::chrono::microseconds duration) {
        const IdTable& idTable = pair.idTable_;
        ++callCounter;
        if (callCounter == 1) {
          EXPECT_EQ(idTable1, idTable);
          EXPECT_EQ(pair.localVocab_.size(), 1);
          EXPECT_GE(duration, 1ms);
        } else if (callCounter == 2) {
          EXPECT_EQ(idTable2, idTable);
          EXPECT_EQ(pair.localVocab_.size(), 0);
          EXPECT_GE(duration, 3ms);
        } else if (callCounter == 3) {
          EXPECT_EQ(idTable3, idTable);
          EXPECT_EQ(pair.localVocab_.size(), 0);
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
      []() -> Result::Generator {
        throw std::runtime_error{"verifyRunOnNewChunkCallsFinishOnError"};
        co_return;
      }(),
      {}};
  uint32_t callCounterGenerator = 0;
  uint32_t callCounterFinished = 0;

  result.runOnNewChunkComputed(
      [&](const IdTableVocabPair&, std::chrono::microseconds) {
        ++callCounterGenerator;
      },
      [&](bool error) {
        EXPECT_TRUE(error);
        ++callCounterFinished;
      });

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      consumeGenerator(result.idTables()),
      HasSubstr("verifyRunOnNewChunkCallsFinishOnError"), std::runtime_error);

  EXPECT_EQ(callCounterGenerator, 0);
  EXPECT_EQ(callCounterFinished, 1);
}

// _____________________________________________________________________________
TEST(Result, verifyRunOnNewChunkCallsFinishOnPartialConsumption) {
  uint32_t callCounterGenerator = 0;
  uint32_t callCounterFinished = 0;

  {
    Result result{[](IdTable idTable) -> Result::Generator {
                    co_yield {std::move(idTable), LocalVocab{}};
                  }(makeIdTableFromVector({{}})),
                  {}};

    result.runOnNewChunkComputed(
        [&](const IdTableVocabPair&, std::chrono::microseconds) {
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
  EXPECT_THROW(result.cacheDuringConsumption(
                   [](const std::optional<IdTableVocabPair>&,
                      const IdTableVocabPair&) { return true; },
                   [](Result) {}),
               ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(Result, verifyCacheDuringConsumptionRespectsPassedParameters) {
  auto idTable = makeIdTableFromVector({{0, 7}, {1, 6}, {2, 5}, {3, 4}});

  // Test positive case
  for (auto& generator : getAllSubSplits(idTable)) {
    Result result{std::move(generator), {0}};
    result.cacheDuringConsumption(
        [predictedSize = 0](const std::optional<IdTableVocabPair>& aggregator,
                            const IdTableVocabPair& newTable) mutable {
          if (aggregator.has_value()) {
            EXPECT_EQ(aggregator.value().idTable_.numColumns(), predictedSize);
          } else {
            EXPECT_EQ(predictedSize, 0);
          }
          predictedSize += newTable.idTable_.numColumns();
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
    Result result{std::move(generator), {}};
    result.cacheDuringConsumption(
        [&](const std::optional<IdTableVocabPair>& aggregator,
            const IdTableVocabPair&) {
          EXPECT_FALSE(aggregator.has_value());
          ++callCounter;
          return false;
        },
        [&](Result) { ++callCounter; });
    EXPECT_EQ(callCounter, 0);
  }
}

// _____________________________________________________________________________
TEST(Result, cacheDuringConsumptionAbortsValueWhenRunningIntoMemoryLimit) {
  bool flag = false;
  Result result{[](bool& innerFlag) -> Result::Generator {
                  co_yield {IdTable{1, ad_utility::makeAllocatorWithLimit<Id>(
                                           ad_utility::MemorySize::bytes(0))},
                            LocalVocab{}};
                  IdTable idTable{1, ad_utility::makeUnlimitedAllocator<Id>()};
                  idTable.push_back({Id::makeFromBool(true)});
                  co_yield {std::move(idTable), LocalVocab{}};
                  innerFlag = true;
                }(flag),
                {0}};
  result.cacheDuringConsumption(
      [](const std::optional<IdTableVocabPair>&, const IdTableVocabPair&) {
        return true;
      },
      [&](Result) { ADD_FAILURE() << "The result should not get cached."; });
  consumeGenerator(result.idTables());
  EXPECT_TRUE(flag);
}

// _____________________________________________________________________________
TEST(
    Result,
    cacheDuringConsumptionAbortsValueWhenRunningIntoMemoryLimitOnInitialClone) {
  bool flag = false;
  Result result{[](bool& innerFlag) -> Result::Generator {
                  IdTable idTable{
                      1, ad_utility::makeAllocatorWithLimit<Id>(
                             ad_utility::MemorySize::bytes(1) * sizeof(Id))};
                  idTable.push_back({Id::makeFromBool(true)});
                  co_yield {std::move(idTable), LocalVocab{}};
                  innerFlag = true;
                }(flag),
                {0}};
  result.cacheDuringConsumption(
      [](const std::optional<IdTableVocabPair>&, const IdTableVocabPair&) {
        return true;
      },
      [&](Result) { ADD_FAILURE() << "The result should not get cached."; });
  consumeGenerator(result.idTables());
  EXPECT_TRUE(flag);
}

// _____________________________________________________________________________
TEST(Result, verifyApplyLimitOffsetDoesCorrectlyApplyLimitAndOffset) {
  auto idTable =
      makeIdTableFromVector({{0, 9}, {1, 8}, {2, 7}, {3, 6}, {4, 5}});
  LimitOffsetClause limitOffset{2, 2};
  {
    auto comparisonTable = makeIdTableFromVector({{2, 7}, {3, 6}});
    uint32_t callCounter = 0;
    Result result{idTable.clone(), {}, LocalVocab{}};
    result.applyLimitOffset(
        limitOffset, [&](std::chrono::microseconds, const IdTable& innerTable) {
          // NOTE: duration can't be tested here, processors are too fast
          EXPECT_EQ(innerTable, comparisonTable);
          ++callCounter;
        });
    EXPECT_EQ(callCounter, 1);
    EXPECT_EQ(result.idTable(), comparisonTable);
  }

  for (auto& generator : getAllSubSplits(idTable)) {
    std::vector<size_t> colSizes{};
    uint32_t totalRows = 0;
    Result result{std::move(generator), {}};
    result.applyLimitOffset(
        limitOffset, [&](std::chrono::microseconds, const IdTable& innerTable) {
          // NOTE: duration can't be tested here, processors are too fast
          for (const auto& row : innerTable) {
            ASSERT_EQ(row.size(), 2);
            // Make sure we never get values that were supposed to be filtered
            // out.
            EXPECT_THAT(row[0].getVocabIndex().get(), Not(AnyOf(0, 1, 4)));
            EXPECT_THAT(row[1].getVocabIndex().get(), Not(AnyOf(9, 8, 5)));
          }
          totalRows += innerTable.size();
          colSizes.push_back(innerTable.numColumns());
        });

    EXPECT_EQ(totalRows, 0);
    EXPECT_TRUE(colSizes.empty());

    for (const auto& innerTable : result.idTables()) {
      for (const auto& row : innerTable.idTable_) {
        ASSERT_EQ(row.size(), 2);
        // Make sure we never get values that were supposed to be filtered
        // out.
        EXPECT_THAT(row[0].getVocabIndex().get(), Not(AnyOf(0, 1, 4)));
        EXPECT_THAT(row[1].getVocabIndex().get(), Not(AnyOf(9, 8, 5)));
      }
    }

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
    Result result{std::move(generator), {}};
    result.applyLimitOffset(
        limitOffset,
        [&](std::chrono::microseconds, const IdTable&) { ++callCounter; });

    consumeGenerator(result.idTables());

    EXPECT_EQ(callCounter, 0);
  }
}

// _____________________________________________________________________________
TEST(Result, verifyApplyLimitOffsetHandlesNonZeroOffsetWithoutLimitCorrectly) {
  auto idTable = makeIdTableFromVector({{0, 7}, {1, 6}, {2, 5}, {3, 4}});
  LimitOffsetClause limitOffset{std::nullopt, 1};
  {
    uint32_t callCounter = 0;
    Result result{idTable.clone(), {}, LocalVocab{}};
    result.applyLimitOffset(
        limitOffset, [&](std::chrono::microseconds, const IdTable& innerTable) {
          EXPECT_EQ(innerTable.numRows(), 3);
          ++callCounter;
        });
    EXPECT_EQ(callCounter, 1);
  }

  for (auto& generator : getAllSubSplits(idTable)) {
    uint32_t callCounter = 0;
    Result result{std::move(generator), {}};
    result.applyLimitOffset(
        limitOffset, [&](std::chrono::microseconds, const IdTable& innerTable) {
          for (const auto& row : innerTable) {
            ASSERT_EQ(row.size(), 2);
            // Make sure we never get values that were supposed to be filtered
            // out.
            EXPECT_NE(row[0].getVocabIndex().get(), 0);
            EXPECT_NE(row[1].getVocabIndex().get(), 7);
          }
          ++callCounter;
        });

    consumeGenerator(result.idTables());

    EXPECT_GE(callCounter, 1);
  }
}

// _____________________________________________________________________________
TEST(Result, verifyApplyLimitOffsetIsNoOpWhenLimitClauseIsRedundant) {
  auto idTable = makeIdTableFromVector({{0, 7}, {1, 6}, {2, 5}, {3, 4}});
  LimitOffsetClause limitOffset{std::nullopt, 0};
  uint32_t callCounter = 0;
  {
    Result result{idTable.clone(), {}, LocalVocab{}};
    result.applyLimitOffset(
        limitOffset,
        [&](std::chrono::microseconds, const IdTable&) { ++callCounter; });
    EXPECT_EQ(callCounter, 0);
  }

  for (auto& generator : getAllSubSplits(idTable)) {
    Result result{std::move(generator), {}};
    result.applyLimitOffset(
        limitOffset,
        [&](std::chrono::microseconds, const IdTable&) { ++callCounter; });

    consumeGenerator(result.idTables());

    EXPECT_EQ(callCounter, 0);
  }
}

// _____________________________________________________________________________
using LIC = LimitOffsetClause;
class ResultLimitTest : public testing::TestWithParam<std::tuple<bool, LIC>> {};

TEST_P(ResultLimitTest,
       verifyAssertThatLimitWasRespectedDoesNotThrowIfLimitWasRespected) {
  auto idTable = makeIdTableFromVector({{0, 7}, {1, 6}, {2, 5}, {3, 4}});
  {
    Result result{idTable.clone(), {}, LocalVocab{}};
    if (std::get<0>(GetParam())) {
      EXPECT_NO_THROW(
          result.assertThatLimitWasRespected(std::get<1>(GetParam())));
    } else {
      EXPECT_THROW(result.assertThatLimitWasRespected(std::get<1>(GetParam())),
                   ad_utility::Exception);
    }
  }

  for (auto& generator : getAllSubSplits(idTable)) {
    Result result{std::move(generator), {}};
    result.assertThatLimitWasRespected(std::get<1>(GetParam()));

    if (std::get<0>(GetParam())) {
      EXPECT_NO_THROW(consumeGenerator(result.idTables()));
    } else {
      EXPECT_THROW(consumeGenerator(result.idTables()), ad_utility::Exception);
    }
  }
}

INSTANTIATE_TEST_SUITE_P(SuccessCases, ResultLimitTest,
                         Combine(Values(true),
                                 Values(LIC{}, LIC{4, 0}, LIC{4, 1337},
                                        LIC{42, 0}, LIC{42, 1337})));

INSTANTIATE_TEST_SUITE_P(FailureCases, ResultLimitTest,
                         Combine(Values(false),
                                 Values(LIC{3, 0}, LIC{3, 1}, LIC{3, 2})));

// _____________________________________________________________________________
class ResultDefinednessTest
    : public testing::TestWithParam<std::tuple<bool, const IdTable*>> {};

auto u = Id::makeUndefined();
auto correctTable1 = makeIdTableFromVector({{0, 7}, {1, 6}, {2, 5}, {3, 4}});
auto correctTable2 = makeIdTableFromVector({{0, u}, {1, 6}, {2, 5}, {3, 4}});
auto correctTable3 = makeIdTableFromVector({{0, 7}, {1, 6}, {2, 5}, {3, u}});
auto correctTable4 = makeIdTableFromVector({{0, u}, {1, u}, {2, u}, {3, u}});
auto wrongTable1 = makeIdTableFromVector({{u, 7}, {1, 6}, {2, 5}, {3, 4}});
auto wrongTable2 = makeIdTableFromVector({{u, 7}, {u, 6}, {u, 5}, {u, 4}});
auto wrongTable3 = makeIdTableFromVector({{0, 7}, {1, 6}, {2, 5}, {u, 4}});

TEST_P(ResultDefinednessTest,
       verifyCheckDefinednessDoesThrowIfColumnIsNotDefinedWhenClaimingItIs) {
  if constexpr (!ad_utility::areExpensiveChecksEnabled) {
    GTEST_SKIP_("Expensive checks are disabled, skipping test.");
  }
  VariableToColumnMap map{
      {Variable{"?a"}, {0, ColumnIndexAndTypeInfo::AlwaysDefined}},
      {Variable{"?b"}, {1, ColumnIndexAndTypeInfo::PossiblyUndefined}}};

  {
    Result result{std::get<1>(GetParam())->clone(), {}, LocalVocab{}};
    if (std::get<0>(GetParam())) {
      EXPECT_NO_THROW(result.checkDefinedness(map));
    } else {
      EXPECT_THROW(result.checkDefinedness(map), ad_utility::Exception);
    }
  }
  for (auto& generator : getAllSubSplits(*std::get<1>(GetParam()))) {
    Result result{std::move(generator), {}};
    result.checkDefinedness(map);
    if (std::get<0>(GetParam())) {
      EXPECT_NO_THROW(consumeGenerator(result.idTables()));
    } else {
      EXPECT_THROW(consumeGenerator(result.idTables()), ad_utility::Exception);
    }
  }
}

INSTANTIATE_TEST_SUITE_P(SuccessCases, ResultDefinednessTest,
                         Combine(Values(true),
                                 Values(&correctTable1, &correctTable2,
                                        &correctTable3, &correctTable4)));

INSTANTIATE_TEST_SUITE_P(
    FailureCases, ResultDefinednessTest,
    Combine(Values(false), Values(&wrongTable1, &wrongTable2, &wrongTable3)));
