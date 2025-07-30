//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <type_traits>

#include "./util/IdTableHelpers.h"
#include "backports/concepts.h"
#include "engine/AddCombinedRowToTable.h"

namespace {
static constexpr auto U = Id::makeUndefined();

class RowAdderTest : public ::testing::TestWithParam<std::tuple<size_t, bool>> {
};

// Check that the result of the `adder` matches the `expectedResult` and that
// the number of undefined values per columns (as reported by the `adder`) match
// the `expectedNumUndefined`. If `keepJoinColumns` is `false`, then the first
// `numJoinColumns` columns will be removed from `expectedResult` and
// `expectedNumUndefined`respectively before comparing them to the actual
// result.
void testAdder(ad_utility::AddCombinedRowToIdTable& adder,
               IdTable& expectedResult,
               std::vector<size_t> expectedNumUndefined, size_t numJoinColumns,
               bool keepJoinColumns) {
  auto numUndefined = adder.numUndefinedPerColumn();
  auto result = std::move(adder).resultTable();
  if (!keepJoinColumns) {
    for ([[maybe_unused]] auto i : ad_utility::integerRange(numJoinColumns)) {
      expectedResult.deleteColumn(0);
      expectedNumUndefined.erase(expectedNumUndefined.begin());
    }
  }
  EXPECT_EQ(result, expectedResult);

  EXPECT_EQ(numUndefined, expectedNumUndefined);
}

// _______________________________________________________________________________
TEST_P(RowAdderTest, OneJoinColumn) {
  auto [bufferSize, keepJoinColumns] = GetParam();
  auto left = makeIdTableFromVector({{3, 4}, {7, 8}, {11, 10}, {14, 11}});
  auto right =
      makeIdTableFromVector({{7, 14, 0}, {9, 10, 1}, {14, 8, 2}, {33, 5, 3}});
  auto result = makeIdTableFromVector({});
  size_t numColsResult = keepJoinColumns ? 4 : 3;
  result.setNumColumns(numColsResult);
  auto adder = ad_utility::AddCombinedRowToIdTable(
      1, left.asStaticView<0>(), right.asStaticView<0>(), std::move(result),
      std::make_shared<ad_utility::CancellationHandle<>>(), keepJoinColumns,
      bufferSize);
  adder.addRow(1, 0);
  adder.setOnlyLeftInputForOptionalJoin(left);
  adder.addOptionalRow(2);
  adder.setInput(left, right);
  adder.addRow(3, 2);

  auto expected =
      makeIdTableFromVector({{7, 8, 14, 0}, {11, 10, U, U}, {14, 11, 8, 2}});
  auto expectedUndefined = std::vector<size_t>{0, 0, 1, 1};
  testAdder(adder, expected, expectedUndefined, 1, keepJoinColumns);
};

// _______________________________________________________________________________
TEST_P(RowAdderTest, AddRows) {
  auto [bufferSize, keepJoinColumns] = GetParam();
  auto left = makeIdTableFromVector({{3, 4}, {7, 8}, {7, 10}, {14, 11}});
  auto right =
      makeIdTableFromVector({{7, 14, 0}, {7, 12, 1}, {14, 8, 2}, {33, 5, 3}});
  auto result = makeIdTableFromVector({});
  size_t numColsResult = keepJoinColumns ? 4 : 3;
  result.setNumColumns(numColsResult);
  auto adder = ad_utility::AddCombinedRowToIdTable(
      1, left.asStaticView<0>(), right.asStaticView<0>(), std::move(result),
      std::make_shared<ad_utility::CancellationHandle<>>(), keepJoinColumns,
      bufferSize);
  adder.addOptionalRow(0);
  adder.addRows(ql::views::iota(1u, 3u), ql::views::iota(0u, 2u));

  auto expected = makeIdTableFromVector({{3, 4, U, U},
                                         {7, 8, 14, 0},
                                         {7, 8, 12, 1},
                                         {7, 10, 14, 0},
                                         {7, 10, 12, 1}});
  auto expectedUndefined = std::vector<size_t>{0, 0, 1, 1};
  testAdder(adder, expected, expectedUndefined, 1, keepJoinColumns);
}

// _______________________________________________________________________________
TEST_P(RowAdderTest, AddRowsZeroColumns) {
  auto [bufferSize, keepJoinColumns] = GetParam();
  auto left = makeIdTableFromVector({{3}, {3}, {3}, {7}});
  auto right = makeIdTableFromVector({{2}, {3}, {3}, {5}});
  auto result = makeIdTableFromVector({});
  size_t numColsResult = keepJoinColumns ? 1 : 0;
  result.setNumColumns(numColsResult);
  auto adder = ad_utility::AddCombinedRowToIdTable(
      1, left.asStaticView<0>(), right.asStaticView<0>(), std::move(result),
      std::make_shared<ad_utility::CancellationHandle<>>(), keepJoinColumns,
      bufferSize);
  adder.addRows(ql::views::iota(0u, 3u), ql::views::iota(1u, 3u));
  adder.addOptionalRow(3);

  auto expected = makeIdTableFromVector({{3}, {3}, {3}, {3}, {3}, {3}, {7}});
  auto expectedUndefined = std::vector<size_t>{0};
  testAdder(adder, expected, expectedUndefined, 1, keepJoinColumns);
}

// _______________________________________________________________________________
TEST_P(RowAdderTest, TwoJoinColumns) {
  auto [bufferSize, keepJoinColumns] = GetParam();
  auto left = makeIdTableFromVector({{3, 4}, {7, 8}, {11, 10}, {14, U}});
  auto right =
      makeIdTableFromVector({{U, 8, 0}, {9, 10, 1}, {14, 11, 2}, {33, 5, 3}});
  auto result = makeIdTableFromVector({});
  static constexpr size_t numJoinCols = 2;
  size_t numColsResult = keepJoinColumns ? 3 : 1;
  result.setNumColumns(numColsResult);
  auto adder = ad_utility::AddCombinedRowToIdTable(
      numJoinCols, left.asStaticView<0>(), right.asStaticView<0>(),
      std::move(result), std::make_shared<ad_utility::CancellationHandle<>>(),
      keepJoinColumns, bufferSize);
  adder.addRow(1, 0);
  adder.addOptionalRow(2);
  adder.addRow(3, 2);

  auto expected = makeIdTableFromVector({{7, 8, 0}, {11, 10, U}, {14, 11, 2}});
  auto expectedUndefined = std::vector<size_t>{0, 0, 1};
  testAdder(adder, expected, expectedUndefined, numJoinCols, keepJoinColumns);
}

// _______________________________________________________________________________
TEST_P(RowAdderTest, UndefInInput) {
  auto [bufferSize, keepJoinCol] = GetParam();
  auto left = makeIdTableFromVector({{U, 5}, {2, U}, {3, U}, {4, U}});
  auto right = makeIdTableFromVector({{1}, {3}, {4}, {U}});
  auto result = makeIdTableFromVector({});
  result.setNumColumns(keepJoinCol ? 2 : 1);
  auto adder = ad_utility::AddCombinedRowToIdTable(
      1, left.asStaticView<0>(), right.asStaticView<0>(), std::move(result),
      std::make_shared<ad_utility::CancellationHandle<>>(), keepJoinCol,
      bufferSize);
  adder.addRow(0, 0);
  adder.addRow(0, 1);
  adder.addRow(2, 1);
  adder.addRow(0, 2);
  adder.addRow(3, 2);
  adder.addRow(0, 3);

  auto expected =
      makeIdTableFromVector({{1, 5}, {3, 5}, {3, U}, {4, 5}, {4, U}, {U, 5}});
  auto expectedUndefined = std::vector<size_t>{1, 2};
  testAdder(adder, expected, expectedUndefined, 1, keepJoinCol);
}

// _______________________________________________________________________________
TEST_P(RowAdderTest, setInput) {
  auto [bufferSize, keepJoinCol] = GetParam();
  {
    auto result = makeIdTableFromVector({});
    result.setNumColumns(keepJoinCol ? 2 : 1);
    auto adder = ad_utility::AddCombinedRowToIdTable(
        1, std::move(result),
        std::make_shared<ad_utility::CancellationHandle<>>(), keepJoinCol,
        bufferSize);
    // It is okay to flush even if no inputs were specified, as long as we
    // haven't pushed any rows yet.
    EXPECT_NO_THROW(adder.flush());

    if (ad_utility::areExpensiveChecksEnabled || bufferSize <= 1) {
      EXPECT_ANY_THROW(adder.addRow(0, 0));
    } else {
      adder.addRow(0, 0);
      EXPECT_ANY_THROW(adder.flush());
    }
  }

  auto result = makeIdTableFromVector({});
  result.setNumColumns(keepJoinCol ? 3 : 2);
  auto adder = ad_utility::AddCombinedRowToIdTable(
      1, std::move(result),
      std::make_shared<ad_utility::CancellationHandle<>>(), keepJoinCol,
      bufferSize);
  auto left = makeIdTableFromVector({{U, 5}, {2, U}, {3, U}, {4, U}});
  auto right = makeIdTableFromVector({{1, 2}, {3, 4}, {4, 7}, {U, 8}});
  adder.setInput(left, right);
  adder.addRow(0, 0);
  adder.addRow(0, 1);
  adder.addRow(2, 1);
  adder.addRow(0, 2);
  adder.addRow(3, 2);
  adder.addRow(0, 3);
  adder.setInput(right, left);
  adder.addRow(0, 0);
  adder.addRow(1, 0);
  adder.addRow(1, 2);
  adder.addRow(2, 0);
  adder.addRow(2, 3);
  adder.addRow(3, 0);

  auto expected = makeIdTableFromVector({{1, 5, 2},
                                         {3, 5, 4},
                                         {3, U, 4},
                                         {4, 5, 7},
                                         {4, U, 7},
                                         {U, 5, 8},
                                         {1, 2, 5},
                                         {3, 4, 5},
                                         {3, 4, U},
                                         {4, 7, 5},
                                         {4, 7, U},
                                         {U, 8, 5}});
  std::vector<size_t> expectedUndefined{2, 2, 2};
  testAdder(adder, expected, expectedUndefined, 1, keepJoinCol);
}

// _______________________________________________________________________________
TEST_P(RowAdderTest, cornerCases) {
  auto [bufferSize, keepJoinCol] = GetParam();
  auto result = makeIdTableFromVector({});
  result.setNumColumns(keepJoinCol ? 3 : 1);
  auto adder = ad_utility::AddCombinedRowToIdTable(
      2, std::move(result),
      std::make_shared<ad_utility::CancellationHandle<>>(), keepJoinCol,
      bufferSize);
  auto left = makeIdTableFromVector({{U, 5}, {2, U}, {3, U}, {4, U}});
  auto right = makeIdTableFromVector({{1, 2}, {3, 4}, {4, 7}, {U, 8}});
  // We have specified two join columns and our inputs have two columns each,
  // so the result should also have two columns, but it has three.
  EXPECT_ANY_THROW(adder.setInput(left, right));

  left = makeIdTableFromVector({{1}, {2}, {3}});

  // Left has only one column, but we have specified two join columns.
  EXPECT_ANY_THROW(adder.setInput(left, right));
  // The same test with the arguments switched.
  EXPECT_ANY_THROW(adder.setInput(right, left));
}
// _______________________________________________________________________________
TEST(AddCombinedRowToTable, BufferSizeZeroThrows) {
  auto left = makeIdTableFromVector({{3, 4}, {7, 8}, {11, 10}, {14, 11}});
  auto right =
      makeIdTableFromVector({{7, 14, 0}, {9, 10, 1}, {14, 8, 2}, {33, 5, 3}});
  auto result = makeIdTableFromVector({});
  result.setNumColumns(4);
  EXPECT_ANY_THROW(ad_utility::AddCombinedRowToIdTable(
      1, left.asStaticView<0>(), right.asStaticView<0>(), std::move(result),
      std::make_shared<ad_utility::CancellationHandle<>>(), true, 0));
  EXPECT_ANY_THROW(ad_utility::AddCombinedRowToIdTable(
      1, left.asStaticView<0>(), right.asStaticView<0>(), std::move(result),
      std::make_shared<ad_utility::CancellationHandle<>>(), false, 0));
};

// _______________________________________________________________________________
TEST(AddCombinedRowToTable, flushDoesCheckCancellation) {
  auto result = makeIdTableFromVector({});
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  ad_utility::AddCombinedRowToIdTable adder{0, std::move(result),
                                            cancellationHandle, true, 10};

  cancellationHandle->cancel(ad_utility::CancellationState::MANUAL);
  EXPECT_THROW(adder.flush(), ad_utility::CancellationException);
}

namespace {
struct IdTableWithVocab {
  IdTable idTable_;
  LocalVocab localVocab_;

  const LocalVocab& getLocalVocab() const { return localVocab_; }

  template <size_t I = 0>
  IdTableView<I> asStaticView() const {
    return idTable_.asStaticView<I>();
  }
};

using ad_utility::triple_component::Literal;

Literal fromString(std::string_view string) {
  return Literal::fromStringRepresentation(absl::StrCat("\"", string, "\""));
}

// _____________________________________________________________________________
LocalVocab createVocabWithSingleString(std::string_view string) {
  LocalVocab localVocab;
  localVocab.getIndexAndAddIfNotContained(LocalVocabEntry{fromString(string)});
  return localVocab;
}

// _____________________________________________________________________________
bool vocabContainsString(const LocalVocab& vocab, std::string_view string) {
  return ad_utility::contains(vocab.getAllWordsForTesting(),
                              LocalVocabEntry{fromString(string)});
}
}  // namespace

// _____________________________________________________________________________
TEST(AddCombinedRowToTable, verifyLocalVocabIsUpdatedCorrectly) {
  auto outputTable = makeIdTableFromVector({});
  outputTable.setNumColumns(3);
  std::vector<LocalVocab> localVocabs;
  ad_utility::AddCombinedRowToIdTable adder{
      1,
      std::move(outputTable),
      std::make_shared<ad_utility::CancellationHandle<>>(),
      true,
      2,
      [&localVocabs](IdTable& idTable, LocalVocab& localVocab) {
        localVocabs.push_back(std::move(localVocab));
        // Clear to trigger new merging of local vocabs, in practice
        // `localVocab` is not altered without altering `idTable` as well.
        idTable.clear();
      }};

  IdTableWithVocab input1{makeIdTableFromVector({{0, 1}}),
                          createVocabWithSingleString("a")};
  IdTableWithVocab input2{makeIdTableFromVector({{0, 2}}),
                          createVocabWithSingleString("b")};
  IdTableWithVocab input3{makeIdTableFromVector({{0, 3}}),
                          createVocabWithSingleString("c")};

  using ::testing::SizeIs;

  adder.setInput(input1, input2);
  adder.addRow(0, 0);
  adder.addRow(0, 0);
  EXPECT_THAT(localVocabs, SizeIs(1));
  adder.addRow(0, 0);

  adder.setInput(input2, input3);
  EXPECT_THAT(localVocabs, SizeIs(2));
  adder.addRow(0, 0);
  adder.flush();
  EXPECT_THAT(localVocabs, SizeIs(3));

  adder.setOnlyLeftInputForOptionalJoin(input1);
  adder.addOptionalRow(0);
  adder.addOptionalRow(0);
  EXPECT_THAT(localVocabs, SizeIs(4));
  adder.addOptionalRow(0);

  localVocabs.push_back(std::move(adder.localVocab()));

  ASSERT_THAT(localVocabs, SizeIs(5));

  EXPECT_TRUE(vocabContainsString(localVocabs[0], "a"));
  EXPECT_TRUE(vocabContainsString(localVocabs[0], "b"));
  EXPECT_FALSE(vocabContainsString(localVocabs[0], "c"));

  EXPECT_TRUE(vocabContainsString(localVocabs[1], "a"));
  EXPECT_TRUE(vocabContainsString(localVocabs[1], "b"));
  EXPECT_FALSE(vocabContainsString(localVocabs[1], "c"));

  EXPECT_FALSE(vocabContainsString(localVocabs[2], "a"));
  EXPECT_TRUE(vocabContainsString(localVocabs[2], "b"));
  EXPECT_TRUE(vocabContainsString(localVocabs[2], "c"));

  EXPECT_TRUE(vocabContainsString(localVocabs[3], "a"));
  EXPECT_FALSE(vocabContainsString(localVocabs[3], "b"));
  EXPECT_FALSE(vocabContainsString(localVocabs[3], "c"));

  EXPECT_TRUE(vocabContainsString(localVocabs[4], "a"));
  EXPECT_FALSE(vocabContainsString(localVocabs[4], "b"));
  EXPECT_FALSE(vocabContainsString(localVocabs[4], "c"));
}

// _____________________________________________________________________________
TEST(AddCombinedRowToTable, verifyLocalVocabIsRetainedWhenNotMoving) {
  auto outputTable = makeIdTableFromVector({});
  outputTable.setNumColumns(3);
  ad_utility::AddCombinedRowToIdTable adder{
      1, std::move(outputTable),
      std::make_shared<ad_utility::CancellationHandle<>>(), 1};

  IdTableWithVocab input1{makeIdTableFromVector({{0, 1}}),
                          createVocabWithSingleString("a")};
  IdTableWithVocab input2{makeIdTableFromVector({{0, 2}}),
                          createVocabWithSingleString("b")};

  adder.setInput(input1, input2);
  adder.addRow(0, 0);
  adder.flush();
  adder.addRow(0, 0);

  LocalVocab localVocab = std::move(adder.localVocab());

  EXPECT_TRUE(vocabContainsString(localVocab, "a"));
  EXPECT_TRUE(vocabContainsString(localVocab, "b"));
  EXPECT_THAT(localVocab.getAllWordsForTesting(), ::testing::SizeIs(2));
}

// _____________________________________________________________________________
TEST(AddCombinedRowToTable, localVocabIsOnlyClearedWhenLegal) {
  auto outputTable = makeIdTableFromVector({});
  outputTable.setNumColumns(3);
  ad_utility::AddCombinedRowToIdTable adder{
      1, std::move(outputTable),
      std::make_shared<ad_utility::CancellationHandle<>>(), 1};

  IdTableWithVocab input1{makeIdTableFromVector({{0, 1}}),
                          createVocabWithSingleString("a")};
  IdTableWithVocab input2{makeIdTableFromVector({{0, 2}}),
                          createVocabWithSingleString("b")};

  adder.setInput(input1, input2);
  adder.addRow(0, 0);
  IdTableWithVocab input3{makeIdTableFromVector({{3, 1}}),
                          createVocabWithSingleString("c")};
  IdTableWithVocab input4{makeIdTableFromVector({{3, 2}}),
                          createVocabWithSingleString("d")};
  // NOTE: This seemingly redundant call to `setInput` is important, as it tests
  // a previous bug: Each call to `setInput` implicitly also calls `flush` and
  // also possibly clears the local vocab if it is not used anymore. In this
  // case however we may not clear the local vocab, as the result of the
  // previous calls to `addRow` has not yet been extracted.
  adder.setInput(input1, input2);
  adder.setInput(input3, input4);
  adder.addRow(0, 0);
  auto localVocab = adder.localVocab().clone();

  EXPECT_TRUE(vocabContainsString(localVocab, "a"));
  EXPECT_TRUE(vocabContainsString(localVocab, "b"));
  EXPECT_TRUE(vocabContainsString(localVocab, "c"));
  EXPECT_TRUE(vocabContainsString(localVocab, "d"));
  EXPECT_THAT(localVocab.getAllWordsForTesting(), ::testing::SizeIs(4));
}

INSTANTIATE_TEST_SUITE_P(AddCombinedRowToTable, RowAdderTest,
                         ::testing::Combine(::testing::Values(1u, 2u, 3u, 4u,
                                                              5u, 6u, 7u, 8u,
                                                              9u, 10u, 100000u),
                                            ::testing::Values(true, false)));
}  // namespace
