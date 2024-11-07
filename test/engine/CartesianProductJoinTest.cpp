//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../engine/ValuesForTesting.h"
#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "engine/CartesianProductJoin.h"
#include "engine/QueryExecutionTree.h"

using namespace ad_utility::testing;
using ad_utility::source_location;
constexpr auto CHUNK_SIZE = CartesianProductJoin::CHUNK_SIZE;
using O = std::optional<size_t>;

// Create a `CartesianProductJoin` the children of which are `ValuesForTesting`
// with results create from the `inputs`. The children will have disjoint sets
// of variable as required by the `CartesianProductJoin`. If
// `useLimitInSuboperations` is true, then the `ValuesForTesting` support the
// LIMIT operation directly (this makes a difference in the `computeResult`
// method of `CartesianProductJoin`).
CartesianProductJoin makeJoin(const std::vector<VectorTable>& inputs,
                              bool useLimitInSuboperations = false) {
  auto qec =
      ad_utility::testing::getQec("<only> <for> <cartesianProductJoinTests>");
  std::vector<std::shared_ptr<QueryExecutionTree>> valueOperations;
  size_t i = 0;
  auto v = [&i]() mutable { return Variable{"?" + std::to_string(i++)}; };
  for (const auto& input : inputs) {
    std::vector<std::optional<Variable>> vars;
    std::generate_n(std::back_inserter(vars),
                    input.empty() ? 0 : input.at(0).size(), std::ref(v));
    valueOperations.push_back(ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector(input), std::move(vars),
        useLimitInSuboperations));
  }
  // Test that passing the same subtree in twice is illegal because it leads to
  // non-disjoint variable sets.
  if (!valueOperations.empty() && i > 0) {
    auto subtrees2 = valueOperations;
    subtrees2.insert(subtrees2.end(), valueOperations.begin(),
                     valueOperations.end());
    EXPECT_ANY_THROW(CartesianProductJoin(qec, std::move(subtrees2)));
  }
  return CartesianProductJoin{qec, std::move(valueOperations)};
}

// Test that a Cartesian product between the `inputs` yields the `expected`
// result. For the meaning of the `useLimitInSuboperations` see `makeJoin`
// above.
void testCartesianProductImpl(VectorTable expected,
                              std::vector<VectorTable> inputs,
                              bool useLimitInSuboperations = false,
                              source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  {
    auto join = makeJoin(inputs, useLimitInSuboperations);
    EXPECT_EQ(makeIdTableFromVector(expected),
              join.computeResultOnlyForTesting().idTable());
  }

  for (size_t limit = 0; limit < expected.size(); ++limit) {
    for (size_t offset = 0; offset < expected.size(); ++offset) {
      LimitOffsetClause limitClause{limit, 0, offset};
      auto join = makeJoin(inputs, useLimitInSuboperations);
      join.setLimit(limitClause);
      VectorTable partialResult;
      std::copy(expected.begin() + limitClause.actualOffset(expected.size()),
                expected.begin() + limitClause.upperBound(expected.size()),
                std::back_inserter(partialResult));
      EXPECT_EQ(makeIdTableFromVector(partialResult),
                join.computeResultOnlyForTesting().idTable())
          << "failed at offset " << offset << " and limit " << limit;
    }
  }
}
// Test that a Cartesian product between the `inputs` yields the `expected`
// result. Perform the test for children that directly support the LIMIT
// operation as well for children that don't (see `makeJoin` above for details).
void testCartesianProduct(VectorTable expected, std::vector<VectorTable> inputs,
                          source_location l = source_location::current()) {
  auto t = generateLocationTrace(l);
  testCartesianProductImpl(expected, inputs, true);
  testCartesianProductImpl(expected, inputs, false);
}

// ______________________________________________________________
TEST(CartesianProductJoin, computeResult) {
  // Simple base cases.
  VectorTable v{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}};
  VectorTable empty{};
  testCartesianProduct(v, {v});
  testCartesianProduct(empty, {empty, v, empty});
  testCartesianProduct(empty, {empty, empty});

  // Test cases where some or all of the inputs are Neutral elements (1 row,
  // zero columns) that are automatically filtered out by the
  // `CartesianProductJoin`.
  VectorTable neutral{{}};
  testCartesianProduct(neutral, {neutral});
  testCartesianProduct(v, {v, neutral});
  testCartesianProduct(v, {neutral, v, neutral});
  testCartesianProduct(neutral, {neutral, neutral, neutral});
  testCartesianProduct(empty, {neutral, empty, neutral});
  testCartesianProduct(empty, {neutral, empty, v});

  // Fails because of an empty input.
  EXPECT_ANY_THROW(makeJoin({}));

  // Fails because of the nullptrs.
  EXPECT_ANY_THROW(CartesianProductJoin(getQec(), {nullptr, nullptr}));

  // Join with a single result.
  VectorTable v2{{1, 2, 3, 7}, {4, 5, 6, 7}, {7, 8, 9, 7}};
  testCartesianProduct(v2, {v, {{7}}});

  // A classic pattern.
  testCartesianProduct({{0, 2, 4},
                        {1, 2, 4},
                        {0, 3, 4},
                        {1, 3, 4},
                        {0, 2, 5},
                        {1, 2, 5},
                        {0, 3, 5},
                        {1, 3, 5}},
                       {{{0}, {1}}, {{2}, {3}}, {{4}, {5}}});

  // Heterogeneous sizes.
  testCartesianProduct(
      {{0, 2, 4}, {1, 2, 4}, {0, 2, 5}, {1, 2, 5}, {0, 2, 6}, {1, 2, 6}},
      {{{0}, {1}}, {{2}}, {{4}, {5}, {6}}});

  // A larger input to cover the `CALL_FIXED_SIZE` optimization for the stride.
  testCartesianProduct(
      {{0, 0},
       {1, 0},
       {2, 0},
       {3, 0},
       {4, 0},
       {5, 0},
       {6, 0},
       {7, 0},
       {8, 0},
       {9, 0},
       {10, 0},
       {11, 0}},
      {{{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}}, {{0}}});
}

// Test the throwing of the custom out of memory exception inside the
// cartesian product join.
TEST(CartesianProductJoin, outOfMemoryException) {
  std::vector<VectorTable> tables;
  VectorTable largeTable;
  largeTable.emplace_back();
  for (size_t i = 0; i < 10; ++i) {
    largeTable.back().push_back(0);
  }
  for (size_t i = 0; i < 10; ++i) {
    tables.push_back(largeTable);
  }
  auto largeJoin = makeJoin(tables);
  auto allocator = largeJoin.getExecutionContext()->getAllocator();
  // Manually deplete the allocator.
  auto left = allocator.amountMemoryLeft().getBytes() / sizeof(Id);
  auto ptr = allocator.allocate(left);
  AD_EXPECT_THROW_WITH_MESSAGE(largeJoin.computeResultOnlyForTesting(),
                               ::testing::HasSubstr("cross-product"));
  // Avoid memory leaks and failures of unit tests that reuse the (static)
  // allocator.
  allocator.deallocate(ptr, left);
}

// ______________________________________________________________
TEST(CartesianProductJoin, basicMemberFunctions) {
  auto join = makeJoin({{{3, 5}, {7, 9}}, {{4}, {5}, {2}}});
  EXPECT_EQ(join.getDescriptor(), "Cartesian Product Join");
  EXPECT_FALSE(join.knownEmptyResult());
  EXPECT_EQ(join.getSizeEstimate(), 6);
  EXPECT_EQ(join.getResultWidth(), 3);
  EXPECT_EQ(join.getCostEstimate(), 11);
  EXPECT_EQ(join.getMultiplicity(1023), 1.0f);
  EXPECT_EQ(join.getMultiplicity(0), 1.0f);

  auto children = join.getChildren();
  EXPECT_THAT(join.getCacheKey(),
              ::testing::ContainsRegex("CARTESIAN PRODUCT JOIN"));
  EXPECT_THAT(join.getCacheKey(),
              ::testing::ContainsRegex("Values for testing with 2 columns"));
  EXPECT_THAT(join.getCacheKey(),
              ::testing::ContainsRegex("Values for testing with 1 col"));
  EXPECT_EQ(children.size(), 2u);
  EXPECT_NE(children.at(0), join.getChildren().at(1));
}

// Test that the `variableToColumnMap` is also correct if the sub results have
// columns that are not connected to a variable (can happen when subqueries are
// present).
TEST(CartesianProductJoin, variableColumnMap) {
  auto qec = getQec();
  std::vector<std::shared_ptr<QueryExecutionTree>> subtrees;
  using Vars = std::vector<std::optional<Variable>>;
  subtrees.push_back(ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{3, 4}, {4, 7}}),
      Vars{Variable{"?x"}, std::nullopt}));
  subtrees.push_back(ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{3, 4, 3, Id::makeUndefined()}}),
      Vars{std::nullopt, Variable{"?y"}, std::nullopt, Variable{"?z"}}));
  CartesianProductJoin join{qec, std::move(subtrees)};

  using enum ColumnIndexAndTypeInfo::UndefStatus;
  VariableToColumnMap expectedVariables{
      {Variable{"?x"}, {0, AlwaysDefined}},
      {Variable{"?y"}, {3, AlwaysDefined}},
      {Variable{"?z"}, {5, PossiblyUndefined}}};
  EXPECT_THAT(join.getExternallyVisibleVariableColumns(),
              ::testing::UnorderedElementsAreArray(expectedVariables));
}

// First parameter indicates "seed" for splitting the last input table.
// Second parameter indicates the offset for the LIMIT clause.
// Third parameter indicates the limit for the LIMIT clause.
class CartesianProductJoinLazyTest
    : public testing::TestWithParam<
          std::tuple<uint64_t, size_t, std::optional<size_t>>> {
  static std::vector<IdTable> splitIntoRandomSubtables(const IdTable& idTable) {
    // Ensure results are reproducible.
    std::mt19937_64 generator{std::get<0>(GetParam())};
    // The average size of splits.
    size_t averageSplitSize =
        std::uniform_int_distribution<size_t>{0, idTable.size()}(generator);
    std::uniform_int_distribution<size_t> distribution{0, averageSplitSize};
    std::vector<IdTable> result;
    result.emplace_back(idTable.numColumns(), idTable.getAllocator());
    for (const auto& row : idTable) {
      result.back().emplace_back();
      result.back().back() = row;
      if (distribution(generator) == 0) {
        result.emplace_back(idTable.numColumns(), idTable.getAllocator());
      }
    }
    return result;
  }

  static size_t varIndex;

  static std::vector<std::optional<Variable>> makeUniqueVariables(
      const IdTable& idTable) {
    std::vector<std::optional<Variable>> result;
    for (size_t i = 0; i < idTable.numColumns(); ++i) {
      result.emplace_back(Variable{"?v" + std::to_string(varIndex++)});
    }
    return result;
  }

 protected:
  static CartesianProductJoin makeJoin(std::vector<IdTable> tables) {
    AD_CONTRACT_CHECK(tables.size() >= 2);
    auto* qec = ad_utility::testing::getQec();
    CartesianProductJoin::Children children{};
    for (IdTable& table : std::span{tables}.subspan(0, tables.size() - 1)) {
      children.push_back(ad_utility::makeExecutionTree<ValuesForTesting>(
          qec, table.clone(), makeUniqueVariables(table)));
    }
    if (std::get<0>(GetParam()) == 0) {
      children.push_back(ad_utility::makeExecutionTree<ValuesForTesting>(
          qec, tables.back().clone(), makeUniqueVariables(tables.back()), false,
          std::vector<ColumnIndex>{}, LocalVocab{}, std::nullopt, true));
    } else {
      children.push_back(ad_utility::makeExecutionTree<ValuesForTesting>(
          qec, splitIntoRandomSubtables(tables.back()),
          makeUniqueVariables(tables.back())));
    }
    CartesianProductJoin join{qec, std::move(children)};
    join.setLimit(LimitOffsetClause{std::get<2>(GetParam()), getOffset()});
    return join;
  }

  static size_t getOffset() { return std::get<1>(GetParam()); }

  static size_t getLimit() {
    return std::get<2>(GetParam()).has_value()
               ? std::get<2>(GetParam()).value()
               : std::numeric_limits<size_t>::max();
  }

  static size_t clampSize(size_t maximum) {
    // Apply offset
    maximum -= std::min(maximum, getOffset());
    // Apply limit
    maximum = std::min(maximum, getLimit());
    return maximum;
  }

  static size_t getExpectedSize(const std::vector<IdTable>& idTables) {
    size_t totalExpectedSize = 1;
    for (const auto& idTable : idTables) {
      totalExpectedSize *= idTable.size();
    }
    return totalExpectedSize;
  }

  static std::vector<size_t> getOccurenceCountWithoutLimit(
      const std::vector<IdTable>& idTables) {
    size_t totalExpectedSize = getExpectedSize(idTables);
    std::vector<size_t> result;
    for (const auto& idTable : idTables) {
      for (size_t i = 0; i < idTable.numColumns(); ++i) {
        result.push_back(totalExpectedSize / idTable.size());
      }
    }
    return result;
  }

  static std::vector<size_t> getValueCount(
      const std::vector<IdTable>& idTables) {
    std::vector<size_t> result;
    for (const auto& idTable : idTables) {
      for (size_t i = 0; i < idTable.numColumns(); ++i) {
        result.push_back(idTable.size());
      }
    }
    return result;
  }

  static IdTable trimToLimitAndOffset(IdTable idTable, size_t offset,
                                      size_t limit) {
    idTable.erase(idTable.begin(),
                  idTable.begin() + std::min(idTable.size(), offset));
    idTable.erase(idTable.begin() + std::min(idTable.size(), limit),
                  idTable.end());
    return idTable;
  }

  static void expectCorrectResult(
      CartesianProductJoin& join, size_t expectedSize,
      const std::vector<size_t>& occurenceCounts,
      const std::vector<size_t>& valueCount,
      source_location loc = source_location::current()) {
    auto trace = generateLocationTrace(loc);
    Result result = join.computeResultOnlyForTesting(true);
    ASSERT_FALSE(result.isFullyMaterialized());
    std::vector<std::unordered_map<uint64_t, size_t>> counter{
        occurenceCounts.size()};
    size_t elementCounter = 0;
    for (auto& [idTable, localVocab] : result.idTables()) {
      for (size_t i = 0; i < idTable.numColumns(); ++i) {
        for (Id id : idTable.getColumn(i)) {
          counter.at(i)[id.getBits()]++;
        }
      }
      elementCounter += idTable.size();
    }
    EXPECT_EQ(elementCounter, clampSize(expectedSize));
    size_t penalty = expectedSize - clampSize(expectedSize);
    using namespace ::testing;
    for (size_t col = 0; col < occurenceCounts.size(); col++) {
      for (const auto& [id, count] : counter.at(col)) {
        size_t occurenceCount = occurenceCounts.at(col);
        EXPECT_LE(count, occurenceCount)
            << "Column " << col << " did contain too many occurences for id "
            << Id::fromBits(id);
        EXPECT_GE(count, occurenceCount - std::min(occurenceCount, penalty))
            << "Column " << col << " did contain too few occurences for id "
            << Id::fromBits(id);
      }
      size_t expectedCount = valueCount.at(col);
      EXPECT_LE(counter.at(col).size(), expectedCount)
          << "The id table did contain too many unique values "
             "for column "
          << col;
      EXPECT_GE(counter.at(col).size(),
                expectedCount - std::min(expectedCount, penalty))
          << "The id table did contain too few unique values "
             "for column "
          << col;
    }
  }

  template <size_t N>
  static void expectGeneratorYieldsValues(
      std::array<IdTable, N> tables, Result::Generator generator,
      source_location loc = source_location::current()) {
    auto trace = generateLocationTrace(loc);
    size_t rowsConsumed = 0;
    size_t remainingOffset = getOffset();
    auto iterator = generator.begin();
    for (IdTable& table : tables) {
      if (rowsConsumed >= getLimit()) {
        // TODO<RobinTF> Don't yield empty tables.
        if (iterator != generator.end()) {
          EXPECT_THAT(iterator->idTable_, ::testing::IsEmpty());
          ++iterator;
        }
        break;
      }
      ASSERT_NE(iterator, generator.end());
      size_t untrimmedSize = table.size();
      IdTable reference =
          trimToLimitAndOffset(std::move(table), remainingOffset,
                               getLimit() - std::min(getLimit(), rowsConsumed));
      EXPECT_EQ(iterator->idTable_, reference)
          << "Expected " << reference.size() << " rows and got "
          << iterator->idTable_.size();
      rowsConsumed += reference.size();
      remainingOffset -= std::min(untrimmedSize, remainingOffset);
      ++iterator;
    }
    ASSERT_EQ(iterator, generator.end());
  }
};

size_t CartesianProductJoinLazyTest::varIndex = 0;

// _____________________________________________________________________________
TEST_P(CartesianProductJoinLazyTest, allTablesSmallerThanChunk) {
  // All tables combined smaller than chunk size
  std::vector<IdTable> tables;
  tables.push_back(makeIdTableFromVector({{0, 10}, {1, 11}}));
  tables.push_back(makeIdTableFromVector({{100}, {101}, {102}}));
  tables.push_back(makeIdTableFromVector({{1000}}));
  tables.push_back(makeIdTableFromVector({{10000, 100000}, {10001, 100001}}));

  auto occurenceCounts = getOccurenceCountWithoutLimit(tables);
  auto valueCount = getValueCount(tables);
  size_t expectedSize = getExpectedSize(tables);
  auto join = makeJoin(std::move(tables));
  expectCorrectResult(join, expectedSize, occurenceCounts, valueCount);

  // For a table this small we can assert the result directly
  auto result = join.computeResultOnlyForTesting(true);
  ASSERT_FALSE(result.isFullyMaterialized());
  IdTable reference = makeIdTableFromVector({
      {0, 10, 100, 1000, 10000, 100000},
      {1, 11, 100, 1000, 10000, 100000},
      {0, 10, 101, 1000, 10000, 100000},
      {1, 11, 101, 1000, 10000, 100000},
      {0, 10, 102, 1000, 10000, 100000},
      {1, 11, 102, 1000, 10000, 100000},
      {0, 10, 100, 1000, 10001, 100001},
      {1, 11, 100, 1000, 10001, 100001},
      {0, 10, 101, 1000, 10001, 100001},
      {1, 11, 101, 1000, 10001, 100001},
      {0, 10, 102, 1000, 10001, 100001},
      {1, 11, 102, 1000, 10001, 100001},
  });

  expectGeneratorYieldsValues<1>({std::move(reference)},
                                 std::move(result.idTables()));
}

// _____________________________________________________________________________
TEST_P(CartesianProductJoinLazyTest, leftTableBiggerThanChunk) {
  // Leftmost table individually larger than chunk size
  IdTable bigTable{1, ad_utility::testing::makeAllocator()};
  bigTable.resize(CHUNK_SIZE + 1);
  std::ranges::copy(std::views::iota(static_cast<int64_t>(0),
                                     static_cast<int64_t>(CHUNK_SIZE + 1)) |
                        std::views::transform(Id::makeFromInt),
                    bigTable.getColumn(0).begin());
  std::vector<IdTable> tables;
  tables.push_back(bigTable.clone());
  tables.push_back(makeIdTableFromVector({{0, 10}, {1, 11}, {2, 12}}));
  tables.push_back(makeIdTableFromVector({{100}}));

  auto occurenceCounts = getOccurenceCountWithoutLimit(tables);
  auto valueCount = getValueCount(tables);
  size_t expectedSize = getExpectedSize(tables);
  auto join = makeJoin(std::move(tables));
  expectCorrectResult(join, expectedSize, occurenceCounts, valueCount);

  bigTable.addEmptyColumn();
  bigTable.addEmptyColumn();
  bigTable.addEmptyColumn();
  std::ranges::fill(bigTable.getColumn(3),
                    Id::makeFromVocabIndex(VocabIndex::make(100)));

  auto result = join.computeResultOnlyForTesting(true);
  ASSERT_FALSE(result.isFullyMaterialized());

  std::ranges::fill(bigTable.getColumn(1),
                    Id::makeFromVocabIndex(VocabIndex::make(0)));
  std::ranges::fill(bigTable.getColumn(2),
                    Id::makeFromVocabIndex(VocabIndex::make(10)));
  IdTable table1 = bigTable.clone();

  std::ranges::fill(bigTable.getColumn(1),
                    Id::makeFromVocabIndex(VocabIndex::make(1)));
  std::ranges::fill(bigTable.getColumn(2),
                    Id::makeFromVocabIndex(VocabIndex::make(11)));
  IdTable table2 = bigTable.clone();
  std::ranges::fill(bigTable.getColumn(1),
                    Id::makeFromVocabIndex(VocabIndex::make(2)));
  std::ranges::fill(bigTable.getColumn(2),
                    Id::makeFromVocabIndex(VocabIndex::make(12)));
  IdTable table3 = bigTable.clone();

  expectGeneratorYieldsValues<3>(
      {std::move(table1), std::move(table2), std::move(table3)},
      std::move(result.idTables()));
}

// _____________________________________________________________________________
TEST_P(CartesianProductJoinLazyTest, tablesAccumulatedBiggerThanChunk) {
  // All tables individually smaller than chunk size, but larger when combined.
  size_t rootSize = std::sqrt(CHUNK_SIZE) + 1;
  IdTable table1{2, ad_utility::testing::makeAllocator()};
  table1.resize(rootSize);
  std::ranges::copy(std::views::iota(static_cast<int64_t>(0),
                                     static_cast<int64_t>(rootSize)) |
                        std::views::transform(Id::makeFromInt),
                    table1.getColumn(0).begin());
  std::ranges::copy(std::views::iota(static_cast<int64_t>(-rootSize),
                                     static_cast<int64_t>(0)) |
                        std::views::transform(Id::makeFromInt),
                    table1.getColumn(1).begin());

  IdTable table2{2, ad_utility::testing::makeAllocator()};
  table2.resize(rootSize);
  std::ranges::copy(std::views::iota(static_cast<int64_t>(0),
                                     static_cast<int64_t>(rootSize)) |
                        std::views::transform(Id::makeFromInt),
                    table2.getColumn(0).begin());
  std::ranges::copy(std::views::iota(static_cast<int64_t>(-rootSize),
                                     static_cast<int64_t>(0)) |
                        std::views::transform(Id::makeFromInt),
                    table2.getColumn(1).begin());

  std::vector<IdTable> tables;
  tables.push_back(std::move(table1));
  tables.push_back(std::move(table2));

  auto occurenceCounts = getOccurenceCountWithoutLimit(tables);
  auto valueCount = getValueCount(tables);
  size_t expectedSize = getExpectedSize(tables);
  auto join = makeJoin(std::move(tables));
  expectCorrectResult(join, expectedSize, occurenceCounts, valueCount);
}

// _____________________________________________________________________________
TEST_P(CartesianProductJoinLazyTest, twoTablesMatchChunkSize) {
  // 2 tables multiplied together match chunk size exactly
  size_t rootSize = std::sqrt(CHUNK_SIZE);
  IdTable table1{2, ad_utility::testing::makeAllocator()};
  table1.resize(rootSize);
  std::ranges::copy(std::views::iota(static_cast<int64_t>(0),
                                     static_cast<int64_t>(rootSize)) |
                        std::views::transform(Id::makeFromInt),
                    table1.getColumn(0).begin());
  std::ranges::copy(std::views::iota(static_cast<int64_t>(-rootSize),
                                     static_cast<int64_t>(0)) |
                        std::views::transform(Id::makeFromInt),
                    table1.getColumn(1).begin());

  IdTable table2{2, ad_utility::testing::makeAllocator()};
  table2.resize(rootSize);
  std::ranges::copy(std::views::iota(static_cast<int64_t>(rootSize),
                                     static_cast<int64_t>(rootSize * 2)) |
                        std::views::transform(Id::makeFromInt),
                    table2.getColumn(0).begin());
  std::ranges::copy(std::views::iota(static_cast<int64_t>(-rootSize * 2),
                                     static_cast<int64_t>(-rootSize)) |
                        std::views::transform(Id::makeFromInt),
                    table2.getColumn(1).begin());

  std::vector<IdTable> tables;
  tables.push_back(std::move(table1));
  tables.push_back(std::move(table2));

  auto occurenceCounts = getOccurenceCountWithoutLimit(tables);
  auto valueCount = getValueCount(tables);
  size_t expectedSize = getExpectedSize(tables);
  auto join = makeJoin(std::move(tables));
  expectCorrectResult(join, expectedSize, occurenceCounts, valueCount);
}

// _____________________________________________________________________________

using ::testing::Range;
using ::testing::Values;
INSTANTIATE_TEST_SUITE_P(
    CartesianProdctJoinTestSuite, CartesianProductJoinLazyTest,
    ::testing::Combine(
        Range<uint64_t>(0, 5),
        Values(0, 1, 25, CHUNK_SIZE, CHUNK_SIZE + 1, CHUNK_SIZE * 2),
        Values(O{0}, O{1}, O{25}, O{CHUNK_SIZE}, O{CHUNK_SIZE * 2},
               O{CHUNK_SIZE * 10}, std::nullopt)),
    [](const testing::TestParamInfo<
        std::tuple<uint64_t, size_t, std::optional<size_t>>>& info) {
      std::ostringstream stream;
      if (std::get<0>(info.param) == 0) {
        stream << "FullyMaterialized";
      } else {
        stream << "WithSplitSeed_" << std::get<0>(info.param);
      }
      stream << "_Offset_" << std::get<1>(info.param);
      stream << "_Limit_";
      if (std::get<2>(info.param).has_value()) {
        stream << std::get<2>(info.param).value();
      } else {
        stream << "None";
      }
      return std::move(stream).str();
    });
