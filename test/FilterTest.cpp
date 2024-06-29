//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "engine/Filter.h"
#include "engine/ValuesForTesting.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "util/IndexTestHelpers.h"

using ::testing::ElementsAre;

class LazyValueOperation : public Operation {
 public:
  std::vector<QueryExecutionTree*> getChildren() override { return {}; }
  string getCacheKeyImpl() const override { return "Cache Key"; }
  string getDescriptor() const override { return "Descriptor"; }
  size_t getResultWidth() const override { return 0; }
  size_t getCostEstimate() override { return 0; }
  uint64_t getSizeEstimateBeforeLimit() override { return 0; }
  float getMultiplicity(size_t) override { return 1; }
  bool knownEmptyResult() override { return false; }
  [[nodiscard]] vector<ColumnIndex> resultSortedOn() const override {
    return {};
  }
  VariableToColumnMap computeVariableToColumnMap() const override {
    return {{Variable{"?x"},
             ColumnIndexAndTypeInfo{
                 0, ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined}}};
  }

  std::vector<IdTable> idTables_;

  explicit LazyValueOperation(QueryExecutionContext* qec,
                              std::vector<IdTable> idTables)
      : Operation{qec}, idTables_{std::move(idTables)} {
    AD_CONTRACT_CHECK(!idTables_.empty());
  }

  Result computeResult(bool requestLaziness) override {
    if (requestLaziness) {
      std::vector<IdTable> clones;
      clones.reserve(idTables_.size());
      for (const IdTable& idTable : idTables_) {
        clones.push_back(idTable.clone());
      }
      auto generator = [](auto idTables) -> cppcoro::generator<IdTable> {
        for (IdTable& idTable : idTables) {
          co_yield std::move(idTable);
        }
      }(std::move(clones));
      return {std::move(generator), resultSortedOn(), LocalVocab{}};
    }
    IdTable aggregateTable{idTables_.at(0).numColumns(),
                           idTables_.at(0).getAllocator()};
    for (const IdTable& idTable : idTables_) {
      aggregateTable.insertAtEnd(idTable);
    }
    return {std::move(aggregateTable), resultSortedOn(), LocalVocab{}};
  }
};

IdTable makeIdTable(std::initializer_list<bool> bools) {
  IdTable idTable{1, ad_utility::makeUnlimitedAllocator<Id>()};
  for (bool b : bools) {
    idTable.push_back({Id::makeFromBool(b)});
  }
  return idTable;
}

columnBasedIdTable::Row<Id> makeRow(bool b) {
  columnBasedIdTable::Row<Id> row{1};
  row[0] = Id::makeFromBool(b);
  return row;
}

// _____________________________________________________________________________
TEST(Filter, verifyPredicateIsAppliedCorrectlyOnLazyEvaluation) {
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache();
  std::vector<IdTable> idTables;
  idTables.push_back(makeIdTable({true, true, false, false, true}));
  idTables.push_back(makeIdTable({true, false}));
  idTables.push_back(makeIdTable({}));
  idTables.push_back(makeIdTable({false, false, false}));
  idTables.push_back(makeIdTable({true}));

  LazyValueOperation values{qec, std::move(idTables)};
  QueryExecutionTree subTree{
      qec, std::make_shared<LazyValueOperation>(std::move(values))};
  Filter filter{
      qec,
      std::make_shared<QueryExecutionTree>(std::move(subTree)),
      {std::make_unique<sparqlExpression::VariableExpression>(Variable{"?x"}),
       "Expression ?x"}};

  auto result = filter.getResult(false, ComputationMode::LAZY_IF_SUPPORTED);
  ASSERT_FALSE(result->isDataEvaluated());
  auto generator = result->idTables();

  auto iterator = generator.begin();
  ASSERT_NE(iterator, generator.end());
  EXPECT_THAT(*iterator,
              ElementsAre(makeRow(true), makeRow(true), makeRow(true)));

  ++iterator;
  ASSERT_NE(iterator, generator.end());
  EXPECT_THAT(*iterator, ElementsAre(makeRow(true)));

  ++iterator;
  ASSERT_NE(iterator, generator.end());
  EXPECT_THAT(*iterator, ElementsAre());

  ++iterator;
  ASSERT_NE(iterator, generator.end());
  EXPECT_THAT(*iterator, ElementsAre());

  ++iterator;
  ASSERT_NE(iterator, generator.end());
  EXPECT_THAT(*iterator, ElementsAre(makeRow(true)));

  ++iterator;
  EXPECT_EQ(iterator, generator.end());
}

// _____________________________________________________________________________
TEST(Filter, verifyPredicateIsAppliedCorrectlyOnNonLazyEvaluation) {
  QueryExecutionContext* qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache();
  std::vector<IdTable> idTables;
  idTables.push_back(makeIdTable({true, true, false, false, true}));
  idTables.push_back(makeIdTable({true, false}));
  idTables.push_back(makeIdTable({}));
  idTables.push_back(makeIdTable({false, false, false}));
  idTables.push_back(makeIdTable({true}));

  LazyValueOperation values{qec, std::move(idTables)};
  QueryExecutionTree subTree{
      qec, std::make_shared<LazyValueOperation>(std::move(values))};
  Filter filter{
      qec,
      std::make_shared<QueryExecutionTree>(std::move(subTree)),
      {std::make_unique<sparqlExpression::VariableExpression>(Variable{"?x"}),
       "Expression ?x"}};

  auto result = filter.getResult(false, ComputationMode::FULLY_MATERIALIZED);
  ASSERT_TRUE(result->isDataEvaluated());
  EXPECT_THAT(result->idTable(),
              ElementsAre(makeRow(true), makeRow(true), makeRow(true),
                          makeRow(true), makeRow(true)));
}
