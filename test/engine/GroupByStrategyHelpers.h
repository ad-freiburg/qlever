#ifndef QLEVER_GROUPBY_SAMPLING_HELPERS_H
#define QLEVER_GROUPBY_SAMPLING_HELPERS_H

#include <memory>
#include <vector>
#include <string>
#include <functional>

#include "engine/Operation.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Sort.h"
#include "engine/GroupByImpl.h"
#include "engine/idTable/IdTable.h"
#include "engine/Result.h"
#include "engine/Values.h"
#include "global/RuntimeParameters.h"
#include "parser/GraphPatternOperation.h"

#include "../util/AllocatorTestHelpers.h"
#include "../util/IdTestHelpers.h"
#include "../util/IndexTestHelpers.h"

using ad_utility::AllocatorWithLimit;
using ad_utility::testing::IntId;

// A mock operation that returns a pre-computed result.
class MockOperation : public Operation {
 private:
  IdTable table_;

 public:
  MockOperation(QueryExecutionContext* qec, const IdTable& table)
      : Operation(qec), table_(table.clone()) {}

  std::string getCacheKeyImpl() const override { return "MockOperation"; }
  std::string getDescriptor() const override { return "MockOperation"; }
  size_t getResultWidth() const override { return table_.numColumns(); }
  std::vector<ColumnIndex> resultSortedOn() const override { return {}; }
  bool knownEmptyResult() override { return table_.empty(); }
  float getMultiplicity(size_t) override { return 1; }
  uint64_t getSizeEstimateBeforeLimit() override { return table_.size(); }
  uint64_t getCostEstimate() override { return table_.size(); }
  std::vector<QueryExecutionTree*> getChildren() override { return {}; }

  VariableToColumnMap computeVariableToColumnMap() const override {
    VariableToColumnMap map;
    // Single variable ?a mapped to column 0, always defined
    map[Variable{"?a"}] = ColumnIndexAndTypeInfo{
        0, ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined};
    return map;
  }

  std::unique_ptr<Operation> cloneImpl() const override {
    return std::make_unique<MockOperation>(getExecutionContext(), table_);
  }

 private:
  Result computeResult(bool /*requestLaziness*/) override {
    LocalVocab localVocab{};
    return Result(table_.clone(), std::vector<ColumnIndex>{},
                  std::move(localVocab));
  }
};

// Helper to create a `GroupByImpl` operation with a simple subtree that
// returns the given `table`, wrapped in a Sort to enable hash-map fallback.
inline std::unique_ptr<GroupByImpl> setupGroupBy(const IdTable& table,
                                                 QueryExecutionContext* qec) {
  Variable varA{"?a"};
  // Create mock operation returning the table
  auto mockOp = std::make_shared<MockOperation>(qec, table);
  auto mockTree = std::make_shared<QueryExecutionTree>(qec, mockOp);
  // Wrap in Sort so GroupByImpl sees a sorted child and can use hash-map path
  std::vector<ColumnIndex> sortCols{0};
  auto sortOp = std::make_shared<Sort>(qec, mockTree, sortCols);
  auto sortTree = std::make_shared<QueryExecutionTree>(qec, sortOp);
  return std::make_unique<GroupByImpl>(qec, std::vector{varA},
                                       std::vector<Alias>{}, sortTree);
}

// Helper to create a simple IdTable with one column.
inline IdTable createIdTable(size_t numRows,
                             const std::function<int64_t(size_t)>& generator,
                             AllocatorWithLimit<Id>& allocator) {
  IdTable table(1, allocator);
  table.resize(numRows);
  for (size_t i = 0; i < numRows; ++i) {
    table(i, 0) = IntId(generator(i));
  }
  return table;
}

#endif  // QLEVER_GROUPBY_SAMPLING_HELPERS_H
