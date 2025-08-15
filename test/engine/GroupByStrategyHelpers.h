#ifndef QLEVER_GROUPBY_SAMPLING_HELPERS_H
#define QLEVER_GROUPBY_SAMPLING_HELPERS_H

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "../util/AllocatorTestHelpers.h"
#include "../util/IdTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "engine/GroupByImpl.h"
#include "engine/Operation.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Result.h"
#include "engine/Values.h"
#include "engine/idTable/IdTable.h"
#include "global/RuntimeParameters.h"
#include "parser/GraphPatternOperation.h"

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
    // Map each column index i to variable ?a, ?b, ?c, ...
    for (size_t i = 0; i < table_.numColumns(); ++i) {
      std::string varName = "?";
      varName.push_back(static_cast<char>('a' + i));
      map[Variable{varName}] = ColumnIndexAndTypeInfo{
          static_cast<ColumnIndex>(i),
          ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined};
    }
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

// Helper to create a `GroupByImpl` that groups on all table columns ("?a",
// "?b", ...) and automatically injects a Sort on these columns before grouping.
inline std::unique_ptr<GroupByImpl> setupGroupBy(const IdTable& table,
                                                 QueryExecutionContext* qec) {
  // Infer grouping variables and sort columns from column count
  size_t numCols = table.numColumns();
  std::vector<Variable> groupVars;
  std::vector<ColumnIndex> sortCols;
  groupVars.reserve(numCols);
  sortCols.reserve(numCols);
  for (size_t i = 0; i < numCols; ++i) {
    groupVars.emplace_back(std::string("?") + static_cast<char>('a' + i));
    sortCols.emplace_back(static_cast<ColumnIndex>(i));
  }
  // Build mock subtree
  auto mockOperation = std::make_shared<MockOperation>(qec, table);
  std::shared_ptr<QueryExecutionTree> subtree =
      std::make_shared<QueryExecutionTree>(qec, mockOperation);
  // Inject sort if there are grouping columns
  if (!sortCols.empty()) {
    auto sortOp = std::make_shared<Sort>(qec, subtree, sortCols);
    subtree = std::make_shared<QueryExecutionTree>(qec, sortOp);
  }
  return std::make_unique<GroupByImpl>(qec, groupVars, std::vector<Alias>{},
                                       subtree);
}

// Helper to create a simple IdTable with one column.
// Generic helper to create an IdTable with numCols and numRows.
// generator(row, col) should return the integer value for (row, col).
template <typename Func>
inline IdTable createIdTable(size_t numCols, size_t numRows,
                             const Func& generator,
                             AllocatorWithLimit<Id>& allocator) {
  IdTable table(numCols, allocator);
  table.resize(numRows);
  for (size_t r = 0; r < numRows; ++r) {
    for (size_t c = 0; c < numCols; ++c) {
      table(r, c) = IntId(generator(r, c));
    }
  }
  return table;
}

// Convenience overload: single-column table
inline IdTable createIdTable(size_t numRows,
                             const std::function<int64_t(size_t)>& generator,
                             AllocatorWithLimit<Id>& allocator) {
  return createIdTable(
      1u, numRows, [&](size_t r, size_t) { return generator(r); }, allocator);
}

#endif  // QLEVER_GROUPBY_SAMPLING_HELPERS_H
