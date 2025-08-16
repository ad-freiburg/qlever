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
#include "engine/Sort.h"
#include "engine/Values.h"
#include "engine/idTable/IdTable.h"
#include "global/RuntimeParameters.h"
#include "parser/GraphPatternOperation.h"

using ad_utility::AllocatorWithLimit;
using ad_utility::testing::IntId;
using RowData = std::vector<int64_t>;
using TableData = std::vector<RowData>;
using ChunkedRowData = std::vector<RowData>;
using ChunkedTableData = std::vector<TableData>;

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

// Create an IdTable from a single-column data vector
inline IdTable createIdTable(const RowData& rowData,
                             AllocatorWithLimit<Id>& allocator) {
  IdTable table(1u, allocator);
  table.resize(rowData.size());
  for (size_t r = 0; r < rowData.size(); ++r) {
    table(r, 0) = IntId(rowData[r]);
  }
  return table;
}

// Create an IdTable from multi-column row data
inline IdTable createIdTable(const TableData& tableData,
                             AllocatorWithLimit<Id>& allocator) {
  size_t numRows = tableData.size();
  size_t numCols = numRows ? tableData[0].size() : 0;
  IdTable table(numCols, allocator);
  table.resize(numRows);
  for (size_t r = 0; r < numRows; ++r) {
    for (size_t c = 0; c < numCols; ++c) {
      table(r, c) = IntId(tableData[r][c]);
    }
  }
  return table;
}

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

// _____________________________________________________________________________
// A mock operation that returns multiple input blocks to test partial fallback.
class ChunkedMockOperation : public Operation {
 private:
  std::vector<IdTable> tables_;

 public:
  ChunkedMockOperation(QueryExecutionContext* qec,
                       std::vector<IdTable>&& tables)
      : Operation(qec), tables_(std::move(tables)) {}
  std::string getCacheKeyImpl() const override {
    return "ChunkedMockOperation";
  }
  std::string getDescriptor() const override { return "ChunkedMockOperation"; }
  size_t getResultWidth() const override {
    return tables_.empty() ? 0 : tables_[0].numColumns();
  }
  std::vector<ColumnIndex> resultSortedOn() const override { return {}; }
  bool knownEmptyResult() override {
    return tables_.empty() || tables_[0].empty();
  }
  float getMultiplicity(size_t) override { return 1; }
  uint64_t getSizeEstimateBeforeLimit() override {
    uint64_t sum = 0;
    for (auto& t : tables_) sum += t.size();
    return sum;
  }
  uint64_t getCostEstimate() override { return getSizeEstimateBeforeLimit(); }
  std::vector<QueryExecutionTree*> getChildren() override { return {}; }
  VariableToColumnMap computeVariableToColumnMap() const override {
    VariableToColumnMap map;
    for (size_t i = 0; i < getResultWidth(); ++i) {
      std::string var = "?";
      var.push_back(static_cast<char>('a' + i));
      map[Variable{var}] = ColumnIndexAndTypeInfo{
          static_cast<ColumnIndex>(i),
          ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined};
    }
    return map;
  }
  std::unique_ptr<Operation> cloneImpl() const override {
    // Deep-clone each IdTable since IdTable is not copyable
    std::vector<IdTable> clones;
    clones.reserve(tables_.size());
    for (const auto& t : tables_) {
      clones.push_back(t.clone());
    }
    return std::make_unique<ChunkedMockOperation>(getExecutionContext(),
                                                  std::move(clones));
  }
  Result computeResult(bool /*requestLaziness*/) override {
    // Build a vector of idTables to yield lazily without mutating tables_
    std::vector<Result::IdTableVocabPair> pairs;
    pairs.reserve(tables_.size());
    for (const auto& t : tables_) {
      // Clone each table into the pair
      pairs.emplace_back(t.clone(), LocalVocab{});
    }
    // Wrap in LazyResult via type erasure
    return Result{Result::LazyResult{std::move(pairs)},
                  std::vector<ColumnIndex>{}};
  }
};

// Convenience overloads for chunked inputs
// Single-column chunks
inline std::vector<IdTable> createLazyIdTables(
    const ChunkedRowData& chunks, AllocatorWithLimit<Id>& allocator) {
  std::vector<IdTable> tables;
  tables.reserve(chunks.size());
  for (auto& rowData : chunks) {
    tables.push_back(createIdTable(rowData, allocator));
  }
  return tables;
}

// Multi-column chunks
inline std::vector<IdTable> createLazyIdTables(
    const ChunkedTableData& chunks, AllocatorWithLimit<Id>& allocator) {
  std::vector<IdTable> tables;
  tables.reserve(chunks.size());
  for (auto& tableData : chunks) {
    tables.push_back(createIdTable(tableData, allocator));
  }
  return tables;
}

// Setup a GroupByImpl that groups on all columns and takes lazy input chunks.
inline std::unique_ptr<GroupByImpl> setupLazyGroupBy(
    std::vector<IdTable>&& tables, QueryExecutionContext* qec) {
  // Infer grouping variables and sort columns from column count
  size_t numCols = tables.empty() ? 0 : tables[0].numColumns();
  std::vector<Variable> groupVars;
  std::vector<ColumnIndex> sortCols;
  groupVars.reserve(numCols);
  sortCols.reserve(numCols);
  for (size_t i = 0; i < numCols; ++i) {
    groupVars.emplace_back(std::string("?") + static_cast<char>('a' + i));
    sortCols.emplace_back(static_cast<ColumnIndex>(i));
  }
  // Build chunked mock subtree
  auto chunkOp = std::make_shared<ChunkedMockOperation>(qec, std::move(tables));
  std::shared_ptr<QueryExecutionTree> subtree =
      std::make_shared<QueryExecutionTree>(qec, chunkOp);
  // Inject sort if grouping columns exist
  if (!sortCols.empty()) {
    auto sortOp = std::make_shared<Sort>(qec, subtree, sortCols);
    subtree = std::make_shared<QueryExecutionTree>(qec, sortOp);
  }
  return std::make_unique<GroupByImpl>(qec, groupVars, std::vector<Alias>{},
                                       subtree);
}

#endif  // QLEVER_GROUPBY_SAMPLING_HELPERS_H
