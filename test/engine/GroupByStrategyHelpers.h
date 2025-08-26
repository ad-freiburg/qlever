#ifndef QLEVER_GROUPBY_STRATEGY_HELPERS_H
#define QLEVER_GROUPBY_STRATEGY_HELPERS_H

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "../util/AllocatorTestHelpers.h"
#include "../util/IdTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "ValuesForTesting.h"
#include "engine/GroupByImpl.h"
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

// Helper to create a `GroupByImpl` that groups on all table columns and injects
// a Sort. Important: Do NOT advertise pre-sorted input to `ValuesForTesting`,
// the explicit Sort node enforces the ordering.
inline std::unique_ptr<GroupByImpl> setupGroupBy(IdTable& table,
                                                 QueryExecutionContext* qec) {
  size_t numCols = table.numColumns();
  std::vector<Variable> groupVars;
  std::vector<ColumnIndex> sortCols;
  groupVars.reserve(numCols);
  sortCols.reserve(numCols);
  for (size_t i = 0; i < numCols; ++i) {
    groupVars.emplace_back(std::string("?") + static_cast<char>('a' + i));
    sortCols.emplace_back(static_cast<ColumnIndex>(i));
  }
  // Build ValuesForTesting op (eager). Don't claim sortedness here.
  std::vector<std::optional<Variable>> varOpts;
  for (auto& var : groupVars) varOpts.emplace_back(var);
  auto valuesOp =
      std::make_shared<ValuesForTesting>(qec, std::move(table), std::move(varOpts),
                                         /*supportsLimit=*/false,
                                         /*sortedColumns=*/std::vector<ColumnIndex>{});
  auto subtree = std::make_shared<QueryExecutionTree>(qec, valuesOp);
  if (!sortCols.empty()) {
    subtree = std::make_shared<QueryExecutionTree>(
        qec, std::make_shared<Sort>(qec, subtree, sortCols));
  }
  return std::make_unique<GroupByImpl>(qec, groupVars, std::vector<Alias>{},
                                       subtree);
}

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
// Important: The input is not advertised as pre-sorted; the Sort node below
// establishes the ordering.
inline std::unique_ptr<GroupByImpl> setupLazyGroupBy(
    std::vector<IdTable>&& tables, QueryExecutionContext* qec) {
  size_t numCols = tables.empty() ? 0 : tables[0].numColumns();
  std::vector<Variable> groupVars;
  std::vector<ColumnIndex> sortCols;
  groupVars.reserve(numCols);
  sortCols.reserve(numCols);
  for (size_t i = 0; i < numCols; ++i) {
    groupVars.emplace_back(std::string("?") + static_cast<char>('a' + i));
    sortCols.emplace_back(static_cast<ColumnIndex>(i));
  }
  // Build ValuesForTesting op (lazy). Don't claim sortedness here.
  std::vector<std::optional<Variable>> varOpts;
  for (auto& var : groupVars) varOpts.emplace_back(var);
  auto valuesOp = std::make_shared<ValuesForTesting>(
      qec, std::move(tables), std::move(varOpts),
      /*unlikelyToFitInCache=*/false,
      /*sortedColumns=*/std::vector<ColumnIndex>{});
  auto subtree = std::make_shared<QueryExecutionTree>(qec, valuesOp);
  if (!sortCols.empty()) {
    subtree = std::make_shared<QueryExecutionTree>(
        qec, std::make_shared<Sort>(qec, subtree, sortCols));
  }
  return std::make_unique<GroupByImpl>(qec, groupVars, std::vector<Alias>{},
                                       subtree);
}

#endif  // QLEVER_GROUPBY_STRATEGY_HELPERS_H
