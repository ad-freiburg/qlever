#ifndef QLEVER_GROUPBY_STRATEGY_HELPERS_H
#define QLEVER_GROUPBY_STRATEGY_HELPERS_H

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "../util/AllocatorTestHelpers.h"
#include "../util/IdTableHelpers.h"
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

// Helper to create a `GroupByImpl` that groups on all table columns and injects
// a Sort. The input is not advertised as pre-sorted; the Sort node below
// establishes the ordering.
// _____________________________________________________________________________

// Generic implementation that accepts either an IdTable or a
// std::vector<IdTable> and forwards it to ValuesForTesting. This removes
// duplication between the eager and lazy setups.
template <typename Tables>
inline std::unique_ptr<GroupByImpl> setupGroupByGeneric(
    Tables&& tables, QueryExecutionContext* qec) {
  using Decayed = std::remove_reference_t<Tables>;
  size_t numCols = 0;
  if constexpr (std::is_same_v<Decayed, IdTable>) {
    // If `tables` is only one IdTable, we get the number of columns directly.
    numCols = tables.numColumns();
  } else if (!tables.empty()) {
    // If `tables` is a vector of IdTables, we get it from the first table.
    numCols = tables[0].numColumns();
  }

  std::vector<Variable> groupVars;
  std::vector<ColumnIndex> sortCols;
  groupVars.reserve(numCols);
  sortCols.reserve(numCols);
  for (size_t i = 0; i < numCols; ++i) {
    groupVars.emplace_back(std::string("?") + static_cast<char>('a' + i));
    sortCols.emplace_back(static_cast<ColumnIndex>(i));
  }

  // Build ValuesForTesting op. Don't claim sortedness here.
  std::vector<std::optional<Variable>> varOpts;
  for (auto& var : groupVars) varOpts.emplace_back(var);

  std::shared_ptr<Operation> valuesOp;
  if constexpr (std::is_same_v<Decayed, IdTable>) {
    // If `tables` is a single IdTable, move it to avoid copying.
    valuesOp = std::make_shared<ValuesForTesting>(qec, std::move(tables),
                                                  std::move(varOpts), false,
                                                  std::vector<ColumnIndex>{});
  } else {
    // If `tables` is a vector of IdTables, forward as is.
    valuesOp = std::make_shared<ValuesForTesting>(
        qec, std::forward<Tables>(tables), std::move(varOpts), false,
        std::vector<ColumnIndex>{});
  }
  auto subtree = std::make_shared<QueryExecutionTree>(qec, valuesOp);
  if (!sortCols.empty()) {
    subtree = QueryExecutionTree::createSortedTree(subtree, sortCols);
  }
  return std::make_unique<GroupByImpl>(qec, groupVars, std::vector<Alias>{},
                                       subtree);
}

// Thin wrappers to preserve previous call sites and signatures.
// _____________________________________________________________________________

// Setup a GroupByImpl that groups on all columns and takes a single
// materialized input table.
inline std::unique_ptr<GroupByImpl> setupGroupBy(IdTable& table,
                                                 QueryExecutionContext* qec) {
  return setupGroupByGeneric(table, qec);
}

// Setup a GroupByImpl that groups on all columns and takes lazy input chunks.
inline std::unique_ptr<GroupByImpl> setupLazyGroupBy(
    std::vector<IdTable>&& tables, QueryExecutionContext* qec) {
  return setupGroupByGeneric(std::move(tables), qec);
}

#endif  // QLEVER_GROUPBY_STRATEGY_HELPERS_H
