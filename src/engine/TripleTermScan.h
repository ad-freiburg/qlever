// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_TRIPLETERMSCAN_H
#define QLEVER_SRC_ENGINE_TRIPLETERMSCAN_H

#include <atomic>
#include <memory>
#include <string>

#include "engine/ExplicitIdTableOperation.h"
#include "engine/QueryExecutionTree.h"
#include "index/TripleTermIndex.h"

namespace qlever::tripleTermPrototype {

// Materialize the prototype POIS scan as a normal QLever operation. This
// eager adapter lets ordinary joins consume the bindings before a dedicated
// disk-backed/lazy scan and planner integration are implemented.
inline std::shared_ptr<QueryExecutionTree> makeTripleTermScan(
    QueryExecutionContext* qec, const TripleTermIndexBuilder& index,
    Id predicate, Id object, const Variable& subjectVariable,
    const Variable& termVariable) {
  AD_CONTRACT_CHECK(qec != nullptr);
  AD_CONTRACT_CHECK(subjectVariable != termVariable);
  qec->cancellationHandle()->throwIfCancelled();
  auto table = std::make_shared<IdTable>(2, qec->getAllocator());
  size_t count = 0;
  for (const auto& row : index.scanIds(predicate, object)) {
    table->push_back(row);
    if (++count % 1000 == 0) {
      qec->cancellationHandle()->throwIfCancelled();
    }
  }
  qec->cancellationHandle()->throwIfCancelled();
  VariableToColumnMap variables{{subjectVariable, makeAlwaysDefinedColumn(0)},
                                {termVariable, makeAlwaysDefinedColumn(1)}};
  // Different in-memory dictionaries may reuse term IDs. Unique snapshot keys
  // prevent ancestor operations from reusing a result from another dictionary.
  static std::atomic<uint64_t> nextSnapshot = 0;
  auto key = "TripleTermPrototypeScan:" + std::to_string(nextSnapshot++);
  return ad_utility::makeExecutionTree<ExplicitIdTableOperation>(
      qec, std::shared_ptr<const IdTable>{std::move(table)},
      std::move(variables), std::vector<ColumnIndex>{1}, LocalVocab{},
      std::move(key));
}
}  // namespace qlever::tripleTermPrototype

#endif  // QLEVER_SRC_ENGINE_TRIPLETERMSCAN_H
