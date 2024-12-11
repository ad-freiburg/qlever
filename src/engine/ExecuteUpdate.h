//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

#include <gtest/gtest_prod.h>

#include "index/Index.h"
#include "parser/ParsedQuery.h"
#include "util/CancellationHandle.h"

// Metadata about the time spent on different parts of an update.
struct UpdateMetadata {
  std::chrono::milliseconds triplePreparationTime_ =
      std::chrono::milliseconds::zero();
  std::chrono::milliseconds insertionTime_ = std::chrono::milliseconds::zero();
  std::chrono::milliseconds deletionTime_ = std::chrono::milliseconds::zero();
};

class ExecuteUpdate {
 public:
  using CancellationHandle = ad_utility::SharedCancellationHandle;
  using IdOrVariableIndex = std::variant<Id, ColumnIndex>;
  using TransformedTriple = std::array<IdOrVariableIndex, 4>;

  // Execute an update. This function is comparable to
  // `ExportQueryExecutionTrees::computeResult` for queries.
  static UpdateMetadata executeUpdate(
      const Index& index, const ParsedQuery& query,
      const QueryExecutionTree& qet, DeltaTriples& deltaTriples,
      const CancellationHandle& cancellationHandle);

 private:
  // Resolve all `TripleComponent`s and `Graph`s in a vector of
  // `SparqlTripleSimpleWithGraph` into `Variable`s or `Id`s.
  static std::pair<std::vector<TransformedTriple>, LocalVocab>
  transformTriplesTemplate(const Index::Vocab& vocab,
                           const VariableToColumnMap& variableColumns,
                           std::vector<SparqlTripleSimpleWithGraph>&& triples);
  FRIEND_TEST(ExecuteUpdate, transformTriplesTemplate);

  // Resolve a single `IdOrVariable` to an `Id` by looking up the value in the
  // result row. The `Id`s will never be undefined. If (and only if) the input
  // `Id` or the `Id` looked up in the `IdTable` is undefined then
  // `std::nullopt` is returned.
  static std::optional<Id> resolveVariable(const IdTable& idTable,
                                           const uint64_t& rowIdx,
                                           IdOrVariableIndex idOrVar);
  FRIEND_TEST(ExecuteUpdate, resolveVariable);

  // Calculate and add the set of quads for the update that results from
  // interpolating one result row into the template. The resulting `IdTriple`s
  // consist of only `Id`s.
  static void computeAndAddQuadsForResultRow(
      const std::vector<TransformedTriple>& templates,
      std::vector<IdTriple<>>& result, const IdTable& idTable, uint64_t rowIdx);
  FRIEND_TEST(ExecuteUpdate, computeAndAddQuadsForResultRow);

  struct IdTriplesAndLocalVocab {
    std::vector<IdTriple<>> idTriples_;
    LocalVocab localVocab_;
  };
  // Compute the set of quads to insert and delete for the given update. The
  // ParsedQuery's clause must be an UpdateClause. The UpdateClause's operation
  // must be a GraphUpdate.
  static std::pair<IdTriplesAndLocalVocab, IdTriplesAndLocalVocab>
  computeGraphUpdateQuads(const Index& index, const ParsedQuery& query,
                          const QueryExecutionTree& qet,
                          const CancellationHandle& cancellationHandle,
                          UpdateMetadata& metadata);
  FRIEND_TEST(ExecuteUpdate, computeGraphUpdateQuads);
};
