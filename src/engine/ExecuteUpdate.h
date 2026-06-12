//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_EXECUTEUPDATE_H
#define QLEVER_SRC_ENGINE_EXECUTEUPDATE_H

#include <gtest/gtest_prod.h>

#include "engine/UpdateMetadata.h"
#include "global/IdTriple.h"
#include "index/DeltaTriples.h"
#include "index/Index.h"
#include "parser/ParsedQuery.h"
#include "util/CancellationHandle.h"
#include "util/TimeTracer.h"

class ExecuteUpdate {
 public:
  using CancellationHandle = ad_utility::SharedCancellationHandle;
  using IdOrVariableIndex = std::variant<Id, ColumnIndex>;
  using TransformedTriple = std::array<IdOrVariableIndex, 4>;

  // Execute an update. This function is comparable to
  // `ExportQueryExecutionTrees::computeResult` for queries. If
  // `returnDeltaTriples` is true, the returned `UpdateMetadata::delta_` field
  // is populated with the genuine materialized delta (Stage-2: triples that
  // actually changed the store), serialized as N-Quads lines.
  static UpdateMetadata executeUpdate(
      const Index& index, const ParsedQuery& query,
      const QueryExecutionTree& qet, DeltaTriples& deltaTriples,
      const CancellationHandle& cancellationHandle,
      ad_utility::timer::TimeTracer& tracer =
          ad_utility::timer::DEFAULT_TIME_TRACER,
      bool returnDeltaTriples = false);

 private:
  // Resolve all `TripleComponent`s and `Graph`s in a vector of
  // `SparqlTripleSimpleWithGraph` into `Variable`s or `Id`s.
  static std::pair<std::vector<ExecuteUpdate::TransformedTriple>, LocalVocab>
  transformTriplesTemplate(
      const IndexImpl& index, const VariableToColumnMap& variableColumns,
      const std::vector<SparqlTripleSimpleWithGraph>& triples);
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
                          const Result& result,
                          const VariableToColumnMap& variableColumns,
                          const CancellationHandle& cancellationHandle,
                          UpdateMetadata& metadata,
                          ad_utility::timer::TimeTracer& tracer =
                              ad_utility::timer::DEFAULT_TIME_TRACER);
  FRIEND_TEST(ExecuteUpdate, computeGraphUpdateQuads);

  // After the operation the vector is sorted and contains no duplicate
  // elements.
  static void sortAndRemoveDuplicates(std::vector<IdTriple<>>& container);
  FRIEND_TEST(ExecuteUpdate, sortAndRemoveDuplicates);

  // Convert a single `Id` to its N-Quads / N-Triples term representation.
  // Triggers `AD_CORRECTNESS_CHECK` if `id` is `Undefined` (which should
  // never occur for triples produced by `computeAndAddQuadsForResultRow`).
  static std::string idToNQuadsTerm(const Index& index, Id id,
                                    const LocalVocab& localVocab);

  // Serialize a single `IdTriple<0>` as one N-Quads / N-Triples line. When
  // the graph component is QLever's internal DEFAULT_GRAPH_IRI, a 3-column
  // N-Triples line is emitted (no graph term); otherwise a 4-column N-Quads
  // line is emitted. The internal default-graph IRI is never exposed to
  // callers.
  static std::string tripleToNQuadsLine(const Index& index,
                                        const IdTriple<0>& triple,
                                        const LocalVocab& localVocab);

  // Build an `UpdateDelta` by serializing every triple in `inserted` and
  // `deleted` to N-Quads lines using `localVocab` for ID resolution.
  static UpdateDelta serializeMaterializedDelta(
      const Index& index, const DeltaTriples::Triples& inserted,
      const DeltaTriples::Triples& deleted, const LocalVocab& localVocab);

  // For two sorted vectors `A` and `B` return a new vector
  // that contains the element of `A\B`.
  // Precondition: the inputs must be sorted.
  static std::vector<IdTriple<>> setMinus(const std::vector<IdTriple<>>& a,
                                          const std::vector<IdTriple<>>& b);
  FRIEND_TEST(ExecuteUpdate, setMinus);
};

#endif  // QLEVER_SRC_ENGINE_EXECUTEUPDATE_H
