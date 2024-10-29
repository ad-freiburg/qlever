//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#pragma once

class ExecuteUpdate {
 public:
  using CancellationHandle = ad_utility::SharedCancellationHandle;

  // Execute an update. This function is comparable to
  // `ExportQueryExecutionTrees::computeResult` for queries.
  static void executeUpdate(const Index& index, const ParsedQuery& query,
                            const QueryExecutionTree& qet,
                            CancellationHandle cancellationHandle);

 private:
  using IdOrVariableIndex = std::variant<Id, ColumnIndexAndTypeInfo>;
  using TransformedTriple = std::array<IdOrVariableIndex, 4>;

  // Resolve all `TripleComponent`s and `Graph`s in a vector of
  // `SparqlTripleSimpleWithGraph` into `Variable`s and `Id`s.
  static std::pair<std::vector<TransformedTriple>, LocalVocab>
  transformTriplesTemplate(const Index::Vocab& vocab,
                           const VariableToColumnMap& variableColumns,
                           std::vector<SparqlTripleSimpleWithGraph>&& triples);

  // Resolve a single `IdOrVariable` to an `Id` by looking up the value in the
  // result row. The `Id`s will never be undefined.
  static std::optional<Id> resolveVariable(const IdTable& idTable,
                                           const size_t& row,
                                           IdOrVariableIndex idOrVar);

  // Calculate and add the set of quads for the update that results
  // interpolating one result row into the template. This yields quads without
  // variables which can be used with `DeltaTriples`.
  static void computeAndAddQuadsForResultRow(
      std::vector<TransformedTriple>& templates,
      std::vector<IdTriple<>>& result, const IdTable& idTable, uint64_t row);
};
