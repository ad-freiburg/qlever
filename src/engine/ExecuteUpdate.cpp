//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "ExecuteUpdate.h"

#include "ExportQueryExecutionTrees.h"

// _____________________________________________________________________________
void ExecuteUpdate::executeUpdate(const Index& index, const ParsedQuery& query,
                                  const QueryExecutionTree& qet,
                                  CancellationHandle cancellationHandle) {
  AD_CONTRACT_CHECK(query.hasUpdateClause());
  auto updateClause = query.updateClause();
  if (!std::holds_alternative<updateClause::GraphUpdate>(updateClause.op_)) {
    throw std::runtime_error(
        "Only INSERT/DELETE update operations are currently supported.");
  }
  auto graphUpdate = std::get<updateClause::GraphUpdate>(updateClause.op_);
  auto res = qet.getResult(true);

  auto& vocab = index.getVocab();

  auto [toInsertTemplates, localVocabInsert] =
      ExecuteUpdate::transformTriplesTemplate(vocab, qet.getVariableColumns(),
                                              std::move(graphUpdate.toInsert_));
  auto [toDeleteTemplates, localVocabDelete] =
      ExecuteUpdate::transformTriplesTemplate(vocab, qet.getVariableColumns(),
                                              std::move(graphUpdate.toDelete_));

  std::vector<IdTriple<>> toInsert;
  std::vector<IdTriple<>> toDelete;
  // The maximum result size is size(query result) x num template rows. The
  // actual result can be smaller if there are template rows with variables for
  // which a result row does not have a value.
  toInsert.reserve(res->idTable().size() * toInsertTemplates.size());
  toDelete.reserve(res->idTable().size() * toDeleteTemplates.size());
  for (const auto& [pair, range] :
       ExportQueryExecutionTrees::getRowIndices(query._limitOffset, *res)) {
    auto& idTable = pair.idTable_;
    for (uint64_t i : range) {
      ExecuteUpdate::computeAndAddQuadsForResultRow(toInsertTemplates, toInsert,
                                                    idTable, i);
      cancellationHandle->throwIfCancelled();

      ExecuteUpdate::computeAndAddQuadsForResultRow(toDeleteTemplates, toDelete,
                                                    idTable, i);
      cancellationHandle->throwIfCancelled();
    }
  }

  // TODO<qup42> use the actual DeltaTriples object
  DeltaTriples deltaTriples(index);
  deltaTriples.insertTriples(cancellationHandle, std::move(toInsert));
  deltaTriples.deleteTriples(cancellationHandle, std::move(toDelete));
}

// _____________________________________________________________________________
std::pair<std::vector<ExecuteUpdate::TransformedTriple>, LocalVocab>
ExecuteUpdate::transformTriplesTemplate(
    const Index::Vocab& vocab, const VariableToColumnMap& variableColumns,
    std::vector<SparqlTripleSimpleWithGraph>&& triples) {
  // This LocalVocab only contains IDs that are related to the
  // template. Most of the IDs will be added to the DeltaTriples' LocalVocab. An
  // ID will only not be added if it belongs to a Quad with a variable that has
  // no solutions.
  LocalVocab localVocab{};

  auto transformSparqlTripleComponent =
      [&vocab, &localVocab,
       &variableColumns](TripleComponent component) -> IdOrVariableIndex {
    if (component.isVariable()) {
      return variableColumns.at(component.getVariable());
    } else {
      return std::move(component).toValueId(vocab, localVocab);
    }
  };
  Id defaultGraphIri = [&transformSparqlTripleComponent] {
    IdOrVariableIndex defaultGraph =
        transformSparqlTripleComponent(DEFAULT_GRAPH_IRI);
    AD_CORRECTNESS_CHECK(std::holds_alternative<Id>(defaultGraph));
    return std::get<Id>(defaultGraph);
  }();
  auto transformGraph =
      [&vocab, &localVocab, &defaultGraphIri,
       &variableColumns](SparqlTripleSimpleWithGraph::Graph graph) {
        return std::visit(
            ad_utility::OverloadCallOperator{
                [&defaultGraphIri](const std::monostate&) -> IdOrVariableIndex {
                  return defaultGraphIri;
                },
                [&vocab, &localVocab](const Iri& iri) -> IdOrVariableIndex {
                  ad_utility::triple_component::Iri i =
                      ad_utility::triple_component::Iri::fromIriref(iri.iri());
                  return TripleComponent(i).toValueId(vocab, localVocab);
                },
                [&variableColumns](const Variable& var) -> IdOrVariableIndex {
                  return variableColumns.at(var);
                }},
            graph);
      };
  auto transformSparqlTripleSimple =
      [&transformSparqlTripleComponent,
       &transformGraph](SparqlTripleSimpleWithGraph triple) {
        return std::array{transformSparqlTripleComponent(std::move(triple.s_)),
                          transformSparqlTripleComponent(std::move(triple.p_)),
                          transformSparqlTripleComponent(std::move(triple.o_)),
                          transformGraph(std::move(triple.g_))};
      };
  return {
      ad_utility::transform(std::move(triples), transformSparqlTripleSimple),
      std::move(localVocab)};
}

// _____________________________________________________________________________
std::optional<Id> ExecuteUpdate::resolveVariable(const IdTable& idTable,
                                                 const size_t& row,
                                                 IdOrVariableIndex idOrVar) {
  return std::visit(
      ad_utility::OverloadCallOperator{
          [&idTable, &row](
              const ColumnIndexAndTypeInfo& columnInfo) -> std::optional<Id> {
            const auto id = idTable(row, columnInfo.columnIndex_);
            return id.isUndefined() ? std::optional<Id>{} : id;
          },
          [](const Id& id) -> std::optional<Id> {
            return id.isUndefined() ? std::optional<Id>{} : id;
          }},
      idOrVar);
}

// _____________________________________________________________________________
void ExecuteUpdate::computeAndAddQuadsForResultRow(
    std::vector<TransformedTriple>& templates, std::vector<IdTriple<>>& result,
    const IdTable& idTable, const uint64_t row) {
  for (const auto& [s, p, o, g] : templates) {
    auto subject = ExecuteUpdate::resolveVariable(idTable, row, s);
    auto predicate = ExecuteUpdate::resolveVariable(idTable, row, p);
    auto object = ExecuteUpdate::resolveVariable(idTable, row, o);
    auto graph = ExecuteUpdate::resolveVariable(idTable, row, g);

    if (!subject.has_value() || !predicate.has_value() || !object.has_value() ||
        !graph.has_value()) {
      continue;
    }
    result.emplace_back(std::array{*subject, *predicate, *object, *graph});
  }
}
