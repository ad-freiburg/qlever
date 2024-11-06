//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "engine/ExecuteUpdate.h"

#include "engine/ExportQueryExecutionTrees.h"

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
      AD_CORRECTNESS_CHECK(variableColumns.contains(component.getVariable()));
      return variableColumns.at(component.getVariable()).columnIndex_;
    } else {
      return std::move(component).toValueId(vocab, localVocab);
    }
  };
  Id defaultGraphIri = [&transformSparqlTripleComponent] {
    IdOrVariableIndex defaultGraph = transformSparqlTripleComponent(
        ad_utility::triple_component::Iri::fromIriref(DEFAULT_GRAPH_IRI));
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
                  AD_CORRECTNESS_CHECK(variableColumns.contains(var));
                  return variableColumns.at(var).columnIndex_;
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
                                                 const uint64_t& rowIdx,
                                                 IdOrVariableIndex idOrVar) {
  auto visitId = [](const Id& id) {
    return id.isUndefined() ? std::optional<Id>{} : id;
  };
  return std::visit(
      ad_utility::OverloadCallOperator{
          [&idTable, &rowIdx, &visitId](const ColumnIndex& columnInfo) {
            return visitId(idTable(rowIdx, columnInfo));
          },
          visitId},
      idOrVar);
}

// _____________________________________________________________________________
void ExecuteUpdate::computeAndAddQuadsForResultRow(
    const std::vector<TransformedTriple>& templates,
    std::vector<IdTriple<>>& result, const IdTable& idTable,
    const uint64_t rowIdx) {
  for (const auto& [s, p, o, g] : templates) {
    auto subject = resolveVariable(idTable, rowIdx, s);
    auto predicate = resolveVariable(idTable, rowIdx, p);
    auto object = resolveVariable(idTable, rowIdx, o);
    auto graph = resolveVariable(idTable, rowIdx, g);

    if (!subject.has_value() || !predicate.has_value() || !object.has_value() ||
        !graph.has_value()) {
      continue;
    }
    result.emplace_back(std::array{*subject, *predicate, *object, *graph});
  }
}
