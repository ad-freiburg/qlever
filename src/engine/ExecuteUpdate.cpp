//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "engine/ExecuteUpdate.h"

#include "engine/ExportQueryExecutionTrees.h"

// _____________________________________________________________________________
UpdateMetadata ExecuteUpdate::executeUpdate(
    const Index& index, const ParsedQuery& query, const QueryExecutionTree& qet,
    DeltaTriples& deltaTriples, const CancellationHandle& cancellationHandle) {
  UpdateMetadata metadata{};
  auto [toInsert, toDelete] =
      computeGraphUpdateQuads(index, query, qet, cancellationHandle, metadata);

  // "The deletion of the triples happens before the insertion." (SPARQL 1.1
  // Update 3.1.3)
  ad_utility::Timer timer{ad_utility::Timer::InitialStatus::Started};
  deltaTriples.deleteTriples(cancellationHandle,
                             std::move(toDelete.idTriples_));
  metadata.deletionTime_ = timer.msecs();
  timer.reset();
  deltaTriples.insertTriples(cancellationHandle,
                             std::move(toInsert.idTriples_));
  metadata.insertionTime_ = timer.msecs();
  return metadata;
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

bool ExecuteUpdate::templatedTriplesExist(
    const vector<SparqlTripleSimpleWithGraph>& templates,
    const parsedQuery::GraphPattern& graphPattern) {
  // Triples with non-default graph are represented in graph patterns in a way
  // that we do not detect.
  if (std::ranges::any_of(
          templates, [](const SparqlTripleSimpleWithGraph& triple) {
            return !std::holds_alternative<std::monostate>(triple.g_);
          })) {
    return false;
  }
  // The triples all have the default graph as graph, we can treat them as a
  // `SparqlTripleSimple`.
  if (graphPattern._graphPatterns.size() != 1) {
    return false;
  }
  const parsedQuery::GraphPatternOperation rootOperation =
      graphPattern._graphPatterns[0];
  if (!std::holds_alternative<parsedQuery::BasicGraphPattern>(rootOperation)) {
    return false;
  }
  const parsedQuery::BasicGraphPattern& rootGraphPattern =
      rootOperation.getBasic();
  vector<SparqlTriple> triplesGraphPattern = rootGraphPattern._triples;
  // If any of the predicates is a non-trivial (meaning not just an Iri)
  // property path then we cannot determine whether the triples exist in the
  // index.
  if (std::ranges::any_of(triplesGraphPattern, [](const SparqlTriple& triple) {
        return !triple.p_.isIri();
      })) {
    return false;
  }
  // The `SparqlTriple` are now all simple.
  if (triplesGraphPattern.size() < templates.size()) {
    return false;
  }
  vector<SparqlTripleSimple> triplesGraphPatternSimple =
      ad_utility::transform(triplesGraphPattern, &SparqlTriple::getSimple);
  // Return whether the triples in the Graph Pattern are a subset of the triples
  // in the Delete clause.
  return templates.empty() ||
         ql::ranges::all_of(
             templates, [&triplesGraphPatternSimple](
                            const SparqlTripleSimpleWithGraph& triple) {
               return std::ranges::find(triplesGraphPatternSimple, triple) !=
                      triplesGraphPatternSimple.end();
             });
}

// _____________________________________________________________________________
std::pair<ExecuteUpdate::IdTriplesAndLocalVocab,
          ExecuteUpdate::IdTriplesAndLocalVocab>
ExecuteUpdate::computeGraphUpdateQuads(
    const Index& index, const ParsedQuery& query, const QueryExecutionTree& qet,
    const CancellationHandle& cancellationHandle, UpdateMetadata& metadata) {
  AD_CONTRACT_CHECK(query.hasUpdateClause());
  auto updateClause = query.updateClause();
  if (!std::holds_alternative<updateClause::GraphUpdate>(updateClause.op_)) {
    throw std::runtime_error(
        "Only INSERT/DELETE update operations are currently supported.");
  }
  auto graphUpdate = std::get<updateClause::GraphUpdate>(updateClause.op_);
  // Fully materialize the result for now. This makes it easier to execute the
  // update.
  auto result = qet.getResult(false);

  // Start the timer once the where clause has been evaluated.
  ad_utility::Timer timer{ad_utility::Timer::InitialStatus::Started};
  const auto& vocab = index.getVocab();

  auto prepareTemplateAndResultContainer =
      [&vocab, &qet,
       &result](std::vector<SparqlTripleSimpleWithGraph>&& tripleTemplates) {
        auto [transformedTripleTemplates, localVocab] =
            transformTriplesTemplate(vocab, qet.getVariableColumns(),
                                     std::move(tripleTemplates));
        std::vector<IdTriple<>> updateTriples;
        // The maximum result size is size(query result) x num template rows.
        // The actual result can be smaller if there are template rows with
        // variables for which a result row does not have a value.
        updateTriples.reserve(result->idTable().size() *
                              transformedTripleTemplates.size());

        return std::tuple{std::move(transformedTripleTemplates),
                          std::move(updateTriples), std::move(localVocab)};
      };

  auto exist = std::bind(templatedTriplesExist, std::placeholders::_1,
                         query._rootGraphPattern);
  bool allDeleteExist = exist(graphUpdate.toDelete_);
  bool allInsertExist = exist(graphUpdate.toInsert_);
  auto [toInsertTemplates, toInsert, localVocabInsert] =
      prepareTemplateAndResultContainer(std::move(graphUpdate.toInsert_));
  auto [toDeleteTemplates, toDelete, localVocabDelete] =
      prepareTemplateAndResultContainer(std::move(graphUpdate.toDelete_));

  uint64_t resultSize = 0;
  for (const auto& [pair, range] : ExportQueryExecutionTrees::getRowIndices(
           query._limitOffset, *result, resultSize)) {
    auto& idTable = pair.idTable_;
    for (const uint64_t i : range) {
      computeAndAddQuadsForResultRow(toInsertTemplates, toInsert, idTable, i);
      cancellationHandle->throwIfCancelled();

      computeAndAddQuadsForResultRow(toDeleteTemplates, toDelete, idTable, i);
      cancellationHandle->throwIfCancelled();
    }
  }
  auto sortAndRemoveDuplicates = [](auto& vec) {
    ql::ranges::sort(vec);
    auto first = ql::ranges::unique(vec).begin();
    vec.erase(first, vec.end());
  };
  // We could also do this once directly one the triple templates, if they had
  // an ordering.
  auto setMinus = []<typename T>(std::vector<T>& left, std::vector<T>& right) {
    vector<T> optimisedLeft;
    ql::ranges::set_difference(left, right, std::back_inserter(optimisedLeft));
    return optimisedLeft;
  };
  auto pairwiseSetMinus = [&setMinus]<typename T>(std::vector<T>& left,
                                                  std::vector<T>& right) {
    vector<T> optimisedLeft = setMinus(left, right);
    vector<T> optimisedRight = setMinus(right, left);
    left = std::move(optimisedLeft);
    right = std::move(optimisedRight);
  };
  // The following observations allows us to optimise the number of triples we
  // store for updates:
  // Assume the update `DELETE { <A'> } INSERT { <B> } WHERE { <A''> }`.
  // - if A' <= A'' then all triples that will be deleted by this query are in
  // the index
  // - if all triples that are being deleted we can omit triples that are both
  // deleted and inserted by this query
  // We only have to delete the triples A'\B and insert B\A'.
  sortAndRemoveDuplicates(toInsert);
  sortAndRemoveDuplicates(toDelete);
  if (allInsertExist) {
    // If all triples to be inserted already exist, we can remove them from the
    // deletion and then can insert nothing.
    toDelete = setMinus(toDelete, toInsert);
    toInsert.clear();
  } else if (allDeleteExist) {
    // If all triples to be deleted exist, we only have to insert/delete the
    // triples that are not in the other set.
    pairwiseSetMinus(toInsert, toDelete);
  }

  metadata.triplePreparationTime_ = timer.msecs();

  return {
      IdTriplesAndLocalVocab{std::move(toInsert), std::move(localVocabInsert)},
      IdTriplesAndLocalVocab{std::move(toDelete), std::move(localVocabDelete)}};
}
