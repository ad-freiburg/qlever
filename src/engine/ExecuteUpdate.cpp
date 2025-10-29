//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "engine/ExecuteUpdate.h"

#include "engine/ExportQueryExecutionTrees.h"

// _____________________________________________________________________________
UpdateMetadata ExecuteUpdate::executeUpdate(
    const Index& index, const ParsedQuery& query, const QueryExecutionTree& qet,
    DeltaTriples& deltaTriples, const CancellationHandle& cancellationHandle,
    ad_utility::timer::TimeTracer& tracer) {
  UpdateMetadata metadata{};
  // Fully materialize the result for now. This makes it easier to execute the
  // update. We have to keep the local vocab alive until the triples are
  // inserted using `deleteTriples`/`insertTriples` to keep LocalVocabIds valid.
  tracer.beginTrace("materializeResult");
  auto result = qet.getResult(false);
  tracer.endTrace("materializeResult");
  auto [toInsert, toDelete] =
      computeGraphUpdateQuads(index, query, *result, qet.getVariableColumns(),
                              cancellationHandle, metadata, tracer);

  // "The deletion of the triples happens before the insertion." (SPARQL 1.1
  // Update 3.1.3)
  ad_utility::Timer timer{ad_utility::Timer::InitialStatus::Started};
  tracer.beginTrace("deleteTriples");
  if (!toDelete.idTriples_.empty()) {
    deltaTriples.deleteTriples(cancellationHandle,
                               std::move(toDelete.idTriples_), tracer);
  }
  tracer.endTrace("deleteTriples");
  tracer.beginTrace("insertTriples");
  timer.start();
  if (!toInsert.idTriples_.empty()) {
    deltaTriples.insertTriples(cancellationHandle,
                               std::move(toInsert.idTriples_), tracer);
  }
  tracer.endTrace("insertTriples");
  return metadata;
}

// _____________________________________________________________________________
std::pair<std::vector<ExecuteUpdate::TransformedTriple>, LocalVocab>
ExecuteUpdate::transformTriplesTemplate(
    const EncodedIriManager& encodedIriManager, const Index::Vocab& vocab,
    const VariableToColumnMap& variableColumns,
    const std::vector<SparqlTripleSimpleWithGraph>& triples) {
  // - collect all TripleComponents that live in the Vocabulary (literals and
  // IRIs)
  // - deduplicate and sort them according to the order in the Vocabulary
  // - create a map with the TC and their respective Id
  // - transform the triples using the map and the variableColumns to
  // `IdOrVariableIndex`
  // We do this to access the disk in an optimal pattern. The vocabulary does a
  // scan with jumps over the disk for lookups instead of random accesses.
  ad_utility::HashSet<TripleComponent> lookupItems;
  // Collect the Literals and Iri for the Vocab lookup.
  auto addTCToLookup = [&lookupItems](const TripleComponent& tc) {
    if (tc.isLiteral() || tc.isIri()) {
      lookupItems.insert(tc);
    }
  };
  auto addGraphToLookup =
      [&lookupItems](const SparqlTripleSimpleWithGraph::Graph& g) {
        // Ignore the default graph (std::monostate) here, because it is always
        // added.
        if (const auto* iri = std::get_if<TripleComponent::Iri>(&g)) {
          lookupItems.insert(TripleComponent(*iri));
        }
      };
  for (const auto& triple : triples) {
    addTCToLookup(triple.s_);
    addTCToLookup(triple.p_);
    addTCToLookup(triple.o_);
    addGraphToLookup(triple.g_);
  }
  // Always adding the default graph is no overhead and we need it for
  // `defaultGraphIri`.
  lookupItems.insert(TripleComponent(
      ad_utility::triple_component::Iri::fromIriref(DEFAULT_GRAPH_IRI)));
  std::vector lookupVec(lookupItems.begin(), lookupItems.end());
  ql::ranges::sort(
      lookupVec, vocab.getCaseComparator(), [](const TripleComponent& tc) {
        AD_CORRECTNESS_CHECK(tc.isLiteral() || tc.isIri());
        return tc.isLiteral() ? tc.getLiteral().toStringRepresentation()
                              : tc.getIri().toStringRepresentation();
      });
  // This LocalVocab only contains IDs that are related to the
  // template. Most of the IDs will be added to the DeltaTriples' LocalVocab. An
  // ID will only not be added if it belongs to a Quad with a variable that has
  // no solutions.
  LocalVocab localVocab{};
  ad_utility::HashMap<TripleComponent, Id> lookupMap;
  for (const auto& tc : lookupVec) {
    // Explicit copy because `toValueId` is `&&`.
    lookupMap.emplace(tc, TripleComponent(tc).toValueId(vocab, localVocab,
                                                        encodedIriManager));
  }
  auto lookupTc = [&encodedIriManager, &lookupMap, &variableColumns](
                      const TripleComponent& tc) -> IdOrVariableIndex {
    // Everything except for Literals and Iris can directly be converted to Ids.
    if (tc.isVariable()) {
      AD_CORRECTNESS_CHECK(variableColumns.contains(tc.getVariable()));
      return variableColumns.at(tc.getVariable()).columnIndex_;
    }
    if (!(tc.isLiteral() || tc.isIri())) {
      return tc.toValueIdIfNotString(&encodedIriManager).value();
    }
    auto found = lookupMap.find(tc);
    AD_CORRECTNESS_CHECK(found != lookupMap.end());
    return found->second;
  };
  Id defaultGraphIri = [&lookupTc] {
    const IdOrVariableIndex defaultGraph = lookupTc(
        ad_utility::triple_component::Iri::fromIriref(DEFAULT_GRAPH_IRI));
    AD_CORRECTNESS_CHECK(std::holds_alternative<Id>(defaultGraph));
    return std::get<Id>(defaultGraph);
  }();
  auto lookupGraph = [&defaultGraphIri, &variableColumns, &lookupTc](
                         const SparqlTripleSimpleWithGraph::Graph& graph)
      -> IdOrVariableIndex {
    return std::visit(
        ad_utility::OverloadCallOperator{
            [&defaultGraphIri](const std::monostate&) -> IdOrVariableIndex {
              return defaultGraphIri;
            },
            [&lookupTc](const ad_utility::triple_component::Iri& iri)
                -> IdOrVariableIndex { return lookupTc(TripleComponent(iri)); },
            [&variableColumns](const Variable& var) -> IdOrVariableIndex {
              AD_CORRECTNESS_CHECK(variableColumns.contains(var));
              return variableColumns.at(var).columnIndex_;
            }},
        graph);
  };
  auto transformTriple =
      [&lookupTc, &lookupGraph](const SparqlTripleSimpleWithGraph& triple) {
        return std::array{lookupTc(triple.s_), lookupTc(triple.p_),
                          lookupTc(triple.o_), lookupGraph(triple.g_)};
      };
  return {ad_utility::transform(triples, transformTriple),
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

// _____________________________________________________________________________
std::pair<ExecuteUpdate::IdTriplesAndLocalVocab,
          ExecuteUpdate::IdTriplesAndLocalVocab>
ExecuteUpdate::computeGraphUpdateQuads(
    const Index& index, const ParsedQuery& query, const Result& result,
    const VariableToColumnMap& variableColumns,
    const CancellationHandle& cancellationHandle, UpdateMetadata& metadata,
    ad_utility::timer::TimeTracer& tracer) {
  AD_CONTRACT_CHECK(query.hasUpdateClause());
  auto updateClause = query.updateClause();
  auto& graphUpdate = updateClause.op_;

  // Start the timer once the where clause has been evaluated.
  tracer.beginTrace("preparation");
  ad_utility::Timer timer{ad_utility::Timer::InitialStatus::Started};
  const auto& vocab = index.getVocab();
  const auto& encodedIriManager = index.encodedIriManager();

  auto prepareTemplateAndResultContainer =
      [&vocab, &variableColumns, &encodedIriManager,
       &result](std::vector<SparqlTripleSimpleWithGraph>&& tripleTemplates) {
        auto [transformedTripleTemplates, localVocab] =
            transformTriplesTemplate(encodedIriManager, vocab, variableColumns,
                                     tripleTemplates);
        std::vector<IdTriple<>> updateTriples;
        // The maximum result size is size(query result) x num template rows.
        // The actual result can be smaller if there are template rows with
        // variables for which a result row does not have a value.
        updateTriples.reserve(result.idTable().size() *
                              transformedTripleTemplates.size());

        return std::tuple{std::move(transformedTripleTemplates),
                          std::move(updateTriples), std::move(localVocab)};
      };

  tracer.beginTrace("transforming");
  auto [toInsertTemplates, toInsert, localVocabInsert] =
      prepareTemplateAndResultContainer(
          std::move(graphUpdate.toInsert_.triples_));
  AD_LOG_INFO << "Transformed insert triples template size: "
              << toInsertTemplates.size()
              << ", local vocab size: " << localVocabInsert.size() << std::endl;
  auto [toDeleteTemplates, toDelete, localVocabDelete] =
      prepareTemplateAndResultContainer(
          std::move(graphUpdate.toDelete_.triples_));
  AD_LOG_INFO << "Transformed delete triples template size: "
              << toDeleteTemplates.size()
              << ", local vocab size: " << localVocabDelete.size() << std::endl;
  tracer.endTrace("transforming");

  tracer.beginTrace("resultInterpolation");
  uint64_t resultSize = 0;
  for (const auto& [pair, range] : ExportQueryExecutionTrees::getRowIndices(
           query._limitOffset, result, resultSize)) {
    auto& idTable = pair.idTable_;
    for (const uint64_t i : range) {
      computeAndAddQuadsForResultRow(toInsertTemplates, toInsert, idTable, i);
      cancellationHandle->throwIfCancelled();

      computeAndAddQuadsForResultRow(toDeleteTemplates, toDelete, idTable, i);
      cancellationHandle->throwIfCancelled();
    }
  }
  tracer.endTrace("resultInterpolation");
  tracer.beginTrace("deduplication");
  sortAndRemoveDuplicates(toInsert);
  sortAndRemoveDuplicates(toDelete);
  metadata.inUpdate_ = DeltaTriplesCount{static_cast<int64_t>(toInsert.size()),
                                         static_cast<int64_t>(toDelete.size())};
  toDelete = setMinus(toDelete, toInsert);
  tracer.endTrace("deduplication");
  tracer.endTrace("preparation");

  return {
      IdTriplesAndLocalVocab{std::move(toInsert), std::move(localVocabInsert)},
      IdTriplesAndLocalVocab{std::move(toDelete), std::move(localVocabDelete)}};
}

// _____________________________________________________________________________
void ExecuteUpdate::sortAndRemoveDuplicates(
    std::vector<IdTriple<>>& container) {
  ql::ranges::sort(container);
  container.erase(std::unique(container.begin(), container.end()),
                  container.end());
}

// _____________________________________________________________________________
std::vector<IdTriple<>> ExecuteUpdate::setMinus(
    const std::vector<IdTriple<>>& a, const std::vector<IdTriple<>>& b) {
  std::vector<IdTriple<>> reducedToDelete;
  reducedToDelete.reserve(a.size());
  ql::ranges::set_difference(a, b, std::back_inserter(reducedToDelete));
  return reducedToDelete;
}
