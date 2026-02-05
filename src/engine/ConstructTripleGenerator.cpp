// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructTripleGenerator.h"

#include "engine/ConstructRowProcessor.h"
#include "engine/ExportQueryExecutionTrees.h"

using ad_utility::InputRangeTypeErased;
using StringTriple = ConstructTripleGenerator::StringTriple;
using CancellationHandle = ad_utility::SharedCancellationHandle;

// _____________________________________________________________________________
// Adapter that transforms InstantiatedTriple to formatted strings.
class FormattedTripleAdapter
    : public ad_utility::InputRangeFromGet<std::string> {
 public:
  FormattedTripleAdapter(std::unique_ptr<ConstructRowProcessor> processor,
                         ad_utility::MediaType format)
      : processor_(std::move(processor)), format_(format) {}

  std::optional<std::string> get() override {
    auto triple = processor_->get();
    if (!triple) return std::nullopt;
    return ConstructTripleInstantiator::formatTriple(
        triple->subject_, triple->predicate_, triple->object_, format_);
  }

 private:
  std::unique_ptr<ConstructRowProcessor> processor_;
  ad_utility::MediaType format_;
};

// _____________________________________________________________________________
// Adapter that transforms InstantiatedTriple to StringTriple.
class StringTripleAdapter : public ad_utility::InputRangeFromGet<StringTriple> {
 public:
  explicit StringTripleAdapter(std::unique_ptr<ConstructRowProcessor> processor)
      : processor_(std::move(processor)) {}

  std::optional<StringTriple> get() override {
    auto triple = processor_->get();
    if (!triple) return std::nullopt;
    return StringTriple{InstantiatedTriple::getValue(triple->subject_),
                        InstantiatedTriple::getValue(triple->predicate_),
                        InstantiatedTriple::getValue(triple->object_)};
  }

 private:
  std::unique_ptr<ConstructRowProcessor> processor_;
};

// _____________________________________________________________________________
ConstructTripleGenerator::ConstructTripleGenerator(
    Triples constructTriples, std::shared_ptr<const Result> result,
    const VariableToColumnMap& variableColumns, const Index& index,
    CancellationHandle cancellationHandle)
    : templateTriples_(std::move(constructTriples)),
      result_(std::move(result)),
      variableColumns_(variableColumns),
      index_(index),
      cancellationHandle_(std::move(cancellationHandle)) {
  // Analyze template: precompute constants and identify variables/blank nodes.
  preprocessedConstructTemplate_ = ConstructTemplatePreprocessor::preprocess(
      templateTriples_, index_.get(), variableColumns_.get(),
      cancellationHandle_);
}

// _____________________________________________________________________________
ad_utility::InputRangeTypeErased<StringTriple>
ConstructTripleGenerator::generateStringTriplesForResultTable(
    const TableWithRange& table) {
  const size_t currentRowOffset = rowOffset_;
  rowOffset_ += table.tableWithVocab_.idTable().numRows();

  auto processor = std::make_unique<ConstructRowProcessor>(
      preprocessedConstructTemplate_, table, currentRowOffset);

  return ad_utility::InputRangeTypeErased<StringTriple>{
      std::make_unique<StringTripleAdapter>(std::move(processor))};
}

// _____________________________________________________________________________
// Entry point for generating CONSTRUCT query output.
// Used when caller needs structured access to triple components.
ad_utility::InputRangeTypeErased<QueryExecutionTree::StringTriple>
ConstructTripleGenerator::generateStringTriples(
    const QueryExecutionTree& qet,
    const ad_utility::sparql_types::Triples& constructTriples,
    const LimitOffsetClause& limitAndOffset,
    std::shared_ptr<const Result> result, uint64_t& resultSize,
    CancellationHandle cancellationHandle) {
  // The `resultSizeMultiplicator`(last argument of `getRowIndices`) is
  // explained by the following: For each result from the WHERE clause, we
  // produce up to `constructTriples.size()` triples. We do not account for
  // triples that are filtered out because one of the components is UNDEF (it
  // would require materializing the whole result)
  auto rowIndices = ExportQueryExecutionTrees::getRowIndices(
      limitAndOffset, *result, resultSize, constructTriples.size());

  ConstructTripleGenerator generator(
      constructTriples, std::move(result), qet.getVariableColumns(),
      qet.getQec()->getIndex(), std::move(cancellationHandle));

  // Transform the range of tables into a flattened range of triples.
  // We move the generator into the transformation lambda to extend its
  // lifetime. Because the transformation is stateful (it tracks rowOffset_),
  // the lambda must be marked 'mutable'.
  auto tableTriples = ql::views::transform(
      ad_utility::OwningView{std::move(rowIndices)},
      [generator = std::move(generator)](const TableWithRange& table) mutable {
        // conceptually, the generator now handles the following pipeline:
        // table -> processing batch -> table rows -> triple patterns -> string
        // triples
        return generator.generateStringTriplesForResultTable(table);
      });
  return InputRangeTypeErased(ql::views::join(std::move(tableTriples)));
}

// _____________________________________________________________________________
// Entry point for generating CONSTRUCT query output.
// More efficient for streaming output than `generateStringTriples` (avoids
// `StringTriple` allocations).
ad_utility::InputRangeTypeErased<std::string>
ConstructTripleGenerator::generateFormattedTriples(
    const TableWithRange& table, ad_utility::MediaType mediaType) {
  const size_t currentRowOffset = rowOffset_;
  rowOffset_ += table.tableWithVocab_.idTable().numRows();

  auto processor = std::make_unique<ConstructRowProcessor>(
      preprocessedConstructTemplate_, table, currentRowOffset);

  return ad_utility::InputRangeTypeErased<std::string>{
      std::make_unique<FormattedTripleAdapter>(std::move(processor),
                                               mediaType)};
}
