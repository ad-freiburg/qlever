// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructTripleGenerator.h"

#include "engine/ConstructRowProcessor.h"
#include "engine/ConstructTemplatePreprocessor.h"
#include "engine/ExportQueryExecutionTrees.h"

namespace qlever::constructExport {

using ad_utility::InputRangeTypeErased;
using StringTriple =
    qlever::constructExport::ConstructTripleGenerator::StringTriple;
using CancellationHandle = ad_utility::SharedCancellationHandle;

// _____________________________________________________________________________
// Adapter that transforms `EvaluatedTriple` to formatted strings.
template <ad_utility::MediaType format>
class FormattedTripleAdapter
    : public ad_utility::InputRangeFromGet<std::string> {
 public:
  explicit FormattedTripleAdapter(
      std::unique_ptr<ConstructRowProcessor> processor)
      : processor_(std::move(processor)) {}

  std::optional<std::string> get() override {
    auto triple = processor_->get();
    if (!triple) return std::nullopt;
    return ConstructTripleInstantiator::formatTriple<format>(
        triple->subject_, triple->predicate_, triple->object_);
  }

 private:
  std::unique_ptr<ConstructRowProcessor> processor_;
};

// _____________________________________________________________________________
// Adapter that transforms `EvaluatedTriple` to `StringTriple`.
class StringTripleAdapter : public ad_utility::InputRangeFromGet<StringTriple> {
 public:
  explicit StringTripleAdapter(std::unique_ptr<ConstructRowProcessor> processor)
      : processor_(std::move(processor)) {}

  std::optional<StringTriple> get() override {
    auto triple = processor_->get();
    if (!triple) return std::nullopt;
    return StringTriple{EvaluatedTriple::getValue(triple->subject_),
                        EvaluatedTriple::getValue(triple->predicate_),
                        EvaluatedTriple::getValue(triple->object_)};
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
      index_(index),
      cancellationHandle_(std::move(cancellationHandle)) {
  preprocessedTemplate_ = ConstructTemplatePreprocessor::preprocess(
      templateTriples_, variableColumns);
}

// _____________________________________________________________________________
ad_utility::InputRangeTypeErased<StringTriple>
ConstructTripleGenerator::generateStringTriplesForResultTable(
    const TableWithRange& table) {
  const size_t currentRowOffset = rowOffset_;
  rowOffset_ += table.tableWithVocab_.idTable().numRows();

  auto processor = std::make_unique<ConstructRowProcessor>(
      preprocessedTemplate_, index_.get(), cancellationHandle_, table,
      currentRowOffset);

  return ad_utility::InputRangeTypeErased<StringTriple>{
      std::make_unique<StringTripleAdapter>(std::move(processor))};
}

// _____________________________________________________________________________
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
  // would require materializing the whole result).
  auto rowIndices = ExportQueryExecutionTrees::getRowIndices(
      limitAndOffset, *result, resultSize, constructTriples.size());

  ConstructTripleGenerator generator(
      constructTriples, std::move(result), qet.getVariableColumns(),
      qet.getQec()->getIndex(), std::move(cancellationHandle));

  auto tableTriples = ql::views::transform(
      ad_utility::OwningView{std::move(rowIndices)},
      [generator = std::move(generator)](const TableWithRange& table) mutable {
        return generator.generateStringTriplesForResultTable(table);
      });

  return InputRangeTypeErased(ql::views::join(std::move(tableTriples)));
}

// _____________________________________________________________________________
template <ad_utility::MediaType format>
ad_utility::InputRangeTypeErased<std::string>
ConstructTripleGenerator::generateFormattedTriples(
    const TableWithRange& table) {
  const size_t currentRowOffset = rowOffset_;
  rowOffset_ += table.tableWithVocab_.idTable().numRows();

  auto processor = std::make_unique<ConstructRowProcessor>(
      preprocessedTemplate_, index_.get(), cancellationHandle_, table,
      currentRowOffset);

  return ad_utility::InputRangeTypeErased<std::string>{
      std::make_unique<FormattedTripleAdapter<format>>(std::move(processor))};
}

// _____________________________________________________________________________
template <ad_utility::MediaType format>
ad_utility::InputRangeTypeErased<std::string>
ConstructTripleGenerator::generateAllFormattedTriples(
    ad_utility::InputRangeTypeErased<TableWithRange> rowIndices) {
  auto tableTriples =
      ql::views::transform(ad_utility::OwningView{std::move(rowIndices)},
                           [this](const TableWithRange& table) {
                             return generateFormattedTriples<format>(table);
                           });

  return InputRangeTypeErased<std::string>(
      ql::views::join(std::move(tableTriples)));
}

// Explicit instantiations.
template ad_utility::InputRangeTypeErased<std::string>
ConstructTripleGenerator::generateFormattedTriples<
    ad_utility::MediaType::turtle>(const TableWithRange&);
template ad_utility::InputRangeTypeErased<std::string>
ConstructTripleGenerator::generateFormattedTriples<ad_utility::MediaType::csv>(
    const TableWithRange&);
template ad_utility::InputRangeTypeErased<std::string>
ConstructTripleGenerator::generateFormattedTriples<ad_utility::MediaType::tsv>(
    const TableWithRange&);
template ad_utility::InputRangeTypeErased<std::string>
    ConstructTripleGenerator::generateAllFormattedTriples<
        ad_utility::MediaType::turtle>(
        ad_utility::InputRangeTypeErased<TableWithRange>);
template ad_utility::InputRangeTypeErased<std::string>
    ConstructTripleGenerator::generateAllFormattedTriples<
        ad_utility::MediaType::csv>(
        ad_utility::InputRangeTypeErased<TableWithRange>);
template ad_utility::InputRangeTypeErased<std::string>
    ConstructTripleGenerator::generateAllFormattedTriples<
        ad_utility::MediaType::tsv>(
        ad_utility::InputRangeTypeErased<TableWithRange>);

}  // namespace qlever::constructExport
