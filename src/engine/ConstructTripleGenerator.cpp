// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/ConstructTripleGenerator.h"

#include "engine/ConstructBatchEvaluator.h"
#include "engine/ConstructTemplatePreprocessor.h"
#include "engine/ConstructTripleInstantiator.h"
#include "util/InputRangeUtils.h"
#include "util/Views.h"

namespace qlever::constructExport {

using ad_utility::InputRangeTypeErased;
using StringTriple = ConstructTripleGenerator::StringTriple;

namespace {

constexpr size_t DEFAULT_BATCH_SIZE = 1024;
constexpr size_t CACHE_ENTRIES_PER_VARIABLE = 2048;

IdCache makeIdCache(const PreprocessedConstructTemplate& tmpl) {
  return IdCache{std::max(tmpl.uniqueVariableColumns_.size(), size_t{1}) *
                 CACHE_ENTRIES_PER_VARIABLE};
}

// Lazily evaluates all rows of `table` in batches of DEFAULT_BATCH_SIZE,
// yielding one `EvaluatedTriple` at a time. `rowOffset` is the absolute
// row index of the first row of `table` within the full result (used for
// blank-node ID generation).
//
// `tmpl` and `index` are passed as reference_wrappers so the lambda can
// capture them by value without copying the underlying objects.
InputRangeTypeErased<EvaluatedTriple> evaluateTable(
    std::reference_wrapper<const PreprocessedConstructTemplate> tmpl,
    std::reference_wrapper<const Index> index,
    ad_utility::SharedCancellationHandle cancellationHandle,
    TableWithRange table, size_t rowOffset) {
  const size_t numRows = ql::ranges::distance(table.view_);
  const size_t firstRow = numRows > 0 ? *ql::ranges::begin(table.view_) : 0;
  const size_t numBatches =
      (numRows + DEFAULT_BATCH_SIZE - 1) / DEFAULT_BATCH_SIZE;

  return InputRangeTypeErased<EvaluatedTriple>{
      ad_utility::CachingContinuableTransformInputRange(
          ql::views::iota(size_t{0}, numBatches),
          // Captures by value: `tmpl` and `index` are cheap reference_wrappers;
          // `table` holds reference_wrappers to the IdTable/LocalVocab (kept
          // alive by ConstructTripleGenerator::result_); `idCache` is moved in.
          [tmpl, index, cancellationHandle, table, firstRow, numRows, rowOffset,
           idCache = makeIdCache(tmpl.get())](size_t batchIdx) mutable
          -> ad_utility::LoopControl<EvaluatedTriple> {
            cancellationHandle->throwIfCancelled();
            const size_t batchStart = batchIdx * DEFAULT_BATCH_SIZE;
            const size_t batchEnd =
                std::min(batchStart + DEFAULT_BATCH_SIZE, numRows);
            BatchEvaluationContext ctx{table.tableWithVocab_.idTable(),
                                       firstRow + batchStart,
                                       firstRow + batchEnd};
            auto batchResult = ConstructBatchEvaluator::evaluateBatch(
                tmpl.get().uniqueVariableColumns_, ctx,
                table.tableWithVocab_.localVocab(), index.get(), idCache);
            const size_t blankNodeBase = rowOffset + firstRow + batchStart;
            return ad_utility::LoopControl<EvaluatedTriple>::yieldAll(
                instantiateBatch(tmpl.get(), batchResult, blankNodeBase));
          })};
}

}  // namespace

// _____________________________________________________________________________
ConstructTripleGenerator::ConstructTripleGenerator(
    Triples constructTriples, std::shared_ptr<const Result> result,
    const VariableToColumnMap& variableColumns, const Index& index,
    CancellationHandle cancellationHandle)
    : result_(std::move(result)),
      preprocessedTemplate_(ConstructTemplatePreprocessor::preprocess(
          constructTriples, variableColumns)),
      index_(index),
      cancellationHandle_(std::move(cancellationHandle)) {}

// _____________________________________________________________________________
InputRangeTypeErased<std::string>
ConstructTripleGenerator::generateFormattedTriples(
    InputRangeTypeErased<TableWithRange> tables,
    ad_utility::MediaType format) && {
  auto tableStrings = ql::views::transform(
      ad_utility::OwningView{std::move(tables)},
      [gen = std::move(*this), format](const TableWithRange& table) mutable {
        auto triples = evaluateTable(
            std::cref(gen.preprocessedTemplate_), std::cref(gen.index_.get()),
            gen.cancellationHandle_, table, gen.rowOffset_);
        gen.rowOffset_ += ql::ranges::distance(table.view_);
        return InputRangeTypeErased<std::string>{
            ad_utility::CachingTransformInputRange(
                ad_utility::OwningView{std::move(triples)},
                [format](EvaluatedTriple triple) {
                  return formatTriple(triple.subject_, triple.predicate_,
                                      triple.object_, format);
                })};
      });
  return InputRangeTypeErased<std::string>{
      ql::views::join(std::move(tableStrings))};
}

// _____________________________________________________________________________
InputRangeTypeErased<StringTriple>
ConstructTripleGenerator::generateStringTriples(
    InputRangeTypeErased<TableWithRange> tables) && {
  auto tableStringTriples = ql::views::transform(
      ad_utility::OwningView{std::move(tables)},
      [gen = std::move(*this)](const TableWithRange& table) mutable {
        auto triples = evaluateTable(
            std::cref(gen.preprocessedTemplate_), std::cref(gen.index_.get()),
            gen.cancellationHandle_, table, gen.rowOffset_);
        gen.rowOffset_ += ql::ranges::distance(table.view_);
        return InputRangeTypeErased<StringTriple>{
            ad_utility::CachingTransformInputRange(
                ad_utility::OwningView{std::move(triples)},
                [](EvaluatedTriple triple) -> StringTriple {
                  return {formatTerm(*triple.subject_, true),
                          formatTerm(*triple.predicate_, true),
                          formatTerm(*triple.object_, true)};
                })};
      });
  return InputRangeTypeErased<StringTriple>{
      ql::views::join(std::move(tableStringTriples))};
}

}  // namespace qlever::constructExport
