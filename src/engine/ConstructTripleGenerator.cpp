// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures

#include "engine/ConstructTripleGenerator.h"

#include "engine/ConstructQueryEvaluator.h"

TripleEvaluator::StringTriple TripleEvaluator::operator()(
    const std::array<GraphTerm, 3>& triple) const {
  cancellationHandle_->throwIfCancelled();

  using enum PositionInTriple;

  auto subject =
      ConstructQueryEvaluator::evaluate(triple[0], context_, SUBJECT);
  auto predicate =
      ConstructQueryEvaluator::evaluate(triple[1], context_, PREDICATE);
  auto object = ConstructQueryEvaluator::evaluate(triple[2], context_, OBJECT);

  if (!subject.has_value() || !predicate.has_value() || !object.has_value()) {
    return StringTriple();  // Empty triple indicates UNDEF
  }

  return StringTriple(std::move(subject.value()), std::move(predicate.value()),
                      std::move(object.value()));
}

auto RowTripleProducer::operator()(uint64_t rowIdx) const {
  ConstructQueryExportContext context{rowIdx,           idTable_, localVocab_,
                                      variableColumns_, index_,   rowOffset_};

  TripleEvaluator evaluator(cancellationHandle_, std::move(context));

  return constructTriples_ | ql::views::transform(std::move(evaluator)) |
         ql::views::filter([](const StringTriple& t) { return !t.isEmpty(); });
}

// Process a TableWithRange and return a joined range of all triples.
template <typename TableWithRangeT>
auto TableTripleProducer::operator()(const TableWithRangeT& tableWithRange) {
  const auto& idTable = tableWithRange.tableWithVocab_.idTable();
  const auto& localVocab = tableWithRange.tableWithVocab_.localVocab();

  size_t currentRowOffset = rowOffset_;
  rowOffset_ += idTable.size();

  RowTripleProducer rowProducer(constructTriples_, idTable, localVocab,
                                variableColumns_, index_, cancellationHandle_,
                                currentRowOffset);

  return tableWithRange.view_ | ql::views::transform(std::move(rowProducer)) |
         ql::views::join;
}
