// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
#define QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H

#include "engine/QueryExecutionTree.h"
#include "parser/data/ConstructQueryExportContext.h"
#include "util/CancellationHandle.h"

// Forward declaration
class ExportQueryExecutionTrees;

// TripleEvaluator: Evaluates a single CONSTRUCT triple pattern.
// This callable takes a triple pattern (subject, predicate, object) and
// evaluates it using the provided context. Returns an empty StringTriple
// if any component evaluates to UNDEF.
class TripleEvaluator {
 public:
  using CancellationHandle = ad_utility::SharedCancellationHandle;
  using StringTriple = QueryExecutionTree::StringTriple;

  TripleEvaluator(CancellationHandle cancellationHandle,
                  ConstructQueryExportContext context)
      : cancellationHandle_(std::move(cancellationHandle)),
        context_(std::move(context)) {}

  StringTriple operator()(const std::array<GraphTerm, 3>& triple) const;

 private:
  CancellationHandle cancellationHandle_;
  ConstructQueryExportContext context_;
};

// RowTripleProducer: Generates all triples for a single result row.
// This callable takes a row index and produces a filtered range of
// StringTriples by evaluating each construct pattern for that row.
class RowTripleProducer {
 public:
  using CancellationHandle = ad_utility::SharedCancellationHandle;
  using StringTriple = QueryExecutionTree::StringTriple;
  using Triples = ad_utility::sparql_types::Triples;

  RowTripleProducer(const Triples& constructTriples, const IdTable& idTable,
                    const LocalVocab& localVocab,
                    const VariableToColumnMap& variableColumns,
                    const Index& index, CancellationHandle cancellationHandle,
                    size_t rowOffset)
      : constructTriples_(constructTriples),
        idTable_(idTable),
        localVocab_(localVocab),
        variableColumns_(variableColumns),
        index_(index),
        cancellationHandle_(std::move(cancellationHandle)),
        rowOffset_(rowOffset) {}

  // Returns a filtered range of StringTriples for the given row index.
  auto operator()(uint64_t rowIdx) const;

 private:
  const Triples& constructTriples_;  // Reference to TableTripleProducer's copy
  const IdTable& idTable_;
  const LocalVocab& localVocab_;
  const VariableToColumnMap& variableColumns_;
  const Index& index_;
  CancellationHandle cancellationHandle_;
  size_t rowOffset_;
};

// TableTripleProducer: Generates all triples for a table of result rows.
// This callable takes a TableWithRange and produces a joined range of
// all StringTriples from all rows in that table.
class TableTripleProducer {
 public:
  using CancellationHandle = ad_utility::SharedCancellationHandle;
  using Triples = ad_utility::sparql_types::Triples;

  TableTripleProducer(Triples constructTriples,
                      std::shared_ptr<const Result> result,
                      const VariableToColumnMap& variableColumns,
                      const Index& index, CancellationHandle cancellationHandle)
      : constructTriples_(std::move(constructTriples)),
        result_(std::move(result)),
        variableColumns_(variableColumns),
        index_(index),
        cancellationHandle_(std::move(cancellationHandle)) {}

  // Process a TableWithRange and return a joined range of all triples.
  template <typename TableWithRangeT>
  auto operator()(const TableWithRangeT& tableWithRange);

 private:
  Triples constructTriples_;  // Owned copy
  std::shared_ptr<const Result> result_;
  const VariableToColumnMap& variableColumns_;
  const Index& index_;
  CancellationHandle cancellationHandle_;
  size_t rowOffset_ = 0;
};

inline auto RowTripleProducer::operator()(uint64_t rowIdx) const {
  ConstructQueryExportContext context{rowIdx,           idTable_, localVocab_,
                                      variableColumns_, index_,   rowOffset_};

  TripleEvaluator evaluator(cancellationHandle_, std::move(context));

  return constructTriples_ | ql::views::transform(std::move(evaluator)) |
         ql::views::filter([](const StringTriple& t) { return !t.isEmpty(); });
}

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

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTTRIPLEGENERATOR_H
