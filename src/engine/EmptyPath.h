//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#ifndef QLEVER_SRC_ENGINE_EMPTYPATH_H
#define QLEVER_SRC_ENGINE_EMPTYPATH_H

#include <memory>
#include <optional>
#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "index/ScanSpecification.h"
#include "util/Generator.h"

// Operation that yields all the entities that occur as a subject or an object
// in the knowledge graph, optionally together with the graphs they occur in.
// Those are exactly the values that a property path of length zero (the "empty
// path") matches, so `?x <p>* ?y` additionally yields `?x = ?y = <entity>` for
// each such entity.
//
// If a `child` is given, this operation instead performs an existence check on
// a single column of the child's result: All values of that column that don't
// occur in the knowledge graph are filtered out. This is much cheaper than a
// join with the full set of entities, in particular if the child only yields
// few values (a very common pattern, think of a `VALUES` clause or a constant
// `BIND`), because then only very few blocks of the index have to be read.
// The remaining columns of the child are simply carried over.
//
// If a graph variable is set, then the graph IDs become part of the result.
// Depending on whether the child already provides the graph variable, they are
// either added (a single input value is expanded to one output row per graph it
// occurs in) or checked (only those input rows survive for which the pair of
// value and graph actually occurs in the knowledge graph). UNDEF values (both
// in the checked column and in the graph column) match everything and are
// expanded accordingly.
class EmptyPath : public Operation {
 public:
  using Graphs = ScanSpecificationAsTripleComponent::GraphFilter;

 private:
  // The number of rows after which a new `IdTable` is yielded.
  static constexpr size_t chunkSize_ = 100'000;

  // The variable that holds the entities. It is always written to column 0.
  Variable variable_;
  // The graphs that are active in the current context.
  Graphs activeGraphs_;
  // If set, the graph IDs are written to column 1 using this variable.
  std::optional<Variable> graphVariable_;
  // If set, only the values of the `joinColumn_` of this child's result that
  // occur in the knowledge graph are part of the result (see the comment for
  // this class above). If it is `nullptr`, all entities of the knowledge graph
  // are returned.
  std::shared_ptr<QueryExecutionTree> child_;
  ColumnIndex joinColumn_;
  // The column of `child_` that holds the graph IDs, if the child already
  // provides them. In this case pairs of value and graph ID are checked instead
  // of only the value.
  std::optional<ColumnIndex> childGraphColumn_;
  // The columns of `child_` that are simply carried over, in ascending order.
  std::vector<ColumnIndex> payloadColumns_;
  VariableToColumnMap variableColumns_;
  size_t resultWidth_;

 public:
  // If `child` is `nullptr`, all entities of the knowledge graph are returned,
  // else the values in the `joinColumn` of the child's result are checked
  // against the knowledge graph (see the comment for this class above).
  EmptyPath(QueryExecutionContext* qec, Variable variable, Graphs activeGraphs,
            std::optional<Variable> graphVariable,
            std::shared_ptr<QueryExecutionTree> child = nullptr,
            ColumnIndex joinColumn = 0);

  // Getters, mainly for testing.
  const Variable& variable() const { return variable_; }
  const std::optional<Variable>& graphVariable() const {
    return graphVariable_;
  }

  std::vector<QueryExecutionTree*> getChildren() override;
  std::string getDescriptor() const override;
  size_t getResultWidth() const override;
  size_t getCostEstimate() override;
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;
  bool columnOriginatesFromGraphOrUndef(
      const Variable& variable) const override;

 protected:
  std::vector<ColumnIndex> resultSortedOn() const override;

 private:
  std::string getCacheKeyImpl() const override;
  uint64_t getSizeEstimateBeforeLimit() override;
  [[nodiscard]] bool isDeterministicImpl() const override { return true; }
  std::unique_ptr<Operation> cloneImpl() const override;
  Result computeResult(bool requestLaziness) override;
  VariableToColumnMap computeVariableToColumnMap() const override;

  // The number of columns that come from the knowledge graph (1 or 2).
  size_t numIdColumns() const { return graphVariable_.has_value() ? 2 : 1; }

  // The index of the first column that is carried over from `child_`.
  size_t firstPayloadColumn() const { return numIdColumns(); }

  // Return all distinct entities of the knowledge graph (as tables with
  // `numIdColumns()` columns), sorted and without duplicates. If `idFilter` is
  // set, only the entities contained in it are returned. It has to be sorted
  // and must neither contain duplicates nor undefined IDs.
  cppcoro::generator<IdTable> scanIndex(
      std::optional<std::vector<Id>> idFilter) const;

  // Implementation of `computeResult` for the case that no `child_` is set.
  Result::Generator computeAllEntities() const;

  // Implementation of `computeResult` for the case that a `child_` is set.
  Result::Generator computeExistenceCheck(
      std::shared_ptr<const Result> childResult) const;

  // Perform the existence check for a single table of the child's result. The
  // `table` is passed by value because it is a view that has to be stored in
  // the frame of this coroutine, `localVocab` has to be kept alive by the
  // caller.
  Result::Generator processTable(IdTableView<0> table,
                                 const LocalVocab& localVocab) const;

  // Append a single row to `result`: `id` (and `graph` if a graph variable is
  // set), followed by the payload columns of row `inputRow` of `input`.
  void appendRow(IdTable& result, const IdTableView<0>& input, size_t inputRow,
                 Id id, Id graph) const;

  // Return true iff the given `graph` from the knowledge graph is compatible
  // with the graph of row `inputRow` of `input` (which is always the case if
  // the child doesn't provide a graph column or if its value is UNDEF).
  bool graphMatches(const IdTableView<0>& input, size_t inputRow,
                    Id graph) const;
};

#endif  // QLEVER_SRC_ENGINE_EMPTYPATH_H

#endif
