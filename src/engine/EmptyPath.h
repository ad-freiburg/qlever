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

#include <absl/functional/function_ref.h>

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
//
// NOTE: The implementation deliberately favors simplicity over speed in a few
// places (see the `TODO`s in `EmptyPath.cpp`): the result is written row by row
// although `IdTable`s are stored column-major, and UNDEF values are expanded
// via a plain cross product. Either the result is small (an existence check on
// few values), or the runtime is dominated by reading the index anyway.
class EmptyPath : public Operation {
 public:
  using Graphs = ScanSpecificationAsTripleComponent::GraphFilter;

  // The child whose result is checked against the knowledge graph, together
  // with the columns of that result that this operation has to know about (see
  // the comment for this class above). These belong together: either all or
  // none of them are present, and the columns are meaningless without the
  // child they refer to.
  struct CheckedChild {
    // The child. Only those values of its `joinColumn_` that occur in the
    // knowledge graph are part of the result. Must not be `nullptr`.
    std::shared_ptr<QueryExecutionTree> child_;
    // The column of the child's result that is checked.
    ColumnIndex joinColumn_;
    // The column of `child_` that holds the graph IDs, if the child already
    // provides them. In this case pairs of value and graph ID are checked
    // instead of only the value.
    std::optional<ColumnIndex> graphColumn_ = std::nullopt;
    // The columns of `child_` that are simply carried over, in ascending
    // order.
    std::vector<ColumnIndex> payloadColumns_ = {};

    // The `graphColumn_` and the `payloadColumns_` are deliberately not
    // arguments here: they can only be deduced together with the graph
    // variable, which `EmptyPath`'s constructor does. The `joinColumn` has to
    // be a column of the `child`'s result.
    CheckedChild(std::shared_ptr<QueryExecutionTree> child,
                 ColumnIndex joinColumn);

    // A deep copy, with the `child_` cloned. The remaining members are deduced
    // by `EmptyPath`'s constructor and are hence not copied.
    CheckedChild clone() const;
  };

 private:
  // The number of rows after which a new `IdTable` is yielded.
  static constexpr size_t chunkSize_ = 100'000;

  // The variable that holds the entities. It is always written to column 0.
  Variable variable_;
  // The graphs that the entities have to occur in. This is not necessarily all
  // the graphs of the index: the query can restrict them via a `FROM` clause,
  // and the property path that this operation stems from can appear inside a
  // `GRAPH ?g { ... }` (or `GRAPH <g> { ... }`) clause.
  Graphs activeGraphs_;
  // If set, the graph IDs are written to column 1 using this variable.
  std::optional<Variable> graphVariable_;
  // If set, the result is the existence check on this child's result (see the
  // comment for this class above). If it is `std::nullopt`, all entities of
  // the knowledge graph are returned.
  std::optional<CheckedChild> checkedChild_;
  VariableToColumnMap variableColumns_;
  size_t resultWidth_;

 public:
  // If `checkedChildOpt` is `std::nullopt`, all entities of the knowledge graph
  // are returned, else the values in its join column are checked against the
  // knowledge graph (see the comment for this class above).
  EmptyPath(QueryExecutionContext* qec, Variable variable, Graphs activeGraphs,
            std::optional<Variable> graphVariable,
            std::optional<CheckedChild> checkedChildOpt = std::nullopt);

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

  // The execution tree of the `checkedChild_`, which must be set. Constness of
  // this `EmptyPath` doesn't propagate through the `shared_ptr`, so the child
  // can also be used for the (non-const) estimates.
  QueryExecutionTree& child() const { return *checkedChild_.value().child_; }

  // The number of columns that come from the knowledge graph (1 or 2).
  size_t numKgColumns() const { return graphVariable_.has_value() ? 2 : 1; }

  // The index of the first column that is carried over from the child.
  size_t firstPayloadColumn() const { return numKgColumns(); }

  // Return all distinct entities of the knowledge graph (as tables with
  // `numKgColumns()` columns), sorted and without duplicates. If `idFilter` is
  // set, only the entities contained in it are returned. `idFilter` has to be
  // sorted and must neither contain duplicates nor undefined IDs.
  cppcoro::generator<IdTable> scanIndex(
      std::optional<std::vector<Id>> idFilter) const;

  // Implementation of `computeResult` for the case that no child is set.
  Result::Generator computeAllEntities() const;

  // Implementation of `computeResult` for the case that a child is set.
  Result::Generator computeExistenceCheck(
      std::shared_ptr<const Result> childResult) const;

  // Perform the existence check for a single table of the child's result. The
  // `table` is passed by value because it is a view that has to be stored in
  // the frame of this coroutine; `localVocab` and `hasWarnedAboutUndef` (see
  // `processUndefRows`) have to be kept alive by the caller.
  Result::Generator processTable(IdTableView<0> table,
                                 const LocalVocab& localVocab,
                                 bool& hasWarnedAboutUndef) const;

  // The type of the callback that hands out the accumulated rows of the result
  // as soon as there are enough of them (see `yieldIfFull` in `processTable`).
  using YieldIfFull =
      absl::FunctionRef<std::optional<Result::IdTableVocabPair>()>;

  // Yield the result rows for those rows of `input` whose join column is UNDEF.
  // Such a value matches every entity of the knowledge graph, so the full empty
  // path has to be streamed for them. The rows are appended to `result` (the
  // partially filled table of the calling `processTable`) and handed out via
  // `yieldIfFull`; a warning is added unless `hasWarnedAboutUndef` is set. All
  // arguments have to be kept alive by the caller.
  Result::Generator processUndefRows(const IdTableView<0>& input,
                                     IdTable& result, YieldIfFull yieldIfFull,
                                     bool& hasWarnedAboutUndef) const;

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
