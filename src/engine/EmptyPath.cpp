//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include "engine/EmptyPath.h"

#include <absl/strings/str_cat.h>

#include "index/CompressedRelation.h"
#include "index/IndexImpl.h"
#include "index/TripleComponentConversions.h"
#include "util/Views.h"

namespace {
// Return the graph IDs that `id` occurs in according to the `matches` table
// (see `EmptyPath::processTable`).
//
// The `matches` table is sorted and holds the entity IDs in its column 0. If
// graph IDs were requested, it has a second column that holds the graph that
// the entity in the same row occurs in; the same entity then appears once per
// graph. The occurrences of `id` in column 0 hence form a contiguous range,
// and the result is the corresponding range of column 1 (returned as a subspan
// of that column, which works because `IdTable`s are stored in column-major
// order).
//
// If `matches` has no graph column, then a single undefined ID stands in for
// the graphs, such that the caller can treat both cases uniformly: The result
// is a single undefined ID if `id` occurs in `matches` at all, and empty
// otherwise.
ql::span<const Id> graphsOf(const IdTable& matches, Id id) {
  ql::span<const Id> ids = matches.getColumn(0);
  auto matching = ql::ranges::equal_range(ids, id);
  size_t numMatches = ql::ranges::size(matching);
  if (matches.numColumns() == 1) {
    static const Id undefined = Id::makeUndefined();
    return {&undefined, numMatches == 0 ? 0u : 1u};
  }
  return matches.getColumn(1).subspan(matching.begin() - ids.begin(),
                                      numMatches);
}

// The rows of a `table` from `EmptyPath::scanIndex` as a range of
// `(entity, graph)` pairs. If the table has no graph column, then an undefined
// ID stands in for the graph, such that the callers can treat both cases
// uniformly (as in `graphsOf` above). The returned range refers to the `table`,
// which hence has to outlive it.
auto entitiesAndGraphs(const IdTable& table) {
  return ql::views::transform(ad_utility::integerRange(table.numRows()),
                              [&table](size_t row) -> std::pair<Id, Id> {
                                return {table(row, 0), table.numColumns() == 1
                                                           ? Id::makeUndefined()
                                                           : table(row, 1)};
                              });
}
}  // namespace

// _____________________________________________________________________________
EmptyPath::CheckedChild::CheckedChild(std::shared_ptr<QueryExecutionTree> child,
                                      ColumnIndex joinColumn)
    : child_{std::move(child)}, joinColumn_{joinColumn} {
  AD_CONTRACT_CHECK(child_ != nullptr);
}

// _____________________________________________________________________________
EmptyPath::EmptyPath(QueryExecutionContext* qec, Variable variable,
                     Graphs activeGraphs, std::optional<Variable> graphVariable,
                     std::optional<CheckedChild> checkedChildOpt)
    : Operation{qec},
      variable_{std::move(variable)},
      activeGraphs_{std::move(activeGraphs)},
      graphVariable_{std::move(graphVariable)},
      checkedChild_{std::move(checkedChildOpt)} {
  // The graph column is written in addition to the column of `variable_`, so
  // the two variables must not be the same. Callers that join on the graph
  // variable have to pass a helper variable instead.
  AD_CONTRACT_CHECK(graphVariable_ != variable_);
  variableColumns_[variable_] = makeAlwaysDefinedColumn(0);
  if (graphVariable_.has_value()) {
    variableColumns_[graphVariable_.value()] = makeAlwaysDefinedColumn(1);
  }
  if (!checkedChild_.has_value()) {
    resultWidth_ = numKgColumns();
    return;
  }
  CheckedChild& checkedChild = checkedChild_.value();
  AD_CONTRACT_CHECK(checkedChild.joinColumn_ < child().getResultWidth());
  if (graphVariable_.has_value()) {
    checkedChild.graphColumn_ =
        child().getVariableColumnOrNullopt(graphVariable_.value());
    AD_CORRECTNESS_CHECK(checkedChild.graphColumn_ != checkedChild.joinColumn_);
  }
  // All columns of the child except for the join column and the graph column
  // (which are both replaced by the values from the knowledge graph) are
  // carried over. Note that the child might have columns without a variable
  // attached to them, so we iterate over the column indices and not over the
  // variables.
  ql::ranges::copy_if(ad_utility::integerRange(child().getResultWidth()),
                      std::back_inserter(checkedChild.payloadColumns_),
                      [&checkedChild](ColumnIndex column) {
                        return column != checkedChild.joinColumn_ &&
                               column != checkedChild.graphColumn_;
                      });
  resultWidth_ = firstPayloadColumn() + checkedChild.payloadColumns_.size();
  for (const auto& [variable, info] : child().getVariableColumns()) {
    auto column =
        ql::ranges::find(checkedChild.payloadColumns_, info.columnIndex_);
    if (column == checkedChild.payloadColumns_.end()) {
      continue;
    }
    size_t index =
        firstPayloadColumn() +
        ql::ranges::distance(checkedChild.payloadColumns_.begin(), column);
    AD_CORRECTNESS_CHECK(!variableColumns_.contains(variable));
    variableColumns_[variable] = {index, info.mightContainUndef_};
  }
}

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> EmptyPath::getChildren() {
  if (!checkedChild_.has_value()) {
    return {};
  }
  return {checkedChild_.value().child_.get()};
}

// _____________________________________________________________________________
std::string EmptyPath::getDescriptor() const {
  return absl::StrCat("EmptyPath for ", variable_.name(),
                      checkedChild_.has_value() ? " (existence check)" : "");
}

// _____________________________________________________________________________
size_t EmptyPath::getResultWidth() const { return resultWidth_; }

// _____________________________________________________________________________
std::string EmptyPath::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "EMPTY PATH";
  if (graphVariable_.has_value()) {
    os << " with graph column";
  }
  os << ' ';
  activeGraphs_.format(os, &toRdfLiteral);
  if (checkedChild_.has_value()) {
    const CheckedChild& checkedChild = checkedChild_.value();
    os << "\nExistence check on column " << checkedChild.joinColumn_;
    if (checkedChild.graphColumn_.has_value()) {
      os << " and graph column " << checkedChild.graphColumn_.value();
    }
    os << " of:\n" << child().getCacheKey();
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
uint64_t EmptyPath::getSizeEstimateBeforeLimit() {
  if (checkedChild_.has_value()) {
    // The existence check can only remove rows, but adding the graph column can
    // multiply them. We have no information about the number of graphs per
    // entity, so we simply use the child's estimate.
    return child().getSizeEstimate();
  }
  const auto& index = getIndex();
  // We don't know how much the subjects and the objects overlap, so we use the
  // (pessimistic) upper bound for the size of their union.
  return index.numDistinctSubjects().normal + index.numDistinctObjects().normal;
}

// _____________________________________________________________________________
size_t EmptyPath::getCostEstimate() {
  if (!checkedChild_.has_value()) {
    // In the worst case both the subject and the object permutation have to be
    // read completely, so the cost is proportional to the number of triples and
    // not to the (typically much smaller) number of distinct entities.
    return 2 * getIndex().numTriples().normal;
  }
  // Checking a value only requires reading very few blocks, so the cost is
  // dominated by the cost of the child.
  return child().getCostEstimate() + getSizeEstimateBeforeLimit();
}

// _____________________________________________________________________________
float EmptyPath::getMultiplicity(size_t col) {
  if (!checkedChild_.has_value()) {
    // Without a child the entities are distinct, and for the (much rarer) case
    // with a graph column the number of graphs per entity is unknown, so 1 is
    // still a reasonable guess.
    return 1;
  }
  // The existence check only removes rows, so the multiplicities of the child
  // are a good approximation. The values of the graph column don't come from
  // the child, so we know nothing about them.
  if (col == 0) {
    return child().getMultiplicity(checkedChild_.value().joinColumn_);
  }
  if (col < firstPayloadColumn()) {
    return 1;
  }
  return child().getMultiplicity(
      checkedChild_.value().payloadColumns_.at(col - firstPayloadColumn()));
}

// _____________________________________________________________________________
bool EmptyPath::knownEmptyResult() {
  return checkedChild_.has_value() && child().knownEmptyResult();
}

// _____________________________________________________________________________
std::vector<ColumnIndex> EmptyPath::resultSortedOn() const {
  if (!checkedChild_.has_value()) {
    return {0};
  }
  // The rows of the child are processed in order, so the sort order of the join
  // column is preserved. The only exception are UNDEF values, which match every
  // entity of the knowledge graph and are therefore expanded separately.
  ColumnIndex joinColumn = checkedChild_.value().joinColumn_;
  const auto& childSortedOn = child().resultSortedOn();
  const auto& info = child().getVariableAndInfoByColumnIndex(joinColumn).second;
  bool joinColumnMightBeUndef =
      info.mightContainUndef_ !=
      ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined;
  if (childSortedOn.empty() || childSortedOn.at(0) != joinColumn ||
      joinColumnMightBeUndef) {
    return {};
  }
  return {0};
}

// _____________________________________________________________________________
VariableToColumnMap EmptyPath::computeVariableToColumnMap() const {
  return variableColumns_;
}

// _____________________________________________________________________________
bool EmptyPath::columnOriginatesFromGraphOrUndef(
    const Variable& variable) const {
  AD_CONTRACT_CHECK(getExternallyVisibleVariableColumns().contains(variable));
  // The values of this column are subjects or objects of the knowledge graph.
  // Note that this is deliberately not true for the graph column: graph IDs are
  // read from the index, but in RDF only subjects and objects are considered
  // nodes, so a graph name that occurs neither as a subject nor as an object
  // still has to be matched against the knowledge graph (see
  // `IndexScan::columnOriginatesFromGraphOrUndef`).
  if (variable == variable_) {
    return true;
  }
  return Operation::columnOriginatesFromGraphOrUndef(variable);
}

// _____________________________________________________________________________
std::unique_ptr<Operation> EmptyPath::cloneImpl() const {
  std::optional<CheckedChild> checkedChild = std::nullopt;
  if (checkedChild_.has_value()) {
    // The remaining members of `CheckedChild` are deduced by the constructor.
    checkedChild =
        CheckedChild{child().clone(), checkedChild_.value().joinColumn_};
  }
  return std::make_unique<EmptyPath>(getExecutionContext(), variable_,
                                     activeGraphs_, graphVariable_,
                                     std::move(checkedChild));
}

// _____________________________________________________________________________
cppcoro::generator<IdTable> EmptyPath::scanIndex(
    std::optional<std::vector<Id>> idFilter) const {
  const IndexImpl& index = getIndex().getImpl();
  // A scan specification that doesn't fix any of the three columns, such that
  // we get all the subjects (resp. objects) of the knowledge graph.
  auto scanSpec =
      ScanSpecificationAsTripleComponent{std::nullopt, std::nullopt,
                                         std::nullopt, activeGraphs_}
          .toScanSpecification(index);
  bool addGraphColumn = graphVariable_.has_value();
  // The `LazyScanMetadata` of the generators is not used here, so we simply
  // type-erase them.
  auto scan = [this, &index, &scanSpec, addGraphColumn](
                  Permutation::Enum permutation,
                  std::optional<std::vector<Id>> ids) {
    return ad_utility::InputRangeTypeErased<IdTable>{
        index.getPermutation(permutation)
            .getDistinctCol0Ids(scanSpec, addGraphColumn, std::move(ids),
                                cancellationHandle_, locatedTriplesState())};
  };
  // The rows of one of the scans above, as a flat range.
  auto rows = [](ad_utility::InputRangeTypeErased<IdTable> range) {
    return ql::views::join(ad_utility::OwningView{std::move(range)});
  };
  // Merge the distinct subjects and the distinct objects. Both ranges are
  // sorted and free of duplicates, so `set_union` yields each row exactly once.
  auto merged = ::ranges::views::set_union(
      rows(scan(Permutation::SPO, idFilter)),
      rows(scan(Permutation::OPS, std::move(idFilter))),
      ql::ranges::lexicographical_compare);

  IdTable result{numKgColumns(), allocator()};
  result.reserve(chunkSize_);
  // NOTE: `set_union` hands out the rows one at a time, so the result is built
  // row by row although `IdTable`s are stored column-major (see the `TODO` for
  // `appendRow` below). A hand-rolled merge could detect whole runs of rows
  // that come from only one of the two scans and append those in bulk, but
  // that is deliberately not worth its complexity here: either the `idFilter`
  // makes the result tiny (the common case of an existence check on few
  // values), or the whole knowledge graph is scanned, in which case
  // decompressing the blocks of the two permutations dominates by far.
  for (const auto& row : merged) {
    result.push_back(row);
    if (result.numRows() >= chunkSize_) {
      checkCancellation();
      co_yield std::move(result);
      result = IdTable{numKgColumns(), allocator()};
      result.reserve(chunkSize_);
    }
  }
  if (!result.empty()) {
    co_yield std::move(result);
  }
}

// _____________________________________________________________________________
Result::Generator EmptyPath::computeAllEntities() const {
  for (IdTable& table : scanIndex(std::nullopt)) {
    co_yield {std::move(table), LocalVocab{}};
  }
}

// _____________________________________________________________________________
// TODO<RobinTF> This writes a single row at a time, although `IdTable`s are
// stored in column-major order, so each of the writes below touches a different
// column. Appending whole runs of rows per column would be considerably faster
// (all the rows that a single input row is expanded to share their payload
// columns, and all the rows of a single entity share their entity column). This
// is deliberately kept simple for now, see the note at the top of the
// `EmptyPath` class for why this is acceptable.
void EmptyPath::appendRow(IdTable& result, const IdTableView<0>& input,
                          size_t inputRow, Id id, Id graph) const {
  result.emplace_back();
  size_t row = result.numRows() - 1;
  result(row, 0) = id;
  if (graphVariable_.has_value()) {
    result(row, 1) = graph;
  }
  for (const auto& [column, inputColumn] :
       ::ranges::views::enumerate(checkedChild_.value().payloadColumns_)) {
    result(row, firstPayloadColumn() + column) = input(inputRow, inputColumn);
  }
}

// _____________________________________________________________________________
bool EmptyPath::graphMatches(const IdTableView<0>& input, size_t inputRow,
                             Id graph) const {
  const std::optional<ColumnIndex>& graphColumn =
      checkedChild_.value().graphColumn_;
  if (!graphColumn.has_value()) {
    return true;
  }
  Id childGraph = input(inputRow, graphColumn.value());
  // An UNDEF graph matches all the graphs that the entity occurs in.
  return childGraph.isUndefined() || childGraph == graph;
}

// _____________________________________________________________________________
// TODO<RobinTF> The cross product below is computed row by row (see the `TODO`
// for `appendRow`), although its shape is very regular: the entity column of
// the result is the entities of the knowledge graph, each repeated
// `undefRows.size()` times, and the payload columns are the payload columns of
// the `undefRows`, tiled once per entity. Both could be written with a handful
// of bulk copies per chunk instead. This is deliberately kept simple for now,
// see the note at the top of the `EmptyPath` class; note that this case is rare
// (and slow no matter what, because the whole knowledge graph has to be read),
// which is why we warn about it.
Result::Generator EmptyPath::processUndefRows(const IdTableView<0>& input,
                                              IdTable& result,
                                              YieldIfFull yieldIfFull) const {
  addWarning(
      "The empty path is applied to a column that contains UNDEF values. Such "
      "a value matches every entity of the knowledge graph, so all of them "
      "have to be read and combined with each of the affected rows, which can "
      "be very slow.");
  ql::span<const Id> joinColumn =
      input.getColumn(checkedChild_.value().joinColumn_);
  std::vector<size_t> undefRows;
  ql::ranges::copy_if(
      ad_utility::integerRange(input.numRows()), std::back_inserter(undefRows),
      [&joinColumn](size_t i) { return joinColumn[i].isUndefined(); });
  // Note that this doesn't preserve the sort order, which is accounted for by
  // `resultSortedOn`.
  for (const IdTable& part : scanIndex(std::nullopt)) {
    checkCancellation();
    // Each entity of the knowledge graph has to be combined with each of the
    // rows that have an UNDEF value in the join column.
    // TODO<C++23> Use `ql::views::cartesian_product`.
    for (const auto& [entityAndGraph, row] : ::ranges::views::cartesian_product(
             entitiesAndGraphs(part), undefRows)) {
      auto [id, graph] = entityAndGraph;
      if (graphMatches(input, row, graph)) {
        appendRow(result, input, row, id, graph);
      }
      // The check has to happen for every single pair, because a single entity
      // can be combined with arbitrarily many rows of the input.
      if (auto pair = yieldIfFull()) {
        co_yield pair.value();
      }
    }
  }
}

// _____________________________________________________________________________
Result::Generator EmptyPath::processTable(IdTableView<0> table,
                                          const LocalVocab& localVocab) const {
  ql::span<const Id> joinColumn =
      table.getColumn(checkedChild_.value().joinColumn_);
  // The distinct values of the join column that have to be looked up.
  std::vector<Id> ids;
  ids.reserve(joinColumn.size());
  ql::ranges::copy_if(joinColumn, std::back_inserter(ids),
                      [](Id id) { return !id.isUndefined(); });
  bool hasUndef = ids.size() != joinColumn.size();
  ql::ranges::sort(ids);
  // NOTE: `ql::ranges::unique` does not work because of a discrepancy in the
  // return types between `std::ranges` and `range-v3`.
  ids.erase(::ranges::unique(ids), ids.end());

  // The entities (and graphs) of the knowledge graph that match one of the
  // values of the join column. This is typically tiny compared to the whole
  // knowledge graph, which is the whole point of this operation.
  IdTable matches{numKgColumns(), allocator()};
  for (IdTable& part : scanIndex(std::move(ids))) {
    matches.insertAtEnd(part);
  }

  IdTable result{getResultWidth(), allocator()};
  result.reserve(std::min(chunkSize_, table.numRows()));
  // Yield the accumulated rows if there are enough of them and start a new
  // table. Returns `std::nullopt` if the current table isn't full yet.
  auto yieldIfFull =
      [this, &result,
       &localVocab]() -> std::optional<Result::IdTableVocabPair> {
    if (result.numRows() < chunkSize_) {
      return std::nullopt;
    }
    checkCancellation();
    Result::IdTableVocabPair pair{std::move(result), localVocab.clone()};
    result = IdTable{getResultWidth(), allocator()};
    result.reserve(chunkSize_);
    return pair;
  };

  for (size_t row : ad_utility::integerRange(table.numRows())) {
    if (row % chunkSize_ == 0) {
      checkCancellation();
    }
    Id id = joinColumn[row];
    if (id.isUndefined()) {
      // Handled by `processUndefRows` below, because a single UNDEF value
      // matches every entity.
      continue;
    }
    for (Id graph : graphsOf(matches, id)) {
      if (graphMatches(table, row, graph)) {
        appendRow(result, table, row, id, graph);
      }
    }
    if (auto pair = yieldIfFull()) {
      co_yield pair.value();
    }
  }

  if (hasUndef) {
    for (auto& pair : processUndefRows(table, result, yieldIfFull)) {
      co_yield pair;
    }
  }

  if (!result.empty()) {
    co_yield {std::move(result), localVocab.clone()};
  }
}

// _____________________________________________________________________________
Result::Generator EmptyPath::computeExistenceCheck(
    std::shared_ptr<const Result> childResult) const {
  if (childResult->isFullyMaterialized()) {
    for (auto& pair :
         processTable(childResult->idTableView(), childResult->localVocab())) {
      co_yield pair;
    }
    co_return;
  }
  for (auto& [table, localVocab] : childResult->idTables()) {
    for (auto& pair : processTable(table.asStaticView<0>(), localVocab)) {
      co_yield pair;
    }
  }
}

// _____________________________________________________________________________
Result EmptyPath::computeResult(bool requestLaziness) {
  // The only consumer of this operation is `TransitivePathImpl`, which always
  // requests the result lazily.
  AD_CORRECTNESS_CHECK(requestLaziness);
  if (!checkedChild_.has_value()) {
    return {computeAllEntities(), resultSortedOn()};
  }
  return {computeExistenceCheck(child().getResult(true)), resultSortedOn()};
}

#endif
