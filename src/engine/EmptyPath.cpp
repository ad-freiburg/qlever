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
// A cursor over the rows of a lazy range of sorted `IdTable`s, which allows to
// merge several such ranges. Rows are appended to the result in bulk wherever
// possible, because `IdTable`s are stored in column-major order.
class RowCursor {
  ad_utility::InputRangeTypeErased<IdTable> range_;
  std::optional<IdTable> table_ = std::nullopt;
  size_t row_ = 0;
  bool isExhausted_ = false;

  // The rows of the current table that haven't been consumed yet.
  auto remainingRows() const {
    return ql::ranges::subrange{table_.value().begin() + row_,
                                table_.value().end()};
  }

  // Append the rows `[row_, end)` of the current table to `result` and make
  // them consumed. Return the number of appended rows.
  size_t append(IdTable& result, size_t end) {
    size_t numRows = end - row_;
    result.insertAtEnd(table_.value(), row_, end);
    row_ = end;
    return numRows;
  }

 public:
  explicit RowCursor(ad_utility::InputRangeTypeErased<IdTable> range)
      : range_{std::move(range)} {}

  // Make the cursor point at the next row that hasn't been consumed yet. Return
  // false if all rows have been consumed.
  bool findNextRow() {
    while (!table_.has_value() || row_ == table_.value().numRows()) {
      if (isExhausted_) {
        return false;
      }
      table_ = range_.get();
      row_ = 0;
      isExhausted_ = !table_.has_value();
    }
    return true;
  }

  // Append all the remaining rows of the current table to `result`.
  void appendRemainingRows(IdTable& result) {
    append(result, table_.value().numRows());
  }

  // Append the current row to `result` and consume it.
  void appendCurrentRow(IdTable& result) { append(result, row_ + 1); }

  // Consume the current row without appending it anywhere.
  void skipCurrentRow() { ++row_; }

  // The row that this cursor currently points at.
  IdTable::const_row_reference currentRow() const {
    return table_.value()[row_];
  }

  // Append all the rows of the current table that are strictly smaller than the
  // current row of `other` to `result`. Return the number of appended rows,
  // which is zero iff the current row is not smaller than `other`'s.
  size_t appendRowsSmallerThan(IdTable& result, const RowCursor& other) {
    auto rows = remainingRows();
    auto end = ql::ranges::lower_bound(rows, other.currentRow(),
                                       ql::ranges::lexicographical_compare);
    return append(result, row_ + (end - rows.begin()));
  }
};

// Return the graph IDs that `id` occurs in according to the `matches` table
// (see `EmptyPath::processTable`), which is sorted and has the `id`s in its
// first column. If `matches` has no graph column, the result is a single
// undefined graph ID if `id` occurs in `matches` at all, and empty otherwise.
// Note that the graph IDs are returned as a subspan of the graph column, which
// works because `IdTable`s are stored in column-major order.
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
}  // namespace

// _____________________________________________________________________________
EmptyPath::EmptyPath(QueryExecutionContext* qec, Variable variable,
                     Graphs activeGraphs, std::optional<Variable> graphVariable,
                     std::shared_ptr<QueryExecutionTree> child,
                     ColumnIndex joinColumn)
    : Operation{qec},
      variable_{std::move(variable)},
      activeGraphs_{std::move(activeGraphs)},
      graphVariable_{std::move(graphVariable)},
      child_{std::move(child)},
      joinColumn_{joinColumn} {
  // The graph column is written in addition to the column of `variable_`, so
  // the two variables must not be the same. Callers that join on the graph
  // variable have to pass a helper variable instead.
  AD_CONTRACT_CHECK(graphVariable_ != variable_);
  variableColumns_[variable_] = makeAlwaysDefinedColumn(0);
  if (graphVariable_.has_value()) {
    variableColumns_[graphVariable_.value()] = makeAlwaysDefinedColumn(1);
  }
  if (child_ == nullptr) {
    resultWidth_ = numIdColumns();
    return;
  }
  AD_CONTRACT_CHECK(joinColumn_ < child_->getResultWidth());
  if (graphVariable_.has_value()) {
    childGraphColumn_ =
        child_->getVariableColumnOrNullopt(graphVariable_.value());
    AD_CORRECTNESS_CHECK(childGraphColumn_ != joinColumn_);
  }
  // All columns of the child except for the join column and the graph column
  // (which are both replaced by the values from the knowledge graph) are
  // carried over. Note that the child might have columns without a variable
  // attached to them, so we iterate over the column indices and not over the
  // variables.
  ql::ranges::copy_if(
      ad_utility::integerRange(child_->getResultWidth()),
      std::back_inserter(payloadColumns_), [this](ColumnIndex column) {
        return column != joinColumn_ && column != childGraphColumn_;
      });
  resultWidth_ = firstPayloadColumn() + payloadColumns_.size();
  for (const auto& [variable, info] : child_->getVariableColumns()) {
    auto column = ql::ranges::find(payloadColumns_, info.columnIndex_);
    if (column == payloadColumns_.end()) {
      continue;
    }
    size_t index = firstPayloadColumn() +
                   ql::ranges::distance(payloadColumns_.begin(), column);
    AD_CORRECTNESS_CHECK(!variableColumns_.contains(variable));
    variableColumns_[variable] = {index, info.mightContainUndef_};
  }
}

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> EmptyPath::getChildren() {
  if (child_ == nullptr) {
    return {};
  }
  return {child_.get()};
}

// _____________________________________________________________________________
std::string EmptyPath::getDescriptor() const {
  return absl::StrCat("EmptyPath for ", variable_.name(),
                      child_ == nullptr ? "" : " (existence check)");
}

// _____________________________________________________________________________
size_t EmptyPath::getResultWidth() const { return resultWidth_; }

// _____________________________________________________________________________
std::string EmptyPath::getCacheKeyImpl() const {
  std::ostringstream os;
  os << "EMPTY PATH for " << variable_.name();
  if (graphVariable_.has_value()) {
    os << " with graph " << graphVariable_.value().name();
  }
  os << ' ';
  activeGraphs_.format(os, &toRdfLiteral);
  if (child_ != nullptr) {
    os << "\nExistence check on column " << joinColumn_ << " of:\n"
       << child_->getCacheKey();
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
uint64_t EmptyPath::getSizeEstimateBeforeLimit() {
  if (child_ != nullptr) {
    // The existence check can only remove rows, but adding the graph column can
    // multiply them. We have no information about the number of graphs per
    // entity, so we simply use the child's estimate.
    return child_->getSizeEstimate();
  }
  const auto& index = getIndex();
  // We don't know how much the subjects and the objects overlap, so we use the
  // (pessimistic) upper bound for the size of their union.
  return index.numDistinctSubjects().normal + index.numDistinctObjects().normal;
}

// _____________________________________________________________________________
size_t EmptyPath::getCostEstimate() {
  if (child_ == nullptr) {
    // In the worst case both the subject and the object permutation have to be
    // read completely, so the cost is proportional to the number of triples and
    // not to the (typically much smaller) number of distinct entities.
    return 2 * getIndex().numTriples().normal;
  }
  // Checking a value only requires reading very few blocks, so the cost is
  // dominated by the cost of the child.
  return child_->getCostEstimate() + getSizeEstimateBeforeLimit();
}

// _____________________________________________________________________________
float EmptyPath::getMultiplicity(size_t col) {
  if (child_ == nullptr) {
    // Without a child the entities are distinct, and for the (much rarer) case
    // with a graph column the number of graphs per entity is unknown, so 1 is
    // still a reasonable guess.
    return 1;
  }
  // The existence check only removes rows, so the multiplicities of the child
  // are a good approximation. The values of the graph column don't come from
  // the child, so we know nothing about them.
  if (col == 0) {
    return child_->getMultiplicity(joinColumn_);
  }
  if (col < firstPayloadColumn()) {
    return 1;
  }
  return child_->getMultiplicity(
      payloadColumns_.at(col - firstPayloadColumn()));
}

// _____________________________________________________________________________
bool EmptyPath::knownEmptyResult() {
  return child_ != nullptr && child_->knownEmptyResult();
}

// _____________________________________________________________________________
std::vector<ColumnIndex> EmptyPath::resultSortedOn() const {
  if (child_ == nullptr) {
    return {0};
  }
  // The rows of the child are processed in order, so the sort order of the join
  // column is preserved. The only exception are UNDEF values, which match every
  // entity of the knowledge graph and are therefore expanded separately.
  const auto& childSortedOn = child_->resultSortedOn();
  bool joinColumnMightBeUndef =
      child_->getVariableAndInfoByColumnIndex(joinColumn_)
          .second.mightContainUndef_ !=
      ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined;
  if (childSortedOn.empty() || childSortedOn.at(0) != joinColumn_ ||
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
  // The values of these two columns are read directly from the index.
  if (variable == variable_ || variable == graphVariable_) {
    return true;
  }
  return Operation::columnOriginatesFromGraphOrUndef(variable);
}

// _____________________________________________________________________________
std::unique_ptr<Operation> EmptyPath::cloneImpl() const {
  return std::make_unique<EmptyPath>(
      getExecutionContext(), variable_, activeGraphs_, graphVariable_,
      child_ == nullptr ? nullptr : child_->clone(), joinColumn_);
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
  // Merge the distinct subjects and the distinct objects. Both ranges are
  // sorted and free of duplicates, so a simple sorted merge suffices.
  RowCursor subjects{scan(Permutation::SPO, idFilter)};
  RowCursor objects{scan(Permutation::OPS, std::move(idFilter))};

  IdTable result{numIdColumns(), allocator()};
  result.reserve(chunkSize_);
  while (true) {
    bool hasSubject = subjects.findNextRow();
    bool hasObject = objects.findNextRow();
    if (!hasSubject && !hasObject) {
      break;
    }
    if (!hasObject) {
      subjects.appendRemainingRows(result);
    } else if (!hasSubject) {
      objects.appendRemainingRows(result);
    } else if (subjects.appendRowsSmallerThan(result, objects) == 0 &&
               objects.appendRowsSmallerThan(result, subjects) == 0) {
      // Neither of the two rows is smaller than the other one, so they are
      // equal and we only yield them once.
      subjects.appendCurrentRow(result);
      objects.skipCurrentRow();
    }
    // The chunk size is only a lower bound, because we append whole runs of
    // rows at once (which is much cheaper than appending them one by one).
    if (result.numRows() >= chunkSize_) {
      checkCancellation();
      co_yield std::move(result);
      result = IdTable{numIdColumns(), allocator()};
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
void EmptyPath::appendRow(IdTable& result, const IdTableView<0>& input,
                          size_t inputRow, Id id, Id graph) const {
  result.emplace_back();
  size_t row = result.numRows() - 1;
  result(row, 0) = id;
  if (graphVariable_.has_value()) {
    result(row, 1) = graph;
  }
  for (const auto& [column, inputColumn] :
       ::ranges::views::enumerate(payloadColumns_)) {
    result(row, firstPayloadColumn() + column) = input(inputRow, inputColumn);
  }
}

// _____________________________________________________________________________
bool EmptyPath::graphMatches(const IdTableView<0>& input, size_t inputRow,
                             Id graph) const {
  if (!childGraphColumn_.has_value()) {
    return true;
  }
  Id childGraph = input(inputRow, childGraphColumn_.value());
  // An UNDEF graph matches all the graphs that the entity occurs in.
  return childGraph.isUndefined() || childGraph == graph;
}

// _____________________________________________________________________________
Result::Generator EmptyPath::processTable(IdTableView<0> table,
                                          const LocalVocab& localVocab) const {
  ql::span<const Id> joinColumn = table.getColumn(joinColumn_);
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
  IdTable matches{numIdColumns(), allocator()};
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
      // Handled below, because a single UNDEF value matches every entity.
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
    // Rows with an UNDEF value in the join column match every entity of the
    // knowledge graph, so we have to stream the full empty path for them. Note
    // that this doesn't preserve the sort order, which is accounted for by
    // `resultSortedOn`.
    std::vector<size_t> undefRows;
    ql::ranges::copy_if(ad_utility::integerRange(table.numRows()),
                        std::back_inserter(undefRows), [&joinColumn](size_t i) {
                          return joinColumn[i].isUndefined();
                        });
    for (IdTable& part : scanIndex(std::nullopt)) {
      checkCancellation();
      for (size_t partRow : ad_utility::integerRange(part.numRows())) {
        Id id = part(partRow, 0);
        Id graph =
            graphVariable_.has_value() ? part(partRow, 1) : Id::makeUndefined();
        for (size_t row : undefRows) {
          if (graphMatches(table, row, graph)) {
            appendRow(result, table, row, id, graph);
          }
          // The check has to happen in the innermost loop, because a single
          // entity can be combined with arbitrarily many rows of the input.
          if (auto pair = yieldIfFull()) {
            co_yield pair.value();
          }
        }
      }
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
  if (child_ == nullptr) {
    return {computeAllEntities(), resultSortedOn()};
  }
  return {computeExistenceCheck(child_->getResult(true)), resultSortedOn()};
}

#endif
