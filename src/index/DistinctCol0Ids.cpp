//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include "index/DistinctCol0Ids.h"

#include <algorithm>
#include <utility>

#include "backports/algorithm.h"
#include "util/Exception.h"

namespace distinctCol0Ids {

// The smaller of the two IDs, or `std::nullopt` if both of them are
// `std::nullopt`. Note that the comparison of `std::optional` cannot be used
// here, as it orders `std::nullopt` BELOW every value, whereas here it means
// "this source is exhausted" and therefore has to lose against an actual ID.
std::optional<Id> smallerId(std::optional<Id> first, std::optional<Id> second) {
  if (!first.has_value()) {
    return second;
  }
  if (!second.has_value()) {
    return first;
  }
  return std::min(first.value(), second.value());
}

// _____________________________________________________________________________
BlockSelector::BlockSelector(
    const CompressedRelationReader::FilterDuplicatesAndGraphs& filter,
    bool addGraphColumn, const std::optional<std::vector<Id>>& idFilter,
    const LocatedTriplesPerBlock& locatedTriples,
    const CompressedRelationReader::Allocator& allocator)
    : filter_{filter},
      addGraphColumn_{addGraphColumn},
      idFilter_{idFilter},
      locatedTriples_{locatedTriples},
      result_{{}, IdTable{numResultColumns(addGraphColumn), allocator}} {}

// _____________________________________________________________________________
SelectedBlocks BlockSelector::select(
    const ScanSpecAndBlocks& scanSpecAndBlocks) && {
  forEachCandidateBlock(
      scanSpecAndBlocks,
      [this](const CompressedBlockMetadata& block) { handleBlock(block); });
  return std::move(result_);
}

// _____________________________________________________________________________
void BlockSelector::handleBlock(const CompressedBlockMetadata& block) {
  if (filter_.canBlockBeSkipped(block)) {
    return;
  }
  if (blockNeedsToBeRead(block)) {
    result_.toRead_.push_back(block);
    return;
  }
  Id id = block.firstTriple_.col0Id_;
  if (!addGraphColumn_) {
    addToMetadata(id, Id::makeUndefined());
    return;
  }
  AD_CORRECTNESS_CHECK(block.graphInfo_.has_value());
  const auto& graphFilter = filter_.graphFilter_;
  ql::ranges::for_each(
      block.graphInfo_.value() | ql::views::filter([&graphFilter](Id graph) {
        return graphFilter.isGraphAllowed(graph);
      }),
      [this, id](Id graph) { addToMetadata(id, graph); });
}

// _____________________________________________________________________________
bool BlockSelector::blockNeedsToBeRead(
    const CompressedBlockMetadata& block) const {
  // We take the single `col0Id` of the block from its metadata, so the block
  // has to be read if it holds several `col0Id`s (we would miss the ones in
  // between), or if we cannot rule out that all of its triples were deleted by
  // delta triples. Reading the block merges the located triples in and hence
  // gives us the actual contents.
  if (!CompressedRelationReader::columnValuesAreKnownFromMetadata(
          block, 1, locatedTriples_)) {
    return true;
  }
  // At this point the single `col0Id` of the block is known, so we only have to
  // read it if we need graph IDs that the metadata doesn't know, or if we
  // cannot rule out that the graph filter removes all of its triples. Note that
  // the graph info of a block with delta triples is only an upper bound (the
  // graph of a deleted triple is still listed there), so it cannot be used to
  // determine the graphs that actually remain.
  bool graphsAreKnown = block.graphInfo_.has_value() &&
                        !locatedTriples_.containsTriples(block.blockIndex_);
  return !graphsAreKnown &&
         (addGraphColumn_ || !filter_.graphFilter_.areAllGraphsAllowed());
}

// _____________________________________________________________________________
void BlockSelector::addToMetadata(Id id, Id graph) {
  IdTable& table = result_.fromMetadata_;
  size_t numRows = table.numRows();
  if (numRows != 0 && table(numRows - 1, idColumn) == id &&
      (!addGraphColumn_ || table(numRows - 1, graphColumnInResult) == graph)) {
    return;
  }
  if (addGraphColumn_) {
    table.push_back({id, graph});
  } else {
    table.push_back({id});
  }
}

// _____________________________________________________________________________
void BlockSelector::forEachCandidateBlock(
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    absl::FunctionRef<void(const CompressedBlockMetadata&)> action) const {
  if (!idFilter_.has_value()) {
    ql::ranges::for_each(scanSpecAndBlocks.getBlockMetadataView(), action);
    return;
  }
  auto firstCol0Id = [](const CompressedBlockMetadata& metadata) {
    return metadata.firstTriple_.col0Id_;
  };
  auto lastCol0Id = [](const CompressedBlockMetadata& metadata) {
    return metadata.lastTriple_.col0Id_;
  };
  const auto& ids = idFilter_.value();
  auto id = ids.begin();
  // The blocks are sorted by their `col0Id`s, so for each of the requested IDs
  // we can binary search the blocks that might contain it.
  for (const auto& blocks : scanSpecAndBlocks.blockMetadata_) {
    auto blockIt = blocks.begin();
    // A single block can contain several of the requested IDs, so the ranges of
    // candidate blocks of two consecutive requested IDs can overlap. This
    // iterator keeps track of the first block for which `action` hasn't been
    // called yet, such that each block is handled at most once.
    auto firstUnhandledBlockIt = blocks.begin();
    while (id != ids.end()) {
      // Skip all the blocks that only contain smaller `col0Id`s.
      blockIt =
          ql::ranges::lower_bound(blockIt, blocks.end(), *id, {}, lastCol0Id);
      if (blockIt == blocks.end()) {
        break;
      }
      // All the blocks that start with a `col0Id` that is not larger than `*id`
      // might contain it. Note that they all end with a `col0Id` that is at
      // least `*id`, because `blockIt` does and the blocks are sorted.
      auto end =
          ql::ranges::upper_bound(blockIt, blocks.end(), *id, {}, firstCol0Id);
      ql::ranges::for_each(
          ql::ranges::subrange{std::max(blockIt, firstUnhandledBlockIt), end},
          action);
      firstUnhandledBlockIt = std::max(firstUnhandledBlockIt, end);
      ++id;
    }
    if (id == ids.end()) {
      return;
    }
  }
}

// _____________________________________________________________________________
IdCursor::IdCursor(TableSource getNextTable,
                   std::optional<ColumnIndex> graphColumn)
    : getNextTable_{std::move(getNextTable)}, graphColumn_{graphColumn} {}

// _____________________________________________________________________________
IdCursor::IdCursor(IdTable table, std::optional<ColumnIndex> graphColumn)
    // The single table is the current one right from the start, so the source
    // has nothing left to yield.
    : getNextTable_{[]() -> std::optional<IdTable> { return std::nullopt; }},
      graphColumn_{graphColumn},
      currentTable_{std::move(table)} {}

// _____________________________________________________________________________
std::optional<Id> IdCursor::peek() {
  while (!currentTable_.has_value() ||
         rowIdx_ == currentTable_.value().numRows()) {
    if (isExhausted_) {
      return std::nullopt;
    }
    currentTable_ = getNextTable_();
    rowIdx_ = 0;
    isExhausted_ = !currentTable_.has_value();
  }
  return currentTable_.value()(rowIdx_, idColumn);
}

// _____________________________________________________________________________
void IdCursor::consumeId(Id id, GraphSet& graphs) {
  while (peek() == std::optional{id}) {
    if (graphColumn_.has_value()) {
      graphs.insert(currentTable_.value()(rowIdx_, graphColumn_.value()));
    }
    ++rowIdx_;
  }
}

// _____________________________________________________________________________
RequestedIdsCursor::RequestedIdsCursor(
    const std::optional<std::vector<Id>>& ids)
    : remainingIds_{ids.has_value()
                        ? std::optional{ql::span<const Id>{ids.value()}}
                        : std::nullopt} {}

// _____________________________________________________________________________
bool RequestedIdsCursor::advanceTo(Id id) {
  if (!remainingIds_.has_value()) {
    return true;
  }
  auto& remainingIds = remainingIds_.value();
  // Drop all the requested IDs that are smaller than `id`. They can never be
  // asked for again, as the `id`s are ascending.
  auto firstNotSmaller = ql::ranges::lower_bound(remainingIds, id);
  remainingIds = remainingIds.subspan(firstNotSmaller - remainingIds.begin());
  return !remainingIds.empty() && remainingIds.front() == id;
}

// _____________________________________________________________________________
ResultBuilder::ResultBuilder(
    bool addGraphColumn, const std::optional<std::vector<Id>>& idFilter,
    const CompressedRelationReader::Allocator& allocator)
    : allocator_{allocator},
      addGraphColumn_{addGraphColumn},
      // If only few IDs are requested, then a chunk holds at most one row per
      // requested ID. Note that with the graph column there can be several rows
      // per ID, but the number of graphs is typically very small, so we don't
      // bother: reserving slightly too little only costs a reallocation.
      numRowsToReserve_{idFilter.has_value()
                            ? std::min(chunkSize, idFilter.value().size())
                            : chunkSize},
      currentChunk_{numResultColumns(addGraphColumn), allocator},
      sortedGraphs_{allocator} {
  currentChunk_.reserve(numRowsToReserve_);
}

// _____________________________________________________________________________
void ResultBuilder::addId(Id id, const GraphSet& graphs) {
  if (!addGraphColumn_) {
    currentChunk_.push_back({id});
    return;
  }
  // The graph IDs have to be sorted.
  sortedGraphs_.assign(graphs.begin(), graphs.end());
  ql::ranges::sort(sortedGraphs_);
  // `IdTable`s are stored in column-major order, so we write the two columns
  // separately instead of row by row.
  size_t numRows = currentChunk_.numRows();
  currentChunk_.resize(numRows + sortedGraphs_.size());
  ql::ranges::fill(currentChunk_.getColumn(idColumn).subspan(numRows), id);
  ql::ranges::copy(sortedGraphs_,
                   currentChunk_.getColumn(graphColumnInResult).begin() +
                       static_cast<ptrdiff_t>(numRows));
}

// _____________________________________________________________________________
bool ResultBuilder::chunkIsFull() const {
  return currentChunk_.numRows() >= chunkSize;
}

// _____________________________________________________________________________
bool ResultBuilder::chunkIsEmpty() const { return currentChunk_.empty(); }

// _____________________________________________________________________________
IdTable ResultBuilder::extractChunk() {
  IdTable chunk = std::move(currentChunk_);
  currentChunk_ = IdTable{numResultColumns(addGraphColumn_), allocator_};
  currentChunk_.reserve(numRowsToReserve_);
  return chunk;
}

}  // namespace distinctCol0Ids

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
