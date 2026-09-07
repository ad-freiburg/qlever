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

// Create an empty table for the result of `getDistinctCol0Ids`, with enough
// space reserved for one chunk (or for fewer rows if only few IDs were
// requested).
IdTable makeResultTable(bool addGraphColumn,
                        const std::optional<std::vector<Id>>& idFilter,
                        const CompressedRelationReader::Allocator& allocator) {
  IdTable table{addGraphColumn ? 2u : 1u, allocator};
  table.reserve(idFilter.has_value()
                    ? std::min(chunkSize, idFilter.value().size())
                    : chunkSize);
  return table;
}

// Append the rows for a single distinct `id` to `result`. If `result` has a
// graph column, one row per graph ID is appended, else a single row.
void appendRowsForId(IdTable& result, Id id, const GraphSet& graphs,
                     ad_utility::VectorWithMemoryLimit<Id>& sortedGraphs) {
  if (result.numColumns() == 1) {
    result.push_back({id});
    return;
  }
  // The graph IDs have to be sorted. `sortedGraphs` is reused across the
  // `col0Id`s so that this doesn't allocate for each of them.
  sortedGraphs.assign(graphs.begin(), graphs.end());
  ql::ranges::sort(sortedGraphs);
  // `IdTable`s are stored in column-major order, so we write the two columns
  // separately instead of row by row.
  size_t numRows = result.numRows();
  result.resize(numRows + sortedGraphs.size());
  ql::ranges::fill(result.getColumn(0).subspan(numRows), id);
  ql::ranges::copy(sortedGraphs, result.getColumn(1).begin() + numRows);
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
      result_{{}, IdTable{addGraphColumn ? 2u : 1u, allocator}} {}

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
  // between), or if there are delta triples for it, which might have deleted
  // that `col0Id` or added further ones. Reading the block merges the located
  // triples in and hence gives us the actual contents.
  if (!CompressedRelationReader::contentsAreKnownFromMetadata(
          block, 1, locatedTriples_)) {
    return true;
  }
  // At this point the single `col0Id` of the block is known, so we only have to
  // read it if we need graph IDs that the metadata doesn't know, or if we
  // cannot rule out that the graph filter removes all of its triples.
  return !block.graphInfo_.has_value() &&
         (addGraphColumn_ || !filter_.graphFilter_.areAllGraphsAllowed());
}

// _____________________________________________________________________________
void BlockSelector::addToMetadata(Id id, Id graph) {
  IdTable& table = result_.fromMetadata_;
  size_t numRows = table.numRows();
  if (numRows != 0 && table(numRows - 1, 0) == id &&
      (!addGraphColumn_ || table(numRows - 1, 1) == graph)) {
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
    auto block = blocks.begin();
    auto firstUnhandledBlock = blocks.begin();
    while (id != ids.end()) {
      // Skip all the blocks that only contain smaller `col0Id`s.
      block = ql::ranges::lower_bound(block, blocks.end(), *id, {}, lastCol0Id);
      if (block == blocks.end()) {
        break;
      }
      // All the blocks that start with a `col0Id` that is not larger than `*id`
      // might contain it. Note that they all end with a `col0Id` that is at
      // least `*id`, because `block` does and the blocks are sorted.
      auto end =
          ql::ranges::upper_bound(block, blocks.end(), *id, {}, firstCol0Id);
      ql::ranges::for_each(
          ql::ranges::subrange{std::max(block, firstUnhandledBlock), end},
          action);
      firstUnhandledBlock = std::max(firstUnhandledBlock, end);
      ++id;
    }
    if (id == ids.end()) {
      return;
    }
  }
}

// _____________________________________________________________________________
IdCursor::IdCursor(TableSource nextTable,
                   std::optional<ColumnIndex> graphColumn)
    : nextTable_{std::move(nextTable)}, graphColumn_{graphColumn} {}

// _____________________________________________________________________________
IdCursor::IdCursor(IdTable table, std::optional<ColumnIndex> graphColumn)
    // The single table is the current one right from the start, so the source
    // has nothing left to yield.
    : nextTable_{[]() -> std::optional<IdTable> { return std::nullopt; }},
      graphColumn_{graphColumn},
      table_{std::move(table)} {}

// _____________________________________________________________________________
std::optional<Id> IdCursor::peek() {
  while (!table_.has_value() || row_ == table_.value().numRows()) {
    if (isExhausted_) {
      return std::nullopt;
    }
    table_ = nextTable_();
    row_ = 0;
    isExhausted_ = !table_.has_value();
  }
  return table_.value()(row_, 0);
}

// _____________________________________________________________________________
void IdCursor::consumeId(Id id, GraphSet& graphs) {
  while (peek() == std::optional{id}) {
    if (graphColumn_.has_value()) {
      graphs.insert(table_.value()(row_, graphColumn_.value()));
    }
    ++row_;
  }
}

// _____________________________________________________________________________
RequestedIds::RequestedIds(const std::optional<std::vector<Id>>& ids)
    : ids_{ids} {}

// _____________________________________________________________________________
bool RequestedIds::contains(Id id) {
  if (!ids_.has_value()) {
    return true;
  }
  const auto& ids = ids_.value();
  while (index_ < ids.size() && ids[index_] < id) {
    ++index_;
  }
  return index_ < ids.size() && ids[index_] == id;
}

}  // namespace distinctCol0Ids

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
