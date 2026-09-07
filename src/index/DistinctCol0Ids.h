//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#ifndef QLEVER_SRC_INDEX_DISTINCTCOL0IDS_H
#define QLEVER_SRC_INDEX_DISTINCTCOL0IDS_H

#include <absl/functional/function_ref.h>
#include <absl/hash/hash.h>

#include <functional>
#include <optional>
#include <vector>

#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "index/CompressedRelation.h"
#include "index/LocatedTriples.h"
#include "index/ScanSpecification.h"
#include "util/HashSet.h"
#include "util/VectorWithMemoryLimit.h"

// The helper classes and functions for
// `CompressedRelationReader::getDistinctCol0Ids` (see `CompressedRelation.h`
// for its exact semantics). They live in their own translation unit to keep the
// already very large `CompressedRelation.cpp` from growing even further.
namespace distinctCol0Ids {

using ScanSpecAndBlocks = CompressedRelationReader::ScanSpecAndBlocks;

// The number of rows after which `getDistinctCol0Ids` yields a new `IdTable`.
// This is only an upper bound to keep the memory usage bounded, the sizes of
// the yielded tables don't matter otherwise.
constexpr size_t chunkSize = 100'000;

// The index of the graph column in the blocks that `getDistinctCol0Ids` reads.
// The blocks of a full scan always consist of the three triple columns,
// followed by the graph column (if it was requested).
constexpr ColumnIndex graphColumnInBlock = 3;

// The classification of the blocks of a full scan by `BlockSelector` below.
struct SelectedBlocks {
  // The blocks that have to be read from disk.
  std::vector<CompressedBlockMetadata> toRead_;
  // The IDs (and graph IDs, if requested) that are already known from the block
  // metadata alone, in ascending order of the IDs, possibly with duplicates.
  IdTable fromMetadata_;
};

// Split the blocks of a full scan into those that have to be read from disk and
// those whose contribution to the distinct `col0Id`s is already known from
// their metadata (see `CompressedRelationReader::getDistinctCol0Ids`).
class BlockSelector {
  const CompressedRelationReader::FilterDuplicatesAndGraphs& filter_;
  bool addGraphColumn_;
  const std::optional<std::vector<Id>>& idFilter_;
  const LocatedTriplesPerBlock& locatedTriples_;
  SelectedBlocks result_;

 public:
  BlockSelector(
      const CompressedRelationReader::FilterDuplicatesAndGraphs& filter,
      bool addGraphColumn, const std::optional<std::vector<Id>>& idFilter,
      const LocatedTriplesPerBlock& locatedTriples,
      const CompressedRelationReader::Allocator& allocator);

  // Perform the classification described above.
  SelectedBlocks select(const ScanSpecAndBlocks& scanSpecAndBlocks) &&;

 private:
  // Add the given block to `result_`: either to the blocks that have to be
  // read, or (via `addToMetadata`) to the IDs that are already known.
  void handleBlock(const CompressedBlockMetadata& block);

  // Return true iff the given block has to be read to determine the IDs (and
  // graph IDs) that it contributes.
  bool blockNeedsToBeRead(const CompressedBlockMetadata& block) const;

  // Add the given `id` (together with `graph` if graph IDs are requested) to
  // `result_.fromMetadata_`, unless it duplicates the previously added row.
  void addToMetadata(Id id, Id graph);

  // Call `action` for all blocks that might contain one of the requested IDs,
  // in ascending order.
  void forEachCandidateBlock(
      const ScanSpecAndBlocks& scanSpecAndBlocks,
      absl::FunctionRef<void(const CompressedBlockMetadata&)> action) const;
};

// Hash and compare graph IDs by their bit representation, which is much
// cheaper than hashing and comparing `Id`s (whose comparison may have to look
// into the local vocabulary). The graph IDs of a properly normalized index all
// have the same datatype, so their bitwise equality coincides with their actual
// equality. Note that this only affects the deduplication, not the order: the
// graphs are sorted as `Id`s.
struct HashGraphIdByBits {
  size_t operator()(Id id) const { return absl::Hash<Id::T>{}(id.getBits()); }
};
struct GraphIdBitsEqual {
  bool operator()(Id a, Id b) const { return a.getBits() == b.getBits(); }
};

// The distinct graph IDs of a single `col0Id`.
using GraphSet =
    ad_utility::HashSetWithMemoryLimit<Id, HashGraphIdByBits, GraphIdBitsEqual>;

// A cursor over one of the two ascending sources of IDs that
// `getDistinctCol0Ids` merges. The tables are fetched one at a time by the
// `nextTable` function, their first column holds the IDs. If `graphColumn` is
// set, that column holds the graph IDs.
class IdCursor {
 public:
  // Yields the tables of the source one at a time, and `std::nullopt` once the
  // source is exhausted. It is only invoked from `peek`, at most once per
  // table, so the indirection of a `std::function` doesn't matter.
  using TableSource = std::function<std::optional<IdTable>()>;

 private:
  TableSource nextTable_;
  std::optional<ColumnIndex> graphColumn_;
  std::optional<IdTable> table_ = std::nullopt;
  size_t row_ = 0;
  bool isExhausted_ = false;

 public:
  IdCursor(TableSource nextTable, std::optional<ColumnIndex> graphColumn);

  // A cursor over the rows of a single `table`.
  IdCursor(IdTable table, std::optional<ColumnIndex> graphColumn);

  // The ID of the next row that hasn't been consumed yet, or `std::nullopt` if
  // all rows have been consumed. Advances to the next table if necessary.
  std::optional<Id> peek();

  // Consume all the rows that belong to `id`, adding their graph IDs to
  // `graphs` if this cursor has a graph column.
  void consumeId(Id id, GraphSet& graphs);
};

// The IDs that the caller of `getDistinctCol0Ids` requested (all of them if
// `ids` is `std::nullopt`). The IDs have to be passed to `contains` in
// ascending order, which makes it run in amortized constant time.
class RequestedIds {
  const std::optional<std::vector<Id>>& ids_;
  size_t index_ = 0;

 public:
  explicit RequestedIds(const std::optional<std::vector<Id>>& ids);

  bool contains(Id id);
};

// The smaller of the two IDs, or `std::nullopt` if both of them are
// `std::nullopt`.
std::optional<Id> smallerId(std::optional<Id> first, std::optional<Id> second);

// Create an empty table for the result of `getDistinctCol0Ids`, with enough
// space reserved for one chunk (or for fewer rows if only few IDs were
// requested).
IdTable makeResultTable(bool addGraphColumn,
                        const std::optional<std::vector<Id>>& idFilter,
                        const CompressedRelationReader::Allocator& allocator);

// Append the rows for a single distinct `id` to `result`. If `result` has a
// graph column, one row per graph ID is appended, else a single row.
// `sortedGraphs` is a scratch buffer that is reused across the `col0Id`s.
void appendRowsForId(IdTable& result, Id id, const GraphSet& graphs,
                     ad_utility::VectorWithMemoryLimit<Id>& sortedGraphs);

}  // namespace distinctCol0Ids

#endif  // QLEVER_SRC_INDEX_DISTINCTCOL0IDS_H

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
