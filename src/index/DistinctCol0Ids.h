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

#include "backports/span.h"
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

// The column that holds the IDs, both in the result and in the tables from
// which the result is computed.
constexpr ColumnIndex idColumn = 0;

// The index of the graph column in the result (and in the IDs that are known
// from the block metadata), if graph IDs were requested.
constexpr ColumnIndex graphColumnInResult = 1;

// The index of the graph column in the blocks that `getDistinctCol0Ids` reads.
// The blocks of a full scan always consist of the three triple columns,
// followed by the graph column (if it was requested).
constexpr ColumnIndex graphColumnInBlock = 3;

// The number of columns of the result: the IDs, plus the graph IDs if they were
// requested.
constexpr size_t numResultColumns(bool addGraphColumn) {
  return addGraphColumn ? graphColumnInResult + 1 : idColumn + 1;
}

// The given `graphColumn`, or `std::nullopt` if no graph IDs were requested.
// Used to set up the `IdCursor`s below.
constexpr std::optional<ColumnIndex> graphColumnIfRequested(
    bool addGraphColumn, ColumnIndex graphColumn) {
  return addGraphColumn ? std::optional{graphColumn} : std::nullopt;
}

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

// Hash and compare graph IDs by their bit representation, which is much cheaper
// than hashing and comparing `Id`s (whose comparison may have to look into the
// local vocabulary). This is correct because all the graph IDs that we see come
// directly from an index (or from the local vocabulary of an update, which is
// normalized against that index), and in such a normalized setting two
// different bit representations always denote two different values: an entry
// that is representable by the vocabulary of the index never appears as a local
// vocabulary entry, and vice versa. Note that this only affects the
// deduplication, not the order: the graphs are sorted as `Id`s.
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
// `getNextTable` function, their `idColumn` holds the IDs. If `graphColumn` is
// set, that column holds the graph IDs.
class IdCursor {
 public:
  // Yields the tables of the source one at a time, and `std::nullopt` once the
  // source is exhausted. It is only invoked from `peek`, at most once per
  // table, so the indirection of a `std::function` doesn't matter.
  using TableSource = std::function<std::optional<IdTable>()>;

 private:
  TableSource getNextTable_;
  std::optional<ColumnIndex> graphColumn_;
  std::optional<IdTable> currentTable_ = std::nullopt;
  size_t rowIdx_ = 0;
  bool isExhausted_ = false;

 public:
  IdCursor(TableSource getNextTable, std::optional<ColumnIndex> graphColumn);

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
// `ids` is `std::nullopt`). The IDs have to be passed to `advanceTo` in
// ascending order.
class RequestedIdsCursor {
  // The requested IDs that haven't been passed to `advanceTo` yet.
  std::optional<ql::span<const Id>> remainingIds_;

 public:
  explicit RequestedIdsCursor(const std::optional<std::vector<Id>>& ids);

  // Advance the cursor to `id` and return whether `id` is one of the requested
  // IDs. All the requested IDs that are smaller than `id` are consumed in the
  // process, so subsequent calls must pass ascending `id`s.
  bool advanceTo(Id id);
};

// The smaller of the two IDs, or `std::nullopt` if both of them are
// `std::nullopt`.
std::optional<Id> smallerId(std::optional<Id> first, std::optional<Id> second);

// Collects the rows of the result of `getDistinctCol0Ids` and hands them out in
// chunks of bounded size.
class ResultBuilder {
  // The number of rows after which a chunk is handed out. This is a soft bound
  // that only serves to keep the memory usage of a single chunk bounded: all
  // the rows of a single ID always end up in the same chunk, so a chunk may
  // exceed this size by the number of graphs of one ID. The sizes of the chunks
  // don't matter otherwise.
  static constexpr size_t chunkSize = 100'000;

  CompressedRelationReader::Allocator allocator_;
  bool addGraphColumn_;
  size_t numRowsToReserve_;
  IdTable currentChunk_;
  // Scratch space for the sorted graph IDs of a single ID. This is a member and
  // not a local variable of `addId` only to avoid an allocation for each of the
  // IDs; neither its contents on entry nor the contents that it is left with
  // are of any interest.
  ad_utility::VectorWithMemoryLimit<Id> sortedGraphs_;

 public:
  ResultBuilder(bool addGraphColumn,
                const std::optional<std::vector<Id>>& idFilter,
                const CompressedRelationReader::Allocator& allocator);

  // Append the rows for a single distinct `id`. If graph IDs were requested,
  // one row per graph ID is appended (in ascending order of the graph IDs),
  // else a single row.
  void addId(Id id, const GraphSet& graphs);

  // Return true iff the current chunk has reached `chunkSize` rows and should
  // be handed out via `extractChunk`.
  bool chunkIsFull() const;

  // Return true iff no rows have been added to the current chunk.
  bool chunkIsEmpty() const;

  // Hand out the current chunk and start a new one.
  IdTable extractChunk();
};

}  // namespace distinctCol0Ids

#endif  // QLEVER_SRC_INDEX_DISTINCTCOL0IDS_H

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
