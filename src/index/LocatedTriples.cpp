// Copyright 2023 - 2025 The QLever Authors, in particular:
//
// 2023 - 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2024 - 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/LocatedTriples.h"

#include "backports/algorithm.h"
#include "global/RuntimeParameters.h"
#include "index/CompressedRelation.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/GraphComputation.h"
#include "index/Permutation.h"
#include "util/ChunkedForLoop.h"
#include "util/Log.h"
#include "util/ValueIdentity.h"

// ____________________________________________________________________________
std::vector<LocatedTriple> LocatedTriple::locateTriplesInPermutation(
    ql::span<const IdTriple<0>> triples,
    ql::span<const CompressedBlockMetadata> blockMetadata,
    const qlever::KeyOrder& keyOrder, bool insertOrDelete,
    ad_utility::SharedCancellationHandle cancellationHandle) {
  std::vector<LocatedTriple> out;
  out.reserve(triples.size());
  ad_utility::chunkedForLoop<10'000>(
      0, triples.size(),
      [&triples, &out, &blockMetadata, &keyOrder, &insertOrDelete](size_t i) {
        auto triple = triples[i].permute(keyOrder);
        // A triple belongs to the first block that contains at least one triple
        // that larger than or equal to the triple. See `LocatedTriples.h` for a
        // discussion of the corner cases.
        size_t blockIndex =
            ql::ranges::lower_bound(
                blockMetadata, triple.toPermutedTriple(),
                [](const auto& a, const auto& b) {
                  // All identical triples with different graphs are currently
                  // stored in the same block, so we don't need to check the
                  // graph. In particular, if this triple is equal (without
                  // graphs) to the first or last triple of a block, then this
                  // call to `lower_bound` will correctly identify this block.
                  return a.tieWithoutGraph() < b.tieWithoutGraph();
                },
                &CompressedBlockMetadata::lastTriple_) -
            blockMetadata.begin();
        out.push_back({blockIndex, triple, insertOrDelete});
      },
      [&cancellationHandle]() { cancellationHandle->throwIfCancelled(); });

  return out;
}

// ____________________________________________________________________________
boost::optional<const LocatedTriples&>
LocatedTriplesPerBlock::getUpdatesIfPresent(size_t blockIndex) const {
  auto it = map_.find(blockIndex);
  if (it == map_.end()) {
    return boost::optional<const LocatedTriples&>{};
  }
  return boost::optional<const LocatedTriples&>{it->second};
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::consolidateAllBlocks() {
  ql::ranges::for_each(map_ | ql::views::values,
                       [](auto& lts) { lts.consolidate(); });
}

// ____________________________________________________________________________
NumAddedAndDeleted LocatedTriplesPerBlock::numTriples(size_t blockIndex) const {
  if (auto blockUpdateTriples = getUpdatesIfPresent(blockIndex)) {
    // Simply return the number of located triples twice. See the comment in the
    // header file for the reasons and potential improvements.
    return {blockUpdateTriples->sizeUpperBound(),
            blockUpdateTriples->sizeUpperBound()};
  }
  return {0, 0};
}

// ____________________________________________________________________________
NumAddedAndDeleted LocatedTriplesPerBlock::numTriples(
    const CompressedBlockMetadata& blockMetadata) const {
  if (!blockMetadata.isPartial()) {
    return numTriples(blockMetadata.blockIndex_);
  }
  auto blockUpdateTriples = getUpdatesIfPresent(blockMetadata.blockIndex_);
  if (!blockUpdateTriples) {
    return {0, 0};
  }
  // See the comment in the header file for why we return the same number twice.
  size_t numTriplesInPart = blockUpdateTriples->sizeUpperBoundInRange(
      blockMetadata.inclusiveLowerBound(), blockMetadata.exclusiveUpperBound(),
      CompareTripleWithBlockBoundIgnoringGraph{});
  return {numTriplesInPart, numTriplesInPart};
}

namespace {

// This code works for `std::integer_sequence` as well as
// `ad_utility::ValueSequence`.
template <typename Row, template <typename T, T...> typename Tp, size_t... I>
auto tieHelper(Row& row, Tp<size_t, I...>) {
  return std::tie(row[I]...);
};
}  // namespace

// Return a `std::tie` of the relevant entries of a row, according to
// `numIndexColumns` and `includeGraphColumn`. For example, if `numIndexColumns`
// is `2` and `includeGraphColumn` is `true`, the function returns
// `std::tie(row[0], row[1], row[2])`.
CPP_template(size_t numIndexColumns, bool includeGraphColumn,
             typename T)(requires(numIndexColumns >= 1 &&
                                  numIndexColumns <=
                                      3)) auto tieIdTableRow(T& row) {
  return tieHelper(
      row, std::make_index_sequence<numIndexColumns +
                                    static_cast<size_t>(includeGraphColumn)>{});
}

// Return a `std::tie` of the relevant entries of a located triple,
// according to `numIndexColumns` and `includeGraphColumn`. For example, if
// `numIndexColumns` is `2` and `includeGraphColumn` is `true`, the function
// returns `std::tie(ids_[1], ids_[2], ids_[3])`, where `ids_` is from
// `lt->triple_`.
template <size_t numIndexColumns, bool includeGraphColumn>
static constexpr auto tieLocatedTriplesIndices = []() {
  std::array<size_t, numIndexColumns + static_cast<size_t>(includeGraphColumn)>
      a{};
  for (size_t i = 0; i < a.size(); ++i) {
    a[i] = i + (3 - numIndexColumns);
  }
  return a;
}();

// Like `tieLocatedTriple`, but takes a `const LocatedTriple&` value instead of
// an iterator. Needed for algorithms like `set_intersection` that pass values.
CPP_template(size_t numIndexColumns, bool includeGraphColumn,
             typename T)(requires(numIndexColumns >= 1 &&
                                  numIndexColumns <=
                                      3)) auto tieLocatedTripleValue(T& lt) {
  const auto& ids = lt.triple_.ids();
  return tieHelper(
      ids,
      ad_utility::toIntegerSequenceRef<
          tieLocatedTriplesIndices<numIndexColumns, includeGraphColumn>>());
}
CPP_template(size_t numIndexColumns, bool includeGraphColumn,
             typename T)(requires(numIndexColumns >= 1 &&
                                  numIndexColumns <=
                                      3)) auto tieLocatedTriple(T& lt) {
  return tieLocatedTripleValue<numIndexColumns, includeGraphColumn>(*lt);
}

// ____________________________________________________________________________
template <size_t numIndexColumns, bool includeGraphColumn>
IdTable LocatedTriplesPerBlock::mergeTriplesImpl(
    const CompressedBlockMetadata& blockMetadata, const IdTable& block) const {
  // This method should only be called if there are located triples in the
  // specified block.
  AD_CONTRACT_CHECK(map_.contains(blockMetadata.blockIndex_));

  AD_CONTRACT_CHECK(numIndexColumns + static_cast<size_t>(includeGraphColumn) <=
                    block.numColumns());

  auto numInsertsAndDeletes = numTriples(blockMetadata);
  IdTable result{block.numColumns(), block.getAllocator()};
  result.resize(block.numRows() + numInsertsAndDeletes.numAdded_);

  auto lessThan = [](const auto& lt, const auto& row) {
    return tieLocatedTriple<numIndexColumns, includeGraphColumn>(lt) <
           tieIdTableRow<numIndexColumns, includeGraphColumn>(row);
  };
  auto equal = [](const auto& lt, const auto& row) {
    return tieLocatedTriple<numIndexColumns, includeGraphColumn>(lt) ==
           tieIdTableRow<numIndexColumns, includeGraphColumn>(row);
  };

  auto rowIt = block.begin();
  auto sortedLocatedTriples = getUpdatesForBlock(blockMetadata);
  auto locatedTripleIt = sortedLocatedTriples.begin();
  auto locatedTripleEnd = sortedLocatedTriples.end();
  auto resultIt = result.begin();

  // Write the given `locatedTriple` to `result` at position `resultIt` and
  // advance `resultIt` by one. See the example in the comment of the
  // declaration of `mergeTriples` to understand the behavior of this function.
  auto writeLocatedTripleToResult = [&result, &resultIt](auto& locatedTriple) {
    // Write part from `locatedTriple` that also occurs in the input `block` to
    // the result.
    static constexpr auto plusOneIfGraph =
        static_cast<size_t>(includeGraphColumn);
    for (size_t i = 0; i < numIndexColumns + plusOneIfGraph; i++) {
      (*resultIt)[i] = locatedTriple.triple_.ids()[3 - numIndexColumns + i];
    }
    // If the input `block` has payload columns (which located triples don't
    // have), set their values to UNDEF.
    for (size_t i = numIndexColumns + plusOneIfGraph; i < result.numColumns();
         i++) {
      (*resultIt)[i] = ValueId::makeUndefined();
    }
    resultIt++;
  };

  while (rowIt != block.end() && locatedTripleIt != locatedTripleEnd) {
    if (lessThan(locatedTripleIt, *rowIt)) {
      if (locatedTripleIt->insertOrDelete_) {
        // Insertion of a non-existent triple.
        writeLocatedTripleToResult(*locatedTripleIt);
      }
      locatedTripleIt++;
    } else if (equal(locatedTripleIt, *rowIt)) {
      if (!locatedTripleIt->insertOrDelete_) {
        // Deletion of an existing triple.
        rowIt++;
      }
      locatedTripleIt++;
    } else {
      // The rowIt is not deleted - copy it
      *resultIt++ = *rowIt++;
    }
  }

  if (locatedTripleIt != locatedTripleEnd) {
    AD_CORRECTNESS_CHECK(rowIt == block.end());
    ql::ranges::for_each(
        ql::ranges::subrange(locatedTripleIt, locatedTripleEnd) |
            ql::views::filter(&LocatedTriple::insertOrDelete_),
        writeLocatedTripleToResult);
  }
  if (rowIt != block.end()) {
    AD_CORRECTNESS_CHECK(locatedTripleIt == locatedTripleEnd);
    while (rowIt != block.end()) {
      *resultIt++ = *rowIt++;
    }
  }

  result.resize(resultIt - result.begin());
  return result;
}

// ____________________________________________________________________________
IdTable LocatedTriplesPerBlock::mergeTriples(
    const CompressedBlockMetadata& blockMetadata, const IdTable& block,
    size_t numIndexColumns, bool includeGraphColumn) const {
  // The following code does nothing more than turn `numIndexColumns` and
  // `includeGraphColumn` into template parameters of `mergeTriplesImpl`.
  auto mergeTriplesImplHelper = [numIndexColumns, &blockMetadata, &block,
                                 this](auto hasGraphColumn) {
    if (numIndexColumns == 3) {
      return mergeTriplesImpl<3, hasGraphColumn>(blockMetadata, block);
    } else if (numIndexColumns == 2) {
      return mergeTriplesImpl<2, hasGraphColumn>(blockMetadata, block);
    } else {
      AD_CORRECTNESS_CHECK(numIndexColumns == 1);
      return mergeTriplesImpl<1, hasGraphColumn>(blockMetadata, block);
    }
  };
  using ad_utility::use_value_identity::vi;
  if (includeGraphColumn) {
    return mergeTriplesImplHelper(vi<true>);
  } else {
    return mergeTriplesImplHelper(vi<false>);
  }
}

namespace {
// Identify the triples to vacuum for a single block by comparing the
// `locatedTriples` with the `idTable` of the block (which has no updates
// applied).
VacuumStatistics processBlockForVacuum(
    const IdTable& idTable, const LocatedTriples& locatedTriples,
    const qlever::KeyOrder::Array& inverseKeys,
    std::vector<IdTriple<0>>& allDeletionsToRemove,
    std::vector<IdTriple<0>>& allInsertionsToRemove) {
  auto toSpo = [&inverseKeys](auto& ids) {
    return IdTriple<0>{[&]<size_t... I>(std::index_sequence<I...>) {
      std::array<Id, 4> spo{};
      ((spo[inverseKeys[I]] = std::get<I>(ids)), ...);
      return spo;
    }(std::make_index_sequence<4>{})};
  };

  auto ltProj = [](const LocatedTriple& lt)
      -> std::tuple<const Id&, const Id&, const Id&, const Id&> {
    return tieLocatedTripleValue<3, true>(lt);
  };
  auto rowProj = [](const auto& row)
      -> std::tuple<const Id&, const Id&, const Id&, const Id&> {
    return tieIdTableRow<3, true>(row);
  };

  auto rowsAsTuple = idTable | ql::views::transform(rowProj);
  auto filteredTriples = [&](bool isInsertion) {
    return locatedTriples.getSortedView() |
           ql::views::filter([isInsertion](const LocatedTriple& lt) {
             return lt.insertOrDelete_ == isInsertion;
           }) |
           ql::views::transform(ltProj);
  };
  auto processTriples = [&](bool isInsertion, auto setAlgorithm,
                            std::vector<IdTriple<0>>& target) {
    size_t before = target.size();
    setAlgorithm(filteredTriples(isInsertion), rowsAsTuple,
                 ad_utility::IteratorForAssigmentOperator{
                     [&](auto&& ids) { target.push_back(toSpo(ids)); }});
    return target.size() - before;
  };

  auto insertionsRemovedInBlock =
      processTriples(true, ql::ranges::set_intersection, allInsertionsToRemove);
  auto deletionsRemovedInBlock =
      processTriples(false, ql::ranges::set_difference, allDeletionsToRemove);

  // TODO<qup42>: we could also get these without an extra iteration by
  // instrumenting the iteration in `processTriples`.
  size_t insertionsInBlock = 0, deletionsInBlock = 0;
  ql::ranges::for_each(locatedTriples.getSortedView(),
                       [&](const LocatedTriple& lt) {
                         if (lt.insertOrDelete_) {
                           insertionsInBlock++;
                         } else {
                           deletionsInBlock++;
                         }
                       });

  return {deletionsRemovedInBlock, insertionsRemovedInBlock,
          deletionsInBlock - deletionsRemovedInBlock,
          insertionsInBlock - insertionsRemovedInBlock};
}
}  // namespace

// ____________________________________________________________________________
TriplesToVacuum LocatedTriplesPerBlock::identifyTriplesToVacuum(
    const Permutation& perm,
    ad_utility::SharedCancellationHandle cancellationHandle) const {
  size_t minimumBlockSize =
      getRuntimeParameter<&RuntimeParameters::vacuumMinimumBlockSize_>();
  auto blocksToVacuum = map_ |
                        ql::views::filter([minimumBlockSize](const auto& e) {
                          return e.second.sizeUpperBound() >= minimumBlockSize;
                        }) |
                        ql::views::keys;

  VacuumStatistics totalStats{0, 0, 0, 0};
  std::vector<IdTriple<0>> allDeletionsToRemove;
  std::vector<IdTriple<0>> allInsertionsToRemove;

  // The identified triples are output in `SPO` so we need to invert the
  // permutation.
  qlever::KeyOrder::Array inverseKeys{};
  for (uint8_t i = 0; i < 4; ++i) {
    inverseKeys[perm.keyOrder().keys()[i]] = i;
  }

  const auto& reader = perm.reader();
  const auto& blockMetadata = perm.metaData().blockData();
  AD_CORRECTNESS_CHECK(!blockMetadata.empty());

  for (size_t blockIndex : blocksToVacuum) {
    AD_CORRECTNESS_CHECK(blockIndex <= blockMetadata.size());
    // This is one past the last block with index triples. This block always
    // only has updates but no index triples. Pass in an empty `IdTable`.
    if (blockIndex == blockMetadata.size()) {
      ad_utility::AllocatorWithLimit<Id> allocator =
          ad_utility::makeAllocatorWithLimit<Id>(0_B);
      IdTable idTable(4, allocator);
      totalStats +=
          processBlockForVacuum(idTable, map_.at(blockIndex), inverseKeys,
                                allDeletionsToRemove, allInsertionsToRemove);
      continue;
    }

    auto idTable = reader.readBlockWithoutLocatedTriples(
        blockMetadata.at(blockIndex),
        std::vector<ColumnIndex>{ADDITIONAL_COLUMN_GRAPH_ID});

    totalStats +=
        processBlockForVacuum(idTable, map_.at(blockIndex), inverseKeys,
                              allDeletionsToRemove, allInsertionsToRemove);
    cancellationHandle->throwIfCancelled();
  }

  return {std::move(allDeletionsToRemove), std::move(allInsertionsToRemove),
          totalStats};
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::add(ql::span<const LocatedTriple> locatedTriples,
                                 ad_utility::timer::TimeTracer& tracer) {
  tracer.beginTrace("adding");
  for (const auto& locatedTriple : locatedTriples) {
    map_[locatedTriple.blockIndex_].insert(locatedTriple);
  }
  tracer.endTrace("adding");
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::erase(size_t blockIndex, const LocatedTriple& lt) {
  auto blockIter = map_.find(blockIndex);
  AD_CONTRACT_CHECK(blockIter != map_.end(), "Block ", blockIndex,
                    " is not contained");
  auto& block = blockIter->second;
  block.erase(lt);
  if (block.empty()) {
    map_.erase(blockIndex);
  }
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::erase(ql::span<LocatedTriple> sortedTriples) {
  AD_CORRECTNESS_CHECK(
      ql::ranges::is_sorted(sortedTriples, {}, &LocatedTriple::triple_));

  for (const auto chunk :
       ::ranges::views::chunk_by(sortedTriples, [](auto& lt1, auto& lt2) {
         return lt1.blockIndex_ == lt2.blockIndex_;
       })) {
    size_t blockIndex = chunk.front().blockIndex_;
    auto blockIter = map_.find(blockIndex);
    AD_CONTRACT_CHECK(blockIter != map_.end(), "Block ", blockIndex,
                      " is not contained");
    auto& block = blockIter->second;
    block.eraseSorted(chunk);
    if (block.empty()) {
      map_.erase(blockIndex);
    }
  }
}

// ____________________________________________________________________________
size_t LocatedTriplesPerBlock::numTriplesForTesting() const {
  return ::ranges::accumulate(
      map_ | ql::views::values |
          ql::views::transform(&LocatedTriples::sizeForTesting),
      size_t{0});
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::setOriginalMetadata(
    std::shared_ptr<const std::vector<CompressedBlockMetadata>> metadata) {
  originalMetadata_ = std::move(metadata);
}

namespace {

// The information about the update triples of a single part of a block that is
// required to compute the metadata of that part. The parts of a block are
// determined in `appendAugmentedMetadataForBlock`.
struct UpdateInfoForPart {
  using PermutedTriple = CompressedBlockMetadata::PermutedTriple;
  // The first and the last update triple of this part.
  PermutedTriple firstTriple_{};
  PermutedTriple lastTriple_{};
  // The distinct graphs of the *inserted* triples of this part. `nullopt` if
  // there are more than `MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA` of them.
  std::optional<std::vector<Id>> insertedGraphs_{std::vector<Id>{}};
  // True iff this part contains at least one inserted (as opposed to deleted)
  // triple.
  bool hasInsertions_ = false;
  // The number of update triples of this part.
  size_t numTriples_ = 0;

  // Add a single update triple. The triples have to be added in sorted order.
  void addTriple(const LocatedTriple& locatedTriple) {
    auto triple = locatedTriple.triple_.toPermutedTriple();
    if (numTriples_ == 0) {
      firstTriple_ = triple;
    }
    lastTriple_ = triple;
    ++numTriples_;
    if (!locatedTriple.insertOrDelete_) {
      return;
    }
    hasInsertions_ = true;
    if (!insertedGraphs_.has_value()) {
      return;
    }
    auto& graphs = insertedGraphs_.value();
    // Note: We compare the bits, exactly as `computeDistinctGraphs` does.
    if (ql::ranges::find(graphs, triple.graphId_.getBits(), &Id::getBits) !=
        graphs.end()) {
      return;
    }
    if (graphs.size() == MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA) {
      insertedGraphs_.reset();
      return;
    }
    graphs.push_back(triple.graphId_);
  }
};

// Return a copy of the `triple` that can be used as the bound between two parts
// of a block. The graph is set to the minimal possible ID, because the bounds
// are compared while ignoring the graph, and this makes the fact that all
// triples that are equal to the `triple` except for their graph belong to the
// part *after* this bound explicit.
CompressedBlockMetadata::PermutedTriple makeBoundBetweenParts(
    CompressedBlockMetadata::PermutedTriple triple) {
  triple.graphId_ = Id::min();
  return triple;
}

// Update the `blockMetadata`, such that its graph info is consistent with the
// update triples described by `updateInfo` which are added to that block. In
// particular, all graphs to which at least one triple is inserted become part
// of the graph info, and if the number of total graphs becomes larger than the
// configured threshold, then the graph info is set to `nullopt`, which means
// that there is no info.
void updateGraphMetadata(CompressedBlockMetadata& blockMetadata,
                         const UpdateInfoForPart& updateInfo) {
  auto& graphs = blockMetadata.graphInfo_;
  // We only insert graphs, never delete them, so if `graphs` is already
  // `nullopt`, then it will stay `nullopt`.
  if (graphs.has_value()) {
    if (updateInfo.insertedGraphs_.has_value()) {
      graphs = computeDistinctGraphs(updateInfo.insertedGraphs_.value(),
                                     graphs.value());
    } else {
      graphs.reset();
    }
  }

  if (!hasOnlyOneGraph(graphs)) {
    // We do not know anything about the triples contained in the block, so we
    // also cannot know if the update triples introduce duplicates. We thus have
    // to be conservative and assume that there are duplicates when data was
    // inserted.
    blockMetadata.containsDuplicatesWithDifferentGraphs_ |=
        updateInfo.hasInsertions_;
  }
}

}  // namespace

// ____________________________________________________________________________
void LocatedTriplesPerBlock::appendAugmentedMetadataForBlock(
    std::vector<CompressedBlockMetadata>& result,
    CompressedBlockMetadata blockMetadata, size_t blockIndex) const {
  using PermutedTriple = CompressedBlockMetadata::PermutedTriple;
  // Note: The `blockIndex` is the position of the block in the metadata, which
  // is what the `LocatedTriple`s refer to. For metadata that was created by the
  // `CompressedRelationWriter` it is equal to `blockMetadata.blockIndex_`.
  auto blockUpdates = getUpdatesIfPresent(blockIndex);
  if (!blockUpdates) {
    result.push_back(std::move(blockMetadata));
    return;
  }
  // A block without any rows in the underlying file consists of update triples
  // only. This is the case for the very last block, which holds the update
  // triples that are larger than all triples in the index.
  const bool hasRowsInFile =
      blockMetadata.offsetsAndCompressedSize_.has_value();
  AD_CORRECTNESS_CHECK(hasRowsInFile || blockMetadata.numRows_ == 0);

  // Determine into how many parts this block has to be split, such that each
  // part has at most `maxBlockSize` rows (rows in the file plus update
  // triples).
  size_t maxBlockSize =
      getRuntimeParameter<&RuntimeParameters::maxBlockSizeWithUpdates_>();
  size_t numUpdates = blockUpdates->sizeUpperBound();
  size_t totalSize = numUpdates + blockMetadata.numRows_;
  size_t numParts = maxBlockSize > 0 && totalSize > maxBlockSize
                        ? (totalSize + maxBlockSize - 1) / maxBlockSize
                        : 1;
  size_t numUpdatesPerPart =
      std::max<size_t>(1, (numUpdates + numParts - 1) / numParts);

  // Distribute the update triples over the parts. A new part is started as soon
  // as the current part has at least `numUpdatesPerPart` triples, but only at a
  // triple that differs from the previous one also when the graph is ignored,
  // because triples that only differ in their graph must stay in the same part.
  // Note: The number of parts computed above is only an upper bound for the
  // number of parts that are actually created.
  std::vector<UpdateInfoForPart> parts(1);
  for (const LocatedTriple& locatedTriple : blockUpdates->getSortedView()) {
    if (parts.back().numTriples_ >= numUpdatesPerPart &&
        parts.back().lastTriple_.tieWithoutGraph() !=
            locatedTriple.triple_.toPermutedTriple().tieWithoutGraph()) {
      parts.emplace_back();
    }
    parts.back().addTriple(locatedTriple);
  }
  const bool isSplit = parts.size() > 1;

  // Return true iff the part with the given bounds can contain rows of the
  // underlying on-disk block. The rows in the file lie in the range
  // `[firstTriple_, lastTriple_]` of the original metadata.
  auto canContainRowsInFile =
      [&blockMetadata, hasRowsInFile](
          const std::optional<PermutedTriple>& lowerBound,
          const std::optional<PermutedTriple>& upperBound) {
        return hasRowsInFile &&
               (!upperBound.has_value() ||
                blockMetadata.firstTriple_.tieWithoutGraph() <
                    upperBound.value().tieWithoutGraph()) &&
               (!lowerBound.has_value() ||
                lowerBound.value().tieWithoutGraph() <=
                    blockMetadata.lastTriple_.tieWithoutGraph());
      };

  // The bounds of the part with the given index. The first part has no lower
  // and the last part has no upper bound, such that the parts always cover the
  // complete range of the original block. This is important, because update
  // triples may be added after this function has been called; those then still
  // belong to exactly one of the parts.
  auto boundsOfPart = [&parts](size_t partIndex) {
    std::optional<PermutedTriple> lowerBound;
    std::optional<PermutedTriple> upperBound;
    if (partIndex > 0) {
      lowerBound = makeBoundBetweenParts(parts.at(partIndex).firstTriple_);
    }
    if (partIndex + 1 < parts.size()) {
      upperBound = makeBoundBetweenParts(parts.at(partIndex + 1).firstTriple_);
    }
    return std::pair{lowerBound, upperBound};
  };

  // Distribute the rows in the file over those parts that can contain them.
  size_t numPartsWithRowsInFile = 0;
  for (size_t partIndex = 0; partIndex < parts.size(); ++partIndex) {
    auto [lowerBound, upperBound] = boundsOfPart(partIndex);
    numPartsWithRowsInFile +=
        static_cast<size_t>(canContainRowsInFile(lowerBound, upperBound));
  }
  size_t numRowsPerPart =
      numPartsWithRowsInFile == 0
          ? 0
          : (blockMetadata.numRows_ + numPartsWithRowsInFile - 1) /
                numPartsWithRowsInFile;

  for (size_t partIndex = 0; partIndex < parts.size(); ++partIndex) {
    const auto& part = parts.at(partIndex);
    auto [lowerBound, upperBound] = boundsOfPart(partIndex);
    CompressedBlockMetadata metadata = blockMetadata;
    // The first and last triple of a part. For the outermost parts, the triples
    // in the file also have to be taken into account; for the parts in between,
    // the first triple is the inclusive lower and the last triple is the
    // *exclusive* upper bound of that part.
    metadata.firstTriple_ = lowerBound.value_or(
        std::min(blockMetadata.firstTriple_, part.firstTriple_));
    metadata.lastTriple_ = upperBound.value_or(
        std::max(blockMetadata.lastTriple_, part.lastTriple_));
    if (isSplit) {
      if (canContainRowsInFile(lowerBound, upperBound)) {
        metadata.partialBlockInfo_ = CompressedBlockMetadata::PartialBlockInfo{
            static_cast<uint32_t>(std::min<size_t>(
                numRowsPerPart, std::numeric_limits<uint32_t>::max())),
            lowerBound.has_value(), upperBound.has_value()};
      } else {
        // This part provably contains none of the rows in the file, so it does
        // not have to be read from disk at all.
        metadata.offsetsAndCompressedSize_.reset();
        metadata.numRows_ = 0;
        metadata.partialBlockInfo_ = CompressedBlockMetadata::PartialBlockInfo{
            0, lowerBound.has_value(), upperBound.has_value()};
      }
    }
    updateGraphMetadata(metadata, part);
    result.push_back(std::move(metadata));
  }
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::updateAugmentedMetadata() {
  static const std::vector<CompressedBlockMetadata> emptyMetadata{};
  if (!originalMetadata_.has_value()) {
    AD_LOG_WARN << "The original metadata has not been set, but updates are "
                   "being performed. This should only happen in unit tests\n";
  }
  const auto& originalMetadata = originalMetadata_.has_value()
                                     ? *originalMetadata_.value()
                                     : emptyMetadata;

  std::vector<CompressedBlockMetadata> augmentedMetadata;
  augmentedMetadata.reserve(originalMetadata.size() + 1);
  // TODO<C++23> use `ql::views::enumerate`.
  size_t blockIndex = 0;
  for (const auto& blockMetadata : originalMetadata) {
    appendAugmentedMetadataForBlock(augmentedMetadata, blockMetadata,
                                    blockIndex);
    ++blockIndex;
  }
  // Also account for the last block that contains the triples that are larger
  // than all the triples in the index.
  size_t lastBlockIndex = originalMetadata.size();
  if (containsTriples(lastBlockIndex)) {
    // The first `std::nullopt` means that this block contains only
    // `LocatedTriple`s, so it has no rows in the file. Its first and last
    // triple are initialized to the largest and smallest possible triple
    // respectively, such that computing the minimum and maximum with the update
    // triples (in `appendAugmentedMetadataForBlock`) yields exactly the bounds
    // of the update triples.
    using PermutedTriple = CompressedBlockMetadata::PermutedTriple;
    PermutedTriple maxTriple{Id::max(), Id::max(), Id::max(), Id::max()};
    PermutedTriple minTriple{Id::min(), Id::min(), Id::min(), Id::min()};
    CompressedBlockMetadataNoBlockIndex lastBlockN{
        std::nullopt, 0, maxTriple, minTriple, std::nullopt, true};
    lastBlockN.graphInfo_.emplace();
    appendAugmentedMetadataForBlock(
        augmentedMetadata, CompressedBlockMetadata{lastBlockN, lastBlockIndex},
        lastBlockIndex);
  }
  AD_CORRECTNESS_CHECK(CompressedBlockMetadata::checkInvariantsForSortedBlocks(
      augmentedMetadata));
  augmentedMetadata_ = std::move(augmentedMetadata);
}

// ____________________________________________________________________________
void to_json(nlohmann::json& j, const VacuumStatistics& stats) {
  j = nlohmann::json{{"insertionsRemoved", stats.numInsertionsRemoved_},
                     {"deletionsRemoved", stats.numDeletionsRemoved_},
                     {"insertionsKept", stats.numInsertionsKept_},
                     {"deletionsKept", stats.numDeletionsKept_},
                     {"totalRemoved", stats.totalRemoved()},
                     {"totalKept", stats.totalKept()}};
}

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const std::vector<IdTriple<0>>& v) {
  ql::ranges::copy(v, std::ostream_iterator<IdTriple<0>>(os, ", "));
  return os;
}

// ____________________________________________________________________________
bool LocatedTriplesPerBlock::isLocatedTriple(const IdTriple<0>& triple,
                                             bool insertOrDelete) const {
  auto blockContains = [&triple, insertOrDelete](const LocatedTriples& lt,
                                                 size_t blockIndex) {
    LocatedTriple locatedTriple{blockIndex, triple, insertOrDelete};
    locatedTriple.blockIndex_ = blockIndex;
    return ad_utility::contains(lt.getSortedView(), locatedTriple);
  };

  return ql::ranges::any_of(map_, [&blockContains](auto& indexAndBlock) {
    const auto& [index, block] = indexAndBlock;
    return blockContains(block, index);
  });
}

// _____________________________________________________________________________
std::array<std::vector<IdTriple<0>>, 2> LocatedTriplesPerBlock::computeDiff(
    const LocatedTriplesPerBlock& oldBlocks) const {
  std::array<std::vector<IdTriple<0>>, 2> result;
  auto addTriple = [&result](const LocatedTriple& lt) {
    result.at(lt.insertOrDelete_ ? 0 : 1).push_back(lt.triple_);
  };

  // Compute all `LocatedTriples` that are in the new snapshot, but not in the
  // old snapshot requires comparing by the `IdTriple` but also
  // `insertOrDelete_`. Such triples have been newly inserted, newly deleted or
  // changed (from inserted to deleted or vice versa) since the old snapshot.
  for (const auto& [blockIndex, currentTriples] : map_) {
    auto it = oldBlocks.map_.find(blockIndex);
    const LocatedTriples empty;
    const auto& oldTriplesSortedView = it != oldBlocks.map_.end()
                                           ? it->second.getSortedView()
                                           : empty.getSortedView();
    // The default comparator compares the whole `LocatedTriple` with
    // `IdTriple`, `insertOrDelete_` and `blockIndex_`. When the `IdTriple`s are
    // equal the `blockIndex_` is also the same, so this does the right thing.
    ql::ranges::set_difference(
        currentTriples.getSortedView(), oldTriplesSortedView,
        ad_utility::IteratorForAssigmentOperator(addTriple));
  }
  // Account for non-deterministic order introduced by hash map. (Or in case a
  // permutation that is not SPO was used).
  ql::ranges::for_each(result, ql::ranges::sort);

  return result;
}
