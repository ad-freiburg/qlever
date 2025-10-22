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
#include "index/CompressedRelation.h"
#include "index/ConstantsIndexBuilding.h"
#include "util/ChunkedForLoop.h"
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
        out.emplace_back(blockIndex, triple, insertOrDelete);
      },
      [&cancellationHandle]() { cancellationHandle->throwIfCancelled(); });

  return out;
}

// ____________________________________________________________________________
bool LocatedTriplesPerBlock::hasUpdates(size_t blockIndex) const {
  return map_.contains(blockIndex);
}

// ____________________________________________________________________________
NumAddedAndDeleted LocatedTriplesPerBlock::numTriples(size_t blockIndex) const {
  if (!hasUpdates(blockIndex)) {
    return {0, 0};
  } else {
    const auto& blockUpdateTriples = map_.at(blockIndex);
    // Simply return the number of located triples twice. See the comment in the
    // header file for the reasons and potential improvements.
    return {blockUpdateTriples.size(), blockUpdateTriples.size()};
  }
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
CPP_template(size_t numIndexColumns, bool includeGraphColumn,
             typename T)(requires(numIndexColumns >= 1 &&
                                  numIndexColumns <=
                                      3)) auto tieLocatedTriple(T& lt) {
  constexpr auto indices = []() {
    std::array<size_t,
               numIndexColumns + static_cast<size_t>(includeGraphColumn)>
        a;
    for (size_t i = 0; i < numIndexColumns; ++i) {
      a[i] = 3 - numIndexColumns + i;
    }
    if (includeGraphColumn) {
      // The graph column resides at index `3` of the located triple.
      a.back() = 3;
    }
    return a;
  }();
  auto& ids = lt->triple_.ids();
  return tieHelper(ids, ad_utility::toIntegerSequence<indices>());
}

// ____________________________________________________________________________
template <size_t numIndexColumns, bool includeGraphColumn>
IdTable LocatedTriplesPerBlock::mergeTriplesImpl(size_t blockIndex,
                                                 const IdTable& block) const {
  // This method should only be called if there are located triples in the
  // specified block.
  AD_CONTRACT_CHECK(map_.contains(blockIndex));

  AD_CONTRACT_CHECK(numIndexColumns + static_cast<size_t>(includeGraphColumn) <=
                    block.numColumns());

  auto numInsertsAndDeletes = numTriples(blockIndex);
  IdTable result{block.numColumns(), block.getAllocator()};
  result.resize(block.numRows() + numInsertsAndDeletes.numAdded_);

  const auto& locatedTriples = map_.at(blockIndex);

  auto lessThan = [](const auto& lt, const auto& row) {
    return tieLocatedTriple<numIndexColumns, includeGraphColumn>(lt) <
           tieIdTableRow<numIndexColumns, includeGraphColumn>(row);
  };
  auto equal = [](const auto& lt, const auto& row) {
    return tieLocatedTriple<numIndexColumns, includeGraphColumn>(lt) ==
           tieIdTableRow<numIndexColumns, includeGraphColumn>(row);
  };

  auto rowIt = block.begin();
  auto locatedTripleIt = locatedTriples.begin();
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

  while (rowIt != block.end() && locatedTripleIt != locatedTriples.end()) {
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

  if (locatedTripleIt != locatedTriples.end()) {
    AD_CORRECTNESS_CHECK(rowIt == block.end());
    ql::ranges::for_each(
        ql::ranges::subrange(locatedTripleIt, locatedTriples.end()) |
            ql::views::filter(&LocatedTriple::insertOrDelete_),
        writeLocatedTripleToResult);
  }
  if (rowIt != block.end()) {
    AD_CORRECTNESS_CHECK(locatedTripleIt == locatedTriples.end());
    while (rowIt != block.end()) {
      *resultIt++ = *rowIt++;
    }
  }

  result.resize(resultIt - result.begin());
  return result;
}

// ____________________________________________________________________________
IdTable LocatedTriplesPerBlock::mergeTriples(size_t blockIndex,
                                             const IdTable& block,
                                             size_t numIndexColumns,
                                             bool includeGraphColumn) const {
  // The following code does nothing more than turn `numIndexColumns` and
  // `includeGraphColumn` into template parameters of `mergeTriplesImpl`.
  auto mergeTriplesImplHelper = [numIndexColumns, blockIndex, &block,
                                 this](auto hasGraphColumn) {
    if (numIndexColumns == 3) {
      return mergeTriplesImpl<3, hasGraphColumn>(blockIndex, block);
    } else if (numIndexColumns == 2) {
      return mergeTriplesImpl<2, hasGraphColumn>(blockIndex, block);
    } else {
      AD_CORRECTNESS_CHECK(numIndexColumns == 1);
      return mergeTriplesImpl<1, hasGraphColumn>(blockIndex, block);
    }
  };
  using ad_utility::use_value_identity::vi;
  if (includeGraphColumn) {
    return mergeTriplesImplHelper(vi<true>);
  } else {
    return mergeTriplesImplHelper(vi<false>);
  }
}

// ____________________________________________________________________________
std::vector<LocatedTriples::iterator> LocatedTriplesPerBlock::add(
    ql::span<const LocatedTriple> locatedTriples,
    ad_utility::timer::TimeTracer& tracer) {
  tracer.beginTrace("adding");
  std::vector<LocatedTriples::iterator> handles;
  handles.reserve(locatedTriples.size());
  for (auto triple : locatedTriples) {
    LocatedTriples& locatedTriplesInBlock = map_[triple.blockIndex_];
    auto [handle, wasInserted] = locatedTriplesInBlock.emplace(triple);
    AD_CORRECTNESS_CHECK(wasInserted == true);
    AD_CORRECTNESS_CHECK(handle != locatedTriplesInBlock.end());
    ++numTriples_;
    handles.emplace_back(handle);
  }

  tracer.endTrace("adding");
  tracer.beginTrace("updateMetadata");
  updateAugmentedMetadata();
  tracer.endTrace("updateMetadata");

  return handles;
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::erase(size_t blockIndex,
                                   LocatedTriples::iterator iter) {
  auto blockIter = map_.find(blockIndex);
  AD_CONTRACT_CHECK(blockIter != map_.end(), "Block ", blockIndex,
                    " is not contained.");
  auto& block = blockIter->second;
  block.erase(iter);
  numTriples_--;
  if (block.empty()) {
    map_.erase(blockIndex);
  }
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::setOriginalMetadata(
    std::shared_ptr<const std::vector<CompressedBlockMetadata>> metadata) {
  originalMetadata_ = std::move(metadata);
}

// Update the `blockMetadata`, such that its graph info is consistent with the
// `locatedTriples` which are added to that block. In particular, all graphs to
// which at least one triple is inserted become part of the graph info, and if
// the number of total graphs becomes larger than the configured threshold, then
// the graph info is set to `nullopt`, which means that there is no info.
static auto updateGraphMetadata(CompressedBlockMetadata& blockMetadata,
                                const LocatedTriples& locatedTriples) {
  // We do not know anything about the triples contained in the block, so we
  // also cannot know if the `locatedTriples` introduces duplicates. We thus
  // have to be conservative and assume that there are duplicates.
  blockMetadata.containsDuplicatesWithDifferentGraphs_ = true;
  auto& graphs = blockMetadata.graphInfo_;
  if (!graphs.has_value()) {
    // The original block already contains too many graphs, don't store any
    // graph info.
    return;
  }

  // Compute a hash set of all graphs that are originally contained in the block
  // and all the graphs that are added via the `locatedTriples`.
  ad_utility::HashSet<Id> newGraphs(graphs.value().begin(),
                                    graphs.value().end());
  for (auto& lt : locatedTriples) {
    if (!lt.insertOrDelete_) {
      // Don't update the graph info for triples that are deleted.
      continue;
    }
    newGraphs.insert(lt.triple_.ids().at(ADDITIONAL_COLUMN_GRAPH_ID));
    // Handle the case that with the newly added triples we have too many
    // distinct graphs to store them in the graph info.
    if (newGraphs.size() > MAX_NUM_GRAPHS_STORED_IN_BLOCK_METADATA) {
      graphs.reset();
      return;
    }
  }
  graphs.emplace(newGraphs.begin(), newGraphs.end());

  // Sort the stored graphs. Note: this is currently not expected by the code
  // that uses the graph info, but makes testing much easier.
  ql::ranges::sort(graphs.value());
}

// ____________________________________________________________________________
void LocatedTriplesPerBlock::updateAugmentedMetadata() {
  // TODO<C++23> use view::enumerate
  size_t blockIndex = 0;
  // Copy to preserve originalMetadata_.
  if (!originalMetadata_.has_value()) {
    AD_LOG_WARN << "The original metadata has not been set, but updates are "
                   "being performed. This should only happen in unit tests\n";
    augmentedMetadata_.emplace();
  } else {
    augmentedMetadata_ = *originalMetadata_.value();
  }
  for (auto& blockMetadata : augmentedMetadata_.value()) {
    if (hasUpdates(blockIndex)) {
      const auto& blockUpdates = map_.at(blockIndex);
      blockMetadata.firstTriple_ =
          std::min(blockMetadata.firstTriple_,
                   blockUpdates.begin()->triple_.toPermutedTriple());
      blockMetadata.lastTriple_ =
          std::max(blockMetadata.lastTriple_,
                   blockUpdates.rbegin()->triple_.toPermutedTriple());
      updateGraphMetadata(blockMetadata, blockUpdates);
    }
    blockIndex++;
  }
  // Also account for the last block that contains the triples that are larger
  // than all the inserted triples.
  if (hasUpdates(blockIndex)) {
    const auto& blockUpdates = map_.at(blockIndex);
    auto firstTriple = blockUpdates.begin()->triple_.toPermutedTriple();
    auto lastTriple = blockUpdates.rbegin()->triple_.toPermutedTriple();

    using O = CompressedBlockMetadata::OffsetAndCompressedSize;
    O emptyBlock{0, 0};

    // TODO<joka921> We need the appropriate number of columns here, or we need
    // to make the reading code work regardless of the number of columns.
    CompressedBlockMetadataNoBlockIndex lastBlockN{
        std::vector<O>(4, emptyBlock),
        0,
        firstTriple,
        lastTriple,
        std::nullopt,
        true};
    lastBlockN.graphInfo_.emplace();
    CompressedBlockMetadata lastBlock{lastBlockN, blockIndex};
    updateGraphMetadata(lastBlock, blockUpdates);
    augmentedMetadata_->push_back(lastBlock);

    AD_CORRECTNESS_CHECK(
        CompressedBlockMetadata::checkInvariantsForSortedBlocks(
            *augmentedMetadata_));
  }
}

// ____________________________________________________________________________
std::ostream& operator<<(std::ostream& os, const LocatedTriples& lts) {
  os << "{ ";
  ql::ranges::copy(lts, std::ostream_iterator<LocatedTriple>(os, " "));
  os << "}";
  return os;
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
    return ad_utility::contains(lt, locatedTriple);
  };

  return ql::ranges::any_of(map_, [&blockContains](auto& indexAndBlock) {
    const auto& [index, block] = indexAndBlock;
    return blockContains(block, index);
  });
}
