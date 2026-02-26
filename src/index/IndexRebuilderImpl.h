//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_INDEXREBUILDERIMPL_H
#define QLEVER_SRC_INDEX_INDEXREBUILDERIMPL_H

#include <cstdint>
#include <tuple>
#include <utility>
#include <vector>

#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "index/IndexRebuilder.h"
#include "util/CancellationHandle.h"
#include "util/HashMap.h"
#include "util/InputRangeUtils.h"

namespace qlever::indexRebuilder {

using OwnedBlocksEntry =
    ad_utility::BlankNodeManager::LocalBlankNodeManager::OwnedBlocksEntry;
using OwnedBlocks = std::vector<OwnedBlocksEntry>;
using InsertionPositions = std::vector<VocabIndex>;
using LocalVocabMapping = ad_utility::HashMap<Id::T, Id>;
using BlankNodeBlocks = std::vector<uint64_t>;

// Write a new vocabulary that contains all words from `vocab` plus all
// entries in `entries`. Returns a pair consisting of a vector insertion
// positions (the `VocabIndex` of the `LocalVocabEntry`s position in the old
// `vocab`) and a mapping from old local vocab `Id`s bit representation (for
// cheaper hash functions) to new vocab `Id`s.
std::tuple<InsertionPositions, LocalVocabMapping> materializeLocalVocab(
    const std::vector<LocalVocabIndex>& entries, const Index::Vocab& vocab,
    const std::string& newIndexName);

// Turn a vector of `OwnedBlocksEntry`s into a vector of `uint64_t`s
// representing the block ids of the generated blocks.
BlankNodeBlocks flattenBlankNodeBlocks(const OwnedBlocks& ownedBlocks);

// Map old vocab `Id`s to new vocab `Id`s according to the given
// `insertionPositions`. This is the  most performance critical code of the
// rebuild.
Id remapVocabId(Id original, const InsertionPositions& insertionPositions);

// Remaps a blank node `Id` to another blank node `Id` to reduce the gaps in the
// id space left by random allocation of blank node ids.
Id remapBlankNodeId(Id original, const BlankNodeBlocks& blankNodeBlocks,
                    uint64_t minBlankNodeIndex);

// Create a copy of the given `permutation` scanned according to `scanSpec`,
// where all local vocab `Id`s are remapped according to `localVocabMapping`
// and all vocab `Id`s are remapped according to `insertInfo` to create a new
// index where all of these values are all vocab `Id`s in the new vocabulary.
ad_utility::InputRangeTypeErased<IdTableStatic<0>> readIndexAndRemap(
    const Permutation& permutation,
    const BlockMetadataRanges& blockMetadataRanges,
    const LocatedTriplesSharedState& locatedTriplesSharedState,
    const LocalVocabMapping& localVocabMapping,
    const InsertionPositions& insertionPositions,
    const BlankNodeBlocks& blankNodeBlocks, uint64_t minBlankNodeIndex,
    const ad_utility::SharedCancellationHandle& cancellationHandle,
    ql::span<const ColumnIndex> additionalColumns);

// Get the number of columns in the given `blockMetadataRanges`. If this cannot
// be determined, return 4 as a "safe" default, representing subject + predicate
// + object + graph. Additional columns other than graph are only used for
// patterns, which are currently not updated for index rebuilds. So it doesn't
// matter if the columns are actually missing, or present but filled with
// `Id::makeUndefined()`.
size_t getNumColumns(const BlockMetadataRanges& blockMetadataRanges);

// Create a `boost::asio::awaitable<void>` that writes a pair of new
// permutations according to the settings of `newIndex`, based on the data of
// the current index.
boost::asio::awaitable<void> createPermutationWriterTask(
    IndexImpl& newIndex, const Permutation& permutationA,
    const Permutation& permutationB, bool isInternal,
    const LocatedTriplesSharedState& locatedTriplesSharedState,
    const LocalVocabMapping& localVocabMapping,
    const InsertionPositions& insertionPositions,
    const BlankNodeBlocks& blankNodeBlocks, uint64_t minBlankNodeIndex,
    const ad_utility::SharedCancellationHandle& cancellationHandle);

// Analyze how many columns the new permutation will have and which additional
// columns it will have based on the given `blockMetadataRanges`. The number of
// columns is determined by the number of offsets in the metadata of the first
// block. The additional columns are determined by the number of columns and
// just filled in increasing order starting from `ADDITIONAL_COLUMN_GRAPH_ID`.
std::pair<size_t, std::vector<ColumnIndex>>
getNumberOfColumnsAndAdditionalColumns(
    const BlockMetadataRanges& blockMetadataRanges);

}  // namespace qlever::indexRebuilder

#endif  // QLEVER_SRC_INDEX_INDEXREBUILDERIMPL_H
