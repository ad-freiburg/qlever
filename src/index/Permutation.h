// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)
#pragma once

#include <array>
#include <string>

#include "global/Constants.h"
#include "index/IndexMetaData.h"
#include "util/CancellationHandle.h"
#include "util/File.h"
#include "util/Log.h"

// Forward declaration of `IdTable`
class IdTable;

// Helper class to store static properties of the different permutations to
// avoid code duplication. The first template parameter is a search functor for
// STXXL.
class Permutation {
 public:
  /// Identifiers for the six possible permutations.
  enum struct Enum { PSO, POS, SPO, SOP, OPS, OSP };
  // Unfortunately there is a bug in GCC that doesn't allow use to simply use
  // `using enum`.
  static constexpr auto PSO = Enum::PSO;
  static constexpr auto POS = Enum::POS;
  static constexpr auto SPO = Enum::SPO;
  static constexpr auto SOP = Enum::SOP;
  static constexpr auto OPS = Enum::OPS;
  static constexpr auto OSP = Enum::OSP;

  using MetaData = IndexMetaDataMmapView;
  using Allocator = ad_utility::AllocatorWithLimit<Id>;
  using ColumnIndicesRef = CompressedRelationReader::ColumnIndicesRef;
  using ColumnIndices = CompressedRelationReader::ColumnIndices;

  // Convert a permutation to the corresponding string, etc. `PSO` is converted
  // to "PSO".
  static std::string_view toString(Enum permutation);

  // Convert a permutation to the corresponding permutation of [0, 1, 2], etc.
  // `PSO` is converted to [1, 0, 2].
  static std::array<size_t, 3> toKeyOrder(Enum permutation);

  explicit Permutation(Enum permutation, Allocator allocator);

  // everything that has to be done when reading an index from disk
  void loadFromDisk(const std::string& onDiskBase);

  // For a given ID for the col0, retrieve all IDs of the col1 and col2.
  // If `col1Id` is specified, only the col2 is returned for triples that
  // additionally have the specified col1. .This is just a thin wrapper around
  // `CompressedRelationMetaData::scan`.
  IdTable scan(Id col0Id, std::optional<Id> col1Id,
               ColumnIndicesRef additionalColumns,
               ad_utility::SharedCancellationHandle cancellationHandle) const;

  // Typedef to propagate the `MetadataAndblocks` and `IdTableGenerator` type.
  using MetadataAndBlocks = CompressedRelationReader::MetadataAndBlocks;

  using IdTableGenerator = CompressedRelationReader::IdTableGenerator;

  // The function `lazyScan` is similar to `scan` (see above) with
  // the following differences:
  // - The result is returned as a lazy generator of blocks.
  // - The block metadata must be given manually. It can be obtained via the
  // `getMetadataAndBlocks` function below
  //   and then be prefiltered. The blocks must be given in ascending order
  //   and must only contain blocks that contain the given `col0Id` (combined
  //   with the `col1Id` if specified), else the behavior is
  //   undefined.
  // TODO<joka921> We should only communicate this interface via the
  // `MetadataAndBlocks` class and make this a strong class that always
  // maintains its invariants.
  IdTableGenerator lazyScan(
      Id col0Id, std::optional<Id> col1Id,
      std::optional<std::vector<CompressedBlockMetadata>> blocks,
      ColumnIndicesRef additionalColumns,
      ad_utility::SharedCancellationHandle cancellationHandle) const;

  // Return the metadata for the relation specified by the `col0Id`
  // along with the metadata for all the blocks that contain this relation (also
  // prefiltered by the `col1Id` if specified). If the `col0Id` does not exist
  // in this permutation, `nullopt` is returned.
  std::optional<MetadataAndBlocks> getMetadataAndBlocks(
      Id col0Id, std::optional<Id> col1Id) const;

  /// Similar to the previous `scan` function, but only get the size of the
  /// result
  size_t getResultSizeOfScan(Id col0Id, Id col1Id) const;

  // _______________________________________________________
  void setKbName(const string& name) { meta_.setName(name); }

  const CompressedRelationReader& reader() const { return reader_.value(); }

  // for Log output, e.g. "POS"
  const std::string readableName_;
  // e.g. ".pos"
  const std::string fileSuffix_;
  // order of the 3 keys S(0), P(1), and O(2) for which this permutation is
  // sorted, for example {1, 0, 2} for PSO.
  const array<size_t, 3> keyOrder_;

  const MetaData& metaData() const { return meta_; }
  MetaData meta_;

  // This member is `optional` because we initialize it in a deferred way in the
  // `loadFromDisk` method.
  std::optional<CompressedRelationReader> reader_;
  Allocator allocator_;

  bool isLoaded_ = false;
};
