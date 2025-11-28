// Copyright 2018 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_INDEX_PERMUTATION_H
#define QLEVER_SRC_INDEX_PERMUTATION_H

#include <array>
#include <string>

#include "global/Constants.h"
#include "index/CompressedRelation.h"
#include "index/IndexMetaData.h"
#include "index/KeyOrder.h"
#include "parser/data/LimitOffsetClause.h"
#include "util/CancellationHandle.h"
#include "util/File.h"
#include "util/Log.h"

// Forward declaration of `IdTable`
class IdTable;
// Forward declaration of `LocatedTriplesPerBlock`
class LocatedTriplesPerBlock;
class SharedLocatedTriplesSnapshot;
struct LocatedTriplesSnapshot;

// Helper class to store static properties of the different permutations to
// avoid code duplication.
class Permutation {
 public:
  using KeyOrder = qlever::KeyOrder;
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
  static constexpr auto ALL = {Enum::PSO, Enum::POS, Enum::SPO,
                               Enum::SOP, Enum::OPS, Enum::OSP};

  using MetaData = IndexMetaDataMmapView;
  using Allocator = ad_utility::AllocatorWithLimit<Id>;
  using ColumnIndicesRef = CompressedRelationReader::ColumnIndicesRef;
  using ColumnIndices = CompressedRelationReader::ColumnIndices;
  using CancellationHandle = ad_utility::SharedCancellationHandle;
  using ScanSpecAndBlocks = CompressedRelationReader::ScanSpecAndBlocks;

  // Convert a permutation to the corresponding string, etc. `PSO` is converted
  // to "PSO".
  static std::string_view toString(Enum permutation);

  // Convert a permutation to the corresponding permutation of [0, 1, 2], etc.
  // `PSO` is converted to [1, 0, 2].
  static KeyOrder toKeyOrder(Enum permutation);

  explicit Permutation(Enum permutation, Allocator allocator);

  // everything that has to be done when reading an index from disk
  void loadFromDisk(const std::string& onDiskBase,
                    std::function<bool(Id)> isInternalId,
                    bool loadAdditional = false);

  // For a given ID for the col0, retrieve all IDs of the col1 and col2.
  // If `col1Id` is specified, only the col2 is returned for triples that
  // additionally have the specified col1. .This is just a thin wrapper around
  // `CompressedRelationMetaData::scan`.
  IdTable scan(const ScanSpecAndBlocks& scanSpecAndBlocks,
               ColumnIndicesRef additionalColumns,
               const CancellationHandle& cancellationHandle,
               const LocatedTriplesSnapshot& locatedTriplesSnapshot,
               const LimitOffsetClause& limitOffset = {}) const;

  // For a given relation, determine the `col1Id`s and their counts. This is
  // used for `computeGroupByObjectWithCount`. The `col0Id` must have metadata
  // in `meta_`.
  IdTable getDistinctCol1IdsAndCounts(
      Id col0Id, const CancellationHandle& cancellationHandle,
      const LocatedTriplesSnapshot& locatedTriplesSnapshot,
      const LimitOffsetClause& limitOffset) const;

  IdTable getDistinctCol0IdsAndCounts(
      const CancellationHandle& cancellationHandle,
      const LocatedTriplesSnapshot& locatedTriplesSnapshot,
      const LimitOffsetClause& limitOffset) const;

  // Typedef to propagate the `MetadataAndblocks` and `IdTableGenerator` type.
  using MetadataAndBlocks =
      CompressedRelationReader::ScanSpecAndBlocksAndBounds;

  // The function `lazyScan` is similar to `scan` (see above) with
  // the following differences:
  // - The result is returned as a lazy generator of blocks.
  // - The join-specific prefiltered block metadata must be given manually. It
  //   can be obtained via the `getMetadataAndBlocks` function below
  //   and then be prefiltered. The blocks must be given in ascending order
  //   and must only contain blocks that contain the given `col0Id` (combined
  //   with the `col1Id` if specified), else the behavior is
  //   undefined.
  // - In all other cases, the block metadata (optionally generally prefiltered
  //   via `PrefilterExpression`) is already contained as `BlockMetadataRanges`
  //   in `ScanSpecAndBlocks`. The `BlockMetadatRanges` of the
  //   `ScanSpecAndBlocks` are ignored for scanning if `optBlocks` contains the
  //   join-specific prefiltered block metadata.
  // TODO<joka921> We should only communicate this interface via the
  // `ScanSpecAndBlocksAndBounds` class and make this a strong class that always
  // maintains its invariants.
  CompressedRelationReader::IdTableGeneratorInputRange lazyScan(
      const ScanSpecAndBlocks& scanSpecAndBlocks,
      std::optional<std::vector<CompressedBlockMetadata>> optBlocks,
      ColumnIndicesRef additionalColumns,
      const CancellationHandle& cancellationHandle,
      const LocatedTriplesSnapshot& locatedTriplesSnapshot,
      const LimitOffsetClause& limitOffset = {}) const;

  // Returns the corresponding `CompressedRelationReader::ScanSpecAndBlocks`
  // with relevant `BlockMetadataRanges`.
  ScanSpecAndBlocks getScanSpecAndBlocks(
      const ScanSpecification& scanSpec,
      const LocatedTriplesSnapshot& locatedTriplesSnapshot) const;

  std::optional<CompressedRelationMetadata> getMetadata(
      Id col0Id, const LocatedTriplesSnapshot& locatedTriplesSnapshot) const;

  // Return the metadata for the scan specified by the `scanSpecification`
  // along with the metadata for all the blocks that are relevant for this
  // scan. If there are no matching blocks (meaning that the scan result will
  // be empty) return `nullopt`.
  std::optional<MetadataAndBlocks> getMetadataAndBlocks(
      const ScanSpecAndBlocks& scanSpecAndBlocks,
      const LocatedTriplesSnapshot& locatedTriplesSnapshot) const;

  // Get the exact size of the result of a scan, taking into account the
  // given located triples. This requires an exact location of the delta
  // triples within the respective blocks.
  size_t getResultSizeOfScan(
      const ScanSpecAndBlocks& scanSpecAndBlocks,
      const LocatedTriplesSnapshot& locatedTriplesSnapshot) const;

  // Get a lower and upper bound for the size of the result of a scan, taking
  // into account the given `deltaTriples`. For this call, it is enough that
  // each delta triple know to which block it belongs.
  std::pair<size_t, size_t> getSizeEstimateForScan(
      const ScanSpecAndBlocks& scanSpecAndBlocks,
      const LocatedTriplesSnapshot& locatedTriplesSnapshot) const;

  // _______________________________________________________
  void setKbName(const std::string& name) { meta_.setName(name); }

  // _______________________________________________________
  const std::string& getKbName() const { return meta_.getName(); }

  // _______________________________________________________
  const std::string& readableName() const { return readableName_; }

  // _______________________________________________________
  const std::string& onDiskBase() const { return onDiskBase_; }

  // _______________________________________________________
  const std::string& fileSuffix() const { return fileSuffix_; }

  // _______________________________________________________
  const KeyOrder& keyOrder() const { return keyOrder_; };

  // _______________________________________________________
  const bool& isLoaded() const { return isLoaded_; }

  // _______________________________________________________
  const MetaData& metaData() const { return meta_; }

  // _______________________________________________________
  const Permutation& getActualPermutation(const ScanSpecification& spec) const;
  const Permutation& getActualPermutation(Id id) const;

  // From the given snapshot, get the located triples for this permutation.
  const LocatedTriplesPerBlock& getLocatedTriplesForPermutation(
      const LocatedTriplesSnapshot& locatedTriplesSnapshot) const;

  // From the given snapshot, get the augmented block metadata for this
  // permutation.
  BlockMetadataRanges getAugmentedMetadataForPermutation(
      const LocatedTriplesSnapshot& locatedTriplesSnapshot) const;

  const CompressedRelationReader& reader() const { return reader_.value(); }

  Enum permutation() const { return permutation_; }

 private:
  // The base filename of the permutation without the suffix below
  std::string onDiskBase_;
  // Readable name for this permutation, e.g., `POS`.
  std::string readableName_;
  // File name suffix for this permutation, e.g., `.pos`.
  std::string fileSuffix_;
  // The order of the three components (S=0, P=1, O=2) in this permutation,
  // e.g., `{1, 0, 2}` for `PSO`.
  KeyOrder keyOrder_;
  // The metadata for this permutation.
  MetaData meta_;

  // This member is `optional` because we initialize it in a deferred way in
  // the `loadFromDisk` method.
  std::optional<CompressedRelationReader> reader_;
  Allocator allocator_;

  bool isLoaded_ = false;

  Enum permutation_;
  std::unique_ptr<Permutation> internalPermutation_ = nullptr;

  std::function<bool(Id)> isInternalId_;

  bool isInternalPermutation_ = false;
};

#endif  // QLEVER_SRC_INDEX_PERMUTATION_H
