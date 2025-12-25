//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/Permutation.h"

#include <absl/strings/str_cat.h>

#include "index/ConstantsIndexBuilding.h"
#include "index/DeltaTriples.h"
#include "util/StringUtils.h"

// _____________________________________________________________________
Permutation::Permutation(Enum permutation, Allocator allocator)
    : readableName_(toString(permutation)),
      fileSuffix_(absl::StrCat(".", ad_utility::utf8ToLower(readableName_))),
      keyOrder_(toKeyOrder(permutation)),
      allocator_{std::move(allocator)},
      permutation_{permutation} {}

// _____________________________________________________________________
CompressedRelationReader::ScanSpecAndBlocks Permutation::getScanSpecAndBlocks(
    const ScanSpecification& scanSpec,
    const LocatedTriplesSnapshot& locatedTriplesSnapshot) const {
  return {scanSpec, BlockMetadataRanges(getAugmentedMetadataForPermutation(
                        locatedTriplesSnapshot))};
}

// _____________________________________________________________________
void Permutation::loadFromDisk(const std::string& onDiskBase,
                               bool loadInternalPermutation) {
  onDiskBase_ = onDiskBase;
  if (loadInternalPermutation) {
    internalPermutation_ =
        std::make_unique<Permutation>(permutation_, allocator_);
    internalPermutation_->loadFromDisk(
        absl::StrCat(onDiskBase, QLEVER_INTERNAL_INDEX_INFIX), false);
    internalPermutation_->isInternalPermutation_ = true;
  }
  if constexpr (MetaData::isMmapBased_) {
    meta_.setup(onDiskBase + ".index" + fileSuffix_ + MMAP_FILE_SUFFIX,
                ad_utility::ReuseTag(), ad_utility::AccessPattern::Random);
  }
  auto filename = std::string(onDiskBase + ".index" + fileSuffix_);
  ad_utility::File file;
  try {
    file.open(filename, "r");
  } catch (const std::runtime_error& e) {
    AD_THROW("Could not open the index file " + filename +
             " for reading. Please check that you have read access to "
             "this file. If it does not exist, your index is broken. The error "
             "message was: " +
             e.what());
  }
  meta_.readFromFile(&file);
  reader_.emplace(allocator_, std::move(file));
  AD_LOG_INFO << "Registered " << readableName_
              << " permutation: " << meta_.statistics() << std::endl;
  isLoaded_ = true;
}

// _____________________________________________________________________________
void Permutation::setOriginalMetadataForDeltaTriples(
    DeltaTriples& deltaTriples) const {
  deltaTriples.setOriginalMetadata(permutation(), metaData().blockDataShared(),
                                   isInternalPermutation_);
  if (internalPermutation_ != nullptr) {
    internalPermutation().setOriginalMetadataForDeltaTriples(deltaTriples);
  }
}

// _____________________________________________________________________
IdTable Permutation::scan(const ScanSpecAndBlocks& scanSpecAndBlocks,
                          ColumnIndicesRef additionalColumns,
                          const CancellationHandle& cancellationHandle,
                          const LocatedTriplesSnapshot& locatedTriplesSnapshot,
                          const LimitOffsetClause& limitOffset) const {
  if (!isLoaded_) {
    throw std::runtime_error("This query requires the permutation " +
                             readableName_ + ", which was not loaded");
  }
  return reader().scan(scanSpecAndBlocks, additionalColumns, cancellationHandle,
                       getLocatedTriplesForPermutation(locatedTriplesSnapshot),
                       limitOffset);
}

// _____________________________________________________________________
size_t Permutation::getResultSizeOfScan(
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    const LocatedTriplesSnapshot& locatedTriplesSnapshot) const {
  return reader().getResultSizeOfScan(
      scanSpecAndBlocks,
      getLocatedTriplesForPermutation(locatedTriplesSnapshot));
}

// _____________________________________________________________________
std::pair<size_t, size_t> Permutation::getSizeEstimateForScan(
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    const LocatedTriplesSnapshot& locatedTriplesSnapshot) const {
  return reader().getSizeEstimateForScan(
      scanSpecAndBlocks,
      getLocatedTriplesForPermutation(locatedTriplesSnapshot));
}

// ____________________________________________________________________________
IdTable Permutation::getDistinctCol1IdsAndCounts(
    Id col0Id, const CancellationHandle& cancellationHandle,
    const LocatedTriplesSnapshot& locatedTriplesSnapshot,
    const LimitOffsetClause& limitOffset) const {
  return reader().getDistinctCol1IdsAndCounts(
      getScanSpecAndBlocks(
          ScanSpecification{col0Id, std::nullopt, std::nullopt},
          locatedTriplesSnapshot),
      cancellationHandle,
      getLocatedTriplesForPermutation(locatedTriplesSnapshot), limitOffset);
}

// ____________________________________________________________________________
IdTable Permutation::getDistinctCol0IdsAndCounts(
    const CancellationHandle& cancellationHandle,
    const LocatedTriplesSnapshot& locatedTriplesSnapshot,
    const LimitOffsetClause& limitOffset) const {
  ScanSpecification scanSpec{std::nullopt, std::nullopt, std::nullopt};
  return reader().getDistinctCol0IdsAndCounts(
      getScanSpecAndBlocks(scanSpec, locatedTriplesSnapshot),
      cancellationHandle,
      getLocatedTriplesForPermutation(locatedTriplesSnapshot), limitOffset);
}

// _____________________________________________________________________
auto Permutation::toKeyOrder(Permutation::Enum permutation) -> KeyOrder {
  using enum Permutation::Enum;
  switch (permutation) {
    case POS:
      return {1, 2, 0, 3};
    case PSO:
      return {1, 0, 2, 3};
    case SOP:
      return {0, 2, 1, 3};
    case SPO:
      return {0, 1, 2, 3};
    case OPS:
      return {2, 1, 0, 3};
    case OSP:
      return {2, 0, 1, 3};
  }
  AD_FAIL();
}

// _____________________________________________________________________
std::string_view Permutation::toString(Permutation::Enum permutation) {
  using enum Permutation::Enum;
  switch (permutation) {
    case POS:
      return "POS";
    case PSO:
      return "PSO";
    case SOP:
      return "SOP";
    case SPO:
      return "SPO";
    case OPS:
      return "OPS";
    case OSP:
      return "OSP";
  }
  AD_FAIL();
}

// _____________________________________________________________________
std::optional<CompressedRelationMetadata> Permutation::getMetadata(
    Id col0Id, const LocatedTriplesSnapshot& locatedTriplesSnapshot) const {
  if (meta_.col0IdExists(col0Id)) {
    return meta_.getMetaData(col0Id);
  }
  return reader().getMetadataForSmallRelation(
      getScanSpecAndBlocks(
          ScanSpecification{col0Id, std::nullopt, std::nullopt},
          locatedTriplesSnapshot),
      col0Id, getLocatedTriplesForPermutation(locatedTriplesSnapshot));
}

// _____________________________________________________________________
std::optional<Permutation::MetadataAndBlocks> Permutation::getMetadataAndBlocks(
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    const LocatedTriplesSnapshot& locatedTriplesSnapshot) const {
  auto firstAndLastTriple = reader().getFirstAndLastTripleIgnoringGraph(
      scanSpecAndBlocks,
      getLocatedTriplesForPermutation(locatedTriplesSnapshot));
  if (!firstAndLastTriple.has_value()) {
    return std::nullopt;
  }
  return MetadataAndBlocks{scanSpecAndBlocks,
                           std::move(firstAndLastTriple.value())};
}

// _____________________________________________________________________
CompressedRelationReader::IdTableGeneratorInputRange Permutation::lazyScan(
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    std::optional<std::vector<CompressedBlockMetadata>> optBlocks,
    ColumnIndicesRef additionalColumns,
    const CancellationHandle& cancellationHandle,
    const LocatedTriplesSnapshot& locatedTriplesSnapshot,
    const LimitOffsetClause& limitOffset) const {
  ColumnIndices columns{additionalColumns.begin(), additionalColumns.end()};
  if (!optBlocks.has_value()) {
    optBlocks = CompressedRelationReader::convertBlockMetadataRangesToVector(
        scanSpecAndBlocks.blockMetadata_);
  }
  return reader().lazyScan(
      scanSpecAndBlocks.scanSpec_, std::move(optBlocks.value()),
      std::move(columns), cancellationHandle,
      getLocatedTriplesForPermutation(locatedTriplesSnapshot), limitOffset);
}

// ______________________________________________________________________
const LocatedTriplesPerBlock& Permutation::getLocatedTriplesForPermutation(
    const LocatedTriplesSnapshot& locatedTriplesSnapshot) const {
  return isInternalPermutation_
             ? locatedTriplesSnapshot.getInternalLocatedTriplesForPermutation(
                   permutation_)
             : locatedTriplesSnapshot.getLocatedTriplesForPermutation(
                   permutation_);
}

// ______________________________________________________________________
BlockMetadataRanges Permutation::getAugmentedMetadataForPermutation(
    const LocatedTriplesSnapshot& locatedTriplesSnapshot) const {
  BlockMetadataSpan blocks(
      getLocatedTriplesForPermutation(locatedTriplesSnapshot)
          .getAugmentedMetadata());
  return {{blocks.begin(), blocks.end()}};
}

// ______________________________________________________________________
const Permutation& Permutation::internalPermutation() const {
  AD_CONTRACT_CHECK(internalPermutation_ != nullptr);
  return *internalPermutation_;
}
