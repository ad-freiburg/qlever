//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/Permutation.h"

#include <absl/strings/str_cat.h>

#include "index/ConstantsIndexBuilding.h"
#include "index/DeltaTriples.h"
#include "util/GeneratorConverter.h"
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
  return {scanSpec,
          BlockMetadataRanges(
              getActualPermutation(scanSpec).getAugmentedMetadataForPermutation(
                  locatedTriplesSnapshot))};
}

// _____________________________________________________________________
void Permutation::loadFromDisk(const std::string& onDiskBase,
                               std::function<bool(Id)> isInternalId,
                               bool loadInternalPermutation) {
  isInternalId_ = std::move(isInternalId);
  if (loadInternalPermutation) {
    internalPermutation_ =
        std::make_unique<Permutation>(permutation_, allocator_);
    internalPermutation_->loadFromDisk(
        absl::StrCat(onDiskBase, QLEVER_INTERNAL_INDEX_INFIX), isInternalId_,
        false);
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
  const auto& p = getActualPermutation(scanSpecAndBlocks.scanSpec_);
  return p.reader().scan(
      scanSpecAndBlocks, additionalColumns, cancellationHandle,
      p.getLocatedTriplesForPermutation(locatedTriplesSnapshot), limitOffset);
}

// _____________________________________________________________________
size_t Permutation::getResultSizeOfScan(
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    const LocatedTriplesSnapshot& locatedTriplesSnapshot) const {
  const auto& p = getActualPermutation(scanSpecAndBlocks.scanSpec_);
  return p.reader().getResultSizeOfScan(
      scanSpecAndBlocks,
      p.getLocatedTriplesForPermutation(locatedTriplesSnapshot));
}

// _____________________________________________________________________
std::pair<size_t, size_t> Permutation::getSizeEstimateForScan(
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    const LocatedTriplesSnapshot& locatedTriplesSnapshot) const {
  const auto& p = getActualPermutation(scanSpecAndBlocks.scanSpec_);
  return p.reader().getSizeEstimateForScan(
      scanSpecAndBlocks,
      p.getLocatedTriplesForPermutation(locatedTriplesSnapshot));
}

// ____________________________________________________________________________
IdTable Permutation::getDistinctCol1IdsAndCounts(
    Id col0Id, const CancellationHandle& cancellationHandle,
    const LocatedTriplesSnapshot& locatedTriplesSnapshot) const {
  const auto& p = getActualPermutation(col0Id);
  return p.reader().getDistinctCol1IdsAndCounts(
      getScanSpecAndBlocks(
          ScanSpecification{col0Id, std::nullopt, std::nullopt},
          locatedTriplesSnapshot),
      cancellationHandle,
      p.getLocatedTriplesForPermutation(locatedTriplesSnapshot));
}

// ____________________________________________________________________________
IdTable Permutation::getDistinctCol0IdsAndCounts(
    const CancellationHandle& cancellationHandle,
    const LocatedTriplesSnapshot& locatedTriplesSnapshot) const {
  ScanSpecification scanSpec{std::nullopt, std::nullopt, std::nullopt};
  return reader().getDistinctCol0IdsAndCounts(
      getScanSpecAndBlocks(scanSpec, locatedTriplesSnapshot),
      cancellationHandle,
      getLocatedTriplesForPermutation(locatedTriplesSnapshot));
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
  const auto& p = getActualPermutation(col0Id);
  if (p.meta_.col0IdExists(col0Id)) {
    return p.meta_.getMetaData(col0Id);
  }
  return p.reader().getMetadataForSmallRelation(
      getScanSpecAndBlocks(
          ScanSpecification{col0Id, std::nullopt, std::nullopt},
          locatedTriplesSnapshot),
      col0Id, p.getLocatedTriplesForPermutation(locatedTriplesSnapshot));
}

// _____________________________________________________________________
std::optional<Permutation::MetadataAndBlocks> Permutation::getMetadataAndBlocks(
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    const LocatedTriplesSnapshot& locatedTriplesSnapshot) const {
  const auto& p = getActualPermutation(scanSpecAndBlocks.scanSpec_);
  auto firstAndLastTriple = p.reader().getFirstAndLastTripleIgnoringGraph(
      scanSpecAndBlocks,
      p.getLocatedTriplesForPermutation(locatedTriplesSnapshot));
  if (!firstAndLastTriple.has_value()) {
    return std::nullopt;
  }
  return MetadataAndBlocks{scanSpecAndBlocks,
                           std::move(firstAndLastTriple.value())};
}

// _____________________________________________________________________
Permutation::IdTableGenerator Permutation::lazyScan(
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    std::optional<std::vector<CompressedBlockMetadata>> optBlocks,
    ColumnIndicesRef additionalColumns,
    const CancellationHandle& cancellationHandle,
    const LocatedTriplesSnapshot& locatedTriplesSnapshot,
    const LimitOffsetClause& limitOffset) const {
  const auto& p = getActualPermutation(scanSpecAndBlocks.scanSpec_);
  ColumnIndices columns{additionalColumns.begin(), additionalColumns.end()};
  if (!optBlocks.has_value()) {
    optBlocks = CompressedRelationReader::convertBlockMetadataRangesToVector(
        scanSpecAndBlocks.blockMetadata_);
  }
  auto lazyScan{p.reader().lazyScan(
      scanSpecAndBlocks.scanSpec_, std::move(optBlocks.value()),
      std::move(columns), cancellationHandle,
      p.getLocatedTriplesForPermutation(locatedTriplesSnapshot), limitOffset)};

  return cppcoro::fromInputRange<IdTable,
                                 CompressedRelationReader::LazyScanMetadata>(
      std::move(lazyScan));
}

// ______________________________________________________________________
const Permutation& Permutation::getActualPermutation(
    const ScanSpecification& spec) const {
  auto isInternal = [this](const std::optional<Id>& id) {
    return id.has_value() && isInternalId_(id.value());
  };

  bool isInternalScan = isInternal(spec.col0Id()) ||
                        isInternal(spec.col1Id()) || isInternal(spec.col2Id());

  if (!isInternalScan) {
    return *this;
  }
  AD_CORRECTNESS_CHECK(internalPermutation_ != nullptr, [this]() {
    return absl::StrCat("No internal triples were loaded for the permutation ",
                        readableName_);
  });
  return *internalPermutation_;
}

// ______________________________________________________________________
const Permutation& Permutation::getActualPermutation(Id id) const {
  return getActualPermutation(
      ScanSpecification{id, std::nullopt, std::nullopt});
}

// TODO<joka921> The following two functions always assume that there were no
// updates to the additional triples (which is technically true for now, because
// we never modify the additional triples with the delta triples, because there
// is some functionality missing for this. We have to fix this here and in the
// `DeltaTriples` class.

// ______________________________________________________________________
const LocatedTriplesPerBlock& Permutation::getLocatedTriplesForPermutation(
    const LocatedTriplesSnapshot& locatedTriplesSnapshot) const {
  static const LocatedTriplesSnapshot emptySnapshot{
      {}, LocalVocab{}.getLifetimeExtender(), 0};
  const auto& actualSnapshot =
      isInternalPermutation_ ? emptySnapshot : locatedTriplesSnapshot;
  return actualSnapshot.getLocatedTriplesForPermutation(permutation_);
}

// ______________________________________________________________________
BlockMetadataRanges Permutation::getAugmentedMetadataForPermutation(
    const LocatedTriplesSnapshot& locatedTriplesSnapshot) const {
  BlockMetadataSpan blocks(
      isInternalPermutation_
          ? meta_.blockData()
          : getLocatedTriplesForPermutation(locatedTriplesSnapshot)
                .getAugmentedMetadata());
  return {{blocks.begin(), blocks.end()}};
}
