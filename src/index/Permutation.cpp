// Copyright 2023 - 2026 The QLever Authors, in particular:
//
// 2023 - 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/Permutation.h"

#include <absl/strings/str_cat.h>

#include "engine/VariableToColumnMap.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/DeltaTriples.h"
#include "util/StringUtils.h"

// _____________________________________________________________________
Permutation::Permutation(Enum permutation, Allocator allocator,
                         std::optional<std::string> readableName)
    : readableName_{std::move(readableName)
                        .value_or(std::string{toString(permutation)})},
      fileSuffix_(
          absl::StrCat(".", ad_utility::utf8ToLower(toString(permutation)))),
      keyOrder_(toKeyOrder(permutation)),
      allocator_{std::move(allocator)},
      permutation_{permutation} {}

// _____________________________________________________________________
CompressedRelationReader::ScanSpecAndBlocks Permutation::getScanSpecAndBlocks(
    const ScanSpecification& scanSpec,
    const LocatedTriplesState& locatedTriplesState) const {
  return {scanSpec, BlockMetadataRanges(getAugmentedMetadataForPermutation(
                        locatedTriplesState))};
}

// _____________________________________________________________________
void Permutation::loadFromDisk(
    const std::string& onDiskBase, bool loadInternalPermutation,
    Type permutationType,
    ad_utility::HashSet<ColumnIndex> possiblyUndefinedColumns) {
  onDiskBase_ = onDiskBase;
  permutationType_ = permutationType;
  if (loadInternalPermutation) {
    AD_CONTRACT_CHECK(permutationType == Type::NORMAL);
    internalPermutation_ =
        std::make_unique<Permutation>(permutation_, allocator_);
    internalPermutation_->loadFromDisk(
        absl::StrCat(onDiskBase, QLEVER_INTERNAL_INDEX_INFIX), false);
    internalPermutation_->permutationType_ = Type::INTERNAL;
  }
  if constexpr (MetaData::isMmapBased_) {
    meta_.setup(onDiskBase + ".index" + fileSuffix_ + MMAP_FILE_SUFFIX,
                ad_utility::ReuseTag(), ad_utility::AccessPattern::Random);
  }
  possiblyUndefinedColumns_ = std::move(possiblyUndefinedColumns);
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
  // Materialized views never use graph post-processing, while normal and
  // internal permutations always use it.
  bool useGraphPostProcessing = permutationType != Type::MATERIALIZED_VIEW;
  reader_.emplace(allocator_, std::move(file), useGraphPostProcessing);
  AD_LOG_INFO << "Registered " << readableName_
              << " permutation: " << meta_.statistics() << std::endl;
  isLoaded_ = true;
}

// _____________________________________________________________________________
void Permutation::setOriginalMetadataForDeltaTriples(
    DeltaTriples& deltaTriples) const {
  deltaTriples.setOriginalMetadata(permutation(), metaData().blockDataShared(),
                                   permutationType_ == Type::INTERNAL);
  if (internalPermutation_ != nullptr) {
    internalPermutation().setOriginalMetadataForDeltaTriples(deltaTriples);
  }
}

// _____________________________________________________________________
IdTable Permutation::scan(const ScanSpecAndBlocks& scanSpecAndBlocks,
                          ColumnIndicesRef additionalColumns,
                          const CancellationHandle& cancellationHandle,
                          const LocatedTriplesState& locatedTriplesState,
                          const LimitOffsetClause& limitOffset) const {
  if (!isLoaded_) {
    throw std::runtime_error("This query requires the permutation " +
                             readableName_ + ", which was not loaded");
  }
  return reader().scan(scanSpecAndBlocks, additionalColumns, cancellationHandle,
                       getLocatedTriplesForPermutation(locatedTriplesState),
                       limitOffset);
}

// _____________________________________________________________________
size_t Permutation::getResultSizeOfScan(
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    const LocatedTriplesState& locatedTriplesState) const {
  return reader().getResultSizeOfScan(
      scanSpecAndBlocks, getLocatedTriplesForPermutation(locatedTriplesState));
}

// _____________________________________________________________________
std::pair<size_t, size_t> Permutation::getSizeEstimateForScan(
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    const LocatedTriplesState& locatedTriplesState) const {
  return reader().getSizeEstimateForScan(
      scanSpecAndBlocks, getLocatedTriplesForPermutation(locatedTriplesState));
}

// ____________________________________________________________________________
IdTable Permutation::getDistinctCol1IdsAndCounts(
    Id col0Id, const CancellationHandle& cancellationHandle,
    const LocatedTriplesState& locatedTriplesState,
    const LimitOffsetClause& limitOffset) const {
  return reader().getDistinctCol1IdsAndCounts(
      getScanSpecAndBlocks(
          ScanSpecification{col0Id, std::nullopt, std::nullopt},
          locatedTriplesState),
      cancellationHandle, getLocatedTriplesForPermutation(locatedTriplesState),
      limitOffset);
}

// ____________________________________________________________________________
IdTable Permutation::getDistinctCol0IdsAndCounts(
    const CancellationHandle& cancellationHandle,
    const LocatedTriplesState& locatedTriplesState,
    const LimitOffsetClause& limitOffset) const {
  ScanSpecification scanSpec{std::nullopt, std::nullopt, std::nullopt};
  return reader().getDistinctCol0IdsAndCounts(
      getScanSpecAndBlocks(scanSpec, locatedTriplesState), cancellationHandle,
      getLocatedTriplesForPermutation(locatedTriplesState), limitOffset);
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
    Id col0Id, const LocatedTriplesState& locatedTriplesState) const {
  if (meta_.col0IdExists(col0Id)) {
    return meta_.getMetaData(col0Id);
  }
  return reader().getMetadataForSmallRelation(
      getScanSpecAndBlocks(
          ScanSpecification{col0Id, std::nullopt, std::nullopt},
          locatedTriplesState),
      col0Id, getLocatedTriplesForPermutation(locatedTriplesState));
}

// _____________________________________________________________________
std::optional<Permutation::MetadataAndBlocks> Permutation::getMetadataAndBlocks(
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    const LocatedTriplesState& locatedTriplesState) const {
  auto firstAndLastTriple = reader().getFirstAndLastTripleIgnoringGraph(
      scanSpecAndBlocks, getLocatedTriplesForPermutation(locatedTriplesState));
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
    const LocatedTriplesState& locatedTriplesState,
    const LimitOffsetClause& limitOffset) const {
  ColumnIndices columns{additionalColumns.begin(), additionalColumns.end()};
  if (!optBlocks.has_value()) {
    optBlocks = CompressedRelationReader::convertBlockMetadataRangesToVector(
        scanSpecAndBlocks.blockMetadata_);
  }
  return reader().lazyScan(
      scanSpecAndBlocks.scanSpec_, std::move(optBlocks.value()),
      std::move(columns), cancellationHandle,
      getLocatedTriplesForPermutation(locatedTriplesState), limitOffset);
}

// ______________________________________________________________________
const LocatedTriplesPerBlock& Permutation::getLocatedTriplesForPermutation(
    const LocatedTriplesState& locatedTriplesState) const {
  return permutationType_ == Type::INTERNAL
             ? locatedTriplesState.getLocatedTriplesForPermutation<true>(
                   permutation_)
             : locatedTriplesState.getLocatedTriplesForPermutation<false>(
                   permutation_);
}

// ______________________________________________________________________
BlockMetadataRanges Permutation::getAugmentedMetadataForPermutation(
    const LocatedTriplesState& locatedTriplesState) const {
  BlockMetadataSpan blocks(getLocatedTriplesForPermutation(locatedTriplesState)
                               .getAugmentedMetadata());
  return {{blocks.begin(), blocks.end()}};
}

// ______________________________________________________________________
const Permutation& Permutation::internalPermutation() const {
  AD_CONTRACT_CHECK(internalPermutation_ != nullptr);
  return *internalPermutation_;
}

// ______________________________________________________________________
void Permutation::setMaterializedView(
    std::weak_ptr<const MaterializedView> view) {
  AD_CONTRACT_CHECK(permutationType_ == Type::MATERIALIZED_VIEW);
  materializedView_ = std::move(view);
}

// ______________________________________________________________________
std::shared_ptr<const MaterializedView> Permutation::materializedView() const {
  return materializedView_.lock();
}

// ______________________________________________________________________
ColumnIndexAndTypeInfo::UndefStatus Permutation::getColumnUndefStatus(
    ColumnIndex col) const {
  return possiblyUndefinedColumns_.contains(col)
             ? ColumnIndexAndTypeInfo::UndefStatus::PossiblyUndefined
             : ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined;
}
