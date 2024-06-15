//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/Permutation.h"

#include "absl/strings/str_cat.h"
#include "index/LocationTypes.h"
#include "util/StringUtils.h"

// _____________________________________________________________________
Permutation::Permutation(Enum permutation,
                         const LocatedTriplesPerBlock& locatedTriplesPerBlock,
                         Allocator allocator)
    : readableName_(toString(permutation)),
      fileSuffix_(absl::StrCat(".", ad_utility::utf8ToLower(readableName_))),
      keyOrder_(toKeyOrder(permutation)),
      allocator_{std::move(allocator)},
      locatedTriplesPerBlock_{locatedTriplesPerBlock} {}

// _____________________________________________________________________
void Permutation::loadFromDisk(const std::string& onDiskBase) {
  if constexpr (MetaData::isMmapBased_) {
    meta_.setup(onDiskBase + ".index" + fileSuffix_ + MMAP_FILE_SUFFIX,
                ad_utility::ReuseTag(), ad_utility::AccessPattern::Random);
  }
  auto filename = string(onDiskBase + ".index" + fileSuffix_);
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
  reader_.value().enableUpdateUse();
  LOG(INFO) << "Registered " << readableName_
            << " permutation: " << meta_.statistics() << std::endl;
  isLoaded_ = true;
}

// _____________________________________________________________________
IdTable Permutation::scan(const ScanSpecification& scanSpec,
                          ColumnIndicesRef additionalColumns,
                          const CancellationHandle& cancellationHandle) const {
  if (!isLoaded_) {
    throw std::runtime_error("This query requires the permutation " +
                             readableName_ + ", which was not loaded");
  }

  return reader().scan(scanSpec, meta_.blockData(), additionalColumns,
                       cancellationHandle, locatedTriplesPerBlock_, 0UL);
}

// _____________________________________________________________________
size_t Permutation::getResultSizeOfScan(
    const ScanSpecification& scanSpec) const {
  return reader().getResultSizeOfScan(scanSpec, meta_.blockData(),
                                      locatedTriplesPerBlock_);
}

// ____________________________________________________________________________
IdTable Permutation::getDistinctCol1IdsAndCounts(
    Id col0Id, const CancellationHandle& cancellationHandle) const {
  return reader().getDistinctCol1IdsAndCounts(
      col0Id, meta_.blockData(), cancellationHandle, locatedTriplesPerBlock_);
}

// ____________________________________________________________________________
IdTable Permutation::getDistinctCol0IdsAndCounts(
    const CancellationHandle& cancellationHandle) const {
  return reader().getDistinctCol0IdsAndCounts(
      meta_.blockData(), cancellationHandle, locatedTriplesPerBlock_);
}

// _____________________________________________________________________
std::array<size_t, 3> Permutation::toKeyOrder(Permutation::Enum permutation) {
  using enum Permutation::Enum;
  switch (permutation) {
    case POS:
      return {1, 2, 0};
    case PSO:
      return {1, 0, 2};
    case SOP:
      return {0, 2, 1};
    case SPO:
      return {0, 1, 2};
    case OPS:
      return {2, 1, 0};
    case OSP:
      return {2, 0, 1};
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
    Id col0Id) const {
  if (meta_.col0IdExists(col0Id)) {
    return meta_.getMetaData(col0Id);
  }
  return reader().getMetadataForSmallRelation(meta_.blockData(), col0Id,
                                              locatedTriplesPerBlock_);
}

// _____________________________________________________________________
std::optional<Permutation::MetadataAndBlocks> Permutation::getMetadataAndBlocks(
    const ScanSpecification& scanSpec) const {
  MetadataAndBlocks result{scanSpec,
                           std::get<std::span<const CompressedBlockMetadata>>(
                               CompressedRelationReader::getRelevantBlocks(
                                   scanSpec, meta_.blockData())),
                           std::nullopt};

  result.firstAndLastTriple_ =
      reader().getFirstAndLastTriple(result, locatedTriplesPerBlock_);
  return result;
}

// _____________________________________________________________________
Permutation::IdTableGenerator Permutation::lazyScan(
    const ScanSpecification& scanSpec,
    std::optional<std::vector<CompressedBlockMetadata>> blocks,
    ColumnIndicesRef additionalColumns,
    ad_utility::SharedCancellationHandle cancellationHandle) const {
  DisableUpdatesOrBlockOffset offset;
  // TODO<qup42> assumption: block is None <=> the index is scanned;
  //  otherwise virtual/intermediate blocks are scanned
  //  also handle the other case
  if (!blocks.has_value()) {
    auto [blockSpan, beginBlockOffset] =
        CompressedRelationReader::getRelevantBlocks(scanSpec,
                                                    meta_.blockData());
    offset = beginBlockOffset;
    blocks = std::vector(blockSpan.begin(), blockSpan.end());
  } else {
    offset = DisableUpdates{};
  }
  ColumnIndices columns{additionalColumns.begin(), additionalColumns.end()};
  return reader().lazyScan(scanSpec, std::move(blocks.value()),
                           std::move(columns), std::move(cancellationHandle),
                           locatedTriplesPerBlock_, offset);
}
