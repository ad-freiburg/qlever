//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/Permutation.h"

#include "absl/strings/str_cat.h"
#include "util/StringUtils.h"

// _____________________________________________________________________
Permutation::Permutation(Enum permutation, Allocator allocator)
    : readableName_(toString(permutation)),
      fileSuffix_(absl::StrCat(".", ad_utility::utf8ToLower(readableName_))),
      keyOrder_(toKeyOrder(permutation)),
      allocator_{std::move(allocator)} {}

// _____________________________________________________________________
void Permutation::loadFromDisk(const std::string& onDiskBase) {
  if constexpr (MetaData::_isMmapBased) {
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
  LOG(INFO) << "Registered " << readableName_
            << " permutation: " << meta_.statistics() << std::endl;
  isLoaded_ = true;
}

// _____________________________________________________________________
IdTable Permutation::scan(
    Id col0Id, std::optional<Id> col1Id, ColumnIndicesRef additionalColumns,
    ad_utility::SharedCancellationHandle cancellationHandle) const {
  if (!isLoaded_) {
    throw std::runtime_error("This query requires the permutation " +
                             readableName_ + ", which was not loaded");
  }

  const auto& metaData = getMetadata(col0Id);
  if (!metaData.has_value()) {
    size_t numColumns = col1Id.has_value() ? 1 : 2;
    cancellationHandle->throwIfCancelled();
    return IdTable{numColumns, reader().allocator()};
  }

  return reader().scan(metaData.value(), col1Id, meta_.blockData(),
                       additionalColumns, std::move(cancellationHandle));
}

// _____________________________________________________________________
size_t Permutation::getResultSizeOfScan(Id col0Id, Id col1Id) const {
  const auto& metaData = getMetadata(col0Id);
  if (!metaData.has_value()) {
    return 0;
  }

  return reader().getResultSizeOfScan(metaData.value(), col1Id,
                                      meta_.blockData());
}

// ____________________________________________________________________________
IdTable Permutation::getDistinctCol1IdsAndCounts(
    Id col0Id, ad_utility::SharedCancellationHandle cancellationHandle) const {
  // TODO: It's a recurring pattern here and in other methods in this file,
  // that it is first checked whether the `col0Id` exists in the metadata and
  // subsequently the metadata is retrieved. Shoulnd't we avoid this because
  // each of these is a binary search on disk, so not for free?
  const auto& relationMetadata = getMetadata(col0Id);
  AD_CONTRACT_CHECK(relationMetadata.has_value());
  const auto& allBlocksMetadata = meta_.blockData();
  return reader().getDistinctCol1IdsAndCounts(
      relationMetadata.value(), allBlocksMetadata, cancellationHandle);
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

std::optional<CompressedRelationMetadata> Permutation::getMetadata(
    Id col0Id) const {
  if (meta_.col0IdExists(col0Id)) {
    return meta_.getMetaData(col0Id);
  }
  return reader().getMetadataForSmallRelation(meta_.blockData(), col0Id);
}
// _____________________________________________________________________
std::optional<Permutation::MetadataAndBlocks> Permutation::getMetadataAndBlocks(
    Id col0Id, std::optional<Id> col1Id) const {
  auto optionalMetadata = getMetadata(col0Id);
  if (!optionalMetadata.has_value()) {
    return std::nullopt;
  }

  const auto& metadata = optionalMetadata.value();

  MetadataAndBlocks result{metadata,
                           CompressedRelationReader::getBlocksFromMetadata(
                               metadata, col1Id, meta_.blockData()),
                           col1Id, std::nullopt};

  result.firstAndLastTriple_ = reader().getFirstAndLastTriple(result);
  return result;
}

// _____________________________________________________________________
Permutation::IdTableGenerator Permutation::lazyScan(
    Id col0Id, std::optional<Id> col1Id,
    std::optional<std::vector<CompressedBlockMetadata>> blocks,
    ColumnIndicesRef additionalColumns,
    ad_utility::SharedCancellationHandle cancellationHandle) const {
  auto relationMetadata = getMetadata(col0Id);
  if (!relationMetadata.has_value()) {
    return {};
  }
  if (!blocks.has_value()) {
    auto blockSpan = CompressedRelationReader::getBlocksFromMetadata(
        relationMetadata.value(), col1Id, meta_.blockData());
    blocks = std::vector(blockSpan.begin(), blockSpan.end());
  }
  ColumnIndices columns{additionalColumns.begin(), additionalColumns.end()};
  return reader().lazyScan(relationMetadata.value(), col1Id,
                           std::move(blocks.value()), std::move(columns),
                           cancellationHandle);
}
