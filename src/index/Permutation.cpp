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

  if (!meta_.col0IdExists(col0Id)) {
    size_t numColumns = col1Id.has_value() ? 1 : 2;
    cancellationHandle->throwIfCancelled();
    return IdTable{numColumns, reader().allocator()};
  }
  const auto& metaData = meta_.getMetaData(col0Id);

  return reader().scan(metaData, col1Id, meta_.blockData(), additionalColumns,
                       cancellationHandle);
}

// _____________________________________________________________________
size_t Permutation::getResultSizeOfScan(Id col0Id, Id col1Id) const {
  if (!meta_.col0IdExists(col0Id)) {
    return 0;
  }
  const auto& metaData = meta_.getMetaData(col0Id);

  return reader().getResultSizeOfScan(metaData, col1Id, meta_.blockData());
}

// ____________________________________________________________________________
IdTable Permutation::getDistinctCol1IdsAndCounts(Id col0Id) const {
  // TODO: It's a recurring pattern here and in other methods in this file,
  // that it is first checked whether the `col0Id` exists in the metadata and
  // subsequently the metadata is retrieved. Shoulnd't we avoid this because
  // each of these is a binary search on disk, so not for free?
  AD_CONTRACT_CHECK(meta_.col0IdExists(col0Id));
  const auto& relationMetadata = meta_.getMetaData(col0Id);
  const auto& allBlocksMetadata = meta_.blockData();
  return reader().getDistinctCol1IdsAndCounts(relationMetadata,
                                              allBlocksMetadata);
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
std::optional<Permutation::MetadataAndBlocks> Permutation::getMetadataAndBlocks(
    Id col0Id, std::optional<Id> col1Id) const {
  if (!meta_.col0IdExists(col0Id)) {
    return std::nullopt;
  }

  auto metadata = meta_.getMetaData(col0Id);

  MetadataAndBlocks result{meta_.getMetaData(col0Id),
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
  if (!meta_.col0IdExists(col0Id)) {
    return {};
  }
  auto relationMetadata = meta_.getMetaData(col0Id);
  if (!blocks.has_value()) {
    auto blockSpan = CompressedRelationReader::getBlocksFromMetadata(
        relationMetadata, col1Id, meta_.blockData());
    blocks = std::vector(blockSpan.begin(), blockSpan.end());
  }
  ColumnIndices columns{additionalColumns.begin(), additionalColumns.end()};
  return reader().lazyScan(meta_.getMetaData(col0Id), col1Id,
                           std::move(blocks.value()), std::move(columns),
                           cancellationHandle);
}
