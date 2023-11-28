//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/Permutation.h"

#include "absl/strings/str_cat.h"
#include "util/StringUtils.h"

// _____________________________________________________________________
Permutation::Permutation(Enum permutation, Allocator allocator,
                         HasAdditionalTriples hasAdditionalTriples)
    : readableName_(toString(permutation)),
      fileSuffix_(absl::StrCat(".", ad_utility::utf8ToLower(readableName_))),
      keyOrder_(toKeyOrder(permutation)),
      allocator_{std::move(allocator)} {
  if (hasAdditionalTriples == HasAdditionalTriples::True) {
    additionalPermutation_ = std::make_unique<Permutation>(
        permutation, std::move(allocator), HasAdditionalTriples::False);
  }
}

// _____________________________________________________________________
void Permutation::loadFromDisk(const std::string& onDiskBase,
                               bool onlyLoadAdditional) {
  if (!onlyLoadAdditional) {
    if constexpr (MetaData::_isMmapBased) {
      meta_.setup(onDiskBase + ".index" + fileSuffix_ + MMAP_FILE_SUFFIX,
                  ad_utility::ReuseTag(), ad_utility::AccessPattern::Random);
    }
    auto filename = string(onDiskBase + ".index" + fileSuffix_);
    ad_utility::File file;
    try {
      file.open(filename, "r");
    } catch (const std::runtime_error& e) {
      AD_THROW(
          "Could not open the index file " + filename +
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
  if (additionalPermutation_) {
    additionalPermutation_->loadFromDisk(onDiskBase + ADDITIONAL_TRIPLES_SUFFIX,
                                         false);
  }
}

// _____________________________________________________________________
IdTable Permutation::scan(
    Id col0Id, std::optional<Id> col1Id, ColumnIndices additionalColumns,
    std::shared_ptr<ad_utility::CancellationHandle> cancellationHandle) const {
  if (!isLoaded_) {
    throw std::runtime_error("This query requires the permutation " +
                             readableName_ + ", which was not loaded");
  }

  if (!meta_.col0IdExists(col0Id)) {
    if (additionalPermutation_) {
      return additionalPermutation_->scan(col0Id, col1Id, additionalColumns, std::move(cancellationHandle));
    }
    size_t numColumns = col1Id.has_value() ? 1 : 2;
    return IdTable{numColumns, reader().allocator()};
  }
  const auto& metaData = meta_.getMetaData(col0Id);

  return reader().scan(metaData, col1Id, meta_.blockData(), additionalColumns,
                       cancellationHandle);
}

// _____________________________________________________________________
size_t Permutation::getResultSizeOfScan(Id col0Id,
                                        std::optional<Id> col1Id) const {
  if (!meta_.col0IdExists(col0Id)) {
    if (additionalPermutation_) {
      return additionalPermutation_->getResultSizeOfScan(col0Id, col1Id);
    }
    return 0;
  }
  const auto& metaData = meta_.getMetaData(col0Id);

  // TODO<joka921> should be handled inside the CompressedRelationReader.
  if (!col1Id.has_value()) {
    return metaData.getNofElements();
  }

  return reader().getResultSizeOfScan(metaData, col1Id.value(),
                                     meta_.blockData());
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
    if (additionalPermutation_) {
      return additionalPermutation_->getMetadataAndBlocks(col0Id, col1Id);
    }
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
    ColumnIndices additionalColumns,
    std::shared_ptr<ad_utility::CancellationHandle> cancellationHandle) const {
  if (!meta_.col0IdExists(col0Id)) {
    if (additionalPermutation_) {
      return additionalPermutation_->lazyScan(col0Id, col1Id, std::move(blocks),
                                              additionalColumns, std::move(cancellationHandle));
    }
    return {};
  }
  auto relationMetadata = meta_.getMetaData(col0Id);
  if (!blocks.has_value()) {
    auto blockSpan = CompressedRelationReader::getBlocksFromMetadata(
        relationMetadata, col1Id, meta_.blockData());
    blocks = std::vector(blockSpan.begin(), blockSpan.end());
  }
  OwningColumnIndices owningColumns{additionalColumns.begin(),
                                    additionalColumns.end()};
  return reader().lazyScan(meta_.getMetaData(col0Id), col1Id,
                           std::move(blocks.value()), std::move(owningColumns),
                           cancellationHandle);
}
