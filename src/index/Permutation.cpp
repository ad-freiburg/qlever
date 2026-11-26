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
#include "global/FileSuffixConstants.h"
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
void Permutation::throwIfNotLoaded() const {
  if (!isLoaded_) {
    throw std::runtime_error("This query requires the permutation " +
                             readableName_ + ", which was not loaded");
  }
}

// _____________________________________________________________________
CompressedRelationReader::ScanSpecAndBlocks Permutation::getScanSpecAndBlocks(
    const ScanSpecification& scanSpec,
    const LocatedTriplesState& locatedTriplesState) const {
  throwIfNotLoaded();
  return {scanSpec, BlockMetadataRanges(getAugmentedMetadataForPermutation(
                        locatedTriplesState))};
}

// _____________________________________________________________________
void Permutation::loadFromDisk(
    const std::string& onDiskBase, bool loadInternalPermutation,
    Type permutationType,
    ad_utility::HashSet<ColumnIndex> possiblyUndefinedColumns,
    bool logRegistration) {
  onDiskBase_ = onDiskBase;
  permutationType_ = permutationType;
  if (loadInternalPermutation) {
    AD_CONTRACT_CHECK(permutationType == Type::NORMAL);
    internalPermutation_ =
        std::make_unique<Permutation>(permutation_, allocator_);
    internalPermutation_->loadFromDisk(
        absl::StrCat(onDiskBase, QLEVER_INTERNAL_INDEX_INFIX), false,
        Type::NORMAL, {}, logRegistration);
    internalPermutation_->permutationType_ = Type::INTERNAL;
  }
  possiblyUndefinedColumns_ = std::move(possiblyUndefinedColumns);
  auto filename = absl::StrCat(onDiskBase, PERMUTATION_FILE_INFIX, fileSuffix_);
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
  ad_utility::File metaFile{filename + META_FILE_SUFFIX, "r"};
  meta_.readFromFile(file, metaFile);
  // Materialized views never use graph post-processing, and neither do the
  // permutations that don't have the graph as their last column (for them,
  // the fourth column is an ordinary key column, see `IndexScan`). Normal and
  // internal permutations with the graph last always use it.
  bool useGraphPostProcessing =
      permutationType != Type::MATERIALIZED_VIEW && hasGraphLast(permutation_);
  reader_.emplace(allocator_, std::move(file), useGraphPostProcessing);
  if (logRegistration) {
    AD_LOG_INFO << "Registered " << readableName_
                << " permutation: " << meta_.statistics() << std::endl;
  }
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
                          qlever::Executor executor,
                          const LimitOffsetClause& limitOffset,
                          std::optional<ColumnIndices> columnSubset) const {
  throwIfNotLoaded();
  return reader().scan(scanSpecAndBlocks, additionalColumns, cancellationHandle,
                       getLocatedTriplesForPermutation(locatedTriplesState),
                       std::move(executor), limitOffset,
                       std::move(columnSubset));
}

// _____________________________________________________________________
size_t Permutation::getResultSizeOfScan(
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    const LocatedTriplesState& locatedTriplesState,
    const CancellationHandle& cancellationHandle) const {
  return reader().getResultSizeOfScan(
      scanSpecAndBlocks, getLocatedTriplesForPermutation(locatedTriplesState),
      cancellationHandle);
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
  return reader().getDistinctColIdsAndCounts(
      1,
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
  return reader().getDistinctColIdsAndCounts(
      0, getScanSpecAndBlocks(scanSpec, locatedTriplesState),
      cancellationHandle, getLocatedTriplesForPermutation(locatedTriplesState),
      limitOffset);
}

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
// ____________________________________________________________________________
cppcoro::generator<IdTable, CompressedRelationReader::LazyScanMetadata>
Permutation::getDistinctCol0Ids(const ScanSpecification& scanSpec,
                                bool addGraphColumn,
                                std::optional<std::vector<Id>> idFilter,
                                const CancellationHandle& cancellationHandle,
                                const LocatedTriplesState& locatedTriplesState,
                                qlever::Executor executor) const {
  return reader().getDistinctCol0Ids(
      getScanSpecAndBlocks(scanSpec, locatedTriplesState), addGraphColumn,
      std::move(idFilter), cancellationHandle,
      getLocatedTriplesForPermutation(locatedTriplesState),
      std::move(executor));
}
#endif

// _____________________________________________________________________
auto Permutation::toKeyOrder(Permutation::Enum permutation) -> KeyOrder {
  // The name of a permutation lists its columns in order, and a missing
  // fourth letter means that the graph is the last column.
  std::string_view name = toString(permutation);
  std::array<KeyOrder::T, KeyOrder::NumKeys> keys{};
  keys[3] = 3;
  for (size_t i = 0; i < name.size(); ++i) {
    switch (name[i]) {
      case 'S':
        keys[i] = 0;
        break;
      case 'P':
        keys[i] = 1;
        break;
      case 'O':
        keys[i] = 2;
        break;
      case 'G':
        keys[i] = 3;
        break;
      default:
        AD_FAIL();
    }
  }
  return {keys[0], keys[1], keys[2], keys[3]};
}

// _____________________________________________________________________
Permutation::Enum Permutation::fromKeyOrder(const KeyOrder& keyOrder) {
  for (auto permutation : ALL) {
    if (toKeyOrder(permutation).keys() == keyOrder.keys()) {
      return permutation;
    }
  }
  AD_FAIL();
}

// _____________________________________________________________________
bool Permutation::hasGraphLast(Enum permutation) {
  return toKeyOrder(permutation).keys()[3] == 3;
}

// _____________________________________________________________________
Permutation::Enum Permutation::twin(Enum permutation) {
  auto [a, b, c, d] = toKeyOrder(permutation).keys();
  return fromKeyOrder(KeyOrder{a, c, b, d});
}

// _____________________________________________________________________
std::vector<ql::filesystem::path> Permutation::fileNames(
    Enum permutation, std::string_view onDiskBase) {
  ql::filesystem::path filename =
      absl::StrCat(onDiskBase, PERMUTATION_FILE_INFIX, ".",
                   ad_utility::utf8ToLower(toString(permutation)));
  return {filename, absl::StrCat(filename.string(), META_FILE_SUFFIX)};
}

// _____________________________________________________________________
std::string_view Permutation::toString(Permutation::Enum permutation) {
  static constexpr std::array<std::string_view, 24> names{
      "PSO",  "POS",  "SPO",  "SOP",  "OPS",  "OSP",  "GSPO", "GSOP",
      "GPSO", "GPOS", "GOSP", "GOPS", "SGPO", "SGOP", "PGSO", "PGOS",
      "OGSP", "OGPS", "SPGO", "SOGP", "PSGO", "POGS", "OSGP", "OPGS"};
  auto index = static_cast<size_t>(permutation);
  AD_CONTRACT_CHECK(index < names.size());
  return names[index];
}

// _____________________________________________________________________
Permutation::Enum Permutation::fromString(std::string_view name) {
  std::string upper = ad_utility::utf8ToUpper(name);
  // Strip a trailing graph column, which is implicit in the names of the
  // permutations that have the graph as their last column.
  std::string_view withoutTrailingGraph = upper;
  if (withoutTrailingGraph.size() == 4 && withoutTrailingGraph.back() == 'G') {
    withoutTrailingGraph.remove_suffix(1);
  }
  for (auto permutation : ALL) {
    if (toString(permutation) == upper ||
        toString(permutation) == withoutTrailingGraph) {
      return permutation;
    }
  }
  throw std::runtime_error{absl::StrCat(
      "\"", name,
      "\" is not the name of a permutation. Valid names consist of the "
      "letters S, P, O, and optionally G (e.g. \"SPO\", \"PSOG\", or "
      "\"GSPO\"), each of which must occur exactly once; a missing G means "
      "that the graph is the last column")};
}

// _____________________________________________________________________
std::optional<CompressedRelationMetadata> Permutation::getMetadata(
    Id col0Id, const LocatedTriplesState& locatedTriplesState) const {
  auto optionalMetadata = meta_.getMetaDataIfPresent(col0Id);
  if (optionalMetadata.has_value()) {
    return optionalMetadata.value();
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
    const LocatedTriplesState& locatedTriplesState, qlever::Executor executor,
    const LimitOffsetClause& limitOffset,
    std::optional<ColumnIndices> columnSubset) const {
  return lazyScanImpl(reader(), scanSpecAndBlocks, std::move(optBlocks),
                      additionalColumns, cancellationHandle,
                      locatedTriplesState, std::move(executor), limitOffset,
                      std::move(columnSubset));
}

// _____________________________________________________________________________
CompressedRelationReader::IdTableGeneratorInputRange Permutation::lazyScanImpl(
    const CompressedRelationReader& reader,
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    std::optional<std::vector<CompressedBlockMetadata>> optBlocks,
    ColumnIndicesRef additionalColumns,
    const CancellationHandle& cancellationHandle,
    const LocatedTriplesState& locatedTriplesState, qlever::Executor executor,
    const LimitOffsetClause& limitOffset,
    std::optional<ColumnIndices> columnSubset) const {
  ColumnIndices columns{additionalColumns.begin(), additionalColumns.end()};
  if (!optBlocks.has_value()) {
    optBlocks = CompressedRelationReader::convertBlockMetadataRangesToVector(
        scanSpecAndBlocks.blockMetadata_);
  }
  return reader.lazyScan(
      scanSpecAndBlocks.scanSpec_, std::move(optBlocks.value()),
      std::move(columns), cancellationHandle,
      getLocatedTriplesForPermutation(locatedTriplesState), std::move(executor),
      limitOffset, std::move(columnSubset));
}

// _____________________________________________________________________________
Permutation::LazyScanWithReader Permutation::lazyScanWithUnlimitedReader(
    const ScanSpecAndBlocks& scanSpecAndBlocks,
    ColumnIndicesRef additionalColumns,
    const CancellationHandle& cancellationHandle,
    const LocatedTriplesState& locatedTriplesState,
    qlever::Executor executor) const {
  auto independentReader = std::make_unique<CompressedRelationReader>(
      reader().makeReaderWithReboundAllocator(
          ad_utility::makeUnlimitedAllocator<Id>()));
  auto blocks =
      lazyScanImpl(*independentReader, scanSpecAndBlocks, std::nullopt,
                   additionalColumns, cancellationHandle, locatedTriplesState,
                   std::move(executor), {}, std::nullopt);
  return {std::move(independentReader), std::move(blocks)};
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
  throwIfNotLoaded();
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
