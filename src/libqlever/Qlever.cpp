// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "libqlever/Qlever.h"

#include <absl/strings/str_cat.h>

#include <boost/optional.hpp>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string_view>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/MaterializedViews.h"
#include "engine/QueryExecutionContext.h"
#include "global/Constants.h"
#include "index/IndexImpl.h"
#include "index/TextIndexBuilder.h"
#include "libqlever/QleverTypes.h"
#include "parser/SparqlParser.h"
#include "util/Exception.h"
#include "util/http/UrlParser.h"

namespace qlever {

// _____________________________________________________________________________
Qlever::Qlever(const EngineConfig& config)
    : allocator_{ad_utility::AllocatorWithLimit<Id>{
          ad_utility::makeAllocationMemoryLeftThreadsafeObject(
              config.memoryLimit_.value_or(DEFAULT_MEM_FOR_QUERIES)),
          [this](ad_utility::MemorySize numMemoryToAllocate) {
            cache_.makeRoomAsMuchAsPossible(MAKE_ROOM_SLACK_FACTOR *
                                            numMemoryToAllocate);
          }}},
      indexAndViews_{std::make_shared<IndexAndViews>(
          Index{allocator_}, MaterializedViewsManager{})},
      enablePatternTrick_{!config.noPatterns_},
      disableCaching_{config.disableCaching_} {
  // Set runtime parameters relevant for caching and propagate them to the
  // cache.
  globalRuntimeParameters.wlock()->cacheMaxNumEntries_.setOnUpdateAction(
      [this](size_t newValue) { cache_.setMaxNumEntries(newValue); });
  globalRuntimeParameters.wlock()->cacheMaxSize_.setOnUpdateAction(
      [this](ad_utility::MemorySize newValue) { cache_.setMaxSize(newValue); });
  globalRuntimeParameters.wlock()->cacheMaxSizeSingleEntry_.setOnUpdateAction(
      [this](ad_utility::MemorySize newValue) {
        cache_.setMaxSizeSingleEntry(newValue);
      });

  // Grab the freshly constructed `Index` and `MaterializedViewsManager` once.
  // No other thread can observe them yet, so reading the snapshot here is safe.
  // `snapshot` keeps the `shared_ptr`s alive for the references below.
  auto snapshot = indexAndViewsSnapshot();
  auto& [index, materializedViewsManager] = *snapshot;

  // Load the index from disk.
  index.usePatterns() = enablePatternTrick_;
  index.loadAllPermutations() = !config.onlyPsoAndPos_;
  index.doNotLoadPermutations() = config.doNotLoadPermutations_;
  index.createFromOnDiskIndex(config.baseName_, config.persistUpdates_);
  if (config.loadTextIndex_) {
    index.addTextFromOnDiskIndex();
  }

  materializedViewsManager.setOnDiskBase(config.baseName_);

  // Estimate the cost of sorting operations (needed for query planning).
  sortPerformanceEstimator_.computeEstimatesExpensively(
      allocator_, index.numTriples().normalAndInternal_() *
                      PERCENTAGE_OF_TRIPLES_FOR_SORT_ESTIMATE / 100);

  // Preload materialized views as requested by the user.
  for (const auto& viewName : config.preloadMaterializedViews_) {
    try {
      materializedViewsManager.loadView(viewName);
    } catch (const std::exception& ex) {
      AD_LOG_ERROR << "Preloading materialized view '" << viewName
                   << "' failed: " << ex.what() << "." << std::endl;
    }
  }
}

// _____________________________________________________________________________
void Qlever::buildIndex(IndexBuilderConfig config) {
  Index index{ad_utility::makeUnlimitedAllocator<Id>()};

  // Set memory limit and parser buffer size if specified.
  if (config.memoryLimit_.has_value()) {
    index.memoryLimitIndexBuilding() = config.memoryLimit_.value();
  }
  if (config.parserBufferSize_.has_value()) {
    index.parserBufferSize() = config.parserBufferSize_.value();
  }

  // If no text index name was specified, take the part of the wordsfile after
  // the last slash.
  if (config.textIndexName_.empty() && !config.wordsfile_.empty()) {
    config.textIndexName_ =
        ad_utility::getLastPartOfString(config.wordsfile_, '/');
  }

  // Set all other configuration options.
  index.setKbName(config.kbIndexName_);
  index.setTextName(config.textIndexName_);
  index.usePatterns() = !config.noPatterns_;
  index.setOnDiskBase(config.baseName_);
  index.setKeepTempFiles(config.keepTemporaryFiles_);
  index.setSettingsFile(config.settingsFile_);
  index.loadAllPermutations() = !config.onlyPsoAndPos_;
  index.addHasWordTriples() = config.addHasWordTriples_;
  index.getImpl().setVocabularyTypeForIndexBuilding(config.vocabType_);
  index.getImpl().setPrefixesForEncodedValues(config.prefixesForIdEncodedIris_);

  // Build text index if requested (various options).
  if (!config.onlyAddTextIndex_) {
    AD_CONTRACT_CHECK(!config.inputFiles_.empty());
    index.createFromFiles(config.inputFiles_);
  }

  if (config.wordsAndDocsFileSpecified() || config.addWordsFromLiterals_) {
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
    auto textIndexBuilder = TextIndexBuilder(
        ad_utility::makeUnlimitedAllocator<Id>(), index.getOnDiskBase());
    textIndexBuilder.buildTextIndexFile(
        config.wordsAndDocsFileSpecified()
            ? std::optional{std::pair{config.wordsfile_, config.docsfile_}}
            : std::nullopt,
        config.addWordsFromLiterals_, config.textScoringMetric_,
        {config.bScoringParam_, config.kScoringParam_});
    if (!config.docsfile_.empty()) {
      textIndexBuilder.buildDocsDB(config.docsfile_);
    }
#else
    throw std::runtime_error(
        "Building a fulltext index is not supported using this restricted "
        "version of QLever");
#endif
  }

  // Build materialized views if requested.
  if (!config.writeMaterializedViews_.empty()) {
    std::cout << std::endl;
    AD_LOG_INFO << "Loading the new index to execute materialized view write "
                   "queries ..."
                << std::endl;
    Qlever engine{EngineConfig{config}};
    for (auto& [viewName, query] : config.writeMaterializedViews_) {
      engine.writeMaterializedView(viewName, query);
    }
    AD_LOG_INFO << "All materialized views written successfully" << std::endl;
  }
}

// ___________________________________________________________________________
std::string Qlever::query(std::string queryString,
                          ad_utility::MediaType mediaType) const {
  return query(parseAndPlanQuery(std::move(queryString)), mediaType);
}

// ___________________________________________________________________________
std::string Qlever::query(const PlannedQuery& plannedQuery,
                          ad_utility::MediaType mediaType) const {
  ad_utility::Timer timer{ad_utility::Timer::Started};

  const auto& sharedCancellationHandle = plannedQuery.queryExecutionTree()
                                             .getRootOperation()
                                             ->getCancellationHandle();
  std::string result;
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  auto responseGenerator = ExportQueryExecutionTrees::computeResult(
      plannedQuery.parsedQuery(), plannedQuery.queryExecutionTree(), mediaType,
      timer, sharedCancellationHandle);
  for (const auto& batch : responseGenerator) {
    result += batch;
  }
#else
  ad_utility::streams::StringBatcher yielder{
      [&result](std::string_view batch) { result.append(batch); }};
  ExportQueryExecutionTrees::computeResult(
      plannedQuery.parsedQuery(), plannedQuery.queryExecutionTree(), mediaType,
      timer, sharedCancellationHandle, std::ref(yielder));

#endif
  return result;
}

// _____________________________________________________________________________
void Qlever::queryAndPinResultWithName(
    QueryExecutionContext::PinResultWithName options, std::string query) {
  if (options.geoIndexSimplificationInMeters_.has_value() &&
      options.geoIndexSimplificationInMeters_.value() <= 0.0) {
    throw std::runtime_error(
        "`geoIndexSimplificationInMeters_` must be a positive "
        "floating-point number of meters.");
  }
  auto plannedQuery = parseAndPlanQuery(std::move(query));
  plannedQuery.queryExecutionContext().pinResultWithName() = std::move(options);
  [[maybe_unused]] auto result = this->query(plannedQuery);
}

// _____________________________________________________________________________
void Qlever::queryAndPinResultWithName(std::string name, std::string query) {
  queryAndPinResultWithName(
      QueryExecutionContext::PinResultWithName{std::move(name)},
      std::move(query));
}

// _____________________________________________________________________________
void Qlever::clearNamedResultCache() { namedResultCache_.clear(); }

// _____________________________________________________________________________
void Qlever::eraseResultWithName(std::string name) {
  namedResultCache_.erase(name);
}

// ___________________________________________________________________________
PlannedQuery Qlever::planQuery(
    ParsedQuery&& parsedQuery, QueryExecutionContext& qec,
    SharedCancellationHandle handle, std::optional<TimeLimit> timeLimit,
    boost::optional<const ad_utility::Timer&> requestTimer) const {
  handle->throwIfCancelled();
  QueryPlanner qp{&qec, handle};

  qp.setEnablePatternTrick(enablePatternTrick_);
  auto qet = qp.createExecutionTree(parsedQuery);
  qet.isRoot() = true;
  PlannedQuery plannedQuery = {std::move(parsedQuery), std::move(qet), qec};

  auto& rootOperation = *plannedQuery.queryExecutionTree().getRootOperation();
  // Propagate the `cancellationHandle` and the `timeLimit` through the
  // `queryExecutionTree`.
  rootOperation.recursivelySetCancellationHandle(std::move(handle));
  if (timeLimit.has_value()) {
    rootOperation.recursivelySetTimeConstraint(timeLimit.value());
  }

  if (requestTimer.has_value()) {
    auto& qet = plannedQuery.queryExecutionTree();
    auto timeForQueryPlanning = requestTimer->msecs();
    auto& runtimeInfoWholeQuery =
        qet.getRootOperation()->getRuntimeInfoWholeQuery();
    runtimeInfoWholeQuery.timeQueryPlanning = timeForQueryPlanning;
  }
  return plannedQuery;
}

// ___________________________________________________________________________
PlannedQuery Qlever::parseAndPlanQuery(
    std::string query, const std::vector<DatasetClause>& datasetClauses,
    SharedCancellationHandle handle, std::optional<TimeLimit> timeLimit,
    boost::optional<const ad_utility::Timer&> requestTimer,
    std::function<void(std::string)> updateCallback, bool pinSubtrees,
    bool pinResult) const {
  auto qecPtr = createQueryExecutionContext(
      indexAndViewsSnapshot(), std::move(updateCallback), pinSubtrees,
      pinResult, disableCaching_);

  auto parsedQuery = SparqlParser::parseQuery(
      &qecPtr->getIndex().getImpl().encodedIriManager(), std::move(query),
      datasetClauses);

  return planQuery(std::move(parsedQuery), *qecPtr, std::move(handle),
                   timeLimit, requestTimer);
}

// ___________________________________________________________________________
void IndexBuilderConfig::validate() const {
  if (kScoringParam_ < 0) {
    throw std::invalid_argument("The value of bm25-k must be >= 0");
  }
  if (bScoringParam_ < 0 || bScoringParam_ > 1) {
    throw std::invalid_argument(
        "The value of bm25-b must be between and "
        "including 0 and 1");
  }
  if (!(wordsAndDocsFileSpecified() ||
        (wordsfile_.empty() && docsfile_.empty()))) {
    throw std::runtime_error(absl::StrCat(
        "Only specified ", wordsfile_.empty() ? "docsfile" : "wordsfile",
        ". Both or none of docsfile and wordsfile have to be given to build "
        "text index. If none are given the option to add words from literals "
        "has to be true. For details see --help."));
  }
}

// ___________________________________________________________________________
void Qlever::writeMaterializedView(
    std::string name, std::string query,
    const std::vector<DatasetClause>& datasetClauses,
    SharedCancellationHandle cancellationHandle,
    std::optional<TimeLimit> timeLimit,
    boost::optional<const ad_utility::Timer&> requestTimer) const {
  auto plan =
      parseAndPlanQuery(std::move(query), datasetClauses,
                        std::move(cancellationHandle), timeLimit, requestTimer);
  const auto& viewsManager =
      plan.queryExecutionContext().materializedViewsManager();
  auto memoryLimit =
      getRuntimeParameter<&RuntimeParameters::materializedViewWriterMemory_>();
  viewsManager.writeViewToDisk(std::move(name), plan, memoryLimit);
}

// ___________________________________________________________________________
bool Qlever::isMaterializedViewLoaded(const std::string& name) const {
  const auto indexAndViews = indexAndViewsSnapshot();
  return indexAndViews->materializedViewsManager_.isViewLoaded(name);
}

// ___________________________________________________________________________
void Qlever::loadMaterializedView(std::string name) const {
  const auto indexAndViews = indexAndViewsSnapshot();
  indexAndViews->materializedViewsManager_.loadView(name);
}

// ___________________________________________________________________________
std::shared_ptr<QueryExecutionContext> Qlever::createQueryExecutionContext(
    std::shared_ptr<IndexAndViews> indexAndViews,
    std::function<void(std::string)> updateCallback, bool pinSubtrees,
    bool pinResult,
    QueryExecutionContext::DisableCaching disableCaching) const {
  auto [index, viewsManager] = getPointerPair(std::move(indexAndViews));
  return std::make_shared<QueryExecutionContext>(
      std::move(index), &cache_, allocator_, sortPerformanceEstimator_,
      &namedResultCache_, std::move(viewsManager), std::move(updateCallback),
      pinSubtrees, pinResult, disableCaching);
}

// ___________________________________________________________________________
std::string IndexRebuildConfig::tmpBasename() const {
  return (ql::filesystem::path{tmpDirForRebuild_} / basenameForNewIndex_)
      .lexically_normal()
      .string();
}

// ___________________________________________________________________________
std::string IndexRebuildConfig::finalBasename() const {
  return (ql::filesystem::path{dirForNewIndex_} / basenameForNewIndex_)
      .lexically_normal()
      .string();
}

// ___________________________________________________________________________
void Qlever::moveRebuiltIndexIntoPlace(const std::string& originalBase,
                                       IndexAndViews& newIndexAndViews,
                                       const IndexRebuildConfig& config) {
  namespace fs = ql::filesystem;
  auto& [newIndex, newManager] = newIndexAndViews;
  const std::string rebuildBase = newIndex.getOnDiskBase();
  const std::string newBase = config.finalBasename();

  // Move the old index's files (including its view files) into the directory
  // for the old index.
  const auto& oldIndexDir = config.dirForOldIndex_;
  fs::create_directories(oldIndexDir);
  auto moveToOldIndexDir = [&oldIndexDir](const fs::path& file) {
    fs::rename(file, fs::path{oldIndexDir} / file.filename());
  };
  ql::ranges::for_each(IndexImpl::allIndexFiles(originalBase),
                       moveToOldIndexDir);
  ql::ranges::for_each(MaterializedViewsManager::viewFilesOnDisk(originalBase),
                       moveToOldIndexDir);
  // Move the old index's build log with it (it was either built originally or
  // by a previous rebuild, so exactly one of the two variants exists).
  for (auto suffix : {INDEX_LOG_SUFFIX, REBUILD_INDEX_LOG_SUFFIX}) {
    auto logFile = absl::StrCat(originalBase, suffix);
    if (fs::exists(logFile)) {
      moveToOldIndexDir(logFile);
    }
  }

  // Move the new index's files to their final base name.
  for (const auto& file : IndexImpl::allIndexFiles(rebuildBase)) {
    AD_CORRECTNESS_CHECK(ql::starts_with(file, rebuildBase));
    fs::rename(file, absl::StrCat(newBase, std::string_view{file}.substr(
                                               rebuildBase.size())));
  }
  // Move the new index's rebuild log to its final place, next to the index it
  // describes (from where it will later travel into the directory of the old
  // index, when this index is in turn retired by a future rebuild).
  auto rebuildLog = absl::StrCat(rebuildBase, REBUILD_INDEX_LOG_SUFFIX);
  if (fs::exists(rebuildLog)) {
    fs::rename(rebuildLog, absl::StrCat(newBase, REBUILD_INDEX_LOG_SUFFIX));
  }

  // Re-anchor the path-derived state of the new index.
  newIndex.getImpl().setOnDiskBase(newBase);
  if (newIndex.deltaTriplesManager().persists()) {
    newIndex.deltaTriplesManager().setFilenameForPersistentUpdates(
        absl::StrCat(newBase, UPDATE_TRIPLES_SUFFIX));
    newIndex.getImpl().graphNameManager().setFilenameForPersisting(
        absl::StrCat(newBase, ALLOCATED_GRAPHS_SUFFIX));
  }
  newManager.setOnDiskBase(newBase);
}
}  // namespace qlever
