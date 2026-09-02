// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "libqlever/Qlever.h"

#include <absl/strings/str_cat.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <boost/optional.hpp>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string_view>

#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "engine/ExecuteUpdate.h"
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/MaterializedViews.h"
#include "engine/QueryExecutionContext.h"
#include "engine/UpdateMetadata.h"
#include "global/Constants.h"
#include "global/FileSuffixConstants.h"
#include "index/IndexImpl.h"
#include "index/IndexRebuilder.h"
#include "index/TextIndexBuilder.h"
#include "libqlever/QleverTypes.h"
#include "parser/SparqlParser.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/FilesystemHelpers.h"
#include "util/Log.h"
#include "util/TimeTracer.h"

namespace qlever {

// _____________________________________________________________________________
Qlever::Qlever(const EngineConfig& config, bool skipLoading)
    : Qlever(config, skipLoading,
             qlever::makeAllocatorWithLimit<Id>(
                 config.memoryLimit_.value_or(DEFAULT_MEM_FOR_QUERIES),
                 [this](ad_utility::MemorySize numMemoryToAllocate) {
                   cache_.makeRoomAsMuchAsPossible(MAKE_ROOM_SLACK_FACTOR *
                                                   numMemoryToAllocate);
                 })) {}

// _____________________________________________________________________________
Qlever::Qlever(const EngineConfig& config, bool skipLoading,
               Allocator<Id> allocator)
    : allocator_{std::move(allocator)},
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

  // If `skipLoading` is set, we do not touch the on-disk index at all; the
  // instance is expected to be populated later from a blob (see
  // `deserializeVocabAndNamedCacheFromCompressedBlob`).
  if (skipLoading) {
    return;
  }

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

  // Estimate the cost of sorting operations (needed for query planning), unless
  // the user disabled this (potentially expensive) step.
  if (config.computeSortPerformanceEstimators_) {
    sortPerformanceEstimator_.computeEstimatesExpensively(
        allocator_, index.numTriples().normalAndInternal_() *
                        PERCENTAGE_OF_TRIPLES_FOR_SORT_ESTIMATE / 100);
  }

  // Preload materialized views as requested by the user.
  for (const auto& viewName : config.preloadMaterializedViews_) {
    try {
      auto qec = createQueryExecutionContext(indexAndViewsSnapshot());
      materializedViewsManager.loadView(viewName, qec.get());
    } catch (const std::exception& ex) {
      AD_LOG_ERROR << "Preloading materialized view '" << viewName
                   << "' failed: " << ex.what() << "." << std::endl;
    }
  }
}

// _____________________________________________________________________________
void Qlever::buildIndex(IndexBuilderConfig config) {
  // Reject invalid configurations early and with an informative error message.
  config.validate();
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
  index.getImpl().setBlankNodeIriRegexes(config.blankNodeIriRegexes_);

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
UpdateMetadata Qlever::applyUpdate(
    const PlannedQuery& plannedUpdate,
    ad_utility::SharedCancellationHandle cancellationHandle,
    DeltaTriples& deltaTriples, ad_utility::timer::TimeTracer& tracer) {
  const auto& qet = plannedUpdate.queryExecutionTree();
  AD_CORRECTNESS_CHECK(plannedUpdate.parsedQuery().hasUpdateClause());
  AD_CORRECTNESS_CHECK(&plannedUpdate.getIndex().getImpl() ==
                       &deltaTriples.getIndex());

  DeltaTriplesCount countBefore = deltaTriples.getCounts();
  UpdateMetadata updateMetadata = ExecuteUpdate::executeUpdate(
      plannedUpdate.getIndex(), plannedUpdate.parsedQuery(), qet, deltaTriples,
      cancellationHandle, tracer);
  updateMetadata.countBefore_ = countBefore;
  updateMetadata.countAfter_ = deltaTriples.getCounts();

  tracer.beginTrace("clearCache");
  // Clear the cache, because all cache entries have been invalidated by
  // the update anyway (The index of the located triples snapshot is
  // part of the cache key).
  cache_.clearAll();
  namedResultCache_.clear();
  tracer.endTrace("clearCache");

  return updateMetadata;
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
void Qlever::clearQueryResultCache() { cache_.clearAll(); }

// _____________________________________________________________________________
DeltaTriplesCount Qlever::clearDeltaTriples() const {
  auto snapshot = indexAndViewsSnapshot();
  return snapshot->index_.deltaTriplesManager().modify<DeltaTriplesCount>(
      [](auto& deltaTriples) {
        deltaTriples.clear();
        return deltaTriples.getCounts();
      });
}

// _____________________________________________________________________________
nlohmann::json Qlever::vacuumDeltaTriples(
    SharedCancellationHandle handle) const {
  auto snapshot = indexAndViewsSnapshot();
  return snapshot->index_.deltaTriplesManager().modify<nlohmann::json>(
      [handle](auto& deltaTriples) { return deltaTriples.vacuum(handle); });
}

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
PlannedQuery Qlever::planQuery(
    ParsedQueryAndContext parsedQuery, SharedCancellationHandle handle,
    std::optional<TimeLimit> timeLimit,
    boost::optional<const ad_utility::Timer&> requestTimer) const {
  // NOTE: `qec` is a reference into `parsedQuery`, which is alive for the
  // duration of this call, and the resulting `PlannedQuery` takes its own
  // `shared_ptr` to the context.
  auto& qec = parsedQuery.queryExecutionContext();
  return planQuery(std::move(parsedQuery.parsedQuery()), qec, std::move(handle),
                   timeLimit, requestTimer);
}

// ___________________________________________________________________________
ParsedQueryAndContext Qlever::parseQuery(
    std::string query, const std::vector<DatasetClause>& datasetClauses,
    std::function<void(std::string)> updateCallback, bool pinSubtrees,
    bool pinResult) const {
  auto qecPtr = createQueryExecutionContext(
      indexAndViewsSnapshot(), std::move(updateCallback), pinSubtrees,
      pinResult, disableCaching_);

  auto parsedQuery = SparqlParser::parseQuery(
      &qecPtr->getIndex().getImpl().encodedIriManager(), std::move(query),
      datasetClauses);

  return ParsedQueryAndContext{std::move(parsedQuery), std::move(qecPtr)};
}

// ___________________________________________________________________________
ParsedQueryAndContext Qlever::bindParsedQuery(
    ParsedQuery parsedQuery, std::function<void(std::string)> updateCallback,
    bool pinSubtrees, bool pinResult) const {
  auto qecPtr = createQueryExecutionContext(
      indexAndViewsSnapshot(), std::move(updateCallback), pinSubtrees,
      pinResult, disableCaching_);
  return ParsedQueryAndContext{std::move(parsedQuery), std::move(qecPtr)};
}

// ___________________________________________________________________________
PlannedQuery Qlever::parseAndPlanQuery(
    std::string query, const std::vector<DatasetClause>& datasetClauses,
    SharedCancellationHandle handle, std::optional<TimeLimit> timeLimit,
    boost::optional<const ad_utility::Timer&> requestTimer,
    std::function<void(std::string)> updateCallback, bool pinSubtrees,
    bool pinResult) const {
  return planQuery(
      parseQuery(std::move(query), datasetClauses, std::move(updateCallback),
                 pinSubtrees, pinResult),
      std::move(handle), timeLimit, requestTimer);
}

// ___________________________________________________________________________
void IndexBuilderConfig::validate() const {
  // NOTE: The vocabulary types with "holes" (see `VocabularyInMemoryBinSearch`)
  // cannot be built word by word and hence must not be used for index building.
  // They are accepted by the command-line parser (which knows all vocabulary
  // types), so we have to reject them explicitly here.
  if (!vocabType_.isSupportedForIndexBuilding()) {
    throw std::invalid_argument(absl::StrCat(
        "The vocabulary type \"", vocabType_.toString(),
        "\" cannot be used for index building, the supported types are ",
        ad_utility::VocabularyType::getListOfValuesForIndexBuilding()));
  }
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
void Qlever::unloadMaterializedView(const std::string& name) const {
  const auto indexAndViews = indexAndViewsSnapshot();
  indexAndViews->materializedViewsManager_.unloadViewIfLoaded(name);
}

// ___________________________________________________________________________
void Qlever::loadMaterializedView(std::string name) const {
  auto indexAndViews = indexAndViewsSnapshot();
  auto qec = createQueryExecutionContext(indexAndViews);
  indexAndViews->materializedViewsManager_.loadView(name, qec.get());
}

// ___________________________________________________________________________
void Qlever::deleteMaterializedView(std::string name) const {
  const auto indexAndViews = indexAndViewsSnapshot();
  indexAndViews->materializedViewsManager_.deleteView(name);
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
IndexSwapConfig Qlever::makeIndexRebuildConfig(
    const Index& index, std::optional<std::string> rebuildTmpDir,
    std::optional<std::string> rebuildPreviousIndexDir) {
  // The new index is built in `rebuild.<current datetime>.tmp` and the old
  // index is moved to `previous.<datetime of the build of the current
  // index>`. The base name the current index is served from is where the new
  // index has to end up, so that a later restart loads it.
  //
  // NOTE: The non-atomic check-then-use of the default directory names inside
  // `makeIndexSwapConfig` is fine here, because rebuilds are serialized (see
  // `Server::rebuildTracker_`).
  IndexSwapNaming naming{"rebuild.", "previous.",
                         index.getImpl().dateOfIndexBuild(),
                         " or specify a directory explicitly via "
                         "`rebuild-previous-index-dir`"};
  return makeIndexSwapConfig(index.getOnDiskBase(), naming,
                             std::move(rebuildTmpDir),
                             std::move(rebuildPreviousIndexDir));
}

// ___________________________________________________________________________
void Qlever::cleanUpPreviousIndexDirs(const std::string& indexBaseName,
                                      KeepPreviousIndexDirs policy) {
  if (policy == KeepPreviousIndexDirs::All) {
    return;
  }
  // Nothing in here may throw: when this runs, the rebuild has already
  // succeeded, so a failure of the cleanup must only be logged. Deletion
  // failures are handled (and skipped) individually below; the catch covers
  // everything else (e.g. a failure while enumerating the directories or
  // opening the log file).
  try {
    cleanUpPreviousIndexDirsImpl(indexBaseName, policy);
  } catch (const std::exception& e) {
    AD_LOG_ERROR << "Failed to clean up the previous index directories: "
                 << e.what() << std::endl;
  }
}

// ___________________________________________________________________________
void Qlever::cleanUpPreviousIndexDirsImpl(const std::string& indexBaseName,
                                          KeepPreviousIndexDirs policy) {
  namespace fs = ql::filesystem;
  // Collect the `previous.*` directories in the directory of the index. The
  // parent path is empty if the base name lies in the working directory
  // itself.
  fs::path indexDir = fs::path{indexBaseName}.parent_path();
  if (indexDir.empty()) {
    indexDir = fs::current_path();
  }
  auto previousDirs =
      qlever::util::directoriesWithPrefix(indexDir, "previous.");

  // Order the directories from the oldest to the newest by the time they were
  // last written to. Nothing writes into such a directory after the rebuild
  // that created it, so this is the order in which they were created. Two
  // rebuilds within the same second can produce equal timestamps (depending
  // on the filesystem backend); such ties are broken by name, which contains
  // the build date of the retired index (with a numeric suffix for repeated
  // dates, see `makeIndexRebuildConfig`) and hence also increases from the
  // oldest to the newest.
  auto sortKey = [](const fs::path& dir) {
    return std::pair{fs::last_write_time(dir), dir.filename().string()};
  };
  ql::ranges::sort(previousDirs, std::less{}, sortKey);

  // Keep or delete each directory according to the policy. The decisions are
  // written to the log file of the rebuild that has just finished (which was
  // moved into place together with the index), not to the server log.
  auto logFile = ad_utility::makeOfstream(
      absl::StrCat(indexBaseName, REBUILD_INDEX_LOG_SUFFIX), std::ios::app);
  auto log = [&logFile](std::string_view severity = "INFO") -> std::ostream& {
    return logFile << ad_utility::Log::getTimeStamp() << " - " << severity
                   << ": ";
  };
  log() << "Checking which previous index directories to keep or delete ("
        << policy << ")" << std::endl;
  for (size_t i = 0; i < previousDirs.size(); ++i) {
    const fs::path& dir = previousDirs[i];
    bool keep = keepPreviousIndexDir(policy, i, previousDirs.size());
    log() << dir.filename().string() << " -> " << (keep ? "KEEP" : "DELETE")
          << std::endl;
    if (!keep) {
      ql::error_code errorCode;
      fs::remove_all(dir, errorCode);
      if (errorCode) {
        // A failed deletion additionally goes to the server log, where
        // operators look for errors.
        log("ERROR") << "Failed to delete \"" << dir.filename().string()
                     << "\": " << errorCode.message() << std::endl;
        AD_LOG_ERROR << "Failed to delete \"" << dir.filename().string()
                     << "\": " << errorCode.message() << std::endl;
      }
    }
  }
}

// ___________________________________________________________________________
void Qlever::moveRebuiltIndexIntoPlace(IndexAndViews& newIndexAndViews,
                                       const IndexSwapConfig& config,
                                       KeepPreviousIndexDirs policy) {
  auto& [newIndex, newManager] = newIndexAndViews;
  AD_CORRECTNESS_CHECK(newIndex.getOnDiskBase() == config.newIndexSource());

  // The on-disk part of the swap: retire the old index, move the new index to
  // its final place, and remove the directory in which the new index was
  // built (typically a temporary directory created exclusively for the
  // rebuild, see `rebuildIndexToDisk`).
  moveIndexIntoPlace(config);

  // Re-anchor the path-derived state of the new index.
  newIndex.setOnDiskBase(config.newIndexTarget());
  if (newIndex.deltaTriplesManager().persists()) {
    newIndex.getImpl().setFilenamesForPersistentUpdates(false);
  }
  newManager.setOnDiskBase(config.newIndexTarget());

  // Apply the configured policy for which `previous.*` index directories to
  // keep, right after the move that has just retired the old index into such
  // a directory. A failure is only logged (`cleanUpPreviousIndexDirs` never
  // throws): the new index is already in place, so the rebuild as a whole has
  // succeeded.
  cleanUpPreviousIndexDirs(config.newIndexTarget(), policy);
}

// ___________________________________________________________________________
// The two functions below rely on `materializeToIndex` and
// `DeltaTriples::addFromSnapshotDiff`, which are only available in the C++20
// build, so they are not compiled in the reduced C++17 feature set.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
Qlever::RebuildResult Qlever::rebuildIndexToDisk(
    Index& index, const IndexSwapConfig& config,
    const ad_utility::SharedCancellationHandle& handle) const {
  const std::string& indexBaseName = config.newIndexSource();
  ql::filesystem::path directory =
      ql::filesystem::path{indexBaseName}.parent_path();
  if (!directory.empty()) {
    ql::filesystem::create_directories(directory);
  }
  auto logFileName = absl::StrCat(indexBaseName, REBUILD_INDEX_LOG_SUFFIX);
  auto [currentSnapshot, localVocabCopy, ownedBlocks] =
      index.deltaTriplesManager()
          .getCurrentLocatedTriplesSharedStateWithVocab();
  auto mapping =
      materializeToIndex(index, indexBaseName, currentSnapshot, localVocabCopy,
                         ownedBlocks, handle, logFileName);
  auto indexAndViews = std::make_shared<IndexAndViews>(
      Index{allocator()}, MaterializedViewsManager{});
  auto& [newIndex, newManager] = *indexAndViews;
  newIndex.usePatterns() = index.usePatterns();
  newIndex.loadAllPermutations() = index.loadAllPermutations();
  newIndex.createFromOnDiskIndex(indexBaseName,
                                 index.deltaTriplesManager().persists());
  newManager.setOnDiskBase(indexBaseName);
  return {std::move(currentSnapshot), std::move(mapping),
          std::move(indexAndViews)};
}

// ___________________________________________________________________________
void Qlever::swapInRebuiltIndex(
    const Index& index, RebuildResult rebuildResult,
    const ad_utility::SharedCancellationHandle& handle,
    const IndexSwapConfig& config,
    KeepPreviousIndexDirs keepPreviousIndexDirs) {
  auto& [oldSnapshot, mapping, newIndexAndViews] = rebuildResult;
  auto newSnapshot =
      index.deltaTriplesManager().getCurrentLocatedTriplesSharedState();

  // Calling this function also persists the remapped delta triples to
  // disk so that they are not lost if the engine is later restarted on
  // the rebuilt index. The triples that were persisted for the old index
  // are not compatible with the freshly built index (their `Id`s refer to
  // the old vocabulary), so they have to be regenerated.
  newIndexAndViews->index_.deltaTriplesManager().modify<void>(
      [&oldSnapshot, &newSnapshot, &mapping,
       &handle](DeltaTriples& deltaTriples) {
        ad_utility::timer::TimeTracer tracer{"swapIndex"};
        deltaTriples.addFromSnapshotDiff(*oldSnapshot, *newSnapshot, mapping,
                                         handle, tracer);
      },
      true);
  // Move the old index out of the way and the new index into its final
  // place, which by default is the place of the old index (in particular, a
  // subsequent restart of the server then loads the latest index).
  //
  // NOTE: If this throws halfway through, the server keeps running
  // consistently on the old index (the swap below has not happened and open
  // file handles survive the renames), but the on-disk layout has to be
  // repaired manually before the next restart.
  moveRebuiltIndexIntoPlace(*newIndexAndViews, config, keepPreviousIndexDirs);
  swapIndexAndViews(std::move(newIndexAndViews));
  // Clear the query cache, including pinned entries: cached results were
  // computed against the old index, so their `VocabIndex` ids are in the old
  // vocabulary's coordinates and their `LocalVocabEntry`s are anchored to the
  // old index. The cache key alone does not protect against serving them: it
  // only contains the query string and the delta triples version counter,
  // and the counter of the new index starts over and can collide with a
  // pre-swap value.
  cache_.clearAll();
}
#endif
}  // namespace qlever
