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
#include "engine/ExportQueryExecutionTrees.h"
#include "engine/MaterializedViews.h"
#include "engine/QueryExecutionContext.h"
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
#include "util/http/UrlParser.h"

namespace qlever {

// _____________________________________________________________________________
Qlever::Qlever(const EngineConfig& config, bool skipLoading)
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

namespace {
// Two base names "collide" if the prefix-based file enumerators
// (`allIndexFiles`, `viewFilesOnDisk`, `filesWithBaseNameAndSuffix`) could
// confuse the files belonging to one with the files belonging to the other.
// Everything QLever appends to a base name starts with a '.' (`.index.pso`,
// `.meta`, `.vocabulary`, `.internal`, `.view.<name>`, the log suffixes, ...),
// so a merely shared textual prefix is harmless: base names `foo` and `foobar`
// never clash, because `foobar.index...` does not fall inside the `foo.` glob.
// The two dangerous cases are that the (lexically normalized) base names are
// equal, or that one is the other followed by a '.', e.g. `foo` and `foo.view`:
// there `foo.view`'s own index files sit inside the `foo.view.*` glob that
// enumerates `foo`'s materialized views, so moving/replacing one base name
// would sweep up the other's files.
//
// Both cases collapse into a single check once we append the separating '.' to
// each normalized name: they collide iff one dotted form is a prefix of the
// other. Equal names give identical dotted forms; `foo` vs `foo.view` is caught
// because `foo.` is a prefix of `foo.view.`; and `foo` vs `foobar` is not,
// because `foo.` is not a prefix of `foobar.`.
bool baseNamesCollide(const std::string& a, const std::string& b) {
  auto normalizedWithSeparator = [](const std::string& s) {
    return absl::StrCat(ql::filesystem::path{s}.lexically_normal().string(),
                        ".");
  };
  std::string na = normalizedWithSeparator(a);
  std::string nb = normalizedWithSeparator(b);
  return ql::starts_with(na, nb) || ql::starts_with(nb, na);
}
}  // namespace

// ___________________________________________________________________________
IndexRebuildConfig::IndexRebuildConfig(std::string oldIndexSource,
                                       std::string newIndexSource,
                                       std::string oldIndexTarget,
                                       std::string newIndexTarget)
    : oldIndexSource_{std::move(oldIndexSource)},
      newIndexSource_{std::move(newIndexSource)},
      oldIndexTarget_{std::move(oldIndexTarget)},
      newIndexTarget_{std::move(newIndexTarget)} {
  // Both the relocation of the old index and the installation of the new index
  // are implemented (in `Qlever::moveRebuiltIndexIntoPlace`) as "replace the
  // base-name prefix of each file". For this to be well-defined and
  // non-destructive, the involved base names must not collide in ways that
  // would overwrite files that are still needed, or that would turn a move into
  // a (potentially partial) self-overwrite. Note that `newIndexTarget_ ==
  // oldIndexSource_` is the common (and intended) case: the old index is moved
  // away first, so its place is free for the new index.
  AD_CONTRACT_CHECK(
      !baseNamesCollide(oldIndexSource_, newIndexSource_),
      "The currently served index and the freshly rebuilt index must not share "
      "a base name.");
  AD_CONTRACT_CHECK(
      !baseNamesCollide(oldIndexTarget_, oldIndexSource_),
      "The base name for the retired old index must differ from the currently "
      "served index.");
  AD_CONTRACT_CHECK(
      !baseNamesCollide(oldIndexTarget_, newIndexSource_),
      "The base name for the retired old index must differ from the freshly "
      "rebuilt index.");
  AD_CONTRACT_CHECK(
      !baseNamesCollide(oldIndexTarget_, newIndexTarget_),
      "The base names for the retired old index and the new index must "
      "differ.");
}

// ___________________________________________________________________________
nlohmann::json IndexRebuildConfig::successResponseAsJson() const {
  nlohmann::json json;
  json["message"] = "Index successfully rebuilt and swapped in";
  // Report the directory (not the full base name): it mirrors the
  // `rebuild-previous-index-dir` command parameter and is the one piece of
  // information the client cannot know in advance (the default is derived from
  // the build date of the old index). The new index is not mentioned because
  // it is always served from the base name of the old one.
  json["previous-index-dir"] =
      ql::filesystem::path{oldIndexTarget_}.parent_path().string();
  return json;
}

// ___________________________________________________________________________
IndexRebuildConfig Qlever::makeIndexRebuildConfig(
    const Index& index, std::optional<std::string> rebuildTmpDir,
    std::optional<std::string> rebuildPreviousIndexDir) {
  namespace fs = ql::filesystem;

  // The base name the current index is served from. The new index has to end up
  // exactly there, so that a later restart loads it; it is therefore also the
  // base name whose file name and directory the two directories below are
  // derived from and checked against.
  const std::string& currentBaseName = index.getOnDiskBase();

  // Resolve one of the two directories (falling back to `defaultDirectory` if
  // it was not specified) and turn it into a base name.
  // NOTE: Use `ql::pathFilename` and not `path::filename()`, so that a base
  // name with a trailing directory separator yields an empty file name
  // component (and hence a directory base name, see the test
  // `moveRebuiltIndexIntoPlaceWithDirectoryBasename`) in both the
  // `std::filesystem` and the `boost::filesystem` backend.
  auto resolveBaseName =
      [indexFileName = ql::pathFilename(fs::path{currentBaseName})](
          std::optional<std::string> directory, std::string defaultDirectory) {
        return (fs::path{std::move(directory).value_or(
                    std::move(defaultDirectory))} /
                indexFileName)
            .string();
      };

  // The defaults are: build the new index in `rebuild.<current datetime>.tmp`
  // and move the old index to `previous.<datetime of the build of the current
  // index>`.
  //
  // The datetime of the index build has a granularity of one second, so when
  // rebuilds happen in quick succession (e.g. automatic rebuilds on a small
  // index, see `--rebuild-index-strategy`), two index generations can carry
  // the same datetime, and the default directory for the second of them is
  // then already taken. Append `.1`, `.2`, ... in that case (like the
  // numbered backups of `logrotate`). Without this, the rebuild would fail,
  // and since a failed rebuild does not swap (and hence does not re-stamp the
  // datetime of the served index), all subsequent rebuilds would fail the
  // same way. Only the default name is uniquified; an explicitly given
  // directory that is taken remains an error (see the checks below).
  //
  // NOTE: The check-then-use is not atomic; this is fine because rebuilds
  // are serialized (see `Server::rebuildInProgress_`).
  auto uniquify = [](const std::string& directory) {
    std::string candidate = directory;
    for (size_t i = 1; fs::exists(candidate); ++i) {
      if (i > 99) {
        throw std::runtime_error{absl::StrCat(
            "The directories \"", directory, "\" and \"", directory,
            ".1\" through \"", directory,
            ".99\" all already exist; remove some of them or specify a "
            "directory explicitly via `rebuild-previous-index-dir`")};
      }
      candidate = absl::StrCat(directory, ".", i);
    }
    return candidate;
  };
  std::string baseNameForRebuild = resolveBaseName(
      std::move(rebuildTmpDir),
      absl::StrCat("rebuild.", IndexImpl::formatIndexBuildTime(absl::Now()),
                   ".tmp"));
  std::string baseNameForOldIndex = resolveBaseName(
      std::move(rebuildPreviousIndexDir),
      uniquify(absl::StrCat("previous.", index.getImpl().dateOfIndexBuild())));

  // Check the two base names that were derived from the arguments: they must be
  // relative (they are resolved against the working directory of the engine,
  // like the base name of the current index), and their directory must be empty
  // or not exist yet and be a subdirectory of the directory of the current
  // index. Base names that would collide with each other or with the currently
  // served index are rejected by the `IndexRebuildConfig` constructor below.
  for (const auto& baseName : {baseNameForRebuild, baseNameForOldIndex}) {
    fs::path path{baseName};
    if (!path.is_relative()) {
      throw std::runtime_error{absl::StrCat("The directory \"",
                                            path.parent_path().string(),
                                            "\" must be a relative path")};
    }
    // The parent path is empty if the base name lies in the working directory
    // itself, which the checks below then refer to.
    fs::path dir =
        path.has_parent_path() ? path.parent_path() : fs::current_path();
    if (fs::exists(dir) && !fs::is_empty(dir)) {
      throw std::runtime_error{
          absl::StrCat("The directory \"", dir.string(),
                       "\" already exists and is not empty")};
    }
    if (!qlever::util::isSubdirectoryOf(baseName, currentBaseName)) {
      throw std::runtime_error{absl::StrCat(
          "The directory \"", dir.string(),
          "\" is not a subdirectory of the directory of the current index")};
    }
  }

  // The new index ends up at the base name the current index is served from, so
  // that a later restart loads it.
  return IndexRebuildConfig{currentBaseName, baseNameForRebuild,
                            baseNameForOldIndex, currentBaseName};
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
  std::vector<fs::path> previousDirs;
  for (const auto& entry : fs::directory_iterator{indexDir}) {
    if (entry.is_directory() &&
        ql::starts_with(entry.path().filename().string(), "previous.")) {
      previousDirs.push_back(entry.path());
    }
  }

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
  auto logInfo = [&logFile]() -> std::ostream& {
    return logFile << ad_utility::Log::getTimeStamp() << " - INFO: ";
  };
  logInfo() << "Checking which previous index directories to keep or delete ("
            << toString(policy) << ")" << std::endl;
  for (size_t i = 0; i < previousDirs.size(); ++i) {
    const fs::path& dir = previousDirs[i];
    bool keep = keepPreviousIndexDir(policy, i, previousDirs.size());
    logInfo() << dir.filename().string() << " -> " << (keep ? "KEEP" : "DELETE")
              << std::endl;
    if (!keep) {
      ql::error_code errorCode;
      fs::remove_all(dir, errorCode);
      if (errorCode) {
        // A failed deletion additionally goes to the server log, where
        // operators look for errors.
        logFile << ad_utility::Log::getTimeStamp() << " - ERROR: "
                << "Failed to delete \"" << dir.filename().string()
                << "\": " << errorCode.message() << std::endl;
        AD_LOG_ERROR << "Failed to delete \"" << dir.filename().string()
                     << "\": " << errorCode.message() << std::endl;
      }
    }
  }
}

// ___________________________________________________________________________
void Qlever::moveRebuiltIndexIntoPlace(IndexAndViews& newIndexAndViews,
                                       const IndexRebuildConfig& config) {
  namespace fs = ql::filesystem;

  // Move a `file` whose name starts with `fromBasename` so that its base-name
  // prefix becomes `toBasename` while the file-specific suffix is preserved
  // (e.g. `<from>.index.pso` -> `<to>.index.pso`).
  auto moveByBasename = [](const fs::path& file, std::string_view fromBasename,
                           std::string_view toBasename) {
    std::string fileString = file.string();
    AD_CORRECTNESS_CHECK(ql::starts_with(fileString, fromBasename));
    fs::rename(file,
               absl::StrCat(toBasename, std::string_view{fileString}.substr(
                                            fromBasename.size())));
  };

  // Move all files that make up an index from the `source` base name to the
  // `target` base name: its permutation and vocabulary files, its materialized
  // views, and its build log. Both file enumerators and the existence check
  // below only touch files that actually exist, so this is a no-op for anything
  // the index does not have (e.g. the freshly rebuilt new index has no
  // materialized views yet, and only one of the two build-log variants ever
  // exists for a given index).
  auto moveIndex = [&moveByBasename](std::string_view source,
                                     const std::string& target) {
    // Move the index to `target`. Create the containing directory first (the
    // base name may point into a directory that does not exist yet).
    fs::path targetDir = fs::path{target}.parent_path();
    if (!targetDir.empty()) {
      fs::create_directories(targetDir);
    }
    auto move = [&](const fs::path& file) {
      moveByBasename(file, source, target);
    };
    ql::ranges::for_each(IndexImpl::allIndexFiles(source), move);
    ql::ranges::for_each(MaterializedViewsManager::viewFilesOnDisk(source),
                         move);
    // Move the log files along with all the actual index files.
    for (auto suffix : {INDEX_LOG_SUFFIX, REBUILD_INDEX_LOG_SUFFIX}) {
      fs::path logFile = absl::StrCat(source, suffix);
      if (fs::exists(logFile)) {
        move(logFile);
      }
    }
  };

  auto& [newIndex, newManager] = newIndexAndViews;
  AD_CORRECTNESS_CHECK(newIndex.getOnDiskBase() == config.newIndexSource());
  moveIndex(config.oldIndexSource(), config.oldIndexTarget());
  moveIndex(config.newIndexSource(), config.newIndexTarget());

  // Re-anchor the path-derived state of the new index.
  newIndex.setOnDiskBase(config.newIndexTarget());
  if (newIndex.deltaTriplesManager().persists()) {
    newIndex.getImpl().setFilenamesForPersistentUpdates(false);
  }
  newManager.setOnDiskBase(config.newIndexTarget());

  // The move took the new index and its rebuild log out of the directory in
  // which the new index was built (typically a temporary directory created
  // exclusively for the rebuild, see `rebuildIndexToDisk`), so that directory
  // is now empty and can be removed. Everything that matters has already
  // happened at this point, so a failure here is only worth a warning.
  // NOTE: The `error_code` is only there to select the non-throwing overload of
  // `fs::remove`; it does not have to be inspected, because that overload
  // returns `false` whenever it sets an error code (and also if the directory
  // did not exist in the first place, which is just as unexpected here).
  fs::path directoryOfNewIndexSource =
      fs::path{config.newIndexSource()}.parent_path();
  ql::error_code errorCode;
  if (!directoryOfNewIndexSource.empty() &&
      !fs::remove(directoryOfNewIndexSource, errorCode)) {
    AD_LOG_WARN << "Could not remove the directory \""
                << directoryOfNewIndexSource.string()
                << "\" in which the new index was built" << std::endl;
  }
}

// ___________________________________________________________________________
// The two functions below rely on `materializeToIndex` and
// `DeltaTriples::addFromSnapshotDiff`, which are only available in the C++20
// build, so they are not compiled in the reduced C++17 feature set.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
Qlever::RebuildResult Qlever::rebuildIndexToDisk(
    Index& index, const IndexRebuildConfig& config,
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
    const IndexRebuildConfig& config) {
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
  moveRebuiltIndexIntoPlace(*newIndexAndViews, config);
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
