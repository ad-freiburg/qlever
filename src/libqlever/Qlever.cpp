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
#include "global/FileSuffixConstants.h"
#include "index/IndexImpl.h"
#include "index/TextIndexBuilder.h"
#include "libqlever/QleverTypes.h"
#include "parser/SparqlParser.h"
#include "util/Exception.h"
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
  // The schemes are only used to validate that the index was built with
  // exactly these schemes, the actual schemes are restored from the index (see
  // `IndexImpl::setEncodedIriSchemes`).
  index.getImpl().setEncodedIriSchemes(config.encodedIriSchemes_);
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
  // NOTE: The schemes have to be set before the prefixes, because the
  // `EncodedIriManager` is built by the latter call.
  index.getImpl().setEncodedIriSchemes(config.encodedIriSchemes_);
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
}
}  // namespace qlever
