// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "libqlever/Qlever.h"

#include <memory>
#include <stdexcept>

#include "engine/ExportQueryExecutionTrees.h"
#include "engine/MaterializedViews.h"
#include "engine/QueryExecutionContext.h"
#include "index/IndexImpl.h"
#include "index/TextIndexBuilder.h"
#include "libqlever/QleverTypes.h"
#include "parser/SparqlParser.h"
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
          IndexAndViews{Index{allocator_}, MaterializedViewsManager{}})},
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
  Index& index = snapshot->index_;
  MaterializedViewsManager& materializedViewsManager =
      snapshot->materializedViewsManager_;

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
std::string Qlever::query(const QueryPlan& queryPlan,
                          ad_utility::MediaType mediaType) const {
  const auto& [qet, qec, parsedQuery] = queryPlan;
  ad_utility::Timer timer{ad_utility::Timer::Started};

  // TODO<joka921> For cancellation we have to call
  // `recursivelySetCancellationHandle` (see `Server::parseAndPlan`).
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  std::string result;
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  auto responseGenerator = ExportQueryExecutionTrees::computeResult(
      parsedQuery, *qet, mediaType, timer, std::move(handle));
  for (const auto& batch : responseGenerator) {
    result += batch;
  }
#else
  ad_utility::streams::StringBatcher yielder{
      [&result](std::string_view batch) { result.append(batch); }};
  ExportQueryExecutionTrees::computeResult(parsedQuery, *qet, mediaType, timer,
                                           std::move(handle),
                                           std::ref(yielder));

#endif
  return result;
}

// _____________________________________________________________________________
void Qlever::queryAndPinResultWithName(
    QueryExecutionContext::PinResultWithName options, std::string query) {
  auto queryPlan = parseAndPlanQuery(std::move(query));
  auto& [qet, qec, parsedQuery] = queryPlan;
  qec->pinResultWithName() = std::move(options);
  [[maybe_unused]] auto result = this->query(queryPlan);
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
Qlever::QueryPlan Qlever::parseAndPlanQuery(std::string query) const {
  auto indexAndViews = indexAndViewsSnapshot();
  auto qecPtr = createQueryExecutionContext(
      std::move(indexAndViews), [](std::string) {}, false, false,
      disableCaching_);
  // TODO<joka921> support Dataset clauses.
  auto parsedQuery = SparqlParser::parseQuery(
      &qecPtr->getIndex().getImpl().encodedIriManager(), std::move(query), {});
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp{qecPtr.get(), handle};
  qp.setEnablePatternTrick(enablePatternTrick_);
  auto qet = qp.createExecutionTree(parsedQuery);
  qet.isRoot() = true;

  auto qetPtr = std::make_shared<QueryExecutionTree>(std::move(qet));
  return {qetPtr, std::move(qecPtr), std::move(parsedQuery)};
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
void Qlever::writeMaterializedView(std::string name, std::string query) const {
  materializedViewsManager()->writeViewToDisk(
      std::move(name), parseAndPlanQuery(std::move(query)));
}

// ___________________________________________________________________________
bool Qlever::isMaterializedViewLoaded(const std::string& name) const {
  return materializedViewsManager()->isViewLoaded(name);
}

// ___________________________________________________________________________
void Qlever::loadMaterializedView(std::string name) const {
  materializedViewsManager()->loadView(name);
}

// ___________________________________________________________________________
std::shared_ptr<QueryExecutionContext> Qlever::createQueryExecutionContext(
    std::shared_ptr<IndexAndViews> indexAndViews,
    std::function<void(std::string)> updateCallback, bool pinSubtrees,
    bool pinResult,
    QueryExecutionContext::DisableCaching disableCaching) const {
  std::shared_ptr<Index> index{indexAndViews, &indexAndViews->index_};
  auto& viewsManagerRef = indexAndViews->materializedViewsManager_;
  std::shared_ptr<MaterializedViewsManager> viewsManager{
      std::move(indexAndViews), &viewsManagerRef};
  return std::make_shared<QueryExecutionContext>(
      std::move(index), &cache_, allocator_, sortPerformanceEstimator_,
      &namedResultCache_, std::move(viewsManager), std::move(updateCallback),
      pinSubtrees, pinResult, disableCaching);
}
}  // namespace qlever
