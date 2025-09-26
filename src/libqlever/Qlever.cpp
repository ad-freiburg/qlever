// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "libqlever/Qlever.h"

#include "engine/ExportQueryExecutionTrees.h"
#include "index/IndexImpl.h"
#include "index/TextIndexBuilder.h"
#include "parser/SparqlParser.h"

namespace qlever {

// _____________________________________________________________________________
Qlever::Qlever(const EngineConfig& config)
    : allocator_{ad_utility::AllocatorWithLimit<Id>{
          ad_utility::makeAllocationMemoryLeftThreadsafeObject(
              config.memoryLimit_.value_or(DEFAULT_MEM_FOR_QUERIES))}},
      index_{allocator_},
      enablePatternTrick_{!config.noPatterns_} {
  // Set runtime parameters relevant for caching and propagate them to the
  // cache.
  RuntimeParameters().setOnUpdateAction<"cache-max-num-entries">(
      [this](size_t newValue) { cache_.setMaxNumEntries(newValue); });
  RuntimeParameters().setOnUpdateAction<"cache-max-size">(
      [this](ad_utility::MemorySize newValue) { cache_.setMaxSize(newValue); });
  RuntimeParameters().setOnUpdateAction<"cache-max-size-single-entry">(
      [this](ad_utility::MemorySize newValue) {
        cache_.setMaxSizeSingleEntry(newValue);
      });

  // Load the index from disk.
  index_.usePatterns() = enablePatternTrick_;
  index_.loadAllPermutations() = !config.onlyPsoAndPos_;
  index_.createFromOnDiskIndex(config.baseName_, config.persistUpdates_);
  if (config.loadTextIndex_) {
    index_.addTextFromOnDiskIndex();
  }

  // Estimate the cost of sorting operations (needed for query planning).
  sortPerformanceEstimator_.computeEstimatesExpensively(
      allocator_, index_.numTriples().normalAndInternal_() *
                      PERCENTAGE_OF_TRIPLES_FOR_SORT_ESTIMATE / 100);
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
  index.getImpl().setVocabularyTypeForIndexBuilding(config.vocabType_);
  index.getImpl().setPrefixesForEncodedValues(config.prefixesForIdEncodedIris_);

  // Build text index if requested (various options).
  if (!config.onlyAddTextIndex_) {
    AD_CONTRACT_CHECK(!config.inputFiles_.empty());
    index.createFromFiles(config.inputFiles_);
  }
  auto textIndexBuilder = TextIndexBuilder(
      ad_utility::makeUnlimitedAllocator<Id>(), index.getOnDiskBase());
  if (config.wordsAndDocsFileSpecified() || config.addWordsFromLiterals_) {
    textIndexBuilder.buildTextIndexFile(
        config.wordsAndDocsFileSpecified()
            ? std::optional{std::pair{config.wordsfile_, config.docsfile_}}
            : std::nullopt,
        config.addWordsFromLiterals_, config.textScoringMetric_,
        {config.bScoringParam_, config.kScoringParam_});
  }
  if (!config.docsfile_.empty()) {
    textIndexBuilder.buildDocsDB(config.docsfile_);
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
  auto responseGenerator = ExportQueryExecutionTrees::computeResult(
      parsedQuery, *qet, mediaType, timer, std::move(handle));
  std::string result;
  for (const auto& batch : responseGenerator) {
    result += batch;
  }
  return result;
}

// _____________________________________________________________________________
void Qlever::queryAndPinResultWithName(std::string name, std::string query) {
  auto queryPlan = parseAndPlanQuery(std::move(query));
  auto& [qet, qec, parsedQuery] = queryPlan;
  qec->pinResultWithName() = std::move(name);
  [[maybe_unused]] auto result = this->query(queryPlan);
}

// _____________________________________________________________________________
void Qlever::clearNamedResultCache() { namedResultCache_.clear(); }

// _____________________________________________________________________________
void Qlever::eraseResultWithName(std::string name) {
  namedResultCache_.erase(name);
}

// ___________________________________________________________________________
Qlever::QueryPlan Qlever::parseAndPlanQuery(std::string query) const {
  auto qecPtr = std::make_shared<QueryExecutionContext>(
      index_, &cache_, allocator_, sortPerformanceEstimator_,
      &namedResultCache_);
  // TODO<joka921> support Dataset clauses.
  auto parsedQuery = SparqlParser::parseQuery(
      &index_.getImpl().encodedIriManager(), std::move(query), {});
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

}  // namespace qlever
