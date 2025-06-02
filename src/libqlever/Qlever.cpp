//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "libqlever/Qlever.h"

#include "index/IndexImpl.h"
#include "index/TextIndexBuilder.h"

namespace qlever {

// _____________________________________________________________________________
Qlever::Qlever(const EngineConfig& config)
    : allocator_{ad_utility::AllocatorWithLimit<Id>{
          ad_utility::makeAllocationMemoryLeftThreadsafeObject(
              config.memoryLimit.value())}},
      index_{allocator_} {
  ad_utility::setGlobalLoggingStream(&ignoreLogStream);
  // This also directly triggers the update functions and propagates the
  // values of the parameters to the cache.
  RuntimeParameters().setOnUpdateAction<"cache-max-num-entries">(
      [this](size_t newValue) { cache_.setMaxNumEntries(newValue); });
  RuntimeParameters().setOnUpdateAction<"cache-max-size">(
      [this](ad_utility::MemorySize newValue) { cache_.setMaxSize(newValue); });
  RuntimeParameters().setOnUpdateAction<"cache-max-size-single-entry">(
      [this](ad_utility::MemorySize newValue) {
        cache_.setMaxSizeSingleEntry(newValue);
      });
  index_.usePatterns() = !config.noPatterns;
  enablePatternTrick_ = !config.noPatterns;
  index_.loadAllPermutations() = !config.onlyPsoAndPos;

  // Initialize the index.
  index_.createFromOnDiskIndex(config.baseName, config.persistUpdates);
  ;
  if (config.text) {
    index_.addTextFromOnDiskIndex();
  }

  sortPerformanceEstimator_.computeEstimatesExpensively(
      allocator_, index_.numTriples().normalAndInternal_() *
                      PERCENTAGE_OF_TRIPLES_FOR_SORT_ESTIMATE / 100);
}

// _____________________________________________________________________________
void Qlever::buildIndex(IndexBuilderConfig config) {
  ad_utility::setGlobalLoggingStream(&ignoreLogStream);
  Index index{ad_utility::makeUnlimitedAllocator<Id>()};

  if (config.memoryLimit.has_value()) {
    index.memoryLimitIndexBuilding() = config.memoryLimit.value();
  }
  if (config.parserBufferSize.has_value()) {
    index.parserBufferSize() = config.parserBufferSize.value();
  }

  // If no text index name was specified, take the part of the wordsfile after
  // the last slash.
  if (config.textIndexName.empty() && !config.wordsfile.empty()) {
    config.textIndexName =
        ad_utility::getLastPartOfString(config.wordsfile, '/');
  }
  try {
    index.setKbName(config.kbIndexName);
    index.setTextName(config.textIndexName);
    index.usePatterns() = !config.noPatterns;
    index.setOnDiskBase(config.baseName);
    index.setKeepTempFiles(config.keepTemporaryFiles);
    index.setSettingsFile(config.settingsFile);
    index.loadAllPermutations() = !config.onlyPsoAndPos;

    if (!config.onlyAddTextIndex) {
      AD_CONTRACT_CHECK(!config.inputFiles.empty());
      index.createFromFiles(config.inputFiles);
    }

    const auto& wordsfile = config.wordsfile;
    const auto& docsfile = config.docsfile;
    bool wordsAndDocsFileSpecified = !(wordsfile.empty() || docsfile.empty());

    if (!(wordsAndDocsFileSpecified ||
          (wordsfile.empty() && docsfile.empty()))) {
      throw std::runtime_error(absl::StrCat(
          "Only specified ", wordsfile.empty() ? "docsfile" : "wordsfile",
          ". Both or none of docsfile and wordsfile have to be given to build "
          "text index. If none are given the option to add words from literals "
          "has to be true. For details see --help."));
    }
    auto textIndexBuilder = TextIndexBuilder(
        ad_utility::makeUnlimitedAllocator<Id>(), index.getOnDiskBase());

    if (wordsAndDocsFileSpecified || config.addWordsFromLiterals) {
      textIndexBuilder.buildTextIndexFile(
          wordsAndDocsFileSpecified
              ? std::optional{std::pair{wordsfile, docsfile}}
              : std::nullopt,
          config.addWordsFromLiterals, config.textScoringMetric,
          {config.bScoringParam, config.kScoringParam});
    }

    if (!docsfile.empty()) {
      textIndexBuilder.buildDocsDB(docsfile);
    }
  } catch (std::exception& e) {
    LOG(ERROR) << "Creating the index for QLever failed with the following "
                  "exception: "
               << e.what() << std::endl;
    throw;
  }
}

// ___________________________________________________________________________
std::string Qlever::query(std::string query, ad_utility::MediaType mediaType) {
  QueryExecutionContext qec{index_, &cache_, allocator_,
                            sortPerformanceEstimator_};
  auto parsedQuery = SparqlParser::parseQuery(query);
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp{&qec, handle};
  qp.setEnablePatternTrick(enablePatternTrick_);
  auto qet = qp.createExecutionTree(parsedQuery);
  qet.isRoot() = true;
  auto& limitOffset = parsedQuery._limitOffset;

  // TODO<joka921> For cancellation we have to call
  // `recursivelySetCancellationHandle` (see `Server::parseAndPlan`).

  ad_utility::Timer timer{ad_utility::Timer::Started};
  auto responseGenerator = ExportQueryExecutionTrees::computeResult(
      parsedQuery, qet, mediaType, timer, std::move(handle));
  std::string result;
  for (const auto& batch : responseGenerator) {
    result += batch;
  }
  return result;
}

// ___________________________________________________________________________
Qlever::QueryPlan Qlever::parseAndPlanQuery(std::string query) {
  auto qecPtr = std::make_shared<QueryExecutionContext>(
      index_, &cache_, allocator_, sortPerformanceEstimator_);
  auto parsedQuery = SparqlParser::parseQuery(std::move(query));
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp{qecPtr.get(), handle};
  qp.setEnablePatternTrick(enablePatternTrick_);
  auto qet = qp.createExecutionTree(parsedQuery);
  qet.isRoot() = true;

  auto qetPtr = std::make_shared<QueryExecutionTree>(std::move(qet));
  // TODO<joka921> For cancellation we have to call
  // `recursivelySetCancellationHandle` (see `Server::parseAndPlan`).
  return {qetPtr, std::move(qecPtr), std::move(parsedQuery)};
}

void IndexBuilderConfig::validate() {
  if (kScoringParam < 0) {
    throw std::invalid_argument("The value of bm25-k must be >= 0");
  }
  if (bScoringParam < 0 || bScoringParam > 1) {
    throw std::invalid_argument(
        "The value of bm25-b must be between and "
        "including 0 and 1");
  }
}

}  // namespace qlever
