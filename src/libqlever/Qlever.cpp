//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "libqlever/Qlever.h"

#include "index/IndexImpl.h"

namespace qlever {
static std::string getStxxlConfigFileName(const string& location) {
  return absl::StrCat(location, ".stxxl");
}

static std::string getStxxlDiskFileName(const string& location,
                                        const string& tail) {
  return absl::StrCat(location, tail, ".stxxl-disk");
}

// Write a .stxxl config-file.
// All we want is sufficient space somewhere with enough space.
// We can use the location of input files and use a constant size for now.
// The required size can only be estimated anyway, since index size
// depends on the structure of words files rather than their size only,
// because of the "multiplications" performed.
static void writeStxxlConfigFile(const string& location, const string& tail) {
  string stxxlConfigFileName = getStxxlConfigFileName(location);
  ad_utility::File stxxlConfig(stxxlConfigFileName, "w");
  auto configFile = ad_utility::makeOfstream(stxxlConfigFileName);
  // Inform stxxl about .stxxl location
  setenv("STXXLCFG", stxxlConfigFileName.c_str(), true);
  configFile << "disk=" << getStxxlDiskFileName(location, tail) << ","
             << STXXL_DISK_SIZE_INDEX_BUILDER << ",syscall\n";
}

// _____________________________________________________________________________
Qlever::Qlever(const QleverConfig& config)
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

  index_.getImpl().setVocabularyTypeForIndexBuilding(config.vocabularyType_);

  // Init the index.
  index_.createFromOnDiskIndex(config.baseName);
  // TODO<joka921> Enable the loading of the text index via the QLever lib.
  /*
  if (useText) {
    index_.addTextFromOnDiskIndex();
  }
   */

  sortPerformanceEstimator_.computeEstimatesExpensively(
      allocator_, index_.numTriples().normalAndInternal_() *
                      PERCENTAGE_OF_TRIPLES_FOR_SORT_ESTIMATE / 100);
}

// _____________________________________________________________________________
void Qlever::buildIndex(QleverConfig config) {
  ad_utility::setGlobalLoggingStream(&ignoreLogStream);
  Index index{ad_utility::makeUnlimitedAllocator<Id>()};

  if (config.memoryLimit.has_value()) {
    index.memoryLimitIndexBuilding() = config.memoryLimit.value();
  }
  // If no text index name was specified, take the part of the wordsfile after
  // the last slash.
  if (config.textIndexName.empty() && !config.wordsfile.empty()) {
    config.textIndexName =
        ad_utility::getLastPartOfString(config.wordsfile, '/');
  }
  try {
    LOG(TRACE) << "Configuring STXXL..." << std::endl;
    size_t posOfLastSlash = config.baseName.rfind('/');
    string location = config.baseName.substr(0, posOfLastSlash + 1);
    string tail = config.baseName.substr(posOfLastSlash + 1);
    writeStxxlConfigFile(location, tail);
    string stxxlFileName = getStxxlDiskFileName(location, tail);
    LOG(TRACE) << "done." << std::endl;

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

    if (!config.wordsfile.empty() || config.addWordsFromLiterals) {
      index.addTextFromContextFile(config.wordsfile,
                                   config.addWordsFromLiterals);
    }

    if (!config.docsfile.empty()) {
      index.buildDocsDB(config.docsfile);
    }
    ad_utility::deleteFile(stxxlFileName, false);
  } catch (std::exception& e) {
    LOG(ERROR) << "Creating the index for QLever failed with the following "
                  "exception: "
               << e.what() << std::endl;
    throw;
  }
}

// ___________________________________________________________________________
std::string Qlever::query(std::string query) {
  QueryExecutionContext qec{index_, &cache_, allocator_,
                            sortPerformanceEstimator_, &namedQueryCache_};
  auto parsedQuery = SparqlParser::parseQuery(query);
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp{&qec, handle};
  qp.setEnablePatternTrick(enablePatternTrick_);
  auto qet = qp.createExecutionTree(parsedQuery);
  qet.isRoot() = true;
  auto& limitOffset = parsedQuery._limitOffset;

  // TODO<joka921> For cancellation we have to call
  // `recursivelySetCancellationHandle` (see `Server::parseAndPlan`).

  // TODO<joka921> The following interface looks fishy and should be
  // incorporated directly in the query planner or somewhere else.
  // (it is used identically in `Server.cpp`.

  // Make sure that the offset is not applied again when exporting the result
  // (it is already applied by the root operation in the query execution
  // tree). Note that we don't need this for the limit because applying a
  // fixed limit is idempotent.
  AD_CORRECTNESS_CHECK(limitOffset._offset >=
                       qet.getRootOperation()->getLimit()._offset);
  limitOffset._offset -= qet.getRootOperation()->getLimit()._offset;

  ad_utility::Timer timer{ad_utility::Timer::Started};
  auto responseGenerator = ExportQueryExecutionTrees::computeResult(
      parsedQuery, qet, ad_utility::MediaType::sparqlJson, timer,
      std::move(handle));
  std::string result;
  std::cout << "Writing the result:" << std::endl;
  for (const auto& batch : responseGenerator) {
    result += batch;
  }
  return result;
}
// ___________________________________________________________________________
// TODO<joka921> A lot of code duplication here.
void Qlever::pinNamed(std::string query, std::string name) {
  QueryExecutionContext qec{index_, &cache_, allocator_,
                            sortPerformanceEstimator_, &namedQueryCache_};
  qec.pinWithExplicitName() = std::move(name);
  auto parsedQuery = SparqlParser::parseQuery(query);
  auto handle = std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp{&qec, handle};
  qp.setEnablePatternTrick(enablePatternTrick_);
  auto qet = qp.createExecutionTree(parsedQuery);
  qet.isRoot() = true;
  auto& limitOffset = parsedQuery._limitOffset;

  // TODO<joka921> For cancellation we have to call
  // `recursivelySetCancellationHandle` (see `Server::parseAndPlan`).

  // TODO<joka921> The following interface looks fishy and should be
  // incorporated directly in the query planner or somewhere else.
  // (it is used identically in `Server.cpp`.

  // Make sure that the offset is not applied again when exporting the result
  // (it is already applied by the root operation in the query execution
  // tree). Note that we don't need this for the limit because applying a
  // fixed limit is idempotent.
  AD_CORRECTNESS_CHECK(limitOffset._offset >=
                       qet.getRootOperation()->getLimit()._offset);
  limitOffset._offset -= qet.getRootOperation()->getLimit()._offset;

  ad_utility::Timer timer{ad_utility::Timer::Started};
  auto responseGenerator = ExportQueryExecutionTrees::computeResult(
      parsedQuery, qet, ad_utility::MediaType::sparqlJson, timer,
      std::move(handle));
  std::string result;
  std::cout << "Writing the result:" << std::endl;
  for (const auto& batch : responseGenerator) {
    result += batch;
  }
}
}  // namespace qlever
