// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "libqlever/Qlever.h"

#include <absl/strings/str_cat.h>

#include "engine/ExportQueryExecutionTrees.h"
#include "index/IndexImpl.h"
#include "index/TextIndexBuilder.h"
#include "parser/SparqlParser.h"
#include "util/Serializer/ByteBufferSerializer.h"
#include "util/Serializer/CompressedSerializer.h"
#include "util/Serializer/SerializeString.h"

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
  globalRuntimeParameters.wlock()->cacheMaxNumEntries_.setOnUpdateAction(
      [this](size_t newValue) { cache_.setMaxNumEntries(newValue); });
  globalRuntimeParameters.wlock()->cacheMaxSize_.setOnUpdateAction(
      [this](ad_utility::MemorySize newValue) { cache_.setMaxSize(newValue); });
  globalRuntimeParameters.wlock()->cacheMaxSizeSingleEntry_.setOnUpdateAction(
      [this](ad_utility::MemorySize newValue) {
        cache_.setMaxSizeSingleEntry(newValue);
      });

  // Load the index from disk.
  index_.usePatterns() = enablePatternTrick_;
  index_.loadAllPermutations() = !config.onlyPsoAndPos_;
  index_.dontLoadPermutations() = config.dontLoadPermutations_;
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

// ___________________________________________________________________________
std::vector<char> Qlever::serializeToBlob() const {
  using ad_utility::serialization::ByteBufferWriteSerializer;
  using ad_utility::serialization::ZstdWriteSerializer;

  // Magic header for blob format validation.
  const std::string MAGIC_HEADER = "QLVBLOB";
  constexpr uint32_t BLOB_VERSION = 1;

  // First, write to an uncompressed buffer.
  ByteBufferWriteSerializer bufferSerializer;

  // Write magic header.
  bufferSerializer | MAGIC_HEADER;

  // Write version.
  bufferSerializer | BLOB_VERSION;

  // Now compress the rest of the data.
  ZstdWriteSerializer compressedSerializer{std::move(bufferSerializer)};

  // Serialize metadata JSON.
  std::string metadataJsonString = index_.getImpl().configurationJson().dump();
  compressedSerializer | metadataJsonString;

  // Serialize vocabulary.
  index_.getImpl().getVocab().writeToSerializer(compressedSerializer);

  // Serialize named result cache.
  namedResultCache_.writeToSerializer(compressedSerializer);

  // Close the compressed serializer to flush the last block.
  // TODO<joka921> The close function deletes the underlying serializer, which
  // might be undesried, if we want to get data out.
  return std::move(compressedSerializer).underlyingSerializer().data();
}

// ___________________________________________________________________________
void Qlever::deserializeFromBlob(const std::vector<char>& blob) {
  using ad_utility::serialization::ByteBufferReadSerializer;
  using ad_utility::serialization::ZstdReadSerializer;

  ByteBufferReadSerializer bufferSerializer{blob};

  // Validate magic header.
  constexpr std::string_view MAGIC_HEADER = "QLVBLOB";
  constexpr uint32_t BLOB_VERSION = 1;

  std::string headerIn;
  bufferSerializer | headerIn;
  if (headerIn != MAGIC_HEADER) {
    throw std::runtime_error(
        "Invalid blob format: magic header mismatch. Expected a QLever blob "
        "file.");
  }

  // Validate version.
  uint32_t version;
  bufferSerializer | version;
  if (version != BLOB_VERSION) {
    throw std::runtime_error(absl::StrCat(
        "Incompatible blob version. Expected version ", BLOB_VERSION,
        " but found version ", version,
        ". Please regenerate the blob with the current version of QLever."));
  }

  // The rest of the data is compressed.
  ZstdReadSerializer compressedSerializer{std::move(bufferSerializer)};

  // The rest is handled by IndexImpl::createFromBlobData.
  // It will deserialize: metadata JSON, vocabulary, and we'll handle the cache
  // separately.
  index_.getImpl().createFromBlobData(compressedSerializer, false);

  // Deserialize named result cache.
  namedResultCache_.readFromSerializer(compressedSerializer, allocator_,
                                       *index_.getBlankNodeManager());
}
}  // namespace qlever
