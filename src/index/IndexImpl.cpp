// Copyright 2014 - 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <buchhold@cs.uni-freiburg.de> [2014-2017]
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/IndexImpl.h"

#include <absl/strings/str_join.h>

#include <cstdio>
#include <future>
#include <numeric>
#include <optional>
#include <utility>

#include "CompilationInfo.h"
#include "backports/algorithm.h"
#include "engine/AddCombinedRowToTable.h"
#include "index/Index.h"
#include "index/IndexFormatVersion.h"
#include "index/VocabularyMerger.h"
#include "parser/ParallelParseBuffer.h"
#include "util/BatchedPipeline.h"
#include "util/CachingMemoryResource.h"
#include "util/Generator.h"
#include "util/HashMap.h"
#include "util/InputRangeUtils.h"
#include "util/Iterators.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"
#include "util/ProgressBar.h"
#include "util/ThreadSafeQueue.h"
#include "util/Timer.h"
#include "util/TypeTraits.h"
#include "util/Views.h"

using std::array;
using namespace ad_utility::memory_literals;

// During the index building we typically have two permutations present at the
// same time, as we directly push the triples from the first sorting to the
// second sorting. We therefore have to adjust the amount of memory per external
// sorter.
static constexpr size_t NUM_EXTERNAL_SORTERS_AT_SAME_TIME = 2u;

// _____________________________________________________________________________
IndexImpl::IndexImpl(ad_utility::AllocatorWithLimit<Id> allocator)
    : allocator_{std::move(allocator)} {
  globalSingletonIndex_ = this;
  deltaTriples_.emplace(*this);
};

// _____________________________________________________________________________
IndexBuilderDataAsFirstPermutationSorter IndexImpl::createIdTriplesAndVocab(
    std::shared_ptr<RdfParserBase> parser) {
  auto indexBuilderData =
      passFileForVocabulary(std::move(parser), numTriplesPerBatch_);

  auto isQleverInternalTriple = [&indexBuilderData](const auto& triple) {
    auto internal = [&indexBuilderData](Id id) {
      return indexBuilderData.vocabularyMetaData_.isQleverInternalId(id);
    };
    return internal(triple[0]) || internal(triple[1]) || internal(triple[2]);
  };

  auto firstSorter = convertPartialToGlobalIds(
      *indexBuilderData.idTriples, indexBuilderData.actualPartialSizes,
      NUM_TRIPLES_PER_PARTIAL_VOCAB, isQleverInternalTriple);

  return {indexBuilderData, std::move(firstSorter)};
}

// _____________________________________________________________________________
std::unique_ptr<RdfParserBase> IndexImpl::makeRdfParser(
    const std::vector<Index::InputFileSpecification>& files) const {
  AD_CONTRACT_CHECK(
      parserBufferSize().getBytes() > 0,
      "The buffer size of the RDF parser must be greater than zero");
  AD_CONTRACT_CHECK(
      memoryLimitIndexBuilding().getBytes() > 0,
      " memory limit for index building must be greater than zero");
  return std::make_unique<RdfMultifileParser>(files, &encodedIriManager(),
                                              parserBufferSize());
}

// Several helper functions for joining the OSP permutation with the patterns.
namespace {
// Return an input range of the blocks that are returned by the external sorter
// to which `sorterPtr` points. Only the subset/permutation specified by the
// `columnIndices` will be returned for each block.
template <typename T1, typename T2>
static auto lazyScanWithPermutedColumns(T1& sorterPtr, T2 columnIndices) {
  auto setSubset = [columnIndices](auto& idTable) {
    idTable.setColumnSubset(columnIndices);
    return std::move(idTable);
  };

  return ad_utility::CachingTransformInputRange{
      ad_utility::OwningView{sorterPtr->template getSortedBlocks<0>()},
      setSubset};
}

// Perform a lazy optional block join on the first column of `leftInput` and
// `rightInput`. The `resultCallback` will be called for each block of resulting
// rows. Assumes that `leftInput` and `rightInput` have 6 columns in total, so
// the result will have 5 columns.
template <typename T1, typename T2, typename F>
static auto lazyOptionalJoinOnFirstColumn(T1& leftInput, T2& rightInput,
                                          F resultCallback) {
  auto projection = [](const auto& row) -> Id { return row[0]; };
  auto projectionForComparator = [](const auto& rowOrId) -> const Id& {
    using T = std::decay_t<decltype(rowOrId)>;
    if constexpr (ad_utility::SimilarTo<T, Id>) {
      return rowOrId;
    } else {
      return rowOrId[0];
    }
  };
  auto comparator = [&projectionForComparator](const auto& l, const auto& r) {
    return projectionForComparator(l).compareWithoutLocalVocab(
               projectionForComparator(r)) < 0;
  };

  // There are 6 columns in the result (4 from the triple + graph ID, as well as
  // subject patterns of the subject and object).
  IdTable outputTable{NumColumnsIndexBuilding + 2,
                      ad_utility::makeUnlimitedAllocator<Id>()};
  // The first argument is the number of join columns.
  auto rowAdder = ad_utility::AddCombinedRowToIdTable{
      1,
      std::move(outputTable),
      std::make_shared<ad_utility::CancellationHandle<>>(),
      true,
      BUFFER_SIZE_JOIN_PATTERNS_WITH_OSP(),
      resultCallback};

  ad_utility::zipperJoinForBlocksWithoutUndef(leftInput, rightInput, comparator,
                                              rowAdder, projection, projection,
                                              ad_utility::OptionalJoinTag{});
  rowAdder.flush();
}

// Return the array {2, 1, 0, 3, 4, 5, ..., numColumns - 1};
template <size_t numColumns>
constexpr auto makePermutationFirstThirdSwitched = []() {
  static_assert(numColumns >= 3);
  std::array<ColumnIndex, numColumns> permutation{};
  std::iota(permutation.begin(), permutation.end(), ColumnIndex{0});
  std::swap(permutation[0], permutation[2]);
  return permutation;
};
// In the pattern column replace UNDEF (which is created by the optional join)
// by the special `NoPattern` ID and undo the permutation of the columns that
// was only needed for the join algorithm.
template <typename T>
static auto fixBlockAfterPatternJoin(T block) {
  // The permutation must be the inverse of the original permutation, which just
  // switches the third column (the object) into the first column (where the
  // join column is expected by the algorithms).
  static QL_CONSTEXPR auto permutation =
      makePermutationFirstThirdSwitched<NumColumnsIndexBuilding + 2>();
  block.value().setColumnSubset(permutation);
  ql::ranges::for_each(
      block.value().getColumn(ADDITIONAL_COLUMN_INDEX_OBJECT_PATTERN),
      [](Id& id) {
        id = id.isUndefined() ? Id::makeFromInt(Pattern::NoPattern) : id;
      });
  return std::move(block.value()).template toStatic<0>();
}
}  // namespace

// ____________________________________________________________________________
template <typename InternalTripleSorter>
std::unique_ptr<ExternalSorter<SortByPSO, NumColumnsIndexBuilding + 2>>
IndexImpl::buildOspWithPatterns(
    PatternCreator::TripleSorter sortersFromPatternCreator,
    InternalTripleSorter& internalTripleSorter) {
  auto&& [hasPatternPredicateSortedByPSO, secondSorter] =
      sortersFromPatternCreator;
  // We need the patterns twice: once for the additional column, and once for
  // the additional permutation.
  hasPatternPredicateSortedByPSO->moveResultOnMerge() = false;

  // Lambda that creates and returns a generator that reads the `has-pattern`
  // relation. Only reads columns 0 and 2 (subject and object pattern), since
  // column 1 is always the `has-pattern` predicate. The relation is sorted by
  // PSO, so the result is sorted by subject and then object.
  //
  // NOTE: We will iterate over the `hasPatternPredicateSortedByPSO` twice. It
  // is important that the respective generators are not active at the same
  // time, otherwise the assertion `!mergeIsActive_.load()` in
  // `CompressedExternalIdTableSorter::getSortedBlocks` will fail. We therefore
  // scope the lifetime of this generator inside the `joinWithPatternThread`.
  // That is, when we are done with the join (which we now explicitly wait
  // for before starting the second generator, see below), the generator is
  // destroyed.
  auto getLazyPatternScan = [&]() {
    return lazyScanWithPermutedColumns(hasPatternPredicateSortedByPSO,
                                       std::array<ColumnIndex, 2>{0, 2});
  };

  // Create a producer-consumer queue with space for up to four `IdTable`s.
  ad_utility::data_structures::ThreadSafeQueue<IdTable> queue{4};

  // The permutation (2, 1, 0, 3) switches the third column (the object) with
  // the first column (where the join column is expected by the algorithms).
  // This permutation is reverted as part of the `fixBlockAfterPatternJoin`
  // function.
  auto ospAsBlocksTransformed = lazyScanWithPermutedColumns(
      secondSorter,
      makePermutationFirstThirdSwitched<NumColumnsIndexBuilding + 1>());

  // Run the actual join between the OSP permutation and the `has-pattern`
  // predicate on a background thread. The result will be pushed to the `queue`
  // so that we can consume it asynchronously.
  ad_utility::JThread joinWithPatternThread{
      [&queue, &ospAsBlocksTransformed,
       lazyPatternScan = getLazyPatternScan()]() mutable {
        // Setup the callback for the join that will buffer the results and push
        // them to the queue.
        IdTable outputBufferTable{NumColumnsIndexBuilding + 2,
                                  ad_utility::makeUnlimitedAllocator<Id>()};
        auto pushToQueue = [&, bufferSize =
                                   BUFFER_SIZE_JOIN_PATTERNS_WITH_OSP().load()](
                               IdTable& table, LocalVocab&) {
          if (table.numRows() >= bufferSize) {
            if (!outputBufferTable.empty()) {
              queue.push(std::move(outputBufferTable));
              outputBufferTable.clear();
            }
            queue.push(std::move(table));
          } else {
            outputBufferTable.insertAtEnd(table);
            if (outputBufferTable.size() >= bufferSize) {
              queue.push(std::move(outputBufferTable));
              outputBufferTable.clear();
            }
          }
          table.clear();
        };

        lazyOptionalJoinOnFirstColumn(ospAsBlocksTransformed, lazyPatternScan,
                                      pushToQueue);

        // We still might have some buffered results left, push them to the
        // queue and then finish the queue.
        if (!outputBufferTable.empty()) {
          queue.push(std::move(outputBufferTable));
          outputBufferTable.clear();
        }
        queue.finish();
      }};

  // Set up a generator that yields blocks with the following columns:
  // S P O PatternOfS PatternOfO, sorted by OPS.
  // If an exception occurs in the block that is consuming the blocks yielded
  // from this generator, we have to explicitly finish the `queue`, otherwise
  // there will be a deadlock because the threads involved in the queue can
  // never join.
  auto get{[&queue]() -> std::optional<IdTableStatic<0>> {
    if (auto block = queue.pop()) {
      return fixBlockAfterPatternJoin(std::move(block));
    }
    return std::nullopt;
  }};
  using namespace ad_utility;
  auto blockGenerator = InputRangeTypeErased{
      CallbackOnEndView{InputRangeFromGetCallable{std::move(get)},
                        [&queue]() { queue.finish(); }}};
  // Actually create the permutations.
  auto thirdSorter =
      makeSorterPtr<ThirdPermutation, NumColumnsIndexBuilding + 2>("third");
  createSecondPermutationPair(NumColumnsIndexBuilding + 2,
                              std::move(blockGenerator), *thirdSorter);
  joinWithPatternThread.join();
  secondSorter->clear();
  // Add the `ql:has-pattern` predicate to the sorter such that it will become
  // part of the PSO and POS permutation.
  AD_LOG_INFO << "Adding " << hasPatternPredicateSortedByPSO->size()
              << " triples to the POS and PSO permutation for "
                 "the internal `ql:has-pattern` ..."
              << std::endl;
  static_assert(NumColumnsIndexBuilding == 4,
                "When adding additional payload columns, the following code "
                "has to be changed");
  Id internalGraph = idOfInternalGraphDuringIndexBuilding_.value();
  // Note: We are getting the patterns sorted by PSO and then sorting them again
  // by PSO.
  // TODO<joka921> Simply get the output unsorted (should be cheaper).
  for (const auto& row : hasPatternPredicateSortedByPSO->sortedView()) {
    internalTripleSorter.push(
        std::array{row[0], row[1], row[2], internalGraph});
  }
  hasPatternPredicateSortedByPSO->clear();
  return thirdSorter;
}

// _____________________________________________________________________________
template <typename InternalTriplePsoSorter>
std::pair<size_t, size_t> IndexImpl::createInternalPSOandPOS(
    InternalTriplePsoSorter&& internalTriplesPsoSorter) {
  auto onDiskBaseBackup = onDiskBase_;
  auto configurationJsonBackup = configurationJson_;
  onDiskBase_.append(QLEVER_INTERNAL_INDEX_INFIX);

  // TODO<joka921> As soon as `uniqueBlockView` is no longer a `generator` the
  // explicit `BlocksOfTriples` constructor can be removed again.
  auto internalTriplesUnique = BlocksOfTriples{ad_utility::uniqueBlockView(
      internalTriplesPsoSorter.template getSortedBlocks<0>())};
  createPSOAndPOSImpl(NumColumnsIndexBuilding, std::move(internalTriplesUnique),
                      false);
  onDiskBase_ = std::move(onDiskBaseBackup);
  // The "normal" triples from the "internal" index builder are actually
  // internal.
  size_t numTriplesInternal =
      static_cast<NumNormalAndInternal>(configurationJson_["num-triples"])
          .normal;
  size_t numPredicatesInternal =
      static_cast<NumNormalAndInternal>(configurationJson_["num-predicates"])
          .normal;
  configurationJson_ = std::move(configurationJsonBackup);
  return {numTriplesInternal, numPredicatesInternal};
}

// _____________________________________________________________________________
void IndexImpl::updateInputFileSpecificationsAndLog(
    std::vector<Index::InputFileSpecification>& spec,
    std::optional<bool> parallelParsingSpecifiedViaJson) {
  std::string pleaseUseParallelParsingOption =
      "please use the command-line option --parse-parallel or -p";
  // Parallel parsing specified in the `settings.json` file. This is deprecated
  // for a single input stream and forbidden for multiple input streams.
  if (parallelParsingSpecifiedViaJson.has_value()) {
    if (spec.size() == 1) {
      AD_LOG_WARN
          << "Parallel parsing set in the `.settings.json` file; this is "
             "deprecated, "
          << pleaseUseParallelParsingOption << std::endl;
      spec.at(0).parseInParallel_ = parallelParsingSpecifiedViaJson.value();
    } else {
      throw std::runtime_error{absl::StrCat(
          "Parallel parsing set in the `.settings.json` file; this is "
          "forbidden for multiple input streams, ",
          pleaseUseParallelParsingOption)};
    }
  }
  // For a single input stream, if parallel parsing is not specified explicitly
  // on the command line, we set if implicitly for backward compatibility.
  if (!parallelParsingSpecifiedViaJson.has_value() && spec.size() == 1 &&
      !spec.at(0).parseInParallelSetExplicitly_) {
    AD_LOG_WARN
        << "Implicitly using the parallel parser for a single input file "
           "for reasons of backward compatibility; this is deprecated, "
        << pleaseUseParallelParsingOption << std::endl;
    spec.at(0).parseInParallel_ = true;
  }
  // For a single input stream, show the name and whether we parse in parallel.
  // For multiple input streams, only show the number of streams.
  if (spec.size() == 1) {
    AD_LOG_INFO << "Processing triples from single input stream "
                << spec.at(0).filename_ << " (parallel = "
                << (spec.at(0).parseInParallel_ ? "true" : "false") << ") ..."
                << std::endl;
  } else {
    AD_LOG_INFO << "Processing triples from " << spec.size()
                << " input streams ..." << std::endl;
  }
}

// _____________________________________________________________________________
void IndexImpl::createFromFiles(
    std::vector<Index::InputFileSpecification> files) {
  if (!loadAllPermutations_ && usePatterns_) {
    throw std::runtime_error{
        "The patterns can only be built when all 6 permutations are created"};
  }

  configurationJson_["encoded-iri-prefixes"] = encodedIriManager();

  vocab_.resetToType(vocabularyTypeForIndexBuilding_);

  readIndexBuilderSettingsFromFile();

  updateInputFileSpecificationsAndLog(files, useParallelParser_);
  IndexBuilderDataAsFirstPermutationSorter indexBuilderData =
      createIdTriplesAndVocab(makeRdfParser(files));

  // Write the configuration already at this point, so we have it available in
  // case any of the permutations fail.
  writeConfiguration();

  auto& firstSorter = *indexBuilderData.sorter_.firstPermutationSorter_;

  size_t numTriplesInternal = 0;
  size_t numPredicatesInternal = 0;

  // Create the internal PSO and POS permutations. This has to be called AFTER
  // all triples have been added to the `internalTriplesPso_` sorter, in
  // particular, after the patterns have been created.
  auto createInternalPsoAndPosAndSetMetadata = [this, &numTriplesInternal,
                                                &numPredicatesInternal,
                                                &indexBuilderData]() {
    std::tie(numTriplesInternal, numPredicatesInternal) =
        createInternalPSOandPOS(*indexBuilderData.sorter_.internalTriplesPso_);
  };

  // TODO: this will become ad_utility::InputRangeErased so no conversion
  // will be needed after https://github.com/ad-freiburg/qlever/pull/2208
  // For the first permutation, perform a unique.
  auto firstSorterWithUnique{ad_utility::InputRangeTypeErased{
      ad_utility::uniqueBlockView(firstSorter.getSortedOutput())}};

  if (!loadAllPermutations_) {
    createInternalPsoAndPosAndSetMetadata();
    // Only two permutations, no patterns, in this case the `firstSorter` is a
    // PSO sorter, and `createPermutationPair` creates PSO/POS permutations.
    createFirstPermutationPair(NumColumnsIndexBuilding,
                               std::move(firstSorterWithUnique));
    configurationJson_["has-all-permutations"] = false;
  } else if (!usePatterns_) {
    createInternalPsoAndPosAndSetMetadata();
    // Without patterns, we explicitly have to pass in the next sorters to all
    // permutation creating functions.
    auto secondSorter = makeSorter<SecondPermutation>("second");
    createFirstPermutationPair(NumColumnsIndexBuilding,
                               std::move(firstSorterWithUnique), secondSorter);
    firstSorter.clearUnderlying();

    auto thirdSorter = makeSorter<ThirdPermutation>("third");
    createSecondPermutationPair(NumColumnsIndexBuilding,
                                secondSorter.getSortedBlocks<0>(), thirdSorter);
    secondSorter.clear();
    createThirdPermutationPair(NumColumnsIndexBuilding,
                               thirdSorter.getSortedBlocks<0>());
    configurationJson_["has-all-permutations"] = true;
  } else {
    // Load all permutations and also load the patterns. In this case the
    // `createFirstPermutationPair` function returns the next sorter, already
    // enriched with the patterns of the subjects in the triple.
    auto patternOutput = createFirstPermutationPair(
        NumColumnsIndexBuilding, std::move(firstSorterWithUnique));
    firstSorter.clearUnderlying();
    auto thirdSorterPtr =
        buildOspWithPatterns(std::move(patternOutput.value()),
                             *indexBuilderData.sorter_.internalTriplesPso_);
    createInternalPsoAndPosAndSetMetadata();
    createThirdPermutationPair(NumColumnsIndexBuilding + 2,
                               thirdSorterPtr->template getSortedBlocks<0>());
    configurationJson_["has-all-permutations"] = true;
  }

  configurationJson_["num-blank-nodes-total"] =
      indexBuilderData.vocabularyMetaData_.getNextBlankNodeIndex();

  addInternalStatisticsToConfiguration(numTriplesInternal,
                                       numPredicatesInternal);
  AD_LOG_INFO << "Index build completed" << std::endl;
}

// _____________________________________________________________________________
void IndexImpl::addInternalStatisticsToConfiguration(
    size_t numTriplesInternal, size_t numPredicatesInternal) {
  // Combine the number of normal triples and predicates with their internal
  // counterparts.
  auto numTriples =
      static_cast<NumNormalAndInternal>(configurationJson_["num-triples"]);
  auto numPredicates =
      static_cast<NumNormalAndInternal>(configurationJson_["num-predicates"]);
  numTriples.internal = numTriplesInternal;
  numPredicates.internal = numPredicatesInternal;
  configurationJson_["num-triples"] = numTriples;
  configurationJson_["num-predicates"] = numPredicates;
  writeConfiguration();
}

// _____________________________________________________________________________
IndexBuilderDataAsExternalVector IndexImpl::passFileForVocabulary(
    std::shared_ptr<RdfParserBase> parser, size_t linesPerPartial) {
  parser->integerOverflowBehavior() = turtleParserIntegerOverflowBehavior_;
  parser->invalidLiteralsAreSkipped() = turtleParserSkipIllegalLiterals_;
  ad_utility::Synchronized<std::unique_ptr<TripleVec>> idTriples(
      std::make_unique<TripleVec>(onDiskBase_ + ".unsorted-triples.dat", 1_GB,
                                  allocator_));
  AD_LOG_INFO << "Parsing input triples and creating partial vocabularies, one "
                 "per batch ..."
              << std::endl;
  bool parserExhausted = false;

  // already count the numbers of triples that will be used for the language
  // filter
  size_t numFiles = 0;

  // we add extra triples
  std::vector<size_t> actualPartialSizes;

  // Each of these futures corresponds to the processing and writing of one
  // batch of triples and partial vocabulary.
  std::array<std::future<void>, 3> writePartialVocabularyFuture;

  // Show progress and statistics for the number of triples parsed, in
  // particular, the average processing time for a batch of 10M triples (see
  // `DEFAULT_PROGRESS_BAR_BATCH_SIZE`).
  //
  // NOTE: Some input generation processes (for example, `osm2rdf` for the OSM
  // data) have a long startup time before they produce the first triple, but
  // then produce triples fast. If we would count that startup time towards the
  // first batch, the reported average batch processing time would be
  // distorted. We therefore stop the timer here, and then start it when the
  // first triple is parsed (see `numTriplesParsedTimer.cont()` below).
  size_t numTriplesParsed = 0;
  ad_utility::ProgressBar progressBar{numTriplesParsed, "Triples parsed: "};
  ad_utility::Timer& numTriplesParsedTimer = progressBar.getTimer();
  numTriplesParsedTimer.stop();

  ad_utility::CachingMemoryResource cachingMemoryResource;
  ItemAlloc itemAlloc(&cachingMemoryResource);
  while (!parserExhausted) {
    size_t actualCurrentPartialSize = 0;

    std::vector<std::array<Id, NumColumnsIndexBuilding>> localWriter;

    std::array<std::optional<ItemMapManager>, NUM_PARALLEL_ITEM_MAPS> itemArray;

    {
      auto p = ad_pipeline::setupParallelPipeline<NUM_PARALLEL_ITEM_MAPS>(
          parserBatchSize_,
          // when called, returns an optional to the next triple. If
          // `linesPerPartial` triples were parsed, return std::nullopt. when
          // the parser is unable to deliver triples, set parserExhausted to
          // true and return std::nullopt. this is exactly the behavior we need,
          // as a first step in the parallel Pipeline.
          ParserBatcher(parser, linesPerPartial,
                        [&]() { parserExhausted = true; }),
          // get the Ids for the original triple and the possibly added language
          // Tag triples using the provided HashMaps via itemArray. See
          // documentation of the function for more details
          getIdMapLambdas<NUM_PARALLEL_ITEM_MAPS>(&itemArray, linesPerPartial,
                                                  &(vocab_.getCaseComparator()),
                                                  this, itemAlloc));

      while (auto opt = p.getNextValue()) {
        numTriplesParsedTimer.cont();
        for (const auto& innerOpt : opt.value()) {
          if (innerOpt) {
            actualCurrentPartialSize++;
            localWriter.push_back(innerOpt.value());
          }
        }
        numTriplesParsed++;
        if (progressBar.update()) {
          AD_LOG_INFO << progressBar.getProgressString() << std::flush;
        }
      }
      AD_LOG_TIMING << "WaitTimes for Pipeline in msecs\n";
      for (const auto& t : p.getWaitingTime()) {
        AD_LOG_TIMING
            << std::chrono::duration_cast<std::chrono::milliseconds>(t).count()
            << " msecs" << std::endl;
      }

      parser->printAndResetQueueStatistics();
    }

    // localWriter.finish();
    // wait until sorting the last partial vocabulary has finished
    // to control the number of threads and the amount of memory used at the
    // same time. typically sorting is finished before we reach again here so
    // it is not a bottleneck.
    ad_utility::Timer sortFutureTimer{ad_utility::Timer::Started};
    if (writePartialVocabularyFuture[0].valid()) {
      writePartialVocabularyFuture[0].get();
    }
    AD_LOG_TIMING
        << "Time spent waiting for the writing of a previous vocabulary: "
        << sortFutureTimer.msecs().count() << "ms." << std::endl;
    auto moveMap = [](std::optional<ItemMapManager>&& el) {
      return std::move(el.value()).moveMap();
    };
    std::array<ItemMapAndBuffer, NUM_PARALLEL_ITEM_MAPS> convertedMaps =
        ad_utility::transformArray(std::move(itemArray), moveMap);
    auto oldItemPtr = std::make_unique<ItemMapArray>(std::move(convertedMaps));
    for (auto it = writePartialVocabularyFuture.begin() + 1;
         it < writePartialVocabularyFuture.end(); ++it) {
      *(it - 1) = std::move(*it);
    }
    writePartialVocabularyFuture[writePartialVocabularyFuture.size() - 1] =
        writeNextPartialVocabulary(
            numTriplesParsed, numFiles, actualCurrentPartialSize,
            std::move(oldItemPtr), std::move(localWriter), &idTriples);
    numFiles++;
    // Save the information how many triples this partial vocabulary actually
    // deals with we will use this later for mapping from partial to global
    // ids
    actualPartialSizes.push_back(actualCurrentPartialSize);
  }
  AD_LOG_INFO << progressBar.getFinalProgressString() << std::flush;
  for (auto& future : writePartialVocabularyFuture) {
    if (future.valid()) {
      future.get();
    }
  }
  AD_LOG_INFO << "Number of triples created (including QLever-internal ones): "
              << (*idTriples.wlock())->size() << " [may contain duplicates]"
              << std::endl;
  AD_LOG_INFO << "Number of partial vocabularies created: " << numFiles
              << std::endl;

  size_t sizeInternalVocabulary = 0;
  std::vector<std::string> prefixes;

  AD_LOG_INFO << "Merging partial vocabularies ..." << std::endl;
  const ad_utility::vocabulary_merger::VocabularyMetaData mergeRes = [&]() {
    auto sortPred = [cmp = &(vocab_.getCaseComparator())](std::string_view a,
                                                          std::string_view b) {
      return (*cmp)(a, b, decltype(vocab_)::SortLevel::TOTAL);
    };
    auto wordCallbackPtr = vocab_.makeWordWriterPtr(onDiskBase_ + VOCAB_SUFFIX);
    auto& wordCallback = *wordCallbackPtr;
    wordCallback.readableName() = "internal vocabulary";
    auto mergedVocabMeta = ad_utility::vocabulary_merger::mergeVocabulary(
        onDiskBase_, numFiles, sortPred, wordCallback,
        memoryLimitIndexBuilding());
    wordCallback.finish();
    return mergedVocabMeta;
  }();
  AD_LOG_DEBUG << "Finished merging partial vocabularies" << std::endl;
  IndexBuilderDataAsExternalVector res;
  res.vocabularyMetaData_ = mergeRes;
  idOfHasPatternDuringIndexBuilding_ =
      mergeRes.specialIdMapping().at(HAS_PATTERN_PREDICATE);
  idOfInternalGraphDuringIndexBuilding_ =
      mergeRes.specialIdMapping().at(QLEVER_INTERNAL_GRAPH_IRI);
  AD_LOG_INFO << "Number of words in external vocabulary: "
              << res.vocabularyMetaData_.numWordsTotal() -
                     sizeInternalVocabulary
              << std::endl;

  res.idTriples = std::move(*idTriples.wlock());
  res.actualPartialSizes = std::move(actualPartialSizes);

  AD_LOG_DEBUG << "Removing temporary files ..." << std::endl;
  for (size_t n = 0; n < numFiles; ++n) {
    deleteTemporaryFile(
        absl::StrCat(onDiskBase_, PARTIAL_VOCAB_WORDS_INFIX, n));
  }

  return res;
}

// _____________________________________________________________________________
template <typename Func>
auto IndexImpl::convertPartialToGlobalIds(
    TripleVec& data, const std::vector<size_t>& actualLinesPerPartial,
    size_t linesPerPartial, Func isQLeverInternalTriple)
    -> FirstPermutationSorterAndInternalTriplesAsPso {
  AD_LOG_INFO << "Converting triples from local IDs to global IDs ..."
              << std::endl;
  AD_LOG_DEBUG << "Triples per partial vocabulary: " << linesPerPartial
               << std::endl;

  // Iterate over all partial vocabularies.
  auto resultPtr =
      [&]() -> std::unique_ptr<
                ad_utility::CompressedExternalIdTableSorterTypeErased> {
    if (loadAllPermutations()) {
      return makeSorterPtr<FirstPermutation>("first");
    } else {
      return makeSorterPtr<SortByPSO>("first");
    }
  }();
  auto internalTriplesPtr =
      makeSorterPtr<SortByPSO, NumColumnsIndexBuilding>("internalTriples");
  auto& result = *resultPtr;
  auto& internalResult = *internalTriplesPtr;
  auto triplesGenerator = data.getRows();
  // static_assert(!std::is_const_v<decltype(triplesGenerator)>);
  // static_assert(std::is_const_v<decltype(triplesGenerator)>);
  auto it = triplesGenerator.begin();
  using Buffer = IdTableStatic<NumColumnsIndexBuilding>;
  struct Buffers {
    IdTableStatic<NumColumnsIndexBuilding> triples_;
    IdTableStatic<NumColumnsIndexBuilding> internalTriples_;
  };
  using Map = ad_utility::HashMap<Id, Id>;

  ad_utility::TaskQueue<true> lookupQueue(30, 10,
                                          "looking up local to global IDs");
  // This queue will be used to push the converted triples to the sorter. It is
  // important that it has only one thread because it will not be used in a
  // thread-safe way.
  ad_utility::TaskQueue<true> writeQueue(30, 1, "Writing global Ids to file");

  // For all triple elements find their mapping from partial to global ids.
  auto transformTriple = [](Buffer::row_reference& curTriple, auto& idMap) {
    for (auto& id : curTriple) {
      // TODO<joka92> Since the mapping only maps `VocabIndex->VocabIndex`,
      // probably the mapping should also be defined as `HashMap<VocabIndex,
      // VocabIndex>` instead of `HashMap<Id, Id>`
      if (id.getDatatype() != Datatype::VocabIndex) {
        // Check that all the internal, special IDs which we have introduced
        // for performance reasons are eliminated.
        AD_CORRECTNESS_CHECK(id.getDatatype() != Datatype::Undefined);
        continue;
      }
      auto iterator = idMap.find(id);
      AD_CORRECTNESS_CHECK(iterator != idMap.end());
      id = iterator->second;
    }
  };

  // Return a lambda that pushes all the triples to the sorter. Must only be
  // called single-threaded.
  size_t numTriplesConverted = 0;
  ad_utility::ProgressBar progressBar{numTriplesConverted,
                                      "Triples converted: "};
  auto getWriteTask = [&result, &internalResult, &numTriplesConverted,
                       &progressBar](Buffers buffers) {
    return [&result, &internalResult, &numTriplesConverted, &progressBar,
            triples = std::make_shared<IdTableStatic<0>>(
                std::move(buffers.triples_).toDynamic()),
            internalTriples = std::make_shared<IdTableStatic<0>>(
                std::move(buffers.internalTriples_).toDynamic())] {
      result.pushBlock(*triples);
      internalResult.pushBlock(*internalTriples);

      numTriplesConverted += triples->size();
      numTriplesConverted += internalTriples->size();
      if (progressBar.update()) {
        AD_LOG_INFO << progressBar.getProgressString() << std::flush;
      }
    };
  };

  // Return a lambda that for each of the `triples` transforms its partial to
  // global IDs using the `idMap`. The map is passed as a `shared_ptr` because
  // multiple batches need access to the same map.
  auto getLookupTask = [&isQLeverInternalTriple, &writeQueue, &transformTriple,
                        &getWriteTask](Buffer triples,
                                       std::shared_ptr<Map> idMap) {
    return [&isQLeverInternalTriple, &writeQueue,
            triples = std::make_shared<Buffer>(std::move(triples)),
            idMap = std::move(idMap), &getWriteTask,
            &transformTriple]() mutable {
      for (Buffer::row_reference triple : *triples) {
        transformTriple(triple, *idMap);
      }
      auto beginInternal =
          std::partition(triples->begin(), triples->end(),
                         [&isQLeverInternalTriple](const auto& row) {
                           return !isQLeverInternalTriple(row);
                         });
      IdTableStatic<NumColumnsIndexBuilding> internalTriples(
          triples->getAllocator());
      // TODO<joka921> We could leave the partitioned complete block as is,
      // and change the interface of the compressed sorters s.t. we can
      // push only a part of a block. We then would safe the copy of the
      // internal triples here, but I am not sure whether this is worth it.
      internalTriples.insertAtEnd(*triples, beginInternal - triples->begin(),
                                  triples->end() - triples->begin());
      triples->resize(beginInternal - triples->begin());

      Buffers buffers{std::move(*triples), std::move(internalTriples)};

      writeQueue.push(getWriteTask(std::move(buffers)));
    };
  };

  std::atomic<size_t> nextPartialVocabulary = 0;
  // Return the mapping from partial to global Ids for the batch with idx
  // `nextPartialVocabulary` and increase that counter by one. Return `nullopt`
  // if there are no more partial vocabularies to read.
  auto createNextVocab = [&nextPartialVocabulary, &actualLinesPerPartial,
                          this]() -> std::optional<std::pair<size_t, Map>> {
    auto idx = nextPartialVocabulary.fetch_add(1, std::memory_order_relaxed);
    if (idx >= actualLinesPerPartial.size()) {
      return std::nullopt;
    }
    std::string filename =
        absl::StrCat(onDiskBase_, PARTIAL_VOCAB_IDMAP_INFIX, idx);
    auto map =
        ad_utility::vocabulary_merger::IdMapFromPartialIdMapFile(filename);
    // Delete the temporary file in which we stored this map
    deleteTemporaryFile(filename);
    return std::pair{idx, std::move(map)};
  };

  // Set up a generator that yields all the mappings in order, but reads them in
  // parallel.
  auto mappings = ad_utility::data_structures::queueManager<
      ad_utility::data_structures::OrderedThreadSafeQueue<Map>>(
      10, 5, createNextVocab);

  // TODO<C++23> Use `views::enumerate`.
  size_t batchIdx = 0;
  for (auto& mapping : mappings) {
    auto idMap = std::make_shared<Map>(std::move(mapping));

    const size_t bufferSize = BUFFER_SIZE_PARTIAL_TO_GLOBAL_ID_MAPPINGS();
    Buffer buffer{ad_utility::makeUnlimitedAllocator<Id>()};
    buffer.reserve(bufferSize);
    auto pushBatch = [&buffer, &idMap, &lookupQueue, &getLookupTask,
                      bufferSize]() {
      lookupQueue.push(getLookupTask(std::move(buffer), idMap));
      buffer.clear();
      buffer.reserve(bufferSize);
    };
    // Update the triples that belong to this partial vocabulary.
    for ([[maybe_unused]] auto idx :
         ad_utility::integerRange(actualLinesPerPartial[batchIdx])) {
      buffer.push_back(*it);
      if (buffer.size() >= bufferSize) {
        pushBatch();
      }
      ++it;
    }
    if (!buffer.empty()) {
      pushBatch();
    }
    ++batchIdx;
  }
  lookupQueue.finish();
  writeQueue.finish();
  AD_LOG_INFO << progressBar.getFinalProgressString() << std::flush;
  return {std::move(resultPtr), std::move(internalTriplesPtr)};
}

// _____________________________________________________________________________
template <typename T, typename... Callbacks>
std::tuple<size_t, IndexImpl::IndexMetaDataMmapDispatcher::WriteType,
           IndexImpl::IndexMetaDataMmapDispatcher::WriteType>
IndexImpl::createPermutationPairImpl(size_t numColumns,
                                     const std::string& fileName1,
                                     const std::string& fileName2,
                                     T&& sortedTriples,
                                     Permutation::KeyOrder permutation,
                                     Callbacks&&... perTripleCallbacks) {
  using MetaData = IndexMetaDataMmapDispatcher::WriteType;
  MetaData metaData1, metaData2;
  static_assert(MetaData::isMmapBased_);
  metaData1.setup(fileName1 + MMAP_FILE_SUFFIX, ad_utility::CreateTag{});
  metaData2.setup(fileName2 + MMAP_FILE_SUFFIX, ad_utility::CreateTag{});

  CompressedRelationWriter writer1{numColumns, ad_utility::File(fileName1, "w"),
                                   blocksizePermutationPerColumn_};
  CompressedRelationWriter writer2{numColumns, ad_utility::File(fileName2, "w"),
                                   blocksizePermutationPerColumn_};

  // Lift a callback that works on single elements to a callback that works on
  // blocks.
  auto liftCallback = [](auto callback) {
    return [callback](const auto& block) mutable {
      ql::ranges::for_each(block, callback);
    };
  };
  auto callback1 =
      liftCallback([&metaData1](const auto& md) { metaData1.add(md); });
  auto callback2 =
      liftCallback([&metaData2](const auto& md) { metaData2.add(md); });

  std::vector<std::function<void(const IdTableStatic<0>&)>> perBlockCallbacks{
      liftCallback(perTripleCallbacks)...};

  auto [numDistinctCol0, blockData1, blockData2] =
      CompressedRelationWriter::createPermutationPair(
          fileName1, {writer1, callback1}, {writer2, callback2},
          AD_FWD(sortedTriples), permutation, perBlockCallbacks);
  metaData1.blockData() = std::move(blockData1);
  metaData2.blockData() = std::move(blockData2);

  return {numDistinctCol0, std::move(metaData1), std::move(metaData2)};
}

// ________________________________________________________________________
template <typename T, typename... Callbacks>
std::tuple<size_t, IndexImpl::IndexMetaDataMmapDispatcher::WriteType,
           IndexImpl::IndexMetaDataMmapDispatcher::WriteType>
IndexImpl::createPermutations(size_t numColumns, T&& sortedTriples,
                              const Permutation& p1, const Permutation& p2,
                              Callbacks&&... perTripleCallbacks) {
  AD_LOG_INFO << "Creating permutations " << p1.readableName() << " and "
              << p2.readableName() << " ..." << std::endl;
  auto metaData = createPermutationPairImpl(
      numColumns, onDiskBase_ + ".index" + p1.fileSuffix(),
      onDiskBase_ + ".index" + p2.fileSuffix(), AD_FWD(sortedTriples),
      p1.keyOrder(), AD_FWD(perTripleCallbacks)...);

  auto& [numDistinctCol0, meta1, meta2] = metaData;
  meta1.calculateStatistics(numDistinctCol0);
  meta2.calculateStatistics(numDistinctCol0);
  AD_LOG_INFO << "Statistics for " << p1.readableName() << ": "
              << meta1.statistics() << std::endl;
  AD_LOG_INFO << "Statistics for " << p2.readableName() << ": "
              << meta2.statistics() << std::endl;

  return metaData;
}

// ________________________________________________________________________
template <typename SortedTriplesType, typename... CallbackTypes>
size_t IndexImpl::createPermutationPair(size_t numColumns,
                                        SortedTriplesType&& sortedTriples,
                                        const Permutation& p1,
                                        const Permutation& p2,
                                        CallbackTypes&&... perTripleCallbacks) {
  auto [numDistinctC0, metaData1, metaData2] = createPermutations(
      numColumns, AD_FWD(sortedTriples), p1, p2, AD_FWD(perTripleCallbacks)...);
  // Set the name of this newly created pair of `IndexMetaData` objects.
  // NOTE: When `setKbName` was called, it set the name of pso_.meta_,
  // pso_.meta_, ... which however are not used during index building.
  // `getKbName` simple reads one of these names.
  auto writeMetadata = [this](auto& metaData, const auto& permutation) {
    metaData.setName(getKbName());
    ad_utility::File f(
        absl::StrCat(onDiskBase_, ".index", permutation.fileSuffix()), "r+");
    metaData.appendToFile(&f);
  };
  AD_LOG_DEBUG << "Writing meta data for " << p1.readableName() << " and "
               << p2.readableName() << " ..." << std::endl;
  writeMetadata(metaData1, p1);
  writeMetadata(metaData2, p2);
  return numDistinctC0;
}

// _____________________________________________________________________________
void IndexImpl::createFromOnDiskIndex(const std::string& onDiskBase,
                                      bool persistUpdatesOnDisk) {
  setOnDiskBase(onDiskBase);
  readConfiguration();
  vocab_.readFromFile(onDiskBase_ + VOCAB_SUFFIX);
  globalSingletonComparator_ = &vocab_.getCaseComparator();

  AD_LOG_DEBUG << "Number of words in internal and external vocabulary: "
               << vocab_.size() << std::endl;

  // Load the permutations and register the original metadata for the delta
  // triples.
  // The setting of the metadata doesn't affect the contents of the delta
  // triples, so we don't need to call `writeToDisk`, therefore the second
  // argument to `modify` is `false`.
  auto setMetadata = [this](const Permutation& permutation) {
    deltaTriplesManager().modify<void>(
        [&permutation](DeltaTriples& deltaTriples) {
          permutation.setOriginalMetadataForDeltaTriples(deltaTriples);
        },
        false, false);
  };

  auto load = [this, &setMetadata](PermutationPtr permutation,
                                   bool loadInternalPermutation = false) {
    permutation->loadFromDisk(onDiskBase_, loadInternalPermutation);
    setMetadata(*permutation);
  };

  load(pso_, true);
  load(pos_, true);
  if (loadAllPermutations_) {
    load(ops_);
    load(osp_);
    load(spo_);
    load(sop_);
  } else {
    AD_LOG_INFO
        << "Only the PSO and POS permutation were loaded, SPARQL queries "
           "with predicate variables will therefore not work"
        << std::endl;
  }

  // We have to load the patterns first to figure out if the patterns were built
  // at all.
  if (usePatterns_) {
    try {
      PatternCreator::readPatternsFromFile(
          onDiskBase_ + ".index.patterns", avgNumDistinctSubjectsPerPredicate_,
          avgNumDistinctPredicatesPerSubject_,
          numDistinctSubjectPredicatePairs_, patterns_);
    } catch (const std::exception& e) {
      AD_LOG_WARN
          << "Could not load the patterns. The internal predicate "
             "`ql:has-predicate` is therefore not available (and certain "
             "queries that benefit from that predicate will be slower)."
             "To suppress this warning, start the server with "
             "the `--no-patterns` option. The error message was "
          << e.what() << std::endl;
      usePatterns_ = false;
    }
  }
  if (persistUpdatesOnDisk) {
    deltaTriples_.value().setFilenameForPersistentUpdatesAndReadFromDisk(
        onDiskBase + ".update-triples");
  }
}

// _____________________________________________________________________________
void IndexImpl::throwExceptionIfNoPatterns() const {
  AD_CONTRACT_CHECK(
      usePatterns_,
      "The requested feature requires a loaded patterns file ("
      "do not specify the --no-patterns option for this to work)");
}

// _____________________________________________________________________________
const CompactVectorOfStrings<Id>& IndexImpl::getPatterns() const {
  throwExceptionIfNoPatterns();
  return patterns_;
}

// _____________________________________________________________________________
double IndexImpl::getAvgNumDistinctPredicatesPerSubject() const {
  throwExceptionIfNoPatterns();
  return avgNumDistinctPredicatesPerSubject_;
}

// _____________________________________________________________________________
double IndexImpl::getAvgNumDistinctSubjectsPerPredicate() const {
  throwExceptionIfNoPatterns();
  return avgNumDistinctSubjectsPerPredicate_;
}

// _____________________________________________________________________________
size_t IndexImpl::getNumDistinctSubjectPredicatePairs() const {
  throwExceptionIfNoPatterns();
  return numDistinctSubjectPredicatePairs_;
}

// _____________________________________________________________________________
bool IndexImpl::isLiteral(std::string_view object) const {
  return decltype(vocab_)::stringIsLiteral(object);
}

// _____________________________________________________________________________
void IndexImpl::setKbName(const std::string& name) {
  pos_->setKbName(name);
  pso_->setKbName(name);
  sop_->setKbName(name);
  spo_->setKbName(name);
  ops_->setKbName(name);
  osp_->setKbName(name);
}

// ____________________________________________________________________________
void IndexImpl::setOnDiskBase(const std::string& onDiskBase) {
  onDiskBase_ = onDiskBase;
}

// ____________________________________________________________________________
void IndexImpl::setKeepTempFiles(bool keepTempFiles) {
  keepTempFiles_ = keepTempFiles;
}

// _____________________________________________________________________________
bool& IndexImpl::usePatterns() { return usePatterns_; }

// _____________________________________________________________________________
bool& IndexImpl::loadAllPermutations() { return loadAllPermutations_; }

// ____________________________________________________________________________
void IndexImpl::setSettingsFile(const std::string& filename) {
  settingsFileName_ = filename;
}

// ____________________________________________________________________________
void IndexImpl::writeConfiguration() const {
  // Copy the configuration and add the current commit hash.
  auto configuration = configurationJson_;
  configuration["git-hash"] =
      *qlever::version::gitShortHashWithoutLinking.wlock();
  configuration["index-format-version"] = qlever::indexFormatVersion;
  auto f = ad_utility::makeOfstream(onDiskBase_ + CONFIGURATION_FILE);
  f << configuration;
}

// ___________________________________________________________________________
void IndexImpl::readConfiguration() {
  auto f = ad_utility::makeIfstream(onDiskBase_ + CONFIGURATION_FILE);
  f >> configurationJson_;
  if (configurationJson_.find("git-hash") != configurationJson_.end()) {
    AD_LOG_INFO << "The git hash used to build this index was "
                << configurationJson_["git-hash"] << std::endl;
  } else {
    AD_LOG_INFO
        << "The index was built before git commit hashes were stored in "
           "the index meta data"
        << std::endl;
  }

  if (configurationJson_.find("index-format-version") !=
      configurationJson_.end()) {
    auto indexFormatVersion = static_cast<qlever::IndexFormatVersion>(
        configurationJson_["index-format-version"]);
    const auto& currentVersion = qlever::indexFormatVersion;
    if (indexFormatVersion != currentVersion) {
      if (indexFormatVersion.date_.toBits() > currentVersion.date_.toBits()) {
        AD_LOG_ERROR
            << "The version of QLever you are using is too old for this "
               "index. Please use a version of QLever that is "
               "compatible with this index"
               " (PR = "
            << indexFormatVersion.prNumber_
            << ", Date = " << indexFormatVersion.date_.toStringAndType().first
            << ")." << std::endl;
      } else {
        AD_LOG_ERROR
            << "The index is too old for this version of QLever. "
               "We recommend that you rebuild the index and start the "
               "server with the current master. Alternatively start the "
               "engine with a version of QLever that is compatible with "
               "this index (PR = "
            << indexFormatVersion.prNumber_
            << ", Date = " << indexFormatVersion.date_.toStringAndType().first
            << ")." << std::endl;
      }
      throw std::runtime_error{
          "Incompatible index format, see log message for details"};
    }
  } else {
    AD_LOG_ERROR
        << "This index was built before versioning was introduced for "
           "QLever's index format. Please rebuild your index using the "
           "current version of QLever."
        << std::endl;
    throw std::runtime_error{
        "Incompatible index format, see log message for details"};
  }

  if (configurationJson_.find("prefixes-external") !=
      configurationJson_.end()) {
    vocab_.initializeExternalizePrefixes(
        configurationJson_["prefixes-external"]);
  }

  if (configurationJson_.count("ignore-case")) {
    AD_LOG_ERROR << ERROR_IGNORE_CASE_UNSUPPORTED << '\n';
    throw std::runtime_error("Deprecated key \"ignore-case\" in index build");
  }

  if (configurationJson_.count("locale")) {
    std::string lang{configurationJson_["locale"]["language"]};
    std::string country{configurationJson_["locale"]["country"]};
    bool ignorePunctuation{configurationJson_["locale"]["ignore-punctuation"]};
    vocab_.setLocale(lang, country, ignorePunctuation);
    textVocab_.setLocale(lang, country, ignorePunctuation);
  } else {
    AD_LOG_ERROR
        << "Key \"locale\" is missing in the metadata. This is probably "
           "and old index build that is no longer supported by QLever. "
           "Please rebuild your index\n";
    throw std::runtime_error(
        "Missing required key \"locale\" in index build's metadata");
  }

  if (configurationJson_.find("languages-internal") !=
      configurationJson_.end()) {
    vocab_.initializeInternalizedLangs(
        configurationJson_["languages-internal"]);
  }

  auto loadDataMember = [this](std::string_view key, auto& target,
                               std::optional<ql::type_identity_t<
                                   std::decay_t<decltype(target)>>>
                                   defaultValue = std::nullopt) {
    using Target = std::decay_t<decltype(target)>;
    auto it = configurationJson_.find(key);
    if (it == configurationJson_.end()) {
      if (defaultValue.has_value()) {
        target = std::move(defaultValue.value());
      } else {
        throw std::runtime_error{absl::StrCat(
            "The required key \"", key,
            "\" was not found in the `meta-data.json`. Most likely this index "
            "was built with an older version of QLever and should be rebuilt")};
      }
    } else {
      target = static_cast<Target>(*it);
    }
  };

  loadDataMember("git-hash", gitShortHash_);
  loadDataMember("has-all-permutations", loadAllPermutations_, true);
  loadDataMember("num-predicates", numPredicates_);
  // These might be missing if there are only two permutations.
  loadDataMember("num-subjects", numSubjects_, NumNormalAndInternal{});
  loadDataMember("num-objects", numObjects_, NumNormalAndInternal{});
  loadDataMember("num-triples", numTriples_, NumNormalAndInternal{});
  loadDataMember("num-non-literals-text-index", nofNonLiteralsInTextIndex_, 0);
  loadDataMember("text-scoring-metric", textScoringMetric_,
                 TextScoringMetric::EXPLICIT);
  loadDataMember("b-and-k-parameter-for-text-scoring",
                 bAndKParamForTextScoring_, std::make_pair(0.75, 1.75));

  ad_utility::VocabularyType vocabType(
      ad_utility::VocabularyType::Enum::OnDiskCompressed);
  loadDataMember("vocabulary-type", vocabType, vocabType);
  vocab_.resetToType(vocabType);

  // Initialize BlankNodeManager
  uint64_t numBlankNodesTotal;
  loadDataMember("num-blank-nodes-total", numBlankNodesTotal);
  blankNodeManager_ =
      std::make_unique<ad_utility::BlankNodeManager>(numBlankNodesTotal);

  loadDataMember("encoded-iri-prefixes", encodedIriManager_,
                 EncodedIriManager{});

  // Compute unique ID for this index.
  //
  // TODO: This is a simplistic way. It would be better to incorporate bytes
  // from the index files.
  indexId_ = absl::StrCat("#", getKbName(), ".", numTriples_.normal, ".",
                          numSubjects_.normal, ".", numPredicates_.normal, ".",
                          numObjects_.normal);
}

// ___________________________________________________________________________
LangtagAndTriple IndexImpl::tripleToInternalRepresentation(
    TurtleTriple&& triple) const {
  LangtagAndTriple result{"", {}};
  auto& resultTriple = result.triple_;
  if (triple.object_.isLiteral()) {
    const auto& lit = triple.object_.getLiteral();
    if (lit.hasLanguageTag()) {
      result.langtag_ = std::string(asStringViewUnsafe(lit.getLanguageTag()));
    }
  }

  // The following lambda deals with triple elements that might be strings
  // (literals or IRIs) as well as values that can be decoded into the IRI
  // directly. These currently are the object and the graph ID of the triple.
  // The `index` is the index of the element within the triple. For example if
  // the `getter` is `subject_` then the index has to be `0`.
  auto handleStringOrId = [this, &triple, &resultTriple](auto getter,
                                                         size_t index) {
    // If the object of the triple can be directly folded into an ID, do so.
    // Note that the actual folding is done by the `TripleComponent`.
    auto& el = std::invoke(getter, triple);
    std::optional<Id> idIfNotString =
        el.toValueIdIfNotString(&encodedIriManager());

    // TODO<joka921> The following statement could be simplified by a helper
    // function "optionalCast";
    if (idIfNotString.has_value()) {
      resultTriple[index] = idIfNotString.value();
    } else {
      // `toRdfLiteral` handles literals as well as IRIs correctly.
      resultTriple[index] = std::move(el);
    }
  };
  handleStringOrId(&TurtleTriple::subject_, 0);
  handleStringOrId(&TurtleTriple::predicate_, 1);
  handleStringOrId(&TurtleTriple::object_, 2);
  handleStringOrId(&TurtleTriple::graphIri_, 3);
  // If we ever add additional elements to the triples then we have to change
  // this position.
  static_assert(NumColumnsIndexBuilding == 4,
                "This place probably has to be changed when additional payload "
                "columns are added to the index");

  for (auto& el : resultTriple) {
    if (!std::holds_alternative<PossiblyExternalizedIriOrLiteral>(el)) {
      // If we already have an ID, we can just continue;
      continue;
    }
    auto& component = std::get<PossiblyExternalizedIriOrLiteral>(el);
    const auto& iriOrLiteral = component.iriOrLiteral_;
    // TODO<joka921> Perform this normalization right at the beginning of the
    // parsing. iriOrLiteral =
    // vocab_.getLocaleManager().normalizeUtf8(iriOrLiteral);
    if (vocab_.shouldBeExternalized(iriOrLiteral.toRdfLiteral())) {
      component.isExternal_ = true;
    }
  }
  return result;
}

// ___________________________________________________________________________
void IndexImpl::readIndexBuilderSettingsFromFile() {
  nlohmann::json j;  // if we have no settings, we still have to initialize some
                     // default values
  if (!settingsFileName_.empty()) {
    auto f = ad_utility::makeIfstream(settingsFileName_);
    f >> j;
  }

  if (j.find("prefixes-external") != j.end()) {
    vocab_.initializeExternalizePrefixes(j["prefixes-external"]);
    configurationJson_["prefixes-external"] = j["prefixes-external"];
  }

  if (j.count("ignore-case")) {
    AD_LOG_ERROR << ERROR_IGNORE_CASE_UNSUPPORTED << '\n';
    throw std::runtime_error("Deprecated key \"ignore-case\" in settings JSON");
  }

  /**
   * ICU uses two separate arguments for each Locale, the language ("en" or
   * "fr"...) and the country ("GB", "CA"...). The encoding has to be known at
   * compile time for ICU and will always be UTF-8 so it is not part of the
   * locale setting.
   */

  {
    std::string lang{LOCALE_DEFAULT_LANG};
    std::string country{LOCALE_DEFAULT_COUNTRY};
    bool ignorePunctuation = LOCALE_DEFAULT_IGNORE_PUNCTUATION;
    if (j.count("locale")) {
      lang = std::string{j["locale"]["language"]};
      country = std::string{j["locale"]["country"]};
      ignorePunctuation = bool{j["locale"]["ignore-punctuation"]};
    } else {
      AD_LOG_INFO << "Locale was not specified in settings file, default is "
                     "en_US"
                  << std::endl;
    }
    AD_LOG_INFO << "You specified \"locale = " << lang << "_" << country
                << "\" "
                << "and \"ignore-punctuation = " << ignorePunctuation << "\""
                << std::endl;

    if (lang != LOCALE_DEFAULT_LANG || country != LOCALE_DEFAULT_COUNTRY) {
      AD_LOG_WARN
          << "You are using Locale settings that differ from the default "
             "language or country.\n\t"
          << "This should work but is untested by the QLever team. If "
             "you are running into unexpected problems,\n\t"
          << "Please make sure to also report your used locale when "
             "filing a bug report. Also note that changing the\n\t"
          << "locale requires to completely rebuild the index\n";
    }
    vocab_.setLocale(lang, country, ignorePunctuation);
    textVocab_.setLocale(lang, country, ignorePunctuation);
    configurationJson_["locale"]["language"] = lang;
    configurationJson_["locale"]["country"] = country;
    configurationJson_["locale"]["ignore-punctuation"] = ignorePunctuation;
  }

  if (j.find("languages-internal") != j.end()) {
    vocab_.initializeInternalizedLangs(j["languages-internal"]);
    configurationJson_["languages-internal"] = j["languages-internal"];
  }
  if (j.count("ascii-prefixes-only")) {
    onlyAsciiTurtlePrefixes_ = static_cast<bool>(j["ascii-prefixes-only"]);
  }
  if (onlyAsciiTurtlePrefixes_) {
    AD_LOG_INFO << WARNING_ASCII_ONLY_PREFIXES << std::endl;
  }

  if (j.count("parallel-parsing")) {
    useParallelParser_ = static_cast<bool>(j["parallel-parsing"]);
  }
  if (useParallelParser_) {
    AD_LOG_INFO << WARNING_PARALLEL_PARSING << std::endl;
  }

  if (j.count("num-triples-per-batch")) {
    numTriplesPerBatch_ = size_t{j["num-triples-per-batch"]};
    AD_LOG_INFO
        << "You specified \"num-triples-per-batch = " << numTriplesPerBatch_
        << "\", choose a lower value if the index builder runs out of memory"
        << std::endl;
  }

  if (j.count("parser-batch-size")) {
    parserBatchSize_ = size_t{j["parser-batch-size"]};
    AD_LOG_INFO << "Overriding setting parser-batch-size to "
                << parserBatchSize_
                << " This might influence performance during index build."
                << std::endl;
  }

  std::string overflowingIntegersThrow = "overflowing-integers-throw";
  std::string overflowingIntegersBecomeDoubles =
      "overflowing-integers-become-doubles";
  std::string allIntegersBecomeDoubles = "all-integers-become-doubles";
  std::vector<std::string_view> allModes{overflowingIntegersThrow,
                                         overflowingIntegersBecomeDoubles,
                                         allIntegersBecomeDoubles};
  std::string key = "parser-integer-overflow-behavior";
  if (j.count(key)) {
    auto value = static_cast<std::string>(j[key]);
    if (value == overflowingIntegersThrow) {
      AD_LOG_INFO << "Integers that cannot be represented by QLever will throw "
                     "an exception"
                  << std::endl;
      turtleParserIntegerOverflowBehavior_ =
          TurtleParserIntegerOverflowBehavior::Error;
    } else if (value == overflowingIntegersBecomeDoubles) {
      AD_LOG_INFO << "Integers that cannot be represented by QLever will be "
                     "converted to doubles"
                  << std::endl;
      turtleParserIntegerOverflowBehavior_ =
          TurtleParserIntegerOverflowBehavior::OverflowingToDouble;
    } else if (value == allIntegersBecomeDoubles) {
      AD_LOG_INFO << "All integers will be converted to doubles" << std::endl;
      turtleParserIntegerOverflowBehavior_ =
          TurtleParserIntegerOverflowBehavior::OverflowingToDouble;
    } else {
      AD_CONTRACT_CHECK(ql::ranges::find(allModes, value) == allModes.end());
      AD_LOG_ERROR << "Invalid value for " << key << std::endl;
      AD_LOG_INFO << "The currently supported values are "
                  << absl::StrJoin(allModes, ",") << std::endl;
    }
  } else {
    turtleParserIntegerOverflowBehavior_ =
        TurtleParserIntegerOverflowBehavior::Error;
    AD_LOG_INFO << "By default, integers that cannot be represented by QLever "
                   "will throw an "
                   "exception"
                << std::endl;
  }
}

// ___________________________________________________________________________
std::future<void> IndexImpl::writeNextPartialVocabulary(
    size_t numLines, size_t numFiles, size_t actualCurrentPartialSize,
    std::unique_ptr<ItemMapArray> items, auto localIds,
    ad_utility::Synchronized<std::unique_ptr<TripleVec>>* globalWritePtr) {
  using namespace ad_utility::vocabulary_merger;
  AD_LOG_DEBUG << "Input triples read in this section: " << numLines
               << std::endl;
  AD_LOG_DEBUG
      << "Triples processed, also counting internal triples added by QLever: "
      << actualCurrentPartialSize << std::endl;
  std::future<void> resultFuture;
  std::string partialFilename =
      absl::StrCat(onDiskBase_, PARTIAL_VOCAB_WORDS_INFIX, numFiles);

  auto lambda = [localIds = std::move(localIds), globalWritePtr,
                 items = std::move(items), vocab = &vocab_, partialFilename,
                 numFiles]() mutable {
    auto vec = [&]() {
      ad_utility::TimeBlockAndLog l{"vocab maps to vector"};
      return vocabMapsToVector(*items);
    }();
    const auto identicalPred = [&c = vocab->getCaseComparator()](
                                   const auto& a, const auto& b) {
      return c(a.second.splitVal_, b.second.splitVal_,
               decltype(vocab_)::SortLevel::TOTAL);
    };
    {
      ad_utility::TimeBlockAndLog l{"sorting by unicode order"};
      sortVocabVector(&vec, identicalPred, true);
    }
    auto mapping = [&]() {
      ad_utility::TimeBlockAndLog l{"creating internal mapping"};
      return createInternalMapping(vec);
    }();
    AD_LOG_TRACE << "Finished creating of Mapping vocabulary" << std::endl;
    // since now adjacent duplicates also have the same Ids, it suffices to
    // compare those
    {
      ad_utility::TimeBlockAndLog l{"removing duplicates from the input"};
      vec.erase(std::unique(vec.begin(), vec.end(),
                            [](const auto& a, const auto& b) {
                              return a.second.id_ == b.second.id_;
                            }),
                vec.end());
    }
    // The writing to the external vector has to be done in order, to
    // make the update from local to global ids work.

    auto writeTriplesFuture = std::async(
        std::launch::async,
        [&globalWritePtr, &localIds, &mapping, &numFiles]() {
          globalWritePtr->withWriteLockAndOrdered(
              [&](auto& writerPtr) {
                writeMappedIdsToExtVec(localIds, mapping, &writerPtr);
              },
              numFiles);
        });
    {
      ad_utility::TimeBlockAndLog l{"write partial vocabulary"};
      writePartialVocabularyToFile(vec, partialFilename);
    }
    AD_LOG_TRACE << "Finished writing the partial vocabulary" << std::endl;
    vec.clear();
    {
      ad_utility::TimeBlockAndLog l{"writing to global file"};
      writeTriplesFuture.get();
    }
  };

  return std::async(std::launch::async, std::move(lambda));
}

// ____________________________________________________________________________
IndexImpl::NumNormalAndInternal IndexImpl::numTriples() const {
  return numTriples_;
}

// ____________________________________________________________________________
IndexImpl::PermutationPtr IndexImpl::getPermutationPtr(Permutation::Enum p) {
  using enum Permutation::Enum;
  switch (p) {
    case PSO:
      return pso_;
    case POS:
      return pos_;
    case SPO:
      return spo_;
    case SOP:
      return sop_;
    case OSP:
      return osp_;
    case OPS:
      return ops_;
  }
  AD_FAIL();
}

// ____________________________________________________________________________
Permutation& IndexImpl::getPermutation(Permutation::Enum p) {
  return *getPermutationPtr(p);
}

// ____________________________________________________________________________
const Permutation& IndexImpl::getPermutation(Permutation::Enum p) const {
  return const_cast<IndexImpl&>(*this).getPermutation(p);
}

// ____________________________________________________________________________
std::shared_ptr<const Permutation> IndexImpl::getPermutationPtr(
    Permutation::Enum p) const {
  return const_cast<IndexImpl&>(*this).getPermutationPtr(p);
}

// __________________________________________________________________________
Index::NumNormalAndInternal IndexImpl::numDistinctSubjects() const {
  AD_CONTRACT_CHECK(
      hasAllPermutations(),
      "Can only get # distinct subjects if all 6 permutations "
      "have been registered on sever start (and index build time) "
      "with the -a option.");
  return numSubjects_;
}

// __________________________________________________________________________
Index::NumNormalAndInternal IndexImpl::numDistinctObjects() const {
  AD_CONTRACT_CHECK(
      hasAllPermutations(),
      "Can only get # distinct objects if all 6 permutations "
      "have been registered on sever start (and index build time) "
      "with the -a option.");
  return numObjects_;
}

// __________________________________________________________________________
Index::NumNormalAndInternal IndexImpl::numDistinctPredicates() const {
  return numPredicates_;
}

// __________________________________________________________________________
Index::NumNormalAndInternal IndexImpl::numDistinctCol0(
    Permutation::Enum permutation) const {
  switch (permutation) {
    case Permutation::SOP:
    case Permutation::SPO:
      return numDistinctSubjects();
    case Permutation::OPS:
    case Permutation::OSP:
      return numDistinctObjects();
    case Permutation::POS:
    case Permutation::PSO:
      return numDistinctPredicates();
    default:
      AD_FAIL();
  }
}

// ___________________________________________________________________________
size_t IndexImpl::getCardinality(
    Id id, Permutation::Enum permutation,
    const LocatedTriplesState& locatedTriplesState) const {
  if (const auto& meta =
          getPermutation(permutation).getMetadata(id, locatedTriplesState);
      meta.has_value()) {
    return meta.value().numRows_;
  }
  return 0;
}

// ___________________________________________________________________________
size_t IndexImpl::getCardinality(
    const TripleComponent& comp, Permutation::Enum permutation,
    const LocatedTriplesState& locatedTriplesState) const {
  // TODO<joka921> This special case is only relevant for the `PSO` and `POS`
  // permutations, but this internal predicate should never appear in subjects
  // or objects anyway.
  // TODO<joka921> Find out what the effect of this special case is for the
  // query planning.
  if (comp == QLEVER_INTERNAL_TEXT_MATCH_PREDICATE) {
    return TEXT_PREDICATE_CARDINALITY_ESTIMATE;
  }
  if (std::optional<Id> relId =
          comp.toValueId(getVocab(), encodedIriManager())) {
    return getCardinality(relId.value(), permutation, locatedTriplesState);
  }
  return 0;
}

// ___________________________________________________________________________
RdfsVocabulary::AccessReturnType IndexImpl::indexToString(VocabIndex id) const {
  return vocab_[id];
}

// ___________________________________________________________________________
TextVocabulary::AccessReturnType IndexImpl::indexToString(
    WordVocabIndex id) const {
  return textVocab_[id];
}

// ___________________________________________________________________________
Index::Vocab::PrefixRanges IndexImpl::prefixRanges(
    std::string_view prefix) const {
  // TODO<joka921> Do we need prefix ranges for numbers?
  return vocab_.prefixRanges(prefix);
}

// _____________________________________________________________________________
std::vector<float> IndexImpl::getMultiplicities(
    const TripleComponent& key, const Permutation& permutation,
    const LocatedTriplesState& locatedTriplesState) const {
  if (auto keyId = key.toValueId(getVocab(), encodedIriManager())) {
    auto meta = permutation.getMetadata(keyId.value(), locatedTriplesState);
    if (meta.has_value()) {
      return {meta.value().getCol1Multiplicity(),
              meta.value().getCol2Multiplicity()};
    }
  }
  return {1.0f, 1.0f};
}

// _____________________________________________________________________________
std::vector<float> IndexImpl::getMultiplicities(
    const Permutation& permutation) const {
  auto numTriples = static_cast<float>(this->numTriples().normal);
  std::array multiplicities{numTriples / numDistinctSubjects().normal,
                            numTriples / numDistinctPredicates().normal,
                            numTriples / numDistinctObjects().normal};
  auto permuted = permutation.keyOrder().permuteTriple(multiplicities);
  return {permuted.begin(), permuted.end()};
}

// _____________________________________________________________________________
size_t IndexImpl::getResultSizeOfScan(
    const ScanSpecification& scanSpecification,
    const Permutation::Enum& permutation,
    const LocatedTriplesState& locatedTriplesState) const {
  const auto& perm = getPermutation(permutation);
  return perm.getResultSizeOfScan(
      perm.getScanSpecAndBlocks(scanSpecification, locatedTriplesState),
      locatedTriplesState);
}

// _____________________________________________________________________________
void IndexImpl::deleteTemporaryFile(const std::string& path) {
  if (!keepTempFiles_) {
    ad_utility::deleteFile(path);
  }
}

namespace {

// Return a lambda that is called repeatedly with triples that are sorted by the
// `idx`-th column and counts the number of distinct entities that occur in a
// triple. This is used to count the number of distinct subjects, objects,
// and predicates during the index building.
template <size_t idx>
constexpr auto makeNumDistinctIdsCounter = [](size_t& numDistinctIds) {
  return [lastId = std::optional<Id>{},
          &numDistinctIds](const auto& triple) mutable {
    const auto& id = triple[idx];
    if (id != lastId) {
      numDistinctIds++;
      lastId = id;
    }
  };
};
}  // namespace

// _____________________________________________________________________________
CPP_template_def(typename... NextSorter)(requires(
    sizeof...(NextSorter) <=
    1)) void IndexImpl::createPSOAndPOSImpl(size_t numColumns,
                                            BlocksOfTriples sortedTriples,
                                            bool doWriteConfiguration,
                                            NextSorter&&... nextSorter)

{
  size_t numTriplesNormal = 0;
  size_t numTriplesTotal = 0;
  auto countTriplesNormal = [&numTriplesNormal, &numTriplesTotal](
                                [[maybe_unused]] const auto& triple) mutable {
    ++numTriplesTotal;
    ++numTriplesNormal;
  };
  size_t numPredicatesNormal = 0;
  auto predicateCounter = makeNumDistinctIdsCounter<1>(numPredicatesNormal);
  size_t numPredicatesTotal =
      createPermutationPair(numColumns, AD_FWD(sortedTriples), *pso_, *pos_,
                            nextSorter.makePushCallback()...,
                            std::ref(predicateCounter), countTriplesNormal);
  configurationJson_["num-predicates"] =
      NumNormalAndInternal::fromNormalAndTotal(numPredicatesNormal,
                                               numPredicatesTotal);
  configurationJson_["num-triples"] = NumNormalAndInternal::fromNormalAndTotal(
      numTriplesNormal, numTriplesTotal);
  if (doWriteConfiguration) {
    writeConfiguration();
  }
};

// _____________________________________________________________________________
CPP_template_def(typename... NextSorter)(
    requires(sizeof...(NextSorter) <=
             1)) void IndexImpl::createPSOAndPOS(size_t numColumns,
                                                 BlocksOfTriples sortedTriples,
                                                 NextSorter&&... nextSorter) {
  createPSOAndPOSImpl(numColumns, std::move(sortedTriples), true,
                      AD_FWD(nextSorter)...);
}

// _____________________________________________________________________________
CPP_template_def(typename... NextSorter)(requires(sizeof...(NextSorter) <= 1))
    std::optional<PatternCreator::TripleSorter> IndexImpl::createSPOAndSOP(
        size_t numColumns, BlocksOfTriples sortedTriples,
        NextSorter&&... nextSorter) {
  size_t numSubjectsNormal = 0;
  size_t numSubjectsTotal = 0;
  auto numSubjectCounter = makeNumDistinctIdsCounter<0>(numSubjectsNormal);
  std::optional<PatternCreator::TripleSorter> result;
  if (usePatterns_) {
    // We will return the next sorter.
    AD_CORRECTNESS_CHECK(sizeof...(nextSorter) == 0);
    // For now (especially for testing) We build the new pattern format as well
    // as the old one to see that they match.
    PatternCreator patternCreator{
        onDiskBase_ + ".index.patterns",
        idOfHasPatternDuringIndexBuilding_.value(),
        memoryLimitIndexBuilding() / NUM_EXTERNAL_SORTERS_AT_SAME_TIME};
    auto pushTripleToPatterns = [&patternCreator](const auto& triple) {
      bool ignoreForPatterns = false;
      static_assert(NumColumnsIndexBuilding == 4,
                    "this place probably has to be changed when additional "
                    "payload columns are added");
      auto tripleArr = std::array{triple[0], triple[1], triple[2], triple[3]};
      patternCreator.processTriple(tripleArr, ignoreForPatterns);
    };
    numSubjectsTotal = createPermutationPair(
        numColumns, AD_FWD(sortedTriples), *spo_, *sop_,
        nextSorter.makePushCallback()..., pushTripleToPatterns,
        std::ref(numSubjectCounter));
    patternCreator.finish();
    configurationJson_["num-subjects"] =
        NumNormalAndInternal::fromNormalAndTotal(numSubjectsNormal,
                                                 numSubjectsTotal);
    writeConfiguration();
    result = std::move(patternCreator).getTripleSorter();
  } else {
    AD_CORRECTNESS_CHECK(sizeof...(nextSorter) == 1);
    numSubjectsTotal = createPermutationPair(
        numColumns, AD_FWD(sortedTriples), *spo_, *sop_,
        nextSorter.makePushCallback()..., std::ref(numSubjectCounter));
    configurationJson_["num-subjects"] =
        NumNormalAndInternal::fromNormalAndTotal(numSubjectsNormal,
                                                 numSubjectsTotal);
  }
  configurationJson_["num-subjects"] = NumNormalAndInternal::fromNormalAndTotal(
      numSubjectsNormal, numSubjectsTotal);
  writeConfiguration();
  return result;
};

// _____________________________________________________________________________
CPP_template_def(typename... NextSorter)(
    requires(sizeof...(NextSorter) <=
             1)) void IndexImpl::createOSPAndOPS(size_t numColumns,
                                                 BlocksOfTriples sortedTriples,
                                                 NextSorter&&... nextSorter) {
  // For the last pair of permutations we don't need a next sorter, so we
  // have no fourth argument.
  size_t numObjectsNormal = 0;
  auto objectCounter = makeNumDistinctIdsCounter<2>(numObjectsNormal);
  size_t numObjectsTotal = createPermutationPair(
      numColumns, AD_FWD(sortedTriples), *osp_, *ops_,
      nextSorter.makePushCallback()..., std::ref(objectCounter));
  configurationJson_["num-objects"] = NumNormalAndInternal::fromNormalAndTotal(
      numObjectsNormal, numObjectsTotal);
  configurationJson_["has-all-permutations"] = true;
  writeConfiguration();
};

// _____________________________________________________________________________
template <typename Comparator, size_t I, bool returnPtr>
auto IndexImpl::makeSorterImpl(std::string_view permutationName) const {
  using Sorter = ExternalSorter<Comparator, I>;
  auto apply = [](auto&&... args) {
    if constexpr (returnPtr) {
      return std::make_unique<Sorter>(AD_FWD(args)...);
    } else {
      return Sorter{AD_FWD(args)...};
    }
  };
  return apply(absl::StrCat(onDiskBase_, ".", permutationName, "-sorter.dat"),
               memoryLimitIndexBuilding() / NUM_EXTERNAL_SORTERS_AT_SAME_TIME,
               allocator_);
}

// _____________________________________________________________________________
template <typename Comparator, size_t I>
ExternalSorter<Comparator, I> IndexImpl::makeSorter(
    std::string_view permutationName) const {
  return makeSorterImpl<Comparator, I, false>(permutationName);
}
// _____________________________________________________________________________
template <typename Comparator, size_t I>
std::unique_ptr<ExternalSorter<Comparator, I>> IndexImpl::makeSorterPtr(
    std::string_view permutationName) const {
  return makeSorterImpl<Comparator, I, true>(permutationName);
}

// _____________________________________________________________________________
ad_utility::BlankNodeManager* IndexImpl::getBlankNodeManager() const {
  AD_CONTRACT_CHECK(blankNodeManager_);
  return blankNodeManager_.get();
}

// _____________________________________________________________________________
void IndexImpl::setPrefixesForEncodedValues(
    std::vector<std::string> prefixesWithoutAngleBrackets) {
  encodedIriManager_ =
      EncodedIriManager{std::move(prefixesWithoutAngleBrackets)};
}
