// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors:
//   2014-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include "./IndexImpl.h"

#include <algorithm>
#include <cstdio>
#include <future>
#include <optional>
#include <unordered_map>

#include "CompilationInfo.h"
#include "absl/strings/str_join.h"
#include "engine/AddCombinedRowToTable.h"
#include "index/IndexFormatVersion.h"
#include "index/PrefixHeuristic.h"
#include "index/TriplesView.h"
#include "index/VocabularyMerger.h"
#include "parser/ParallelParseBuffer.h"
#include "util/BatchedPipeline.h"
#include "util/CachingMemoryResource.h"
#include "util/HashMap.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"
#include "util/ProgressBar.h"
#include "util/Serializer/FileSerializer.h"
#include "util/ThreadSafeQueue.h"
#include "util/TupleHelpers.h"
#include "util/TypeTraits.h"

using std::array;
using namespace ad_utility::memory_literals;

// During the index building we typically have two permutation sortings present
// at the same time, as we directly push the triples from the first sorting to
// the second sorting. We therefore have to adjust the amount of memory per
// external sorter.
static constexpr size_t NUM_EXTERNAL_SORTERS_AT_SAME_TIME = 2u;

// _____________________________________________________________________________
IndexImpl::IndexImpl(ad_utility::AllocatorWithLimit<Id> allocator)
    : allocator_{std::move(allocator)} {};

// _____________________________________________________________________________
IndexBuilderDataAsFirstPermutationSorter IndexImpl::createIdTriplesAndVocab(
    std::shared_ptr<TurtleParserBase> parser) {
  auto indexBuilderData =
      passFileForVocabulary(std::move(parser), numTriplesPerBatch_);
  // first save the total number of words, this is needed to initialize the
  // dense IndexMetaData variants
  totalVocabularySize_ = indexBuilderData.vocabularyMetaData_.numWordsTotal_;
  LOG(DEBUG) << "Number of words in internal and external vocabulary: "
             << totalVocabularySize_ << std::endl;

  auto firstSorter = convertPartialToGlobalIds(
      *indexBuilderData.idTriples, indexBuilderData.actualPartialSizes,
      NUM_TRIPLES_PER_PARTIAL_VOCAB);

  return {indexBuilderData, std::move(firstSorter)};
}

std::unique_ptr<TurtleParserBase> IndexImpl::makeTurtleParser(
    const std::string& filename) {
  auto setTokenizer = [this,
                       &filename]<template <typename> typename ParserTemplate>()
      -> std::unique_ptr<TurtleParserBase> {
    if (onlyAsciiTurtlePrefixes_) {
      return std::make_unique<ParserTemplate<TokenizerCtre>>(filename);
    } else {
      return std::make_unique<ParserTemplate<Tokenizer>>(filename);
    }
  };

  if (useParallelParser_) {
    return setTokenizer.template operator()<TurtleParallelParser>();
  } else {
    return setTokenizer.template operator()<TurtleStreamParser>();
  }
}

// Several helper functions for joining the OSP permutation with the patterns.
namespace {
// Return an input range of the blocks that are returned by the external sorter
// to which `sorterPtr` points. Only the subset/permutation specified by the
// `columnIndices` will be returned for each block.
auto lazyScanWithPermutedColumns(auto& sorterPtr, auto columnIndices) {
  auto setSubset = [columnIndices](auto& idTable) {
    idTable.setColumnSubset(columnIndices);
  };
  return ad_utility::inPlaceTransformView(
      ad_utility::OwningView{sorterPtr->template getSortedBlocks<0>()},
      setSubset);
}

// Perform a lazy optional block join on the first column of `leftInput` and
// `rightInput`. The `resultCallback` will be called for each block of resulting
// rows. Assumes that `leftInput` and `rightInput` have 6 columns in total, so
// the result will have 5 columns.
auto lazyOptionalJoinOnFirstColumn(auto& leftInput, auto& rightInput,
                                   auto resultCallback) {
  auto projection = [](const auto& row) -> Id { return row[0]; };
  auto projectionForComparator = []<typename T>(const T& rowOrId) {
    if constexpr (ad_utility::SimilarTo<T, Id>) {
      return rowOrId;
    } else {
      return rowOrId[0];
    }
  };
  auto comparator = [&projectionForComparator](const auto& l, const auto& r) {
    return projectionForComparator(l) < projectionForComparator(r);
  };

  // There are 5 columns in the result (3 from the triple, as well as subject
  // patterns of the subject and object).
  IdTable outputTable{5, ad_utility::makeUnlimitedAllocator<Id>()};
  // The first argument is the number of join columns.
  auto rowAdder = ad_utility::AddCombinedRowToIdTable{
      1, std::move(outputTable),
      std::make_shared<ad_utility::CancellationHandle<>>(),
      BUFFER_SIZE_JOIN_PATTERNS_WITH_OSP, resultCallback};

  ad_utility::zipperJoinForBlocksWithoutUndef(leftInput, rightInput, comparator,
                                              rowAdder, projection, projection,
                                              std::true_type{});
  rowAdder.flush();
}

// In the pattern column replace UNDEF (which is created by the optional join)
// by the special `NO_PATTERN` ID and undo the permutation of the columns that
// was only needed for the join algorithm.
auto fixBlockAfterPatternJoin(auto block) {
  // The permutation must be the inverse of the original permutation, which just
  // switches the third column (the object) into the first column (where the
  // join column is expected by the algorithms).
  block.value().setColumnSubset(std::array<ColumnIndex, 5>{2, 1, 0, 3, 4});
  std::ranges::for_each(block.value().getColumn(4), [](Id& id) {
    id = id.isUndefined() ? Id::makeFromInt(NO_PATTERN) : id;
  });
  return std::move(block.value()).template toStatic<0>();
}
}  // namespace

// ____________________________________________________________________________
std::unique_ptr<ExternalSorter<SortByPSO, 5>> IndexImpl::buildOspWithPatterns(
    PatternCreator::TripleSorter sortersFromPatternCreator,
    auto isQleverInternalId) {
  auto&& [hasPatternPredicateSortedByPSO, secondSorter] =
      sortersFromPatternCreator;
  // We need the patterns twice: once for the additional column, and once for
  // the additional permutation.
  hasPatternPredicateSortedByPSO->moveResultOnMerge() = false;
  // The column with index 1 always is `has-predicate` and is not needed here.
  // Note that the order of the columns during index building  is alwasy `SPO`,
  // but the sorting might be different (PSO in this case).
  auto lazyPatternScan = lazyScanWithPermutedColumns(
      hasPatternPredicateSortedByPSO, std::array<ColumnIndex, 2>{0, 2});
  ad_utility::data_structures::ThreadSafeQueue<IdTable> queue{4};

  // The permutation (2, 1, 0, 3) switches the third column (the object) with
  // the first column (where the join column is expected by the algorithms).
  // This permutation is reverted as part of the `fixBlockAfterPatternJoin`
  // function.
  auto ospAsBlocksTransformed = lazyScanWithPermutedColumns(
      secondSorter, std::array<ColumnIndex, 4>{2, 1, 0, 3});

  // Run the actual join between the OSP permutation and the `has-pattern`
  // predicate on a background thread. The result will be pushed to the `queue`
  // so that we can consume it asynchronously.
  ad_utility::JThread joinWithPatternThread{
      [&queue, &ospAsBlocksTransformed, &lazyPatternScan] {
        // Setup the callback for the join that will buffer the results and push
        // them to the queue.
        IdTable outputBufferTable{5, ad_utility::makeUnlimitedAllocator<Id>()};
        auto pushToQueue =
            [&, bufferSize =
                    BUFFER_SIZE_JOIN_PATTERNS_WITH_OSP.load()](IdTable& table) {
              if (table.numRows() >= bufferSize) {
                if (!outputBufferTable.empty()) {
                  queue.push(std::move(outputBufferTable));
                  outputBufferTable.clear();
                }
                queue.push(std::move(table));
              } else {
                outputBufferTable.insertAtEnd(table.begin(), table.end());
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
  auto blockGenerator =
      [](auto& queue) -> cppcoro::generator<IdTableStatic<0>> {
    while (auto block = queue.pop()) {
      co_yield fixBlockAfterPatternJoin(std::move(block));
    }
  }(queue);

  // Actually create the permutations.
  auto thirdSorter =
      makeSorterPtr<ThirdPermutation, NumColumnsIndexBuilding + 2>("third");
  createSecondPermutationPair(NumColumnsIndexBuilding + 2, isQleverInternalId,
                              std::move(blockGenerator), *thirdSorter);
  secondSorter->clear();
  // Add the `ql:has-pattern` predicate to the sorter such that it will become
  // part of the PSO and POS permutation.
  LOG(INFO) << "Adding " << hasPatternPredicateSortedByPSO->size()
            << " triples to the POS and PSO permutation for "
               "the internal `ql:has-pattern` ..."
            << std::endl;
  auto noPattern = Id::makeFromInt(NO_PATTERN);
  static_assert(NumColumnsIndexBuilding == 3);
  for (const auto& row : hasPatternPredicateSortedByPSO->sortedView()) {
    // The repetition of the pattern index (`row[2]`) for the fourth column is
    // useful for generic unit testing, but not needed otherwise.
    thirdSorter->push(std::array{row[0], row[1], row[2], row[2], noPattern});
  }
  hasPatternPredicateSortedByPSO->clear();
  return thirdSorter;
}
// _____________________________________________________________________________
void IndexImpl::createFromFile(const string& filename) {
  if (!loadAllPermutations_ && usePatterns_) {
    throw std::runtime_error{
        "The patterns can only be built when all 6 permutations are created"};
  }
  LOG(INFO) << "Processing input triples from " << filename << " ..."
            << std::endl;

  readIndexBuilderSettingsFromFile();

  IndexBuilderDataAsFirstPermutationSorter indexBuilderData =
      createIdTriplesAndVocab(makeTurtleParser(filename));

  // Write the configuration already at this point, so we have it available in
  // case any of the permutations fail.
  writeConfiguration();

  auto isQleverInternalId = [&indexBuilderData](const auto& id) {
    // The special internal IDs like `ql:has-pattern` (see `SpecialIds.h`)
    // have the datatype `UNDEFINED`.
    return indexBuilderData.vocabularyMetaData_.isQleverInternalId(id) ||
           id.getDatatype() == Datatype::Undefined;
  };

  auto& firstSorter = *indexBuilderData.sorter_;
  // For the first permutation, perform a unique.
  auto firstSorterWithUnique =
      ad_utility::uniqueBlockView(firstSorter.getSortedOutput());

  if (!loadAllPermutations_) {
    // Only two permutations, no patterns, in this case the `firstSorter` is a
    // PSO sorter, and `createPermutationPair` creates PSO/POS permutations.
    createFirstPermutationPair(NumColumnsIndexBuilding, isQleverInternalId,
                               std::move(firstSorterWithUnique));
    configurationJson_["has-all-permutations"] = false;
  } else if (loadAllPermutations_ && !usePatterns_) {
    // Without patterns we explicitly have to pass in the next sorters to all
    // permutation creating functions.
    auto secondSorter = makeSorter<SecondPermutation>("second");
    createFirstPermutationPair(NumColumnsIndexBuilding, isQleverInternalId,
                               std::move(firstSorterWithUnique), secondSorter);
    firstSorter.clearUnderlying();

    auto thirdSorter = makeSorter<ThirdPermutation>("third");
    createSecondPermutationPair(NumColumnsIndexBuilding, isQleverInternalId,
                                secondSorter.getSortedBlocks<0>(), thirdSorter);
    secondSorter.clear();
    createThirdPermutationPair(NumColumnsIndexBuilding, isQleverInternalId,
                               thirdSorter.getSortedBlocks<0>());
    configurationJson_["has-all-permutations"] = true;
  } else {
    // Load all permutations and also load the patterns. In this case the
    // `createFirstPermutationPair` function returns the next sorter, already
    // enriched with the patterns of the subjects in the triple.
    auto patternOutput =
        createFirstPermutationPair(NumColumnsIndexBuilding, isQleverInternalId,
                                   std::move(firstSorterWithUnique));
    firstSorter.clearUnderlying();
    auto thirdSorterPtr = buildOspWithPatterns(std::move(patternOutput.value()),
                                               isQleverInternalId);
    createThirdPermutationPair(NumColumnsIndexBuilding + 2, isQleverInternalId,
                               thirdSorterPtr->template getSortedBlocks<0>());
    configurationJson_["has-all-permutations"] = true;
  }

  // Dump the configuration again in case the permutations have added some
  // information.
  writeConfiguration();
  LOG(INFO) << "Index build completed" << std::endl;
}

// _____________________________________________________________________________
IndexBuilderDataAsStxxlVector IndexImpl::passFileForVocabulary(
    std::shared_ptr<TurtleParserBase> parser, size_t linesPerPartial) {
  parser->integerOverflowBehavior() = turtleParserIntegerOverflowBehavior_;
  parser->invalidLiteralsAreSkipped() = turtleParserSkipIllegalLiterals_;
  ad_utility::Synchronized<std::unique_ptr<TripleVec>> idTriples(
      std::make_unique<TripleVec>(onDiskBase_ + ".unsorted-triples.dat", 1_GB,
                                  allocator_));
  LOG(INFO) << "Parsing input triples and creating partial vocabularies, one "
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

  ad_utility::CachingMemoryResource cachingMemoryResource;
  ItemAlloc itemAlloc(&cachingMemoryResource);
  size_t numTriplesProcessed = 0;
  ad_utility::ProgressBar progressBar{numTriplesProcessed, "Triples parsed: "};
  while (!parserExhausted) {
    size_t actualCurrentPartialSize = 0;

    std::vector<std::array<Id, 3>> localWriter;

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
        for (const auto& innerOpt : opt.value()) {
          if (innerOpt) {
            actualCurrentPartialSize++;
            localWriter.push_back(innerOpt.value());
          }
        }
        numTriplesProcessed++;
        if (progressBar.update()) {
          LOG(INFO) << progressBar.getProgressString() << std::flush;
        }
      }
      LOG(TIMING) << "WaitTimes for Pipeline in msecs\n";
      for (const auto& t : p.getWaitingTime()) {
        LOG(TIMING)
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
    LOG(TIMING)
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
            numTriplesProcessed, numFiles, actualCurrentPartialSize,
            std::move(oldItemPtr), std::move(localWriter), &idTriples);
    numFiles++;
    // Save the information how many triples this partial vocabulary actually
    // deals with we will use this later for mapping from partial to global
    // ids
    actualPartialSizes.push_back(actualCurrentPartialSize);
  }
  LOG(INFO) << progressBar.getFinalProgressString() << std::flush;
  for (auto& future : writePartialVocabularyFuture) {
    if (future.valid()) {
      future.get();
    }
  }
  LOG(INFO) << "Number of triples created (including QLever-internal ones): "
            << (*idTriples.wlock())->size() << " [may contain duplicates]"
            << std::endl;

  size_t sizeInternalVocabulary = 0;
  std::vector<std::string> prefixes;

  LOG(INFO) << "Merging partial vocabularies ..." << std::endl;
  const ad_utility::vocabulary_merger::VocabularyMetaData mergeRes = [&]() {
    auto sortPred = [cmp = &(vocab_.getCaseComparator())](std::string_view a,
                                                          std::string_view b) {
      return (*cmp)(a, b, decltype(vocab_)::SortLevel::TOTAL);
    };
    auto internalVocabularyAction = vocab_.makeWordWriterForInternalVocabulary(
        onDiskBase_ + INTERNAL_VOCAB_SUFFIX);
    internalVocabularyAction.readableName() = "internal vocabulary";
    auto externalVocabularyAction = vocab_.makeWordWriterForExternalVocabulary(
        onDiskBase_ + EXTERNAL_VOCAB_SUFFIX);
    externalVocabularyAction.readableName() = "external vocabulary";
    return ad_utility::vocabulary_merger::mergeVocabulary(
        onDiskBase_, numFiles, sortPred, internalVocabularyAction,
        externalVocabularyAction, memoryLimitIndexBuilding());
  }();
  LOG(DEBUG) << "Finished merging partial vocabularies" << std::endl;
  IndexBuilderDataAsStxxlVector res;
  res.vocabularyMetaData_ = mergeRes;
  LOG(INFO) << "Number of words in external vocabulary: "
            << res.vocabularyMetaData_.numWordsTotal_ - sizeInternalVocabulary
            << std::endl;

  res.idTriples = std::move(*idTriples.wlock());
  res.actualPartialSizes = std::move(actualPartialSizes);

  LOG(DEBUG) << "Removing temporary files ..." << std::endl;
  for (size_t n = 0; n < numFiles; ++n) {
    deleteTemporaryFile(absl::StrCat(onDiskBase_, PARTIAL_VOCAB_FILE_NAME, n));
  }

  return res;
}

// _____________________________________________________________________________
std::unique_ptr<ad_utility::CompressedExternalIdTableSorterTypeErased>
IndexImpl::convertPartialToGlobalIds(
    TripleVec& data, const vector<size_t>& actualLinesPerPartial,
    size_t linesPerPartial) {
  LOG(INFO) << "Converting triples from local IDs to global IDs ..."
            << std::endl;
  LOG(DEBUG) << "Triples per partial vocabulary: " << linesPerPartial
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
  auto& result = *resultPtr;
  auto triplesGenerator = data.getRows();
  auto it = triplesGenerator.begin();
  using Buffer = IdTableStatic<3>;
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
  auto getWriteTask = [&result, &numTriplesConverted,
                       &progressBar](Buffer triples) {
    return [&result, &numTriplesConverted, &progressBar,
            triples = std::make_shared<IdTableStatic<0>>(
                std::move(triples).toDynamic())] {
      result.pushBlock(*triples);
      numTriplesConverted += triples->size();
      if (progressBar.update()) {
        LOG(INFO) << progressBar.getProgressString() << std::flush;
      }
    };
  };

  // Return a lambda that for each of the `triples` transforms its partial to
  // global IDs using the `idMap`. The map is passed as a `shared_ptr` because
  // multiple batches need access to the same map.
  auto getLookupTask = [&writeQueue, &transformTriple, &getWriteTask](
                           Buffer triples, std::shared_ptr<Map> idMap) {
    return
        [&writeQueue, triples = std::make_shared<Buffer>(std::move(triples)),
         idMap = std::move(idMap), &getWriteTask, &transformTriple]() mutable {
          for (Buffer::row_reference triple : *triples) {
            transformTriple(triple, *idMap);
          }
          writeQueue.push(getWriteTask(std::move(*triples)));
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
    std::string mmapFilename = absl::StrCat(onDiskBase_, PARTIAL_MMAP_IDS, idx);
    auto map =
        ad_utility::vocabulary_merger::IdMapFromPartialIdMapFile(mmapFilename);
    // Delete the temporary file in which we stored this map
    deleteTemporaryFile(mmapFilename);
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

    const size_t bufferSize = BUFFER_SIZE_PARTIAL_TO_GLOBAL_ID_MAPPINGS;
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
  LOG(INFO) << progressBar.getFinalProgressString() << std::flush;
  return resultPtr;
}

// _____________________________________________________________________________
std::tuple<size_t, IndexImpl::IndexMetaDataMmapDispatcher::WriteType,
           IndexImpl::IndexMetaDataMmapDispatcher::WriteType>
IndexImpl::createPermutationPairImpl(size_t numColumns, const string& fileName1,
                                     const string& fileName2,
                                     auto&& sortedTriples,
                                     std::array<size_t, 3> permutation,
                                     auto&&... perTripleCallbacks) {
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
      std::ranges::for_each(block, callback);
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

  // There previously was a bug in the CompressedIdTableSorter that lead to
  // semantically correct blocks, but with too large block sizes for the twin
  // relation. This assertion would have caught this bug.
  AD_CORRECTNESS_CHECK(metaData1.blockData().size() ==
                       metaData2.blockData().size());

  return {numDistinctCol0, std::move(metaData1), std::move(metaData2)};
}

// ________________________________________________________________________
std::tuple<size_t, IndexImpl::IndexMetaDataMmapDispatcher::WriteType,
           IndexImpl::IndexMetaDataMmapDispatcher::WriteType>
IndexImpl::createPermutations(size_t numColumns, auto&& sortedTriples,
                              const Permutation& p1, const Permutation& p2,
                              auto&&... perTripleCallbacks) {
  LOG(INFO) << "Creating permutations " << p1.readableName() << " and "
            << p2.readableName() << " ..." << std::endl;
  auto metaData = createPermutationPairImpl(
      numColumns, onDiskBase_ + ".index" + p1.fileSuffix(),
      onDiskBase_ + ".index" + p2.fileSuffix(), AD_FWD(sortedTriples),
      p1.keyOrder(), AD_FWD(perTripleCallbacks)...);

  auto& [numDistinctCol0, meta1, meta2] = metaData;
  meta1.calculateStatistics(numDistinctCol0);
  meta2.calculateStatistics(numDistinctCol0);
  LOG(INFO) << "Statistics for " << p1.readableName() << ": "
            << meta1.statistics() << std::endl;
  LOG(INFO) << "Statistics for " << p2.readableName() << ": "
            << meta2.statistics() << std::endl;

  return metaData;
}

// ________________________________________________________________________
size_t IndexImpl::createPermutationPair(size_t numColumns, auto&& sortedTriples,
                                        const Permutation& p1,
                                        const Permutation& p2,
                                        auto&&... perTripleCallbacks) {
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
  LOG(DEBUG) << "Writing meta data for " << p1.readableName() << " and "
             << p2.readableName() << " ..." << std::endl;
  writeMetadata(metaData1, p1);
  writeMetadata(metaData2, p2);
  return numDistinctC0;
}

// _____________________________________________________________________________
void IndexImpl::createFromOnDiskIndex(const string& onDiskBase) {
  setOnDiskBase(onDiskBase);
  readConfiguration();
  vocab_.readFromFile(onDiskBase_ + INTERNAL_VOCAB_SUFFIX,
                      onDiskBase_ + EXTERNAL_VOCAB_SUFFIX);

  totalVocabularySize_ = vocab_.size() + vocab_.getExternalVocab().size();
  LOG(DEBUG) << "Number of words in internal and external vocabulary: "
             << totalVocabularySize_ << std::endl;

  pso_.loadFromDisk(onDiskBase_);
  pos_.loadFromDisk(onDiskBase_);

  if (loadAllPermutations_) {
    ops_.loadFromDisk(onDiskBase_);
    osp_.loadFromDisk(onDiskBase_);
    spo_.loadFromDisk(onDiskBase_);
    sop_.loadFromDisk(onDiskBase_);
  } else {
    LOG(INFO) << "Only the PSO and POS permutation were loaded, SPARQL queries "
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
      LOG(WARN) << "Could not load the patterns. The internal predicate "
                   "`ql:has-predicate` is therefore not available (and certain "
                   "queries that benefit from that predicate will be slower)."
                   "To suppress this warning, start the server with "
                   "the `--no-patterns` option. The error message was "
                << e.what() << std::endl;
      usePatterns_ = false;
    }
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
bool IndexImpl::isLiteral(const string& object) const {
  return decltype(vocab_)::stringIsLiteral(object);
}

// _____________________________________________________________________________
void IndexImpl::setKbName(const string& name) {
  pos_.setKbName(name);
  pso_.setKbName(name);
  sop_.setKbName(name);
  spo_.setKbName(name);
  ops_.setKbName(name);
  osp_.setKbName(name);
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
  configuration["git-hash"] = qlever::version::GitShortHash;
  configuration["index-format-version"] = qlever::indexFormatVersion;
  auto f = ad_utility::makeOfstream(onDiskBase_ + CONFIGURATION_FILE);
  f << configuration;
}

// ___________________________________________________________________________
void IndexImpl::readConfiguration() {
  auto f = ad_utility::makeIfstream(onDiskBase_ + CONFIGURATION_FILE);
  f >> configurationJson_;
  if (configurationJson_.find("git-hash") != configurationJson_.end()) {
    LOG(INFO) << "The git hash used to build this index was "
              << configurationJson_["git-hash"] << std::endl;
  } else {
    LOG(INFO) << "The index was built before git commit hashes were stored in "
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
        LOG(ERROR) << "The version of QLever you are using is too old for this "
                      "index. Please use a version of QLever that is "
                      "compatible with this index"
                      " (PR = "
                   << indexFormatVersion.prNumber_ << ", Date = "
                   << indexFormatVersion.date_.toStringAndType().first << ")."
                   << std::endl;
      } else {
        LOG(ERROR) << "The index is too old for this version of QLever. "
                      "We recommend that you rebuild the index and start the "
                      "server with the current master. Alternatively start the "
                      "engine with a version of QLever that is compatible with "
                      "this index (PR = "
                   << indexFormatVersion.prNumber_ << ", Date = "
                   << indexFormatVersion.date_.toStringAndType().first << ")."
                   << std::endl;
      }
      throw std::runtime_error{
          "Incompatible index format, see log message for details"};
    }
  } else {
    LOG(ERROR) << "This index was built before versioning was introduced for "
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
    LOG(ERROR) << ERROR_IGNORE_CASE_UNSUPPORTED << '\n';
    throw std::runtime_error("Deprecated key \"ignore-case\" in index build");
  }

  if (configurationJson_.count("locale")) {
    std::string lang{configurationJson_["locale"]["language"]};
    std::string country{configurationJson_["locale"]["country"]};
    bool ignorePunctuation{configurationJson_["locale"]["ignore-punctuation"]};
    vocab_.setLocale(lang, country, ignorePunctuation);
    textVocab_.setLocale(lang, country, ignorePunctuation);
  } else {
    LOG(ERROR) << "Key \"locale\" is missing in the metadata. This is probably "
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

  auto loadDataMember = [this]<typename Target>(
                            std::string_view key, Target& target,
                            std::optional<std::type_identity_t<Target>>
                                defaultValue = std::nullopt) {
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

  loadDataMember("has-all-permutations", loadAllPermutations_, true);
  loadDataMember("num-predicates", numPredicates_);
  // These might be missing if there are only two permutations.
  loadDataMember("num-subjects", numSubjects_, NumNormalAndInternal{});
  loadDataMember("num-objects", numObjects_, NumNormalAndInternal{});
  loadDataMember("num-triples", numTriples_, NumNormalAndInternal{});

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
  resultTriple[0] = std::move(triple.subject_);
  resultTriple[1] = TripleComponent{std::move(triple.predicate_)};
  if (triple.object_.isLiteral()) {
    const auto& lit = triple.object_.getLiteral();
    if (lit.hasLanguageTag()) {
      result.langtag_ = std::string(asStringViewUnsafe(lit.getLanguageTag()));
    }
  }

  // If the object of the triple can be directly folded into an ID, do so. Note
  // that the actual folding is done by the `TripleComponent`.
  std::optional<Id> idIfNotString = triple.object_.toValueIdIfNotString();

  // TODO<joka921> The following statement could be simplified by a helper
  // function "optionalCast";
  if (idIfNotString.has_value()) {
    resultTriple[2] = idIfNotString.value();
  } else {
    // `toRdfLiteral` handles literals as well as IRIs correctly.
    resultTriple[2] = std::move(triple.object_);
  }

  for (size_t i = 0; i < 3; ++i) {
    auto& el = resultTriple[i];
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
  json j;  // if we have no settings, we still have to initialize some default
           // values
  if (!settingsFileName_.empty()) {
    auto f = ad_utility::makeIfstream(settingsFileName_);
    f >> j;
  }

  if (j.find("prefixes-external") != j.end()) {
    vocab_.initializeExternalizePrefixes(j["prefixes-external"]);
    configurationJson_["prefixes-external"] = j["prefixes-external"];
  }

  if (j.count("ignore-case")) {
    LOG(ERROR) << ERROR_IGNORE_CASE_UNSUPPORTED << '\n';
    throw std::runtime_error("Deprecated key \"ignore-case\" in settings JSON");
  }

  /**
   * ICU uses two separate arguments for each Locale, the language ("en" or
   * "fr"...) and the country ("GB", "CA"...). The encoding has to be known at
   * compile time for ICU and will always be UTF-8 so it is not part of the
   * locale setting.
   */

  {
    std::string lang = LOCALE_DEFAULT_LANG;
    std::string country = LOCALE_DEFAULT_COUNTRY;
    bool ignorePunctuation = LOCALE_DEFAULT_IGNORE_PUNCTUATION;
    if (j.count("locale")) {
      lang = std::string{j["locale"]["language"]};
      country = std::string{j["locale"]["country"]};
      ignorePunctuation = bool{j["locale"]["ignore-punctuation"]};
    } else {
      LOG(INFO) << "Locale was not specified in settings file, default is "
                   "en_US"
                << std::endl;
    }
    LOG(INFO) << "You specified \"locale = " << lang << "_" << country << "\" "
              << "and \"ignore-punctuation = " << ignorePunctuation << "\""
              << std::endl;

    if (lang != LOCALE_DEFAULT_LANG || country != LOCALE_DEFAULT_COUNTRY) {
      LOG(WARN) << "You are using Locale settings that differ from the default "
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
    LOG(INFO) << WARNING_ASCII_ONLY_PREFIXES << std::endl;
  }

  if (j.count("parallel-parsing")) {
    useParallelParser_ = static_cast<bool>(j["parallel-parsing"]);
  }
  if (useParallelParser_) {
    LOG(INFO) << WARNING_PARALLEL_PARSING << std::endl;
  }

  if (j.count("num-triples-per-batch")) {
    numTriplesPerBatch_ = size_t{j["num-triples-per-batch"]};
    LOG(INFO)
        << "You specified \"num-triples-per-batch = " << numTriplesPerBatch_
        << "\", choose a lower value if the index builder runs out of memory"
        << std::endl;
  }

  if (j.count("parser-batch-size")) {
    parserBatchSize_ = size_t{j["parser-batch-size"]};
    LOG(INFO) << "Overriding setting parser-batch-size to " << parserBatchSize_
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
      LOG(INFO) << "Integers that cannot be represented by QLever will throw "
                   "an exception"
                << std::endl;
      turtleParserIntegerOverflowBehavior_ =
          TurtleParserIntegerOverflowBehavior::Error;
    } else if (value == overflowingIntegersBecomeDoubles) {
      LOG(INFO) << "Integers that cannot be represented by QLever will be "
                   "converted to doubles"
                << std::endl;
      turtleParserIntegerOverflowBehavior_ =
          TurtleParserIntegerOverflowBehavior::OverflowingToDouble;
    } else if (value == allIntegersBecomeDoubles) {
      LOG(INFO) << "All integers will be converted to doubles" << std::endl;
      turtleParserIntegerOverflowBehavior_ =
          TurtleParserIntegerOverflowBehavior::OverflowingToDouble;
    } else {
      AD_CONTRACT_CHECK(std::ranges::find(allModes, value) == allModes.end());
      LOG(ERROR) << "Invalid value for " << key << std::endl;
      LOG(INFO) << "The currently supported values are "
                << absl::StrJoin(allModes, ",") << std::endl;
    }
  } else {
    turtleParserIntegerOverflowBehavior_ =
        TurtleParserIntegerOverflowBehavior::Error;
    LOG(INFO) << "By default, integers that cannot be represented by QLever "
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
  LOG(DEBUG) << "Input triples read in this section: " << numLines << std::endl;
  LOG(DEBUG)
      << "Triples processed, also counting internal triples added by QLever: "
      << actualCurrentPartialSize << std::endl;
  std::future<void> resultFuture;
  string partialFilename =
      absl::StrCat(onDiskBase_, PARTIAL_VOCAB_FILE_NAME, numFiles);
  string partialCompressionFilename = absl::StrCat(
      onDiskBase_, TMP_BASENAME_COMPRESSION, PARTIAL_VOCAB_FILE_NAME, numFiles);

  auto lambda = [localIds = std::move(localIds), globalWritePtr,
                 items = std::move(items), vocab = &vocab_, partialFilename,
                 partialCompressionFilename, numFiles]() mutable {
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
      return createInternalMapping(&vec);
    }();
    LOG(TRACE) << "Finished creating of Mapping vocabulary" << std::endl;
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
    // The writing to the STXXL vector has to be done in order, to
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
    LOG(TRACE) << "Finished writing the partial vocabulary" << std::endl;
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
Permutation& IndexImpl::getPermutation(Permutation::Enum p) {
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
const Permutation& IndexImpl::getPermutation(Permutation::Enum p) const {
  return const_cast<IndexImpl&>(*this).getPermutation(p);
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
size_t IndexImpl::getCardinality(Id id, Permutation::Enum permutation) const {
  if (const auto& meta = getPermutation(permutation).getMetadata(id);
      meta.has_value()) {
    return meta.value().numRows_;
  }
  return 0;
}

// ___________________________________________________________________________
size_t IndexImpl::getCardinality(const TripleComponent& comp,
                                 Permutation::Enum permutation) const {
  // TODO<joka921> This special case is only relevant for the `PSO` and `POS`
  // permutations, but this internal predicate should never appear in subjects
  // or objects anyway.
  // TODO<joka921> Find out what the effect of this special case is for the
  // query planning.
  if (comp == INTERNAL_TEXT_MATCH_PREDICATE) {
    return TEXT_PREDICATE_CARDINALITY_ESTIMATE;
  }
  if (std::optional<Id> relId = comp.toValueId(getVocab()); relId.has_value()) {
    return getCardinality(relId.value(), permutation);
  }
  return 0;
}

// TODO<joka921> Once we have an overview over the folding this logic should
// probably not be in the index class.
std::optional<string> IndexImpl::idToOptionalString(VocabIndex id) const {
  return vocab_.indexToOptionalString(id);
}

std::optional<string> IndexImpl::idToOptionalString(WordVocabIndex id) const {
  return textVocab_.indexToOptionalString(id);
}

// ___________________________________________________________________________
std::optional<Id> IndexImpl::getIdImpl(const auto& element) const {
  VocabIndex vocabIndex;
  auto success =
      getVocab().getId(element.toStringRepresentation(), &vocabIndex);
  if (!success) {
    return std::nullopt;
  }
  return Id::makeFromVocabIndex(vocabIndex);
}

// ___________________________________________________________________________
std::optional<Id> IndexImpl::getId(
    const ad_utility::triple_component::LiteralOrIri& element) const {
  return getIdImpl(element);
}

// ___________________________________________________________________________
std::optional<Id> IndexImpl::getId(
    const ad_utility::triple_component::Literal& element) const {
  return getIdImpl(element);
}

// ___________________________________________________________________________
std::optional<Id> IndexImpl::getId(
    const ad_utility::triple_component::Iri& element) const {
  return getIdImpl(element);
}

// ___________________________________________________________________________
Index::Vocab::PrefixRanges IndexImpl::prefixRanges(
    std::string_view prefix) const {
  // TODO<joka921> Do we need prefix ranges for numbers?
  return vocab_.prefixRanges(prefix);
}

// _____________________________________________________________________________
vector<float> IndexImpl::getMultiplicities(
    const TripleComponent& key, Permutation::Enum permutation) const {
  if (auto keyId = key.toValueId(getVocab()); keyId.has_value()) {
    auto meta = getPermutation(permutation).getMetadata(keyId.value());
    if (meta.has_value()) {
      return {meta.value().getCol1Multiplicity(),
              meta.value().getCol2Multiplicity()};
    }
  }
  return {1.0f, 1.0f};
}

// ___________________________________________________________________
vector<float> IndexImpl::getMultiplicities(
    Permutation::Enum permutation) const {
  const auto& p = getPermutation(permutation);
  auto numTriples = static_cast<float>(this->numTriples().normalAndInternal_());
  std::array<float, 3> m{
      numTriples / numDistinctSubjects().normalAndInternal_(),
      numTriples / numDistinctPredicates().normalAndInternal_(),
      numTriples / numDistinctObjects().normalAndInternal_()};
  return {m[p.keyOrder()[0]], m[p.keyOrder()[1]], m[p.keyOrder()[2]]};
}

// _____________________________________________________________________________
IdTable IndexImpl::scan(
    const TripleComponent& col0String,
    std::optional<std::reference_wrapper<const TripleComponent>> col1String,
    const Permutation::Enum& permutation,
    Permutation::ColumnIndicesRef additionalColumns,
    const ad_utility::SharedCancellationHandle& cancellationHandle) const {
  std::optional<Id> col0Id = col0String.toValueId(getVocab());
  std::optional<Id> col1Id =
      col1String.has_value() ? col1String.value().get().toValueId(getVocab())
                             : std::nullopt;
  if (!col0Id.has_value() || (col1String.has_value() && !col1Id.has_value())) {
    size_t numColumns = col1String.has_value() ? 1 : 2;
    cancellationHandle->throwIfCancelled();
    return IdTable{numColumns, allocator_};
  }
  return scan(col0Id.value(), col1Id, permutation, additionalColumns,
              cancellationHandle);
}
// _____________________________________________________________________________
IdTable IndexImpl::scan(
    Id col0Id, std::optional<Id> col1Id, Permutation::Enum p,
    Permutation::ColumnIndicesRef additionalColumns,
    const ad_utility::SharedCancellationHandle& cancellationHandle) const {
  return getPermutation(p).scan({col0Id, col1Id, std::nullopt},
                                additionalColumns, cancellationHandle);
}

// _____________________________________________________________________________
size_t IndexImpl::getResultSizeOfScan(
    const TripleComponent& col0, const TripleComponent& col1,
    const Permutation::Enum& permutation) const {
  std::optional<Id> col0Id = col0.toValueId(getVocab());
  std::optional<Id> col1Id = col1.toValueId(getVocab());
  if (!col0Id.has_value() || !col1Id.has_value()) {
    return 0;
  }
  const Permutation& p = getPermutation(permutation);
  return p.getResultSizeOfScan({col0Id.value(), col1Id.value(), std::nullopt});
}

// _____________________________________________________________________________
void IndexImpl::deleteTemporaryFile(const string& path) {
  if (!keepTempFiles_) {
    ad_utility::deleteFile(path);
  }
}

namespace {

// Return a lambda that is called repeatedly with triples that are sorted by the
// `idx`-th column and counts the number of distinct entities that occur in a
// triple where none of the elements fulfills the `isQleverInternalId`
// predicate. This is used to cound the number of distinct subjects, objects,
// and predicates during the index building.
template <size_t idx>
auto makeNumDistinctIdsCounter =
    [](size_t& numDistinctIds,
       ad_utility::InvocableWithExactReturnType<bool, Id> auto
           isQleverInternalId) {
      return [lastId = std::optional<Id>{}, &numDistinctIds,
              isInternalId =
                  std::move(isQleverInternalId)](const auto& triple) mutable {
        const auto& id = triple[idx];
        if (id != lastId && !std::ranges::any_of(triple, isInternalId)) {
          numDistinctIds++;
          lastId = id;
        }
      };
    };
}  // namespace

// _____________________________________________________________________________
template <typename... NextSorter>
requires(sizeof...(NextSorter) <= 1)
void IndexImpl::createPSOAndPOS(size_t numColumns, auto& isInternalId,
                                BlocksOfTriples sortedTriples,
                                NextSorter&&... nextSorter)

{
  size_t numTriplesNormal = 0;
  size_t numTriplesTotal = 0;
  auto countTriplesNormal = [&numTriplesNormal, &numTriplesTotal,
                             &isInternalId](const auto& triple) mutable {
    ++numTriplesTotal;
    numTriplesNormal += std::ranges::none_of(triple, isInternalId);
  };
  size_t numPredicatesNormal = 0;
  auto predicateCounter =
      makeNumDistinctIdsCounter<1>(numPredicatesNormal, isInternalId);
  size_t numPredicatesTotal =
      createPermutationPair(numColumns, AD_FWD(sortedTriples), pso_, pos_,
                            nextSorter.makePushCallback()...,
                            std::ref(predicateCounter), countTriplesNormal);
  configurationJson_["num-predicates"] =
      NumNormalAndInternal::fromNormalAndTotal(numPredicatesNormal,
                                               numPredicatesTotal);
  configurationJson_["num-triples"] = NumNormalAndInternal::fromNormalAndTotal(
      numTriplesNormal, numTriplesTotal);
  writeConfiguration();
};

// _____________________________________________________________________________
template <typename... NextSorter>
requires(sizeof...(NextSorter) <= 1)
std::optional<PatternCreator::TripleSorter> IndexImpl::createSPOAndSOP(
    size_t numColumns, auto& isInternalId, BlocksOfTriples sortedTriples,
    NextSorter&&... nextSorter) {
  size_t numSubjectsNormal = 0;
  size_t numSubjectsTotal = 0;
  auto numSubjectCounter =
      makeNumDistinctIdsCounter<0>(numSubjectsNormal, isInternalId);
  std::optional<PatternCreator::TripleSorter> result;
  if (usePatterns_) {
    // We will return the next sorter.
    AD_CORRECTNESS_CHECK(sizeof...(nextSorter) == 0);
    // For now (especially for testing) We build the new pattern format as well
    // as the old one to see that they match.
    PatternCreator patternCreator{
        onDiskBase_ + ".index.patterns",
        memoryLimitIndexBuilding() / NUM_EXTERNAL_SORTERS_AT_SAME_TIME};
    auto pushTripleToPatterns = [&patternCreator,
                                 &isInternalId](const auto& triple) {
      bool ignoreForPatterns = std::ranges::any_of(triple, isInternalId);
      auto tripleArr = std::array{triple[0], triple[1], triple[2]};
      patternCreator.processTriple(tripleArr, ignoreForPatterns);
    };
    numSubjectsTotal = createPermutationPair(
        numColumns, AD_FWD(sortedTriples), spo_, sop_,
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
        numColumns, AD_FWD(sortedTriples), spo_, sop_,
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
template <typename... NextSorter>
requires(sizeof...(NextSorter) <= 1)
void IndexImpl::createOSPAndOPS(size_t numColumns, auto& isInternalId,
                                BlocksOfTriples sortedTriples,
                                NextSorter&&... nextSorter) {
  // For the last pair of permutations we don't need a next sorter, so we
  // have no fourth argument.
  size_t numObjectsNormal = 0;
  auto objectCounter =
      makeNumDistinctIdsCounter<2>(numObjectsNormal, isInternalId);
  size_t numObjectsTotal = createPermutationPair(
      numColumns, AD_FWD(sortedTriples), osp_, ops_,
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
