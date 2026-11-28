// Copyright 2014 - 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <buchhold@cs.uni-freiburg.de> [2014-2017]
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/IndexImpl.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>
#include <sys/stat.h>

#include <atomic>
#include <boost/sort/pdqsort/pdqsort.hpp>
#include <cstdio>
#include <functional>
#include <future>
#include <numeric>
#include <optional>
#include <type_traits>
#include <utility>

#include "CompilationInfo.h"
#include "backports/StartsWithAndEndsWith.h"
#include "backports/algorithm.h"
#include "backports/filesystem.h"
#include "engine/AddCombinedRowToTable.h"
#include "global/FileSuffixConstants.h"
#include "global/RuntimeParameters.h"
#include "index/Index.h"
#include "index/IndexFormatConverter.h"
#include "index/IndexFormatVersion.h"
#include "index/PartialVocabularyBuilder.h"
#include "index/TripleComponentConversions.h"
#include "index/VocabularyMerger.h"
#include "index/chunkwisePermutationWriter/ChunkwisePermutationWriter.h"
#include "parser/AsyncRdfParserBase.h"
#include "parser/AsyncSerialParserAdapter.h"
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include "parser/RdfAsyncMultifileParser.h"
#endif
#include "parser/WordsAndDocsFileParser.h"
#include "util/CancellationHandle.h"
#include "util/FilesystemHelpers.h"
#include "util/GlobalExecutor.h"
#include "util/HashMap.h"
#include "util/InputRangeUtils.h"
#include "util/Iterators.h"
#include "util/ParallelExecutor.h"
#include "util/ProgressBar.h"
#include "util/Synchronized.h"
#include "util/ThreadSafeQueue.h"
#include "util/Timer.h"
#include "util/TypeTraits.h"
#include "util/UnicodeSupport.h"
#include "util/Views.h"
#include "util/joinAlgorithms/JoinAlgorithms.h"
#include "util/json.h"
#include "util/views/UniqueBlockView.h"

using std::array;
using namespace ad_utility::memory_literals;

// During the index building we typically have two permutations present at the
// same time, as we directly push the triples from the first sorting to the
// second sorting. We therefore have to adjust the amount of memory per external
// sorter.
static constexpr size_t NUM_EXTERNAL_SORTERS_AT_SAME_TIME = 2u;

// The name of this JSON property no longer holds up as soon as blank nodes are
// added or removed via updates. For backwards compatibility we keep the name.
constexpr std::string_view BLANK_NODE_ALLOCATION_START =
    "num-blank-nodes-total";

// _____________________________________________________________________________
IndexImpl::IndexImpl(ad_utility::AllocatorWithLimit<Id> allocator)
    : allocator_{std::move(allocator)} {
  deltaTriples_.emplace(*this);
}

// _____________________________________________________________________________
IndexBuilderDataAsFirstPermutationSorter IndexImpl::createIdTriplesAndVocab(
    ad_utility::InputRangeTypeErased<qlever::InputFileSpecification> files,
    size_t numThreads) {
  auto indexBuilderData =
      passFileForVocabulary(std::move(files), numTriplesPerBatch_, numThreads);

  auto isQleverInternalTriple = [&indexBuilderData](const auto& triple) {
    auto internal = [&indexBuilderData](Id id) {
      return indexBuilderData.vocabularyMetaData_.isQleverInternalId(id);
    };
    return internal(triple[0]) || internal(triple[1]) || internal(triple[2]);
  };

  auto firstSorter = convertPartialToGlobalIds(
      indexBuilderData.parsedTriples_, isQleverInternalTriple, numThreads);

  return {std::move(indexBuilderData.vocabularyMetaData_),
          std::move(firstSorter)};
}

// _____________________________________________________________________________
std::unique_ptr<AsyncRdfParserBase> IndexImpl::makeRdfParser(
    const ql::any_io_executor& executor,
    ad_utility::InputRangeTypeErased<qlever::InputFileSpecification> files)
    const {
  AD_CONTRACT_CHECK(
      parserBufferSize().getBytes() > 0,
      "The buffer size of the RDF parser must be greater than zero");
  AD_CONTRACT_CHECK(
      memoryLimitIndexBuilding().getBytes() > 0,
      " memory limit for index building must be greater than zero");
  // NOTE: The settings have to be passed to the constructor, because the
  // parsers start parsing immediately when they are constructed.
  RdfParserSettings parserSettings{turtleParserIntegerOverflowBehavior_,
                                   turtleParserSkipIllegalLiterals_,
                                   onlyAsciiTurtlePrefixes_};
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  // The reduced feature set has no coroutines (and only Boost 1.71), so the
  // asynchronous multifile parser is not available. Fall back to the
  // synchronous `RdfMultifileParser` behind an `AsyncSerialParserAdapter`. That
  // parser has no parallel parser for a single file and therefore ignores
  // `parseInParallel_` (with a warning, see the comment on that class). The
  // files are still produced lazily.
  // NOTE: The adapter creates the parser lazily on the first `asyncGetBatch()`
  // call, see the constructor of `AsyncSerialParserAdapter`.
  return std::make_unique<AsyncSerialParserAdapter>(
      executor,
      [files = std::move(files), encodedIriManager = &encodedIriManager(),
       bufferSize = parserBufferSize(),
       parserSettings]() mutable -> std::unique_ptr<RdfParserBase> {
        return std::make_unique<RdfMultifileParser>(
            std::move(files), encodedIriManager, bufferSize, parserSettings);
      });
#else
  return std::make_unique<RdfAsyncMultifileParser>(
      executor, std::move(files), &encodedIriManager(), parserBufferSize(),
      parserSettings);
#endif
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
      sorterPtr->template getSortedBlocks<0>(), setSubset};
}

// Perform a lazy optional block join on the first column of `leftInput` and
// `rightInput`. The `resultCallback` will be called for each block of resulting
// rows. Assumes that `leftInput` and `rightInput` have 6 columns in total, so
// the result will have 5 columns.
template <typename T1, typename T2, typename F>
static auto lazyOptionalJoinOnFirstColumn(T1& leftInput, T2& rightInput,
                                          F resultCallback) {
  auto projection = [](const auto& row) -> Id { return row[0]; };
  auto projectionForComparator = [](const auto& rowOrId) -> ConstIdRef {
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
      [](IdRef id) {
        id = id.isUndefined() ? Id::makeFromInt(Pattern::NoPattern) : id;
      });
  return std::move(block.value()).template toStatic<0>();
}
}  // namespace

namespace {
// An adapter for an external `Sorter` that pushes only the given `columns` of
// each block (in the given order) to the `Sorter`. It has the same interface
// for the creation of the push callbacks as the sorter (see
// `toSequentialBlockCallback` and `toConcurrentBlockCallback` below), so it can
// be used as the next sorter of a pair of permutations. It is used to push the
// triples in the column order of a permutation that doesn't have the graph as
// its last column, see `IndexImpl::createPermutationsWithoutGraphLast`.
template <typename Sorter>
class PushInColumnOrder {
 private:
  Sorter& sorter_;
  std::vector<ColumnIndex> columns_;

 public:
  PushInColumnOrder(Sorter& sorter, std::vector<ColumnIndex> columns)
      : sorter_{sorter}, columns_{std::move(columns)} {}

  // Return a callback that synchronously pushes a block.
  auto makePushBlockCallback() {
    return [this](const IdTableStatic<0>& block) {
      sorter_.pushBlock(block.asColumnSubsetView(columns_));
    };
  }

  // Return a callback that asynchronously pushes a block (see
  // `CompressedExternalIdTableSorter::makeAsyncPushBlockCallback`).
  auto makeAsyncPushBlockCallback() {
    return [this](auto block, auto done) {
      // The view (and the rows that it refers to) has to stay alive until the
      // push has completed, which the aliasing `shared_ptr` guarantees.
      using Holder = std::pair<decltype(block), IdTableView<0>>;
      auto holder =
          std::make_shared<Holder>(block, block->asColumnSubsetView(columns_));
      auto* viewPtr = &holder->second;
      std::shared_ptr<const IdTableView<0>> view{std::move(holder), viewPtr};
      sorter_.asyncPushBlock(
          std::move(view),
          [done = std::move(done)](std::exception_ptr e) mutable {
            std::move(done)(std::move(e));
          });
    };
  }
};

// Return the indices of the columns that reorder a block, the columns of which
// are in the order of the key order `from`, into the order of the key order
// `to`.
std::vector<ColumnIndex> columnsToReorder(const qlever::KeyOrder& from,
                                          const qlever::KeyOrder& to) {
  std::vector<ColumnIndex> result;
  for (auto key : to.keys()) {
    auto it = ql::ranges::find(from.keys(), key);
    AD_CORRECTNESS_CHECK(it != from.keys().end());
    result.push_back(static_cast<ColumnIndex>(it - from.keys().begin()));
  }
  return result;
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
                        [&queue]() noexcept { queue.finish(); }}};
  // Actually create the permutations.
  auto thirdSorter =
      makeSorterPtr<SortByPSO, NumColumnsIndexBuilding + 2>("third");
  createOSPAndOPS(NumColumnsIndexBuilding + 2, std::move(blockGenerator),
                  *thirdSorter);
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
        std::array<Id, 4>{row[0], row[1], row[2], internalGraph});
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

  BlocksOfTriples internalTriplesUnique = ad_utility::uniqueBlockView(
      internalTriplesPsoSorter.template getSortedBlocks<0>());
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
      "please use the command-line option --parallel-parsing or -p";
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
                << spec.at(0).filename() << " (parallel = "
                << (spec.at(0).parseInParallel_ ? "true" : "false") << ") ..."
                << std::endl;
  } else {
    AD_LOG_INFO << "Processing triples from " << spec.size()
                << " input streams ..." << std::endl;
  }
}

// _____________________________________________________________________________
void IndexImpl::createFromFiles(
    std::vector<Index::InputFileSpecification> files, size_t numThreads) {
  // The settings file (read by `prepareIndexBuild`) may set
  // `useParallelParser_`, so it must be read before the file specifications
  // are updated.
  prepareIndexBuild();
  updateInputFileSpecificationsAndLog(files, useParallelParser_);
  createFromFilesAfterPreparation(
      ad_utility::InputRangeTypeErased{std::move(files)}, numThreads);
}

// _____________________________________________________________________________
void IndexImpl::createFromFiles(
    ad_utility::InputRangeTypeErased<qlever::InputFileSpecification> files,
    size_t numThreads) {
  prepareIndexBuild();
  createFromFilesAfterPreparation(std::move(files), numThreads);
}

// _____________________________________________________________________________
void IndexImpl::prepareIndexBuild() {
  // This also checks that the requested permutations are supported.
  auto permutations = permutationsToBuild();
  if (usePatterns_ &&
      !ql::ranges::all_of(Permutation::GRAPH_LAST, [&permutations](auto p) {
        return ad_utility::contains(permutations, p);
      })) {
    throw std::runtime_error{
        "The patterns can only be built when all 6 permutations are created"};
  }

  configurationJson_["encoded-iri-prefixes"] = encodedIriManager();
  configurationJson_[DATE_OF_INDEX_BUILD_KEY] =
      formatIndexBuildTime(absl::Now());
  // The block size is stored so that everything that writes sorted lists of
  // this index later on (the server for a materialized view, the index format
  // converter) uses the same block size as this build.
  configurationJson_[INDEX_ROWS_PER_BLOCK_KEY] = rowsPerBlock_;
  // The normalization of the words of the `ql:has-word` triples is stored so
  // that the server can later normalize the words of queries and updates in
  // the same way.
  configurationJson_[std::string{ad_utility::HAS_WORD_NORMALIZATION_KEY}] =
      hasWordNormalization_;
  // Whether the index has `ql:has-word` triples is stored so that the server
  // can later add them for the literals that are inserted by an update.
  configurationJson_[std::string{ad_utility::ADD_HAS_WORD_TRIPLES_KEY}] =
      addHasWordTriples_;
  // The predicates to which the `ql:has-word` triples are restricted are
  // stored, so that the configuration of the index records for which literals
  // it has `ql:has-word` triples.
  configurationJson_[std::string{
      ad_utility::HAS_WORD_RESTRICT_PREDICATES_KEY}] =
      hasWordRestrictPredicates_;

  // Use the encoding of the index for all points that the index build creates
  // (`writeConfiguration` records it), and warn if it is the deprecated
  // `LatMajor`.
  GeoPoint::setEncoding(geoPointEncodingForIndexBuilding_);
  if (geoPointEncodingForIndexBuilding_ ==
      ad_utility::GeoPointEncoding::LatMajor) {
    AD_LOG_WARN << ad_utility::LAT_MAJOR_GEO_POINT_ENCODING_WARNING
                << std::endl;
  }

  vocab_.resetToType(vocabularyTypeForIndexBuilding_);

  readIndexBuilderSettingsFromFile();
}

// _____________________________________________________________________________
void IndexImpl::createFromFilesAfterPreparation(
    ad_utility::InputRangeTypeErased<qlever::InputFileSpecification> files,
    size_t numThreads) {
  IndexBuilderDataAsFirstPermutationSorter indexBuilderData =
      createIdTriplesAndVocab(std::move(files), numThreads);

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

  // The permutations are built in pairs that share their first column (SPO and
  // SOP, OSP and OPS, PSO and POS), in this order. The PSO and POS
  // permutations are always built (see `permutationsToBuild`), the other pairs
  // only if at least one of their permutations was requested. Each pair pushes
  // its triples to the sorter of the next pair.
  //
  // The `firstSorter` is type-erased, because its sort order depends on the
  // first pair that is built (see `makeFirstPermutationSorter`). The chunkwise
  // writer needs the concrete sorter, so it is cast back here.
  const auto permutations = permutationsToBuild();
  const bool buildSubjectPair = buildsSubjectPair(permutations);
  const bool buildObjectPair = buildsObjectPair(permutations);
  // The permutations that don't have the graph as their last column are built
  // last, from the triples that the PSO and POS permutations push to them (see
  // `createPermutationsWithoutGraphLast`).
  const auto pairsWithoutGraphLast = getPairsWithoutGraphLast(permutations);
  std::unique_ptr<ExternalSorter<SortBySPO>> sorterWithoutGraphLast;
  std::optional<PushInColumnOrder<ExternalSorter<SortBySPO>>>
      pushToSorterWithoutGraphLast;
  if (!pairsWithoutGraphLast.empty()) {
    sorterWithoutGraphLast =
        makeSorterPtr<SortBySPO>("permutations-without-graph-last-0");
    pushToSorterWithoutGraphLast.emplace(
        *sorterWithoutGraphLast,
        columnsToReorder(
            qlever::KeyOrder{0, 1, 2, 3},
            Permutation::toKeyOrder(pairsWithoutGraphLast.front().first)));
  }
  auto createPsoAndPos = [this, &pushToSorterWithoutGraphLast](
                             size_t numColumns, auto sortedTriples) {
    if (pushToSorterWithoutGraphLast.has_value()) {
      createPSOAndPOS(numColumns, std::move(sortedTriples),
                      pushToSorterWithoutGraphLast.value());
    } else {
      createPSOAndPOS(numColumns, std::move(sortedTriples));
    }
  };
  if (usePatterns_) {
    // Load all permutations and also load the patterns. In this case the
    // `createSPOAndSOP` function returns the next sorter, already enriched
    // with the patterns of the subjects in the triple.
    //
    // NOTE: The pattern creation needs the triples in order, so the first
    // pair of permutations is written from the sequential (deduplicated)
    // output of the sorter, and the second pair from the output of the join
    // with the patterns (see `buildOspWithPatterns`). Only the last pair is
    // written directly from the chunks of the merge of its sorter.
    AD_CORRECTNESS_CHECK(buildSubjectPair && buildObjectPair);
    BlocksOfTriples firstSorterWithUnique =
        ad_utility::uniqueBlockView(firstSorter.getSortedOutput());
    auto patternOutput = createSPOAndSOP(NumColumnsIndexBuilding,
                                         std::move(firstSorterWithUnique));
    firstSorter.clearUnderlying();
    auto thirdSorterPtr =
        buildOspWithPatterns(std::move(patternOutput.value()),
                             *indexBuilderData.sorter_.internalTriplesPso_);
    createInternalPsoAndPosAndSetMetadata();
    createPsoAndPos(NumColumnsIndexBuilding + 2,
                    ChunkwiseInput{*thirdSorterPtr, RemoveDuplicates::False});
  } else {
    createInternalPsoAndPosAndSetMetadata();
    // Without patterns, all the pairs are written directly from the chunks of
    // the merge of the respective sorter. The first sorter may yield duplicate
    // triples, which are removed by the writer of the first pair.
    auto psoSorter = makeSorter<SortByPSO>("third");
    auto buildObjectPairFrom = [this, &psoSorter](auto& ospSorter,
                                                  RemoveDuplicates unique) {
      createOSPAndOPS(NumColumnsIndexBuilding,
                      ChunkwiseInput{ospSorter, unique}, psoSorter);
    };
    if (buildSubjectPair) {
      auto& spoSorter = dynamic_cast<ExternalSorter<SortBySPO>&>(firstSorter);
      if (buildObjectPair) {
        auto ospSorter = makeSorter<SortByOSP>("second");
        createSPOAndSOP(NumColumnsIndexBuilding,
                        ChunkwiseInput{spoSorter, RemoveDuplicates::True},
                        ospSorter);
        firstSorter.clearUnderlying();
        buildObjectPairFrom(ospSorter, RemoveDuplicates::False);
        ospSorter.clear();
      } else {
        createSPOAndSOP(NumColumnsIndexBuilding,
                        ChunkwiseInput{spoSorter, RemoveDuplicates::True},
                        psoSorter);
        firstSorter.clearUnderlying();
      }
    } else if (buildObjectPair) {
      buildObjectPairFrom(dynamic_cast<ExternalSorter<SortByOSP>&>(firstSorter),
                          RemoveDuplicates::True);
      firstSorter.clearUnderlying();
    }
    if (buildSubjectPair || buildObjectPair) {
      createPsoAndPos(NumColumnsIndexBuilding,
                      ChunkwiseInput{psoSorter, RemoveDuplicates::False});
    } else {
      // Only the PSO and POS permutations are built (with the graph last), so
      // the `firstSorter` is a PSO sorter.
      createPsoAndPos(
          NumColumnsIndexBuilding,
          ChunkwiseInput{dynamic_cast<ExternalSorter<SortByPSO>&>(firstSorter),
                         RemoveDuplicates::True});
    }
  }
  if (sorterWithoutGraphLast) {
    createPermutationsWithoutGraphLast(std::move(sorterWithoutGraphLast),
                                       pairsWithoutGraphLast);
  }
  // A pair of permutations is always built completely, so delete those
  // permutations that were not requested.
  //
  // TODO<joka921> Don't write them in the first place.
  for (auto permutation : Permutation::ALL) {
    if (!ad_utility::contains(permutations, permutation)) {
      for (const auto& file :
           Permutation::fileNames(permutation, onDiskBase_)) {
        if (ql::filesystem::exists(file)) {
          ad_utility::deleteFile(file);
        }
      }
    }
  }
  setPermutationsInConfiguration(permutations);

  configurationJson_[BLANK_NODE_ALLOCATION_START] =
      indexBuilderData.vocabularyMetaData_.getNextBlankNodeIndex();

  addInternalStatisticsToConfiguration(numTriplesInternal,
                                       numPredicatesInternal);
  AD_LOG_INFO << "Index build completed" << std::endl;
}

// _____________________________________________________________________________
bool IndexImpl::buildsSubjectPair(
    const std::vector<Permutation::Enum>& permutations) {
  return ad_utility::contains(permutations, Permutation::SPO) ||
         ad_utility::contains(permutations, Permutation::SOP);
}

// _____________________________________________________________________________
bool IndexImpl::buildsObjectPair(
    const std::vector<Permutation::Enum>& permutations) {
  return ad_utility::contains(permutations, Permutation::OSP) ||
         ad_utility::contains(permutations, Permutation::OPS);
}

// _____________________________________________________________________________
void IndexImpl::setPermutationsInConfiguration(
    const std::vector<Permutation::Enum>& permutations) {
  std::vector<std::string> names;
  for (auto permutation : permutations) {
    names.emplace_back(Permutation::toString(permutation));
  }
  configurationJson_["permutations"] = names;
  // Also write the key that older versions of QLever use.
  configurationJson_["has-all-permutations"] =
      ql::ranges::all_of(Permutation::GRAPH_LAST, [&permutations](auto p) {
        return ad_utility::contains(permutations, p);
      });
  writeConfiguration();
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
BuildPartialVocabulariesResult IndexImpl::buildPartialVocabularies(
    ad_utility::InputRangeTypeErased<qlever::InputFileSpecification> files,
    size_t linesPerPartial, size_t numThreads) {
  AD_CONTRACT_CHECK(numThreads > 0,
                    "The number of threads for the index build must be at "
                    "least 1");

  AD_LOG_INFO << "Parsing input triples and creating partial vocabularies, one "
                 "per batch, using "
              << numThreads << " threads ..." << std::endl;

  using namespace qlever::partialVocabularyBuilder;
  FirstPassSharedState<IndexImpl> shared{this, &vocab_.getCaseComparator(),
                                         linesPerPartial};
  // The thread pool and the parser are owned by `runTaskChains`, which only
  // returns once no asynchronous operation is left.
  runTaskChains(shared, numThreads,
                [this, &files](const ql::any_io_executor& executor) {
                  return makeRdfParser(executor, std::move(files));
                });

  // If the input didn't contain a single triple, we still have to write one
  // partial vocabulary, because the vocabulary has to contain the special IDs
  // (which every `ItemMapManager` adds to its map).
  if (shared.nextPartialVocabIdx_ == 0) {
    ItemMapManager itemMap{0, &vocab_.getCaseComparator()};
    std::vector<IdRow> noTriples;
    writePartialVocabulary(shared.nextPartialVocabIdx_++, itemMap.map_,
                           noTriples);
  }

  // The task chains have claimed all the indices below the counter, and each
  // of them exactly once (see `BuildPartialVocabulariesResult`).
  BuildPartialVocabulariesResult result{shared.nextPartialVocabIdx_.load(),
                                        shared.numTriples_.load()};

  shared.progressBar_.logFinalProgressString();
  AD_LOG_INFO << "Number of triples created (including QLever-internal ones): "
              << result.numTriples_ << " [may contain duplicates]" << std::endl;
  if (addHasWordTriples_) {
    AD_LOG_INFO << "Number of `ql:has-word` triples created: "
                << shared.numHasWordTriples_.load() << std::endl;
  }
  AD_LOG_INFO << "Number of partial vocabularies created: "
              << result.numPartialVocabularies_ << std::endl;
  return result;
}

// _____________________________________________________________________________
IndexBuilderDataAsExternalVector IndexImpl::passFileForVocabulary(
    ad_utility::InputRangeTypeErased<qlever::InputFileSpecification> files,
    size_t linesPerPartial, size_t numThreads) {
  auto parsedTriples =
      buildPartialVocabularies(std::move(files), linesPerPartial, numThreads);
  size_t numPartialVocabularies = parsedTriples.numPartialVocabularies_;

  size_t sizeInternalVocabulary = 0;
  std::vector<std::string> prefixes;

  AD_LOG_INFO << "Merging partial vocabularies ..." << std::endl;
  ad_utility::vocabulary_merger::VocabularyMetaData mergeRes = [&]() {
    auto sortPred = [&cmp = vocab_.getCaseComparator()](std::string_view a,
                                                        std::string_view b) {
      return cmp(a, b, TripleComponentComparator::Level::TOTAL);
    };
    auto writer = vocab_.makeParallelWriterPtr(onDiskBase_ + VOCAB_SUFFIX);
    writer->readableName() = "internal vocabulary";
    return ad_utility::vocabulary_merger::mergeVocabulary(
        onDiskBase_, numPartialVocabularies, sortPred, *writer,
        memoryLimitIndexBuilding(), blankNodeIriRegexes_);
  }();
  AD_LOG_DEBUG << "Finished merging partial vocabularies" << std::endl;
  idOfHasPatternDuringIndexBuilding_ =
      mergeRes.specialIdMapping().at(HAS_PATTERN_PREDICATE);
  idOfInternalGraphDuringIndexBuilding_ =
      mergeRes.specialIdMapping().at(QLEVER_INTERNAL_GRAPH_IRI);
  AD_LOG_INFO << "Number of words in external vocabulary: "
              << mergeRes.numWordsTotal() - sizeInternalVocabulary << std::endl;

  deletePartialVocabularyWordsFiles(
      onDiskBase_, numPartialVocabularies,
      [this](const std::string& filename) { deleteTemporaryFile(filename); });

  AD_LOG_DEBUG << "Triples per partial vocabulary: " << linesPerPartial
               << std::endl;

  return {std::move(mergeRes), std::move(parsedTriples)};
}

namespace {
// A block of triples as `IndexImpl::convertPartialToGlobalIds` handles it, and
// a non-owning view of a part of such a block.
using Buffer = IdTableStatic<NumColumnsIndexBuilding>;
using BufferView = IdTableView<NumColumnsIndexBuilding>;

// Replace the local IDs in all the columns of the `triples` by the global IDs
// from the `idMap` (see `IdMapFromPartialIdMapFile`).
void transformTriples(Buffer& triples,
                      const ad_utility::HashMap<VocabIndex, Id>& idMap) {
  for (IdColumnRef column : triples.getColumns()) {
    for (IdRef id : column) {
      if (id.getDatatype() != Datatype::VocabIndex) {
        // Check that all the internal, special IDs which we have introduced
        // for performance reasons are eliminated.
        AD_CORRECTNESS_CHECK(id.getDatatype() != Datatype::Undefined);
        continue;
      }
      auto iterator = idMap.find(id.getVocabIndex());
      AD_CORRECTNESS_CHECK(iterator != idMap.end());
      id = iterator->second;
    }
  }
}

// The output of `IndexImpl::convertPartialToGlobalIds`: the sorter for the
// normal triples, the sorter for the QLever-internal triples, and the progress
// bar. None of them is thread-safe, and they always have to be updated
// together, so the workers access them exclusively via a single
// `ad_utility::Synchronized` object.
class ConversionOutput {
 private:
  using Sorters = FirstPermutationSorterAndInternalTriplesAsPso;
  Sorters sorters_;
  size_t numTriplesConverted_ = 0;
  ad_utility::ProgressBar progressBar_{numTriplesConverted_,
                                       "Triples converted: "};

  // The sorter for the first permutation. Its type is erased, because its
  // comparator depends on the permutations that are built (see
  // `IndexImpl::createFromFilesAfterPreparation`) and hence is only known at
  // runtime.
  static Sorters::SorterPtr makeFirstPermutationSorter(
      IndexImpl& index, std::string_view permutationName) {
    auto permutations = index.permutationsToBuild();
    if (IndexImpl::buildsSubjectPair(permutations)) {
      return index.makeSorterPtr<SortBySPO>(permutationName);
    }
    if (IndexImpl::buildsObjectPair(permutations)) {
      return index.makeSorterPtr<SortByOSP>(permutationName);
    }
    return index.makeSorterPtr<SortByPSO>(permutationName);
  }

 public:
  // Create the two sorters. The names have to be unique for one index build
  // (see `IndexImpl::makeSorter`).
  ConversionOutput(IndexImpl& index, std::string_view firstPermutationName,
                   std::string_view internalTriplesName)
      : sorters_{makeFirstPermutationSorter(index, firstPermutationName),
                 index.makeSorterPtr<SortByPSO, NumColumnsIndexBuilding>(
                     internalTriplesName)} {}

  // Push a block of normal `triples` and a block of `internalTriples` to their
  // respective sorters and report the progress. Both blocks are views, because
  // the caller obtains them by partitioning a single block (see
  // `convertPartialToGlobalIds`); the sorters copy the rows they are given.
  void push(const BufferView& triples, const BufferView& internalTriples) {
    sorters_.firstPermutationSorter_->pushBlock(triples.asStaticView<0>());
    sorters_.internalTriplesPso_->pushBlock(internalTriples.asStaticView<0>());
    numTriplesConverted_ += triples.numRows() + internalTriples.numRows();
    if (progressBar_.update()) {
      AD_LOG_INFO << progressBar_.getProgressString() << std::flush;
    }
  }

  // Log the final progress and hand out the sorters with all the blocks that
  // were pushed. After this, no more blocks may be pushed.
  Sorters finish() {
    AD_LOG_INFO << progressBar_.getFinalProgressString() << std::flush;
    return std::move(sorters_);
  }
};
}  // namespace

// _____________________________________________________________________________
template <typename Func>
auto IndexImpl::convertPartialToGlobalIds(
    const BuildPartialVocabulariesResult& data, Func isQLeverInternalTriple,
    size_t numThreads) -> FirstPermutationSorterAndInternalTriplesAsPso {
  AD_LOG_INFO << "Converting triples from local IDs to global IDs ..."
              << std::endl;

  ad_utility::Synchronized<ConversionOutput> output{*this, "first",
                                                    "internalTriples"};

  // Convert the triples that were mapped using the partial vocabulary with
  // index `partialVocabIdx` and push them to the sorters. The partial
  // vocabulary and its triples were written as a pair by a single worker in
  // `buildPartialVocabularies`, so this function needs nothing but the
  // `partialVocabIdx`.
  auto convertTriplesOfPartialVocabulary = [this, &output,
                                            &isQLeverInternalTriple](
                                               size_t partialVocabIdx) {
    // The mapping from the partial IDs of this partial vocabulary to the global
    // IDs. Its file is not needed anymore once the mapping has been read.
    std::string idMapFilename =
        partialVocabularyIdMapFilename(onDiskBase_, partialVocabIdx);
    auto idMap =
        ad_utility::vocabulary_merger::IdMapFromPartialIdMapFile(idMapFilename);
    deleteTemporaryFile(idMapFilename);

    // The triples are only needed for this conversion step.
    std::string triplesFilename =
        unsortedTriplesFilename(onDiskBase_, partialVocabIdx);
    Buffer triples =
        ad_utility::vocabulary_merger::readMappedIdsFromFile(triplesFilename);
    deleteTemporaryFile(triplesFilename);

    transformTriples(triples, idMap);

    // Partitioning makes each of the two kinds of triples contiguous, so that
    // each of them can be copied to its sorter in one go. The QLever-internal
    // triples come first.
    auto normalTriples =
        std::partition(triples.begin(), triples.end(), isQLeverInternalTriple);
    size_t numInternalTriples = normalTriples - triples.begin();
    size_t numNormalTriples = triples.size() - numInternalTriples;
    output.wlock()->push(triples.subView(numInternalTriples, numNormalTriples),
                         triples.subView(0, numInternalTriples));
  };

  // Each worker repeatedly claims the next partial vocabulary from a shared
  // counter and holds the ID map and all the triples of that partial
  // vocabulary in RAM. Their number is therefore bounded by `numThreads`, and
  // additionally such that the triples of all the workers together fit into
  // the memory limit of the index build (the ID maps are much smaller than the
  // triples and are not accounted for). There is always at least one worker.
  // NOTE: For the usual batch sizes (5 to 10 million triples) and the default
  // memory limit, the memory bound is far above the usual number of threads
  // and hence irrelevant.
  std::atomic<size_t> nextPartialVocabIdx = 0;
  size_t triplesBytesPerWorker =
      numTriplesPerBatch_ * NumColumnsIndexBuilding * BYTES_PER_ID_COLUMN_ENTRY;
  size_t numWorkersThatFitInMemory =
      std::max<size_t>(1, memoryLimitIndexBuilding().getBytes() /
                              std::max<size_t>(1, triplesBytesPerWorker));
  size_t numWorkers = std::min(
      {data.numPartialVocabularies_, numThreads, numWorkersThatFitInMemory});
  auto tasks =
      ad_utility::integerRange(numWorkers) |
      ql::views::transform([&convertTriplesOfPartialVocabulary,
                            &nextPartialVocabIdx, &data](size_t) {
        return std::packaged_task<void()>([&convertTriplesOfPartialVocabulary,
                                           &nextPartialVocabIdx, &data]() {
          for (;;) {
            size_t partialVocabIdx = nextPartialVocabIdx.fetch_add(1);
            if (partialVocabIdx >= data.numPartialVocabularies_) {
              return;
            }
            convertTriplesOfPartialVocabulary(partialVocabIdx);
          }
        });
      }) |
      ::ranges::to<std::vector>();
  // Waits for all the workers to finish, and rethrows an exception if one of
  // them has thrown.
  ad_utility::runTasksInParallel(std::move(tasks));

  return output.wlock()->finish();
}

// _____________________________________________________________________________

namespace {
// Lift a callback that works on single elements to a callback that works on
// blocks.
template <typename Callback>
auto liftCallback(Callback callback) {
  return [callback = std::move(callback)](const auto& block) mutable {
    ql::ranges::for_each(block, callback);
  };
}

// A callback for `IndexImpl::createPermutationPairImpl` that pushes each block
// of the input to the external sorter for the next pair of permutations, see
// `pushToNextSorter` below.
//
// NOTE: This struct only holds a reference, so `std::reference_wrapper` could
// be used instead. It is used as a tag for the overload resolution of the
// callbacks (see `toSequentialBlockCallback` and `toConcurrentBlockCallback`),
// which we want to be explicitly visible.
template <typename Sorter>
struct PushToNextSorter {
  Sorter& sorter_;
};

// Return a `PushToNextSorter` for the `sorter`.
template <typename Sorter>
PushToNextSorter<Sorter> pushToNextSorter(Sorter& sorter) {
  return {sorter};
}

// Turn one of the callbacks of `IndexImpl::createPermutationPairImpl` into a
// callback for the classical sequential writer, see
// `CompressedRelationWriter::createPermutationPair`.
template <typename Callback>
std::function<void(const IdTableStatic<0>&)> toSequentialBlockCallback(
    Callback callback) {
  if constexpr (ad_utility::isInstantiation<Callback, PushToNextSorter>) {
    // Pushing a complete block into an external sorter is much more efficient
    // than pushing its rows one by one, because the `IdTable`s are stored
    // column-based.
    return callback.sorter_.makePushBlockCallback();
  } else {
    return callback;
  }
}

// Turn one of the callbacks of `IndexImpl::createPermutationPairImpl` into a
// callback for the chunkwise writer, see
// `chunkwisePermutationWriter::ConcurrentBlockCallback`.
template <typename Callback>
chunkwisePermutationWriter::ConcurrentBlockCallback toConcurrentBlockCallback(
    Callback callback) {
  if constexpr (ad_utility::isInstantiation<Callback, PushToNextSorter>) {
    return callback.sorter_.makeAsyncPushBlockCallback();
  } else {
    return chunkwisePermutationWriter::toConcurrentBlockCallback(
        std::move(callback));
  }
}

// Return the blocks of the `input` as a sequential range, with the duplicates
// removed if `removeDuplicates_` is `RemoveDuplicates::True`.
template <typename Sorter>
IndexImpl::BlocksOfTriples toSequentialInput(
    const IndexImpl::ChunkwiseInput<Sorter>& input) {
  IndexImpl::BlocksOfTriples blocks =
      input.sorter_.template getSortedBlocks<0>();
  if (input.removeDuplicates_ == IndexImpl::RemoveDuplicates::True) {
    blocks = ad_utility::uniqueBlockView(std::move(blocks));
  }
  return blocks;
}

// Raise the `maximum` atomically to the `value` if it is smaller.
inline void atomicMax(std::atomic<uint64_t>& maximum, uint64_t value) {
  uint64_t current = maximum.load();
  while (current < value && !maximum.compare_exchange_weak(current, value)) {
  }
}
}  // namespace

// _____________________________________________________________________________
auto IndexImpl::getPairsWithoutGraphLast(
    const std::vector<Permutation::Enum>& permutations)
    -> std::vector<PermutationPair> {
  std::vector<PermutationPair> result;
  std::vector<Permutation::Enum> handled;
  for (auto permutation : permutations) {
    if (Permutation::hasGraphLast(permutation) ||
        ad_utility::contains(handled, permutation)) {
      continue;
    }
    auto twin = Permutation::twin(permutation);
    result.emplace_back(permutation, twin);
    handled.push_back(permutation);
    handled.push_back(twin);
  }
  return result;
}

// _____________________________________________________________________________
void IndexImpl::createPermutationsWithoutGraphLast(
    std::unique_ptr<ExternalSorter<SortBySPO>> sorter,
    const std::vector<PermutationPair>& pairs) {
  for (size_t i = 0; i < pairs.size(); ++i) {
    const auto& p1 = getPermutation(pairs.at(i).first);
    const auto& p2 = getPermutation(pairs.at(i).second);
    // The triples are pushed to the sorter of the next pair while this pair
    // is written.
    std::unique_ptr<ExternalSorter<SortBySPO>> nextSorter;
    std::optional<PushInColumnOrder<ExternalSorter<SortBySPO>>> pushToNext;
    if (i + 1 < pairs.size()) {
      nextSorter = makeSorterPtr<SortBySPO>(
          absl::StrCat("permutations-without-graph-last-", i + 1));
      pushToNext.emplace(
          *nextSorter,
          columnsToReorder(p1.keyOrder(),
                           Permutation::toKeyOrder(pairs.at(i + 1).first)));
    }
    AD_LOG_INFO << "Creating permutations " << p1.readableName() << " and "
                << p2.readableName() << " ..." << std::endl;
    // The triples of the `sorter` are already in the column order of `p1`
    // (and sorted lexicographically), so they are written with the identity
    // as the key order. Its twin `p2` is then derived by the writer as usual.
    ChunkwiseInput<ExternalSorter<SortBySPO>> input{*sorter,
                                                    RemoveDuplicates::False};
    qlever::KeyOrder identity{0, 1, 2, 3};
    auto fileName1 = getFilenameForPermutation(p1, false);
    auto fileName2 = getFilenameForPermutation(p2, false);
    auto [numDistinctCol0, meta1, meta2] =
        pushToNext.has_value()
            ? createPermutationPairImpl(NumColumnsIndexBuilding, fileName1,
                                        fileName2, input, identity,
                                        pushToNextSorter(pushToNext.value()))
            : createPermutationPairImpl(NumColumnsIndexBuilding, fileName1,
                                        fileName2, input, identity);
    meta1.calculateStatistics(numDistinctCol0);
    meta2.calculateStatistics(numDistinctCol0);
    AD_LOG_INFO << "Statistics for " << p1.readableName() << ": "
                << meta1.statistics() << std::endl;
    AD_LOG_INFO << "Statistics for " << p2.readableName() << ": "
                << meta2.statistics() << std::endl;
    writeMetaData(meta1, fileName1);
    writeMetaData(meta2, fileName2);
    sorter->clear();
    sorter = std::move(nextSorter);
  }
}

// _____________________________________________________________________________
std::string IndexImpl::getFilenameForPermutation(const Permutation& permutation,
                                                 bool internal) const {
  return absl::StrCat(onDiskBase_, internal ? QLEVER_INTERNAL_INDEX_INFIX : "",
                      ".index", permutation.fileSuffix());
}

// _____________________________________________________________________________
CompressedRelationWriter::WriterAndCallback IndexImpl::getWriterAndCallback(
    IndexMetaData& metaData, size_t numColumns, const std::string& fileName,
    qlever::Executor executor) const {
  auto writer = std::make_unique<CompressedRelationWriter>(
      numColumns, ad_utility::File(fileName, "w"), rowsPerBlock_,
      std::move(executor));

  auto callback =
      liftCallback([&metaData](const auto& md) { metaData.add(md); });
  return {std::move(writer), std::move(callback)};
}

// _____________________________________________________________________________
template <typename T, typename... Callbacks>
std::tuple<size_t, IndexMetaData, IndexMetaData>
IndexImpl::createPermutationPairImpl(size_t numColumns,
                                     const std::string& fileName1,
                                     const std::string& fileName2,
                                     T&& sortedTriples,
                                     Permutation::KeyOrder permutation,
                                     Callbacks&&... callbacks) {
  constexpr bool isChunkwise =
      ad_utility::isInstantiation<std::decay_t<T>, ChunkwiseInput>;
  static_assert(isChunkwise ||
                std::is_same_v<std::decay_t<T>, BlocksOfTriples>);
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  // There is no chunkwise writer in this mode (see
  // `chunkwisePermutationWriter::createPermutationPair`), so the sorted blocks
  // are consumed sequentially by the classical writer.
  if constexpr (isChunkwise) {
    return createPermutationPairImpl(numColumns, fileName1, fileName2,
                                     toSequentialInput(sortedTriples),
                                     permutation, AD_FWD(callbacks)...);
  }
#endif
  IndexMetaData metaData1;
  auto writerAndCallback1 = getWriterAndCallback(
      metaData1, numColumns, fileName1, ad_utility::globalExecutor());
  IndexMetaData metaData2;
  auto writerAndCallback2 = getWriterAndCallback(
      metaData2, numColumns, fileName2, ad_utility::globalExecutor());

  auto writePermutations = [&]() {
    if constexpr (isChunkwise) {
      // The input is a sorter whose chunks are written directly, see
      // `ChunkwiseInput`. The callbacks are invoked concurrently.
      //
      // NOTE: The callbacks are move-only, so they cannot be passed via an
      // `std::initializer_list`.
      std::vector<chunkwisePermutationWriter::ConcurrentBlockCallback>
          concurrentCallbacks;
      (concurrentCallbacks.push_back(
           toConcurrentBlockCallback(AD_FWD(callbacks))),
       ...);
      chunkwisePermutationWriter::Options options;
      options.removeDuplicates_ =
          sortedTriples.removeDuplicates_ == RemoveDuplicates::True;
      // The twin sorters of the chunks share the memory of the sorter that is
      // being consumed, and the consolidation of the boundary relations (which
      // runs while the chunks are suspended) may use all of it, see
      // `CHUNKWISE_WRITER_MIN_TWIN_SORTER_MEMORY_PER_CHUNK`.
      const auto sorterMemory =
          memoryLimitIndexBuilding() / NUM_EXTERNAL_SORTERS_AT_SAME_TIME;
      options.twinSorterMemoryPerChunk_ =
          std::clamp(sorterMemory / ad_utility::globalExecutorNumThreads(),
                     CHUNKWISE_WRITER_MIN_TWIN_SORTER_MEMORY_PER_CHUNK,
                     CHUNKWISE_WRITER_MAX_TWIN_SORTER_MEMORY_PER_CHUNK);
      options.boundaryTwinMergeMemory_ = sorterMemory;
      return chunkwisePermutationWriter::createPermutationPair(
          fileName1, std::move(writerAndCallback1),
          std::move(writerAndCallback2), sortedTriples.sorter_, permutation,
          std::move(concurrentCallbacks), options);
    } else {
      std::vector<std::function<void(const IdTableStatic<0>&)>>
          sequentialCallbacks{toSequentialBlockCallback(AD_FWD(callbacks))...};
      return CompressedRelationWriter::createPermutationPair(
          fileName1, std::move(writerAndCallback1),
          std::move(writerAndCallback2), AD_FWD(sortedTriples), permutation,
          sequentialCallbacks);
    }
  };
  auto [numDistinctCol0, blockData1, blockData2] = writePermutations();
  metaData1.blockData() = std::move(blockData1);
  metaData2.blockData() = std::move(blockData2);

  return {numDistinctCol0, std::move(metaData1), std::move(metaData2)};
}

// _____________________________________________________________________________
std::tuple<size_t, IndexMetaData> IndexImpl::createPermutationImpl(
    size_t numColumns, const std::string& fileName,
    ad_utility::InputRangeTypeErased<IdTableStatic<0>> sortedTriples,
    qlever::Executor executor) {
  IndexMetaData metaData;
  auto writerAndCallback =
      getWriterAndCallback(metaData, numColumns, fileName, std::move(executor));

  // We can always supply the tables with the correct permutation. No need to
  // re-order everything.
  auto [numDistinctCol0, blockData] =
      CompressedRelationWriter::createPermutation(std::move(writerAndCallback),
                                                  std::move(sortedTriples),
                                                  {0, 1, 2, 3}, {});
  metaData.blockData() = std::move(blockData);

  return {numDistinctCol0, std::move(metaData)};
}

// ________________________________________________________________________
template <typename T, typename... Callbacks>
std::tuple<size_t, IndexMetaData, IndexMetaData> IndexImpl::createPermutations(
    size_t numColumns, T&& sortedTriples, const Permutation& p1,
    const Permutation& p2, Callbacks&&... callbacks) {
  AD_LOG_INFO << "Creating permutations " << p1.readableName() << " and "
              << p2.readableName() << " ..." << std::endl;
  auto metaData = createPermutationPairImpl(
      numColumns, getFilenameForPermutation(p1, false),
      getFilenameForPermutation(p2, false), AD_FWD(sortedTriples),
      p1.keyOrder(), AD_FWD(callbacks)...);

  auto& [numDistinctCol0, meta1, meta2] = metaData;
  meta1.calculateStatistics(numDistinctCol0);
  meta2.calculateStatistics(numDistinctCol0);
  AD_LOG_INFO << "Statistics for " << p1.readableName() << ": "
              << meta1.statistics() << std::endl;
  AD_LOG_INFO << "Statistics for " << p2.readableName() << ": "
              << meta2.statistics() << std::endl;

  return metaData;
}

// _____________________________________________________________________________
void IndexImpl::createPermutationWithoutTwin(size_t numColumns,
                                             BlocksOfTriples sortedTriples,
                                             const Permutation& permutation) {
  AD_LOG_INFO << "Creating permutation " << permutation.readableName() << " ..."
              << std::endl;
  auto fileName = getFilenameForPermutation(permutation, false);
  auto twinFileName = absl::StrCat(fileName, ".twin-tmp");
  absl::Cleanup deleteTwin = [&twinFileName]() {
    ad_utility::deleteFile(twinFileName, false);
  };
  // The `sortedTriples` are already in the column order of the `permutation`.
  auto [numDistinctCol0, metaData, twinMetaData] = createPermutationPairImpl(
      numColumns, fileName, twinFileName, std::move(sortedTriples),
      qlever::KeyOrder{0, 1, 2, 3});
  metaData.calculateStatistics(numDistinctCol0);
  writeMetaData(metaData, fileName);
}

// _____________________________________________________________________________
void IndexImpl::writeMetaData(IndexMetaData& metaData,
                              const std::string& filename) const {
  metaData.setName(getKbName());
  ad_utility::File f(filename, "r+");
  ad_utility::File metaFile(filename + META_FILE_SUFFIX, "w");
  metaData.appendToFile(f, metaFile);
}

// _____________________________________________________________________________
std::pair<size_t, IndexMetaData> IndexImpl::createPermutationWithoutMetadata(
    size_t numColumns,
    ad_utility::InputRangeTypeErased<IdTableStatic<0>> sortedTriples,
    const Permutation& permutation, bool internal, qlever::Executor executor) {
  AD_LOG_INFO << "Creating permutation " << permutation.readableName() << " ..."
              << std::endl;
  std::string fileName = getFilenameForPermutation(permutation, internal);
  auto metaData = createPermutationImpl(
      numColumns, fileName, std::move(sortedTriples), std::move(executor));

  auto& [numDistinctCol0, meta] = metaData;
  meta.calculateStatistics(numDistinctCol0);
  AD_LOG_INFO << "Statistics for " << permutation.readableName() << ": "
              << meta.statistics() << std::endl;

  return std::make_pair(numDistinctCol0, std::move(meta));
}

// _____________________________________________________________________________
void IndexImpl::finalizePermutation(IndexMetaData& meta,
                                    const Permutation& permutation,
                                    bool internal) const {
  std::string fileName = getFilenameForPermutation(permutation, internal);

  writeMetaData(meta, fileName);
}

// ________________________________________________________________________
template <typename SortedTriplesType, typename... CallbackTypes>
size_t IndexImpl::createPermutationPair(size_t numColumns,
                                        SortedTriplesType&& sortedTriples,
                                        const Permutation& p1,
                                        const Permutation& p2,
                                        CallbackTypes&&... callbacks) {
  auto [numDistinctC0, metaData1, metaData2] = createPermutations(
      numColumns, AD_FWD(sortedTriples), p1, p2, AD_FWD(callbacks)...);
  AD_LOG_DEBUG << "Writing meta data for " << p1.readableName() << " and "
               << p2.readableName() << " ..." << std::endl;
  writeMetaData(metaData1, getFilenameForPermutation(p1, false));
  writeMetaData(metaData2, getFilenameForPermutation(p2, false));
  return numDistinctC0;
}

// _____________________________________________________________________________
void IndexImpl::createFromOnDiskIndex(const std::string& onDiskBase,
                                      bool persistUpdatesOnDisk) {
  setOnDiskBase(onDiskBase);
  readConfiguration();
  vocab_.readFromFile(onDiskBase_ + VOCAB_SUFFIX);

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

  if (doNotLoadPermutations_) {
    // Set all permutations to nullptr to indicate they are not loaded.
    ql::ranges::fill(permutations_, nullptr);
    AD_LOG_INFO
        << "No permutations were loaded due to `doNotLoadPermutations` "
           "being set to true. Only queries that don't contain any triples "
           "can be executed."
        << std::endl;
  } else {
    // The PSO and POS permutations are always loaded, together with their
    // internal permutations. Of the others, those are loaded that the index
    // has and that were requested (see `permutationsToBuildOrLoad_`).
    std::vector<Permutation::Enum> toLoad = permutationsOfLoadedIndex_;
    if (permutationsToBuildOrLoad_.has_value()) {
      toLoad.erase(std::remove_if(toLoad.begin(), toLoad.end(),
                                  [this](Permutation::Enum p) {
                                    return !ad_utility::contains(
                                        permutationsToBuildOrLoad_.value(), p);
                                  }),
                   toLoad.end());
    }
    for (auto permutation : Permutation::INTERNAL) {
      load(getPermutationPtr(permutation), true);
    }
    for (auto permutation : toLoad) {
      if (!ad_utility::contains(Permutation::INTERNAL, permutation)) {
        load(getPermutationPtr(permutation));
      }
    }
    if (!hasAllPermutations()) {
      AD_LOG_INFO << "Loaded permutations: "
                  << absl::StrJoin(loadedPermutations(), ", ",
                                   [](std::string* out, Permutation::Enum p) {
                                     out->append(Permutation::toString(p));
                                   })
                  << "; queries for which none of these permutations is a "
                     "good fit will be slow"
                  << std::endl;
    }
  }

  // We have to load the patterns first to figure out if the patterns were built
  // at all.
  if (usePatterns_) {
    try {
      PatternCreator::readPatternsFromFile(
          getPatternFilename(), avgNumDistinctSubjectsPerPredicate_,
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
    setFilenamesForPersistentUpdates(true);
  }

  // Warn if the index uses the deprecated `LatMajor` encoding and may contain
  // points (an index without points does not depend on the encoding). This is
  // done after reading the updates, which may contain points as well.
  if (geoPointEncodingOfLoadedIndex_ ==
          ad_utility::GeoPointEncoding::LatMajor &&
      mayContainGeoPoints()) {
    AD_LOG_WARN << ad_utility::LAT_MAJOR_GEO_POINT_ENCODING_WARNING
                << std::endl;
  }

  // Only set at the very end, so that an index that failed to load (for
  // example, because it has an incompatible format) does not count as loaded
  // and the destructor does not log that it was unloaded.
  wasLoadedFromDisk_ = true;
}

// _____________________________________________________________________________
bool IndexImpl::mayContainGeoPoints() const {
  // Return true if the `OSP` permutation is not loaded (without it, there is
  // no cheap way to tell).
  if (doNotLoadPermutations_ || !hasPermutation(Permutation::OSP)) {
    return true;
  }

  // Get the blocks of the `OSP` permutation, which is sorted by the object, so
  // that the points (the objects of type `GeoPoint`) form one contiguous range
  // of it. The first and last object of each block include the updates.
  auto locatedTriplesState =
      deltaTriplesManager().getCurrentLocatedTriplesSharedState();
  BlockMetadataSpan blocks =
      OSP()
          .getLocatedTriplesForPermutation(*locatedTriplesState)
          .getAugmentedMetadata();

  // Find the first block whose last object is not of a datatype before
  // `GeoPoint`. Return false if there is none or if its first object is of a
  // datatype after `GeoPoint`.
  auto block = ql::ranges::find_if(blocks, [](const auto& block) {
    return block.lastTriple_.col0Id_.getDatatype() >= Datatype::GeoPoint;
  });
  if (block == blocks.end() ||
      block->firstTriple_.col0Id_.getDatatype() > Datatype::GeoPoint) {
    return false;
  }

  // Otherwise, read the objects of that block and check whether one of them is
  // a point. This block contains a point if there is any: either its last
  // object is a point, or it ends after the points.
  //
  // NOTE: The metadata alone cannot decide this, because in most indexes
  // without points, some block starts before and ends after the points (the
  // datatypes before, e.g. `VocabIndex`, and after, e.g. `BlankNodeIndex`, are
  // common).
  CompressedRelationReader::ScanSpecAndBlocks blockOnly{
      {std::nullopt, std::nullopt, std::nullopt},
      {BlockMetadataRange{block, block + 1}}};
  auto objects = OSP().scan(
      blockOnly, {}, std::make_shared<ad_utility::CancellationHandle<>>(),
      *locatedTriplesState, ad_utility::globalExecutor());
  return ql::ranges::any_of(objects.getColumn(0), [](Id id) {
    return id.getDatatype() == Datatype::GeoPoint;
  });
}

// _____________________________________________________________________________
void IndexImpl::setFilenamesForPersistentUpdates(bool readFromDisk) {
  deltaTriplesManager().setFilenameForPersistentUpdates(
      absl::StrCat(onDiskBase_, UPDATE_TRIPLES_SUFFIX), readFromDisk);
  graphNameManager_.setFilenameForPersisting(
      absl::StrCat(onDiskBase_, ALLOCATED_GRAPHS_SUFFIX), readFromDisk);
}

// _____________________________________________________________________________
void IndexImpl::throwExceptionIfNoPatterns() const {
  AD_CONTRACT_CHECK(
      usePatterns_,
      "The requested feature requires a loaded patterns file ("
      "do not specify the --no-patterns option for this to work)");
}

// _____________________________________________________________________________
const Permutation& IndexImpl::getPermutationImpl(
    const PermutationPtr& permutation, std::string_view permutationName) {
  if (!permutation) {
    throw std::runtime_error{
        absl::StrCat("The requested operation requires the ", permutationName,
                     " permutation to be loaded, but it was not loaded. This "
                     "typically happens when the index was loaded with the "
                     "`doNotLoadPermutations` option set to true.")};
  }
  return *permutation;
}

// _____________________________________________________________________________
const CompactVectorOfStrings<Id>& IndexImpl::getPatterns() const {
  throwExceptionIfNoPatterns();
  return patterns_;
}

// _____________________________________________________________________________
CompactVectorOfStrings<Id>& IndexImpl::getPatterns() { return patterns_; }

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
  for (const auto& permutation : permutations_) {
    if (permutation) {
      permutation->setKbName(name);
    }
  }
}

// ____________________________________________________________________________
void IndexImpl::setOnDiskBase(const std::string& onDiskBase) {
  onDiskBase_ = onDiskBase;
}

// ____________________________________________________________________________
std::vector<ql::filesystem::path> IndexImpl::allIndexFiles(
    std::string_view onDiskBase) {
  std::vector<ql::filesystem::path> result;
  auto addIfExists = [&result](ql::filesystem::path file) {
    if (ql::filesystem::exists(file)) {
      result.push_back(std::move(file));
    }
  };

  // The six permutations and the two internal permutations, each with their
  // `.meta` file. `isInternal` is passed as a `std::bool_constant` (a type that
  // carries the `bool` value) because C++17 does not support explicitly
  // templated lambdas.
  auto addPermutationFiles = [&addIfExists](auto isInternal,
                                            std::string_view base) {
    for (auto permutation : Permutation::all<isInternal>()) {
      for (auto& file : Permutation::fileNames(permutation, base)) {
        addIfExists(std::move(file));
      }
    }
  };
  addPermutationFiles(std::bool_constant<false>{}, onDiskBase);
  addPermutationFiles(std::bool_constant<true>{},
                      absl::StrCat(onDiskBase, QLEVER_INTERNAL_INDEX_INFIX));

  // Files with a fixed name. The optional ones (settings, persisted updates,
  // text index) are simply skipped by `addIfExists` when they do not exist.
  for (auto suffix :
       {PATTERNS_FILE_SUFFIX, CONFIGURATION_FILE, SETTINGS_FILE_SUFFIX,
        UPDATE_TRIPLES_SUFFIX, ALLOCATED_GRAPHS_SUFFIX, TEXT_INDEX_FILE_SUFFIX,
        TEXT_VOCAB_FILE_SUFFIX, TEXT_DOCS_DB_FILE_SUFFIX}) {
    addIfExists(absl::StrCat(onDiskBase, suffix));
  }

  // The set of vocabulary files depends on the vocabulary type, but they all
  // start with `<onDiskBase>.vocabulary`, so enumerate them via that prefix
  // (the same mechanism as `MaterializedViewsManager::viewFilesOnDisk`). The
  // helper only returns files that exist, so no extra existence check is
  // needed here.
  ql::ranges::move(
      qlever::util::filesWithBaseNameAndSuffix(onDiskBase, VOCAB_SUFFIX),
      std::back_inserter(result));

  return result;
}

// ____________________________________________________________________________
void IndexImpl::setKeepTempFiles(bool keepTempFiles) {
  keepTempFiles_ = keepTempFiles;
}

// _____________________________________________________________________________
bool& IndexImpl::usePatterns() { return usePatterns_; }

// _____________________________________________________________________________
bool IndexImpl::usePatterns() const { return usePatterns_; }

// _____________________________________________________________________________
// _____________________________________________________________________________
bool& IndexImpl::addHasWordTriples() { return addHasWordTriples_; }

// _____________________________________________________________________________
bool IndexImpl::addHasWordTriples() const { return addHasWordTriples_; }

// _____________________________________________________________________________
bool& IndexImpl::doNotLoadPermutations() { return doNotLoadPermutations_; }

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
  // Record the encoding of the geo points, which is the encoding of the index
  // that this process builds or has loaded (see `GeoPoint::encoding`). This
  // also adds the entry when the configuration of an index in the previous
  // format is written in the current format (for example, when the index is
  // rebuilt or a text index is added to it).
  configuration[std::string{ad_utility::GEO_POINT_ENCODING_KEY}] =
      ad_utility::GeoPointEncoding{GeoPoint::encoding()};
  // Record whether the index was built with ICU (Unicode) support. Indexes
  // built with and without ICU use different collations and are hence not
  // interchangeable; `readConfiguration` throws if the configuration of the
  // loaded index does not match the current binary.
  configuration["has-icu-support"] = ad_utility::useICUDefault;
  // Write the file atomically, so that a failure while it is written (for
  // example when it is rewritten for an existing index that a text index is
  // added to) does not leave an index that cannot be loaded anymore.
  std::string serialized = configuration.dump();
  qlever::util::writeFileAtomically(onDiskBase_ + CONFIGURATION_FILE,
                                    serialized);
}

// ____________________________________________________________________________
std::string IndexImpl::dateOfIndexBuild() const {
  return dateOfIndexBuild(configurationJson_, onDiskBase_);
}

// ____________________________________________________________________________
std::string IndexImpl::dateOfIndexBuild(const nlohmann::json& configurationJson,
                                        const std::string& onDiskBase) {
  if (configurationJson.contains(DATE_OF_INDEX_BUILD_KEY)) {
    return configurationJson[DATE_OF_INDEX_BUILD_KEY].get<std::string>();
  }
  // For indexes that were built before the build date was recorded in the
  // configuration, fall back to the last modification time of the
  // configuration file, which is written at the end of the index build. We
  // deliberately use `stat` and not `std::filesystem::last_write_time`: the
  // latter returns a time point on the `file_clock`, which cannot be
  // converted portably to a wall-clock time in C++17 (`clock_cast` requires
  // C++20), and `std::filesystem` is not available on all toolchains that
  // QLever targets.
  struct stat fileStat {};
  auto configFilename = onDiskBase + CONFIGURATION_FILE;
  AD_CONTRACT_CHECK(stat(configFilename.c_str(), &fileStat) == 0);
  return formatIndexBuildTime(absl::FromTimeT(fileStat.st_mtime));
}

// ____________________________________________________________________________
size_t IndexImpl::rowsPerBlock(const nlohmann::json& configurationJson) {
  size_t rowsPerBlock = configurationJson.value(INDEX_ROWS_PER_BLOCK_KEY,
                                                DEFAULT_INDEX_ROWS_PER_BLOCK);
  if (rowsPerBlock == 0 || rowsPerBlock > MAX_INDEX_ROWS_PER_BLOCK) {
    throw std::runtime_error{
        absl::StrCat("Invalid value ", rowsPerBlock, " for the key \"",
                     INDEX_ROWS_PER_BLOCK_KEY,
                     "\" in the `meta-data.json`, it must be between 1 and ",
                     MAX_INDEX_ROWS_PER_BLOCK)};
  }
  return rowsPerBlock;
}

// ____________________________________________________________________________
std::string IndexImpl::formatIndexBuildTime(absl::Time time) {
  return absl::FormatTime(DATE_OF_INDEX_BUILD_FORMAT, time,
                          absl::UTCTimeZone());
}

// ___________________________________________________________________________
void IndexImpl::readConfiguration() {
  applyConfiguration(
      fileToJson<nlohmann::json>(onDiskBase_ + CONFIGURATION_FILE));

  // Compute unique ID for this index.
  //
  // TODO: This is a simplistic way. It would be better to incorporate bytes
  // from the index files.
  //
  // NOTE: This is computed here rather than in `applyConfiguration`, because
  // `getKbName` reads from the `PSO` permutation, which is only available on
  // the on-disk load path (`applyConfiguration` is also used when loading from
  // a blob, where no permutations are loaded).
  indexId_ = absl::StrCat("#", getKbName(), ".", numTriples_.normal, ".",
                          numSubjects_.normal, ".", numPredicates_.normal, ".",
                          numObjects_.normal);
}

// ___________________________________________________________________________
std::optional<std::string> IndexImpl::checkIndexFormatVersion(
    const nlohmann::json& configuration) const {
  // NOTE: Only non-throwing accessors of `nlohmann::json` are used below (in
  // particular, `find` simply returns `end()` if `configuration` is not an
  // object).
  auto versionIt = configuration.find("index-format-version");
  if (versionIt == configuration.end()) {
    return std::string{
        "This index was built before versioning was introduced for QLever's "
        "index format. Please rebuild your index using the current version of "
        "QLever."};
  }
  const auto& currentVersion = qlever::indexFormatVersion;
  if (*versionIt == nlohmann::json(currentVersion)) {
    return std::nullopt;
  }

  // The version is not the current one. Parse it (without throwing) to check
  // whether it is one of the other loadable versions, or to give a more
  // specific message otherwise.
  auto prNumberIt = versionIt->find("pull-request-number");
  auto dateIt = versionIt->find("date");
  std::optional<DateYearOrDuration> date;
  if (prNumberIt != versionIt->end() && prNumberIt->is_number_unsigned() &&
      dateIt != versionIt->end() && dateIt->is_string()) {
    date = DateYearOrDuration::parseXsdDateGetOptDate(
        dateIt->get_ref<const std::string&>());
  }
  if (!date.has_value()) {
    return absl::StrCat(
        "The index format version stored in the index metadata is malformed: ",
        versionIt->dump(),
        ". Please rebuild your index using the current "
        "version of QLever.");
  }
  qlever::IndexFormatVersion indexFormatVersion{prNumberIt->get<uint64_t>(),
                                                date.value()};
  if (qlever::isLoadableIndexFormatVersion(indexFormatVersion)) {
    return std::nullopt;
  }
  auto versionDescription = absl::StrCat(
      "(PR = ", indexFormatVersion.prNumber_,
      ", Date = ", indexFormatVersion.date_.toStringAndType().first, ").");
  if (indexFormatVersion.date_.toBits() > currentVersion.date_.toBits()) {
    return absl::StrCat(
        "The version of QLever you are using is too old for this index. Please "
        "use a version of QLever that is compatible with this index ",
        versionDescription);
  }
  // If the index is in exactly the format that the `qlever-upgrade-index`
  // binary upgrades from, return one dedicated message instead of the generic
  // advice below, so that the upgrade option is not buried among the generic
  // alternatives. That the current version of QLever can load the upgraded
  // index is checked by `convertIndexToCurrentFormat` (and by a unit test).
  using namespace qlever::indexFormatConverter;
  if (indexFormatVersion == sourceVersion) {
    return absl::StrCat(
        "The index format changed on ",
        targetVersion.date_.toStringAndType().first,
        " (PR = ", targetVersion.prNumber_,
        "), but your index uses the previous format\n\nWe do our best "
        "to keep index format changes rare, but sometimes they are "
        "unavoidable. Either use an older version of QLever, or rebuild "
        "the index from scratch with the version of QLever you are "
        "currently using, or upgrade your index with the following "
        "command. Upgrading your index is more than 10 times faster "
        "than rebuilding it from scratch, and the old index is "
        "preserved in a subdirectory of your index directory in case "
        "something goes wrong.\n\nqlever upgrade-index ",
        std::string(onDiskBase_.size(), ' '),
        "   (if you use the qlever CLI)\n", "qlever-upgrade-index ",
        onDiskBase_, "   (if the qlever-* binaries are in your PATH)\n");
  }
  return absl::StrCat(
      "The index is too old for this version of QLever. Either rebuild the "
      "index from scratch with the version of QLever you are currently using, "
      "or use an older version of QLever that is compatible with this index ",
      versionDescription);
}

// ___________________________________________________________________________
void IndexImpl::applyGeoPointEncoding() {
  // Determine the encoding of the index. An index in the format that predates
  // the entry for the encoding always uses `LatMajor`. Throw if an index in
  // the current format has no entry (its points could then not be decoded
  // reliably).
  const std::string key{ad_utility::GEO_POINT_ENCODING_KEY};
  auto version = static_cast<qlever::IndexFormatVersion>(
      configurationJson_["index-format-version"]);
  auto encoding = ad_utility::GeoPointEncoding::LatMajor;
  if (version != qlever::indexFormatVersionWithLatMajorGeoPoints) {
    if (!configurationJson_.contains(key)) {
      throw std::runtime_error{absl::StrCat(
          "The configuration of the index (\"", onDiskBase_, CONFIGURATION_FILE,
          "\") has no entry \"", key,
          "\", which every index in the current format has; please rebuild "
          "the index")};
    }
    encoding = configurationJson_[key].get<ad_utility::GeoPointEncoding>();
  }

  // Use that encoding for all points of this process.
  GeoPoint::setEncoding(encoding);
  geoPointEncodingOfLoadedIndex_ = encoding;
}

// _____________________________________________________________________________
void IndexImpl::applyConfiguration(const nlohmann::json& configuration) {
  configurationJson_ = configuration;
  if (configurationJson_.find("git-hash") != configurationJson_.end()) {
    AD_LOG_INFO << "The git hash used to build this index was "
                << configurationJson_["git-hash"] << std::endl;
  } else {
    AD_LOG_INFO
        << "The index was built before git commit hashes were stored in "
           "the index meta data"
        << std::endl;
  }

  if (auto error = checkIndexFormatVersion(configurationJson_);
      error.has_value()) {
    throw std::runtime_error{std::move(error).value()};
  }

  applyGeoPointEncoding();

  // The index and the current binary must agree on whether ICU (Unicode)
  // support is available: the two use different string collations, so mixing
  // them would silently produce a wrong sort order. Indexes built before this
  // flag was recorded were always built with ICU, hence the default of `true`.
  bool indexHasIcuSupport = configurationJson_.value("has-icu-support", true);
  if (indexHasIcuSupport != ad_utility::useICUDefault) {
    throw std::runtime_error{absl::StrCat(
        "This index was built ", indexHasIcuSupport ? "with" : "without",
        " ICU (Unicode) support, but the QLever binary you are using was "
        "built ",
        ad_utility::useICUDefault ? "with" : "without",
        " it. The two use different string collations and are not "
        "interchangeable. Please rebuild the index or use a matching QLever "
        "binary.")};
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
           "an old index build that is no longer supported by QLever. "
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
      target = it->get<Target>();
    }
  };

  loadDataMember("git-hash", gitShortHash_);
  // Older indexes only store whether they have all six permutations or only
  // PSO and POS.
  if (auto it = configurationJson_.find("permutations");
      it != configurationJson_.end()) {
    permutationsOfLoadedIndex_.clear();
    for (const auto& name : it->get<std::vector<std::string>>()) {
      permutationsOfLoadedIndex_.push_back(Permutation::fromString(name));
    }
  } else {
    bool hasAllPermutations = true;
    loadDataMember("has-all-permutations", hasAllPermutations, true);
    permutationsOfLoadedIndex_ =
        hasAllPermutations
            ? std::vector<Permutation::Enum>(Permutation::GRAPH_LAST.begin(),
                                             Permutation::GRAPH_LAST.end())
            : std::vector<Permutation::Enum>(Permutation::INTERNAL.begin(),
                                             Permutation::INTERNAL.end());
  }
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

  // The normalization of the words of the `ql:has-word` triples of this index
  // (an index built before this key existed uses `Lowercase`).
  loadDataMember(ad_utility::HAS_WORD_NORMALIZATION_KEY, hasWordNormalization_,
                 ad_utility::HasWordNormalization::Lowercase);

  // Whether this index has `ql:has-word` triples (an index built before this
  // key existed is treated as having none).
  loadDataMember(ad_utility::ADD_HAS_WORD_TRIPLES_KEY, addHasWordTriples_,
                 false);

  // The predicates to which the `ql:has-word` triples of this index are
  // restricted. They were stored sorted and without duplicates.
  //
  // NOTE: `std::greater_equal` finds the first pair that is not strictly
  // increasing, so the check covers both the sortedness (needed for the binary
  // search in `hasWordTriplesForPredicate`) and the uniqueness.
  loadDataMember(ad_utility::HAS_WORD_RESTRICT_PREDICATES_KEY,
                 hasWordRestrictPredicates_, std::vector<std::string>{});
  AD_CORRECTNESS_CHECK(ql::ranges::adjacent_find(hasWordRestrictPredicates_,
                                                 std::greater_equal<>{}) ==
                       hasWordRestrictPredicates_.end());

  // The block size with which the permutations of this index were written. It
  // is needed when further permutations of this index are written later on (a
  // materialized view, for example), so that all permutations of an index have
  // the same block size. Indexes that were built before this key existed were
  // built with the default.
  rowsPerBlock_ = rowsPerBlock(configurationJson_);

  // The geo cell grid of the geo vocabulary, if the index was built with one
  // (see `GeoCellGrid`). The vocabulary needs it before it is opened, because
  // the grid determines how its indices are composed.
  uint64_t geoCellGridLevel = 0;
  loadDataMember("geo-cell-grid-level", geoCellGridLevel, geoCellGridLevel);
  if (geoCellGridLevel > 0) {
    if (geoCellGridLevel > std::numeric_limits<uint8_t>::max()) {
      throw std::runtime_error{absl::StrCat(
          "Invalid value ", geoCellGridLevel,
          " for the key \"geo-cell-grid-level\" in the `meta-data.json`")};
    }
    ad_utility::GeoCellGridScheme geoCellGridScheme =
        ad_utility::GeoCellGridScheme::Flat;
    loadDataMember("geo-cell-grid-scheme", geoCellGridScheme,
                   geoCellGridScheme);
    vocab_.setGeoCellGrid(ad_utility::GeoCellGrid{
        static_cast<uint8_t>(geoCellGridLevel), geoCellGridScheme});
    // Setting the grid is a no-op for a vocabulary type without a
    // `GeoVocabulary` that could hold it.
    if (!vocab_.getGeoCellGrid().has_value()) {
      throw std::runtime_error{
          "A geo cell grid requires the vocabulary type "
          "`on-disk-compressed-geo-split`"};
    }
  }

  // Initialize BlankNodeManager
  uint64_t numBlankNodesTotal;
  loadDataMember(BLANK_NODE_ALLOCATION_START, numBlankNodesTotal);
  blankNodeManager_ =
      std::make_unique<ad_utility::BlankNodeManager>(numBlankNodesTotal);

  loadDataMember("encoded-iri-prefixes", encodedIriManager_,
                 EncodedIriManager{});
  loadDataMember("graphNameManager", graphNameManager_,
                 GraphNameManager(std::string(QLEVER_NEW_GRAPH_PREFIX), 1));
}

// _____________________________________________________________________________
ad_utility::HashMap<std::string, size_t> IndexImpl::computeHasWordFrequencies(
    std::string_view content) const {
  // With `LowercaseNoAccents`, remove the accents before the tokenization
  // (combining marks are not alphanumeric and would otherwise split words).
  // `contentWithoutAccents` owns the result, so that `content` stays valid
  // during the tokenization.
  std::string contentWithoutAccents;
  if (hasWordNormalization_ ==
      ad_utility::HasWordNormalization::LowercaseNoAccents) {
    contentWithoutAccents = ad_utility::removeAccentsUtf8(content);
    content = contentWithoutAccents;
  }

  // Count the occurrences of each normalized word.
  ad_utility::HashMap<std::string, size_t> wordFrequencies;
  for (auto&& word :
       tokenizeAndNormalizeText(content, vocab_.getLocaleManager())) {
    ++wordFrequencies[std::move(word)];
  }
  return wordFrequencies;
}

// ___________________________________________________________________________
ProcessedTriple IndexImpl::processTriple(TurtleTriple&& triple) const {
  ProcessedTriple result;
  auto& resultTriple = result.triple_;
  if (triple.object_.isLiteral()) {
    const auto& lit = triple.object_.getLiteral();
    if (lit.hasLanguageTag()) {
      result.langtag_ = std::string(lit.getLanguageTag());
    }
    // Return the predicate as a string. The predicate is an `Id` if the parser
    // has encoded it, see `EncodedIriManager`. In this case,
    // `decodedPredicate` owns its string, so that the result is a reference in
    // both cases (no copy of the IRI).
    std::string decodedPredicate;
    auto getPredicate = [this, &predicate = triple.predicate_,
                         &decodedPredicate]() -> const std::string& {
      return predicate.isIri()
                 ? predicate.getIri().toStringRepresentation()
                 : (decodedPredicate =
                        encodedIriManager().toString(predicate.getId()));
    };

    // Extract the words of the literal for the `ql:has-word` triples and count
    // their term frequencies (only if the predicate is one of the
    // `hasWordRestrictPredicates_`, or none are set).
    if (addHasWordTriples_ && hasWordTriplesForPredicate(getPredicate)) {
      result.wordFrequencies_ = computeHasWordFrequencies(lit.getContent());
    }
  }

  // The following lambda deals with triple elements that might be strings
  // (literals or IRIs) as well as values that can be encoded into the IRI
  // directly. These currently are the object and the graph ID of the triple.
  // The `index` is the index of the element within the triple. For example if
  // the `getter` is `subject_` then the index has to be `0`.
  auto handleStringOrId = [this, &triple, &resultTriple](auto getter,
                                                         size_t index) {
    // If the object of the triple can be directly folded into an ID, do so.
    // Note that the actual folding is done by the `TripleComponent`.
    auto& el = std::invoke(getter, triple);
    std::optional<Id> idIfNotString =
        toValueIdIfNotString(el, &encodedIriManager());

    // TODO<joka921> The following statement could be simplified by a helper
    // function "optionalCast";
    resultTriple[index] =
        idIfNotString.has_value()
            ? TripleComponent{idIfNotString.value()}
            // `toRdfLiteral` handles literals as well as IRIs correctly.
            : std::move(el);
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

  for (auto& component : resultTriple) {
    if (component.tripleComponent_.isId()) {
      // If we already have an ID, we can just continue;
      continue;
    }
    const auto& iriOrLiteral = component.tripleComponent_;
    // TODO<joka921> Perform this normalization right at the beginning of the
    // parsing. iriOrLiteral =
    // vocab_.getLocaleManager().normalizeUtf8(iriOrLiteral);
    // The view always exists here: `handleStringOrId` above has turned all
    // values that can be directly encoded into an `Id` into one, so what is
    // left is a literal, an IRI, or a blank node string.
    if (vocab_.shouldBeExternalized(toRdfLiteralView(iriOrLiteral).value())) {
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
                << "\" " << "and \"ignore-punctuation = " << ignorePunctuation
                << "\"" << std::endl;

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
  if (useParallelParser_.value_or(false)) {
    AD_LOG_INFO << WARNING_PARALLEL_PARSING << std::endl;
  }

  if (j.count("num-triples-per-batch")) {
    numTriplesPerBatch_ = size_t{j["num-triples-per-batch"]};
    AD_LOG_INFO
        << "You specified \"num-triples-per-batch = " << numTriplesPerBatch_
        << "\", choose a lower value if the index builder runs out of memory"
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
          TurtleParserIntegerOverflowBehavior::AllToDouble;
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
void IndexImpl::writePartialVocabulary(
    size_t partialVocabIdx, const ItemMapAndBuffer& items,
    std::vector<std::array<Id, NumColumnsIndexBuilding>>& localIds) const {
  using namespace ad_utility::vocabulary_merger;
  AD_LOG_DEBUG
      << "Triples processed, also counting internal triples added by QLever: "
      << localIds.size() << std::endl;
  std::string partialFilename =
      partialVocabularyWordsFilename(onDiskBase_, partialVocabIdx);

  auto vec = [&]() {
    ad_utility::TimeBlockAndLog l{"vocab map to vector"};
    return vocabMapsToVector(items);
  }();
  {
    ad_utility::TimeBlockAndLog l{"sorting by unicode order"};
    // This sort is single-threaded because this function runs on the shared
    // thread pool of `buildPartialVocabularies`, which already keeps all cores
    // busy. The sort uses `pdqsort` because it is about 10% faster than
    // `ql::ranges::sort` here (measured on DBLP).
    boost::sort::pdqsort(
        vec.begin(), vec.end(),
        [&c = vocab_.getCaseComparator()](const auto& a, const auto& b) {
          return c.isLessInTotalWithExternalFlag(
              a.word_, a.indexAndFlag_.isExternal(), b.word_,
              b.indexAndFlag_.isExternal());
        });
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
                            return a.indexAndFlag_.index() ==
                                   b.indexAndFlag_.index();
                          }),
              vec.end());
  }
  {
    ad_utility::TimeBlockAndLog l{"writing to file"};
    writeMappedIdsToFile(localIds, mapping,
                         unsortedTriplesFilename(onDiskBase_, partialVocabIdx));
  }
  {
    ad_utility::TimeBlockAndLog l{"write partial vocabulary"};
    writePartialVocabularyToFile(vec, partialFilename);
  }
  AD_LOG_TRACE << "Finished writing the partial vocabulary" << std::endl;
}

// ____________________________________________________________________________
IndexImpl::NumNormalAndInternal IndexImpl::numTriples() const {
  return numTriples_;
}

// ____________________________________________________________________________
IndexImpl::PermutationPtr IndexImpl::getPermutationPtr(Permutation::Enum p) {
  const auto& result = permutations_.at(static_cast<size_t>(p));
  // Throw a readable error if the permutations were not loaded at all.
  getPermutationImpl(result, Permutation::toString(p));
  return result;
}

// ____________________________________________________________________________
auto IndexImpl::makePermutations(
    const ad_utility::AllocatorWithLimit<Id>& allocator)
    -> std::array<PermutationPtr, Permutation::ALL.size()> {
  std::array<PermutationPtr, Permutation::ALL.size()> result;
  for (auto permutation : Permutation::ALL) {
    result.at(static_cast<size_t>(permutation)) =
        std::make_shared<Permutation>(permutation, allocator);
  }
  return result;
}

// ____________________________________________________________________________
bool IndexImpl::hasPermutation(Permutation::Enum permutation) const {
  const auto& ptr = permutations_.at(static_cast<size_t>(permutation));
  return ptr != nullptr && ptr->isLoaded();
}

// ____________________________________________________________________________
bool IndexImpl::hasAllPermutations() const {
  return ql::ranges::all_of(Permutation::GRAPH_LAST, [this](auto permutation) {
    return hasPermutation(permutation);
  });
}

// ____________________________________________________________________________
const Permutation* IndexImpl::firstLoadedPermutation(
    std::initializer_list<Permutation::Enum> permutations) const {
  for (auto permutation : permutations) {
    if (hasPermutation(permutation)) {
      return &getPermutation(permutation);
    }
  }
  return nullptr;
}

// ____________________________________________________________________________
std::vector<Permutation::Enum> IndexImpl::loadedPermutations() const {
  std::vector<Permutation::Enum> result;
  ql::ranges::copy_if(
      Permutation::ALL, std::back_inserter(result),
      [this](auto permutation) { return hasPermutation(permutation); });
  return result;
}

// ____________________________________________________________________________
std::vector<Permutation::Enum> IndexImpl::permutationsToBuild() const {
  std::vector<Permutation::Enum> result;
  const auto& requested = permutationsToBuildOrLoad_;
  // The PSO and POS permutations are always built, see
  // `permutationsToBuildOrLoad_`.
  ql::ranges::copy_if(
      Permutation::ALL, std::back_inserter(result),
      [&requested](auto permutation) {
        if (ad_utility::contains(Permutation::INTERNAL, permutation)) {
          return true;
        }
        if (!requested.has_value()) {
          return Permutation::hasGraphLast(permutation);
        }
        return ad_utility::contains(requested.value(), permutation);
      });
  return result;
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
      firstLoadedPermutation({Permutation::SPO, Permutation::SOP}) != nullptr,
      "Can only get # distinct subjects if the SPO or SOP permutation is "
      "loaded.");
  return numSubjects_;
}

// __________________________________________________________________________
Index::NumNormalAndInternal IndexImpl::numDistinctObjects() const {
  AD_CONTRACT_CHECK(
      firstLoadedPermutation({Permutation::OSP, Permutation::OPS}) != nullptr,
      "Can only get # distinct objects if the OSP or OPS permutation is "
      "loaded.");
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
  if (auto keyId = toValueId(key, *this)) {
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
  // The number of distinct subjects (objects) is only known if a permutation
  // that starts with the subject (object) is loaded. Otherwise, we
  // (optimistically) assume that they are all distinct.
  auto multiplicity = [this, numTriples](
                          std::initializer_list<Permutation::Enum> perms,
                          auto getNumDistinct) {
    if (firstLoadedPermutation(perms) == nullptr) {
      return 1.0f;
    }
    return numTriples / std::invoke(getNumDistinct, this).normal;
  };
  // The number of distinct graphs is not known, so we (pessimistically) assume
  // that there is only one.
  std::array multiplicities{multiplicity({Permutation::SPO, Permutation::SOP},
                                         &IndexImpl::numDistinctSubjects),
                            numTriples / numDistinctPredicates().normal,
                            multiplicity({Permutation::OSP, Permutation::OPS},
                                         &IndexImpl::numDistinctObjects),
                            numTriples};
  // The multiplicities of the first three columns of the `permutation`.
  auto permuted = permutation.keyOrder().permuteTuple(multiplicities);
  return {permuted.begin(), permuted.begin() + 3};
}

// _____________________________________________________________________________
void IndexImpl::deleteTemporaryFile(const std::string& path) {
  if (!keepTempFiles_) {
    ad_utility::deleteFile(path);
  }
}

// _____________________________________________________________________________
std::string IndexImpl::getPatternFilename() const {
  return absl::StrCat(onDiskBase_, PATTERNS_FILE_SUFFIX);
}

// _____________________________________________________________________________
CPP_template_def(typename Input, typename... NextSorter)(
    requires(sizeof...(NextSorter) <= 1))
void IndexImpl::createPSOAndPOSImpl(size_t numColumns, Input sortedTriples,
                                    bool doWriteConfiguration,
                                    NextSorter&... nextSorter) {
  // NOTE: For a `ChunkwiseInput` the callbacks are invoked concurrently, hence
  // the atomics (which are only accessed once per block).
  std::atomic<size_t> numTriples = 0;
  auto countTriples = [&numTriples](const IdTableStatic<0>& block) {
    numTriples.fetch_add(block.numRows());
  };
  std::atomic<uint64_t> nextAvailableIndex = 1;
  auto newGraphPrefixIdx = [this]() {
    auto prefix = encodedIriManager_.getIndexOfPrefix(QLEVER_NEW_GRAPH_PREFIX);
    AD_CORRECTNESS_CHECK(prefix.has_value());
    return std::move(prefix).value();
  }();
  auto determineNextAvailableInternalGraph =
      [&nextAvailableIndex, newGraphPrefixIdx](const auto& triple) mutable {
        const auto& graph = triple[3];
        if (graph.getDatatype() != Datatype::EncodedVal) {
          return;
        }
        // NOTE: The payload may only be decoded after the prefix has been
        // checked, because the payload of a general pattern is not a single
        // decimal number (see `EncodedIriManager`).
        auto [prefix, payload] =
            EncodedIriManager::splitIntoPrefixIdxAndPayload(graph);
        if (prefix != newGraphPrefixIdx) {
          return;
        }
        atomicMax(nextAvailableIndex,
                  EncodedIriManager::decodeDecimalFrom64Bit(payload) + 1);
      };
  size_t numPredicates =
      createPermutationPair(numColumns, std::move(sortedTriples), PSO(), POS(),
                            pushToNextSorter(nextSorter)..., countTriples,
                            liftCallback(determineNextAvailableInternalGraph));
  configurationJson_["num-predicates"] =
      NumNormalAndInternal::fromNormal(numPredicates);
  configurationJson_["num-triples"] =
      NumNormalAndInternal::fromNormal(numTriples.load());
  graphNameManager_ = GraphNameManager(std::string(QLEVER_NEW_GRAPH_PREFIX),
                                       nextAvailableIndex.load());
  configurationJson_["graphNameManager"] = graphNameManager_;
  if (doWriteConfiguration) {
    writeConfiguration();
  }
}

// _____________________________________________________________________________
CPP_template_def(typename Input, typename... NextSorter)(
    requires(sizeof...(NextSorter) <= 1))
void IndexImpl::createPSOAndPOS(size_t numColumns, Input sortedTriples,
                                NextSorter&... nextSorter) {
  createPSOAndPOSImpl(numColumns, std::move(sortedTriples), true,
                      nextSorter...);
}

// _____________________________________________________________________________
CPP_template_def(typename Input, typename... NextSorter)(
    requires(sizeof...(NextSorter) <= 1))
std::optional<PatternCreator::TripleSorter> IndexImpl::createSPOAndSOP(
    size_t numColumns, Input sortedTriples, NextSorter&... nextSorter) {
  std::optional<PatternCreator::TripleSorter> result;
  if (usePatterns_) {
    // The pattern creation needs the triples in order.
    AD_CORRECTNESS_CHECK(
        (!ad_utility::isInstantiation<Input, ChunkwiseInput>),
        "The patterns can't be built while the SPO and SOP permutations are "
        "written chunkwise");
    // We will return the next sorter.
    AD_CORRECTNESS_CHECK(sizeof...(nextSorter) == 0);
    // For now (especially for testing) We build the new pattern format as well
    // as the old one to see that they match.
    PatternCreator patternCreator{
        getPatternFilename(), idOfHasPatternDuringIndexBuilding_.value(),
        memoryLimitIndexBuilding() / NUM_EXTERNAL_SORTERS_AT_SAME_TIME};
    auto pushTriplesToPatterns =
        [&patternCreator](const IdTableStatic<0>& block) {
          static_assert(NumColumnsIndexBuilding == 4,
                        "this place probably has to be changed when additional "
                        "payload columns are added");
          for (const auto& triple : block) {
            auto tripleArr =
                std::array<Id, 4>{triple[0], triple[1], triple[2], triple[3]};
            patternCreator.processTriple(tripleArr);
          }
        };
    size_t numSubjects =
        createPermutationPair(numColumns, std::move(sortedTriples), SPO(),
                              SOP(), pushTriplesToPatterns);
    patternCreator.finish();
    configurationJson_["num-subjects"] =
        NumNormalAndInternal::fromNormal(numSubjects);
    writeConfiguration();
    result = std::move(patternCreator).getTripleSorter();
  } else {
    AD_CORRECTNESS_CHECK(sizeof...(nextSorter) == 1);
    size_t numSubjects =
        createPermutationPair(numColumns, std::move(sortedTriples), SPO(),
                              SOP(), pushToNextSorter(nextSorter)...);
    configurationJson_["num-subjects"] =
        NumNormalAndInternal::fromNormal(numSubjects);
    writeConfiguration();
  }
  return result;
}

// _____________________________________________________________________________
CPP_template_def(typename Input, typename... NextSorter)(
    requires(sizeof...(NextSorter) <= 1))
void IndexImpl::createOSPAndOPS(size_t numColumns, Input sortedTriples,
                                NextSorter&... nextSorter) {
  // For the last pair of permutations we don't need a next sorter, so we
  // have no fourth argument.
  size_t numObjects =
      createPermutationPair(numColumns, std::move(sortedTriples), OSP(), OPS(),
                            pushToNextSorter(nextSorter)...);
  configurationJson_["num-objects"] =
      NumNormalAndInternal::fromNormal(numObjects);
  writeConfiguration();
}

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
    std::vector<std::string> prefixesWithoutAngleBrackets,
    std::vector<encodedIri::Pattern> patterns) {
  encodedIriManager_ = EncodedIriManager{
      std::move(prefixesWithoutAngleBrackets), std::move(patterns)};
}

// _____________________________________________________________________________
void IndexImpl::setHasWordRestrictPredicates(
    const std::vector<std::string>& predicates) {
  // Validate the predicates (which throws for an invalid one, before anything
  // is changed).
  std::vector<std::string> validated;
  for (const std::string& predicate : predicates) {
    validated.push_back(
        ad_utility::triple_component::Iri::fromIrirefValidated(predicate)
            .toStringRepresentation());
  }

  // Sort them (for the binary search in `hasWordTriplesForPredicate`) and
  // remove duplicates (the stored predicates are checked to be strictly
  // increasing when they are loaded, see `applyConfiguration`).
  ql::ranges::sort(validated);
  validated.erase(std::unique(validated.begin(), validated.end()),
                  validated.end());
  hasWordRestrictPredicates_ = std::move(validated);
}

// _____________________________________________________________________________
void IndexImpl::setBlankNodeIriRegexes(
    std::vector<std::string> blankNodeIriRegexes) {
  ql::ranges::for_each(blankNodeIriRegexes, [](const std::string& regex) {
    // The regexes are matched against the full IRI text (including the angle
    // brackets), so each of them has to describe an IRI and must therefore
    // start with `<`.
    if (!ql::starts_with(regex, '<')) {
      throw std::runtime_error{absl::StrCat(
          "A regex for treating IRIs as blank nodes has to match a full "
          "IRI and must therefore start with `<`, but got: ",
          regex)};
    }
  });
  // The compilation of the regexes (which also reports those that are not valid
  // regular expressions) is done by `ad_utility::RegexSet`.
  blankNodeIriRegexes_ =
      ad_utility::RegexSet{std::move(blankNodeIriRegexes),
                           "passed to `--iri-as-blank-node-regexes`"};
}

// _____________________________________________________________________________
void IndexImpl::writePatternsToFile() const {
  PatternStatistics statistics;
  statistics.numDistinctSubjectPredicatePairs_ =
      numDistinctSubjectPredicatePairs_;
  statistics.avgNumDistinctSubjectsPerPredicate_ =
      avgNumDistinctSubjectsPerPredicate_;
  statistics.avgNumDistinctPredicatesPerSubject_ =
      avgNumDistinctPredicatesPerSubject_;
  PatternCreator::writePatternsToFile(getPatternFilename(), patterns_,
                                      statistics);
}

// _____________________________________________________________________________
void IndexImpl::loadConfigFromOldIndex(const std::string& newName,
                                       const IndexImpl& other,
                                       const nlohmann::json& newStats) {
  // Copy the relevant information from an existing index to rebuild the new
  // index and write a fresh configuration file for a new index.
  setOnDiskBase(newName);
  setKbName(other.getKbName());
  rowsPerBlock() = other.rowsPerBlock();
  configurationJson_ = newStats;
  numTriples_ = static_cast<NumNormalAndInternal>(newStats.at("num-triples"));
  numPredicates_ =
      static_cast<NumNormalAndInternal>(newStats.at("num-predicates"));
  numSubjects_ = newStats.value("num-subjects", NumNormalAndInternal{});
  numObjects_ = newStats.value("num-objects", NumNormalAndInternal{});
  avgNumDistinctPredicatesPerSubject_ =
      other.avgNumDistinctPredicatesPerSubject_;
  avgNumDistinctSubjectsPerPredicate_ =
      other.avgNumDistinctSubjectsPerPredicate_;
  numDistinctSubjectPredicatePairs_ = other.numDistinctSubjectPredicatePairs_;
  writeConfiguration();
}

// _____________________________________________________________________________
void IndexImpl::countDistinct(std::optional<Id>& lastId, size_t& counter,
                              const IdTable& table) {
  AD_CORRECTNESS_CHECK(
      !table.empty(), "Empty tables should never be yielded by the lazy scan.");
  auto col = table.getColumn(0);
  counter += ql::ranges::distance(col | ::ranges::views::unique([](Id a, Id b) {
                                    return a.getBits() == b.getBits();
                                  }));
  if (lastId == col.front()) {
    // Avoid double counting in case the last id of the previous block is the
    // same as the first id of this block.
    counter--;
  }
  lastId = col.back();
}

namespace {
// Helper function that returns a packaged task that computes distinct counts
// over all tables produced by scanning the given permutation. The customAction
// is invoked for each table to allow for additional computations while
// scanning.
template <typename CustomAction>
std::packaged_task<void()> computeStatistics(
    const LocatedTriplesSharedState& locatedTriplesSharedState, size_t& counter,
    const Permutation& permutation, qlever::Executor executor,
    CustomAction customAction, const std::function<void(size_t)>& progress) {
  return std::packaged_task<void()>{[&counter, &permutation,
                                     &locatedTriplesSharedState,
                                     executor = std::move(executor), progress,
                                     customAction =
                                         std::move(customAction)]() mutable {
    auto cancellationHandle =
        std::make_shared<ad_utility::SharedCancellationHandle::element_type>();
    ScanSpecification scanSpec{std::nullopt, std::nullopt, std::nullopt};
    std::array<ColumnIndex, 1> additionalColumns{ADDITIONAL_COLUMN_GRAPH_ID};
    auto [reader, tables] = permutation.lazyScanWithUnlimitedReader(
        permutation.getScanSpecAndBlocks(scanSpec, *locatedTriplesSharedState),
        additionalColumns, cancellationHandle, *locatedTriplesSharedState,
        std::move(executor));
    std::optional<Id> lastCol0 = std::nullopt;
    for (const auto& table : tables) {
      std::invoke(customAction, table);
      IndexImpl::countDistinct(lastCol0, counter, table);
      progress(table.numRows());
    }
  }};
}
}  // namespace

// _____________________________________________________________________________
nlohmann::json IndexImpl::recomputeStatistics(
    const LocatedTriplesSharedState& locatedTriplesSharedState,
    qlever::Executor executor,
    const std::function<void(size_t)>& progress) const {
  size_t numTriples = 0;
  size_t numTriplesInternal = 0;
  size_t numSubjects = 0;
  size_t numPredicates = 0;
  size_t numPredicatesInternal = 0;
  size_t numObjects = 0;

  std::vector<std::packaged_task<void()>> tasks;

  auto getCounterTask = [&locatedTriplesSharedState, &executor, &progress](
                            size_t& counter, const Permutation& permutation,
                            auto customAction) {
    return computeStatistics(locatedTriplesSharedState, counter, permutation,
                             executor, customAction, progress);
  };

  tasks.push_back(getCounterTask(
      numPredicates, PSO(),
      [&numTriples](const IdTable& table) { numTriples += table.numRows(); }));

  tasks.push_back(getCounterTask(numPredicatesInternal,
                                 PSO().internalPermutation(),
                                 [&numTriplesInternal](const IdTable& table) {
                                   numTriplesInternal += table.numRows();
                                 }));

  // The distinct subjects (objects) can be counted on any loaded permutation
  // that starts with the subject (object).
  const Permutation* subjectPermutation =
      firstLoadedPermutation({Permutation::SPO, Permutation::SOP});
  const Permutation* objectPermutation =
      firstLoadedPermutation({Permutation::OSP, Permutation::OPS});
  if (subjectPermutation) {
    tasks.push_back(
        getCounterTask(numSubjects, *subjectPermutation, ad_utility::noop));
  }
  if (objectPermutation) {
    tasks.push_back(
        getCounterTask(numObjects, *objectPermutation, ad_utility::noop));
  }
  ad_utility::runTasksInParallel(std::move(tasks));
  auto configuration = configurationJson_;
  configuration["num-triples"] =
      NumNormalAndInternal{numTriples, numTriplesInternal};
  configuration["num-predicates"] =
      NumNormalAndInternal{numPredicates, numPredicatesInternal};
  // These are unused.
  AD_CORRECTNESS_CHECK(numSubjects_.internal == 0);
  AD_CORRECTNESS_CHECK(numObjects_.internal == 0);
  if (subjectPermutation) {
    configuration["num-subjects"] = NumNormalAndInternal{numSubjects, 0};
  }
  if (objectPermutation) {
    configuration["num-objects"] = NumNormalAndInternal{numObjects, 0};
  }
  // Note: We don't update the key represented by `BLANK_NODE_ALLOCATION_START`,
  // because during index rebuilding blank nodes are remapped to sequentially
  // incrementing values instead of using the randomly allocated ids from
  // before. If we updated this value here, it would lead to an incorrect
  // allocation start value during the next index rebuild.
  return configuration;
}
