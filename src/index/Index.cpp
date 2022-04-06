// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./Index.h"

#include <absl/strings/str_join.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <future>
#include <optional>
#include <stxxl/algorithm>
#include <stxxl/map>
#include <unordered_map>

#include "../parser/ParallelParseBuffer.h"
#include "../util/BatchedPipeline.h"
#include "../util/CompressionUsingZstd/ZstdWrapper.h"
#include "../util/Conversions.h"
#include "../util/HashMap.h"
#include "../util/Serializer/FileSerializer.h"
#include "../util/TupleHelpers.h"
#include "./PrefixHeuristic.h"
#include "./VocabularyGenerator.h"
#include "TriplesView.h"

using std::array;

// _____________________________________________________________________________
Index::Index() : _usePatterns(false) {}

// _____________________________________________________________________________
template <class Parser>
IndexBuilderDataAsPsoSorter Index::createIdTriplesAndVocab(
    const string& ntFile) {
  auto indexBuilderData =
      passFileForVocabulary<Parser>(ntFile, _numTriplesPerBatch);
  // first save the total number of words, this is needed to initialize the
  // dense IndexMetaData variants
  _totalVocabularySize = indexBuilderData.nofWords;
  LOG(DEBUG) << "Number of words in internal and external vocabulary: "
             << _totalVocabularySize << std::endl;

  LOG(INFO) << "Converting external vocabulary to binary format ..."
            << std::endl;
  if (_onDiskLiterals) {
    _vocab.externalizeLiteralsFromTextFile(
        _onDiskBase + EXTERNAL_LITS_TEXT_FILE_NAME,
        _onDiskBase + EXTERNAL_VOCAB_SUFFIX);
  }
  deleteTemporaryFile(_onDiskBase + EXTERNAL_LITS_TEXT_FILE_NAME);
  // clear vocabulary to save ram (only information from partial binary files
  // used from now on). This will preserve information about externalized
  // Prefixes etc.
  _vocab.clear();
  auto psoSorter = convertPartialToGlobalIds(
      *indexBuilderData.idTriples, indexBuilderData.actualPartialSizes,
      NUM_TRIPLES_PER_PARTIAL_VOCAB);

  return {indexBuilderData, std::move(psoSorter)};
}
namespace {
// Return a lambda that takes a triple of IDs and returns true iff the predicate
// `triple[1]` is in [langPredLowerBound, langPredUpperBound)
auto predicateHasLanguage(Id langPredLowerBound, Id langPredUpperBound) {
  return [langPredLowerBound, langPredUpperBound](const auto& triple) {
    return triple[1] >= langPredLowerBound && triple[1] < langPredUpperBound;
  };
}

// Compute patterns and write them to `filename`. Triples where the predicate is
// in [langPredLowerBound, langPredUpperBound). `spoTriplesView` must be
// input-spoTriplesView and yield SPO-sorted triples of IDs.
void createPatternsFromSpoTriplesView(auto&& spoTriplesView,
                                      const std::string& filename,
                                      Id langPredLowerBound,
                                      Id langPredUpperBound) {
  PatternCreator patternCreator{filename};
  auto hasLanguage =
      predicateHasLanguage(langPredLowerBound, langPredUpperBound);
  for (const auto& triple : spoTriplesView) {
    if (!hasLanguage(triple)) {
      patternCreator.processTriple(triple);
    }
  }
  patternCreator.finish();
}
}  // namespace

// _____________________________________________________________________________
template <class Parser>
void Index::createFromFile(const string& filename) {
  string indexFilename = _onDiskBase + ".index";
  _configurationJson["external-literals"] = _onDiskLiterals;

  readIndexBuilderSettingsFromFile<Parser>();

  IndexBuilderDataAsPsoSorter indexBuilderData;
  if constexpr (std::is_same_v<std::decay_t<Parser>, TurtleParserAuto>) {
    if (_onlyAsciiTurtlePrefixes) {
      LOG(DEBUG) << "Using the CTRE library for tokenization" << std::endl;
      indexBuilderData =
          createIdTriplesAndVocab<TurtleParallelParser<TokenizerCtre>>(
              filename);
    } else {
      LOG(DEBUG) << "Using the Google RE2 library for tokenization"
                 << std::endl;
      indexBuilderData =
          createIdTriplesAndVocab<TurtleParallelParser<Tokenizer>>(filename);
    }

  } else {
    indexBuilderData = createIdTriplesAndVocab<Parser>(filename);
  }

  // If we have no compression, this will also copy the whole vocabulary.
  // but since we expect compression to be the default case, this  should not
  // hurt.
  string vocabFile = _onDiskBase + INTERNAL_VOCAB_SUFFIX;
  string vocabFileTmp = _onDiskBase + ".vocabularyTmp";
  std::vector<string> prefixes;
  if (_vocabPrefixCompressed) {
    // We have to use the "normally" sorted vocabulary for the prefix
    // compression.
    std::string vocabFileForPrefixCalculation =
        _onDiskBase + TMP_BASENAME_COMPRESSION + INTERNAL_VOCAB_SUFFIX;
    prefixes = calculatePrefixes(vocabFileForPrefixCalculation,
                                 NUM_COMPRESSION_PREFIXES, 1, true);
    deleteTemporaryFile(vocabFileForPrefixCalculation);
    std::ofstream prefixFile(_onDiskBase + PREFIX_FILE);
    AD_CHECK(prefixFile.is_open());
    for (const auto& prefix : prefixes) {
      prefixFile << prefix << std::endl;
    }
  }
  _configurationJson["prefixes"] = _vocabPrefixCompressed;
  LOG(INFO) << "Writing compressed vocabulary to disk ..." << std::endl;

  _vocab.buildCodebookForPrefixCompression(prefixes);
  auto wordReader = _vocab.makeUncompressedDiskIterator(vocabFile);
  auto wordWriter = _vocab.makeCompressedWordWriter(vocabFileTmp);
  for (const auto& word : wordReader) {
    wordWriter.push(word);
  }
  wordWriter.finish();

  LOG(DEBUG) << "Finished writing compressed vocabulary" << std::endl;

  // TODO<joka921> maybe move this to its own function.
  if (std::rename(vocabFileTmp.c_str(), vocabFile.c_str())) {
    LOG(INFO) << "Error: Rename the prefixed vocab file " << vocabFileTmp
              << " to " << vocabFile << " set errno to " << errno
              << ". Terminating..." << std::endl;
    AD_CHECK(false);
  }

  // Write the configuration already at this point, so we have it available in
  // case any of the permutations fail.
  writeConfiguration();

  StxxlSorter<SortBySPO> spoSorter{STXXL_MEMORY_TO_USE};
  auto& psoSorter = *indexBuilderData.psoSorter;
  // For the first permutation, perform a unique.
  auto uniqueSorter = ad_utility::uniqueView(psoSorter.sortedView());

  createPermutationPair<IndexMetaDataHmapDispatcher>(
      std::move(uniqueSorter), _PSO, _POS, spoSorter.makePushCallback());
  psoSorter.clear();

  auto hasLanguagePredicate = predicateHasLanguage(
      indexBuilderData.langPredLowerBound, indexBuilderData.langPredUpperBound);

  if (_loadAllPermutations) {
    // After the SPO permutation, create patterns if so desired.
    StxxlSorter<SortByOSP> ospSorter{STXXL_MEMORY_TO_USE};
    if (_usePatterns) {
      PatternCreator patternCreator{_onDiskBase + ".index.patterns"};
      auto pushTripleToPatterns = [&patternCreator,
                                   &hasLanguagePredicate](const auto& triple) {
        if (!hasLanguagePredicate(triple)) {
          patternCreator.processTriple(triple);
        }
      };
      createPermutationPair<IndexMetaDataMmapDispatcher>(
          spoSorter.sortedView(), _SPO, _SOP, ospSorter.makePushCallback(),
          pushTripleToPatterns);
      patternCreator.finish();
    } else {
      createPermutationPair<IndexMetaDataMmapDispatcher>(
          spoSorter.sortedView(), _SPO, _SOP, ospSorter.makePushCallback());
    }
    spoSorter.clear();

    // For the last pair of permutations we don't need a next sorter, so we have
    // no fourth argument.
    createPermutationPair<IndexMetaDataMmapDispatcher>(ospSorter.sortedView(),
                                                       _OSP, _OPS);
    _configurationJson["has-all-permutations"] = true;
  } else {
    if (_usePatterns) {
      createPatternsFromSpoTriplesView(spoSorter.sortedView(),
                                       _onDiskBase + ".index.patterns",
                                       indexBuilderData.langPredLowerBound,
                                       indexBuilderData.langPredUpperBound);
    }
    _configurationJson["has-all-permutations"] = false;
  }
  LOG(DEBUG) << "Finished writing permutations" << std::endl;

  // Dump the configuration again in case the permutations have added some
  // information.
  writeConfiguration();
  LOG(INFO) << "Index build completed" << std::endl;
}

// Explicit instantiations.
template void Index::createFromFile<TurtleStreamParser<Tokenizer>>(
    const string& filename);
template void Index::createFromFile<TurtleMmapParser<Tokenizer>>(
    const string& filename);
template void Index::createFromFile<TurtleParserAuto>(const string& filename);

// _____________________________________________________________________________
template <class Parser>
IndexBuilderDataAsStxxlVector Index::passFileForVocabulary(
    const string& filename, size_t linesPerPartial) {
  LOG(INFO) << "Processing input triples from " << filename << " ..."
            << std::endl;
  auto parser = std::make_shared<Parser>(filename);
  parser->integerOverflowBehavior() = _turtleParserIntegerOverflowBehavior;
  parser->invalidLiteralsAreSkipped() = _turtleParserSkipIllegalLiterals;
  std::unique_ptr<TripleVec> idTriples(new TripleVec());
  ad_utility::Synchronized<TripleVec::bufwriter_type> writer(*idTriples);
  bool parserExhausted = false;

  size_t i = 0;
  // already count the numbers of triples that will be used for the language
  // filter
  size_t numFiles = 0;

  // we add extra triples
  std::vector<size_t> actualPartialSizes;

  // Each of these futures corresponds to the processing and writing of one
  // batch of triples and partial vocabulary.
  std::array<std::future<void>, 3> writePartialVocabularyFuture;
  while (!parserExhausted) {
    size_t actualCurrentPartialSize = 0;

    std::vector<std::array<Id, 3>> localWriter;

    std::array<ItemMapManager, NUM_PARALLEL_ITEM_MAPS> itemArray;

    {
      auto p = ad_pipeline::setupParallelPipeline<3, NUM_PARALLEL_ITEM_MAPS>(
          _parserBatchSize,
          // when called, returns an optional to the next triple. If
          // `linesPerPartial` triples were parsed, return std::nullopt. when
          // the parser is unable to deliver triples, set parserExhausted to
          // true and return std::nullopt. this is exactly the behavior we need,
          // as a first step in the parallel Pipeline.
          ParserBatcher(parser, linesPerPartial,
                        [&]() { parserExhausted = true; }),
          // convert each triple to the internal representation (e.g. special
          // values for Numbers, externalized literals, etc.)
          [this](TurtleTriple&& t) -> LangtagAndTriple {
            return tripleToInternalRepresentation(std::move(t));
          },

          // get the Ids for the original triple and the possibly added language
          // Tag triples using the provided HashMaps via itemArray. See
          // documentation of the function for more details
          getIdMapLambdas<NUM_PARALLEL_ITEM_MAPS>(
              &itemArray, linesPerPartial, &(_vocab.getCaseComparator())));

      while (auto opt = p.getNextValue()) {
        i++;
        for (const auto& innerOpt : opt.value()) {
          if (innerOpt) {
            actualCurrentPartialSize++;
            localWriter.push_back(innerOpt.value());
          }
        }
        if (i % 100'000'000 == 0) {
          LOG(INFO) << "Input triples processed: " << i << std::endl;
        }
      }
      LOG(TIMING) << "WaitTimes for Pipeline in msecs\n";
      for (const auto& t : p.getWaitingTime()) {
        LOG(TIMING) << t << " msecs" << std::endl;
      }

      if constexpr (requires(Parser p) { p.printAndResetQueueStatistics(); }) {
        parser->printAndResetQueueStatistics();
      }
    }

    // localWriter.finish();
    // wait until sorting the last partial vocabulary has finished
    // to control the number of threads and the amount of memory used at the
    // same time. typically sorting is finished before we reach again here so
    // it is not a bottleneck.
    ad_utility::Timer sortFutureTimer;
    sortFutureTimer.start();
    if (writePartialVocabularyFuture[0].valid()) {
      writePartialVocabularyFuture[0].get();
    }
    sortFutureTimer.stop();
    LOG(TIMING)
        << "Time spent waiting for the writing of a previous vocabulary: "
        << sortFutureTimer.msecs() << "ms." << std::endl;
    std::array<ItemMap, NUM_PARALLEL_ITEM_MAPS> convertedMaps;
    for (size_t j = 0; j < NUM_PARALLEL_ITEM_MAPS; ++j) {
      convertedMaps[j] = std::move(itemArray[j]).moveMap();
    }
    auto oldItemPtr = std::make_unique<ItemMapArray>(std::move(convertedMaps));
    for (auto it = writePartialVocabularyFuture.begin() + 1;
         it < writePartialVocabularyFuture.end(); ++it) {
      *(it - 1) = std::move(*it);
    }
    writePartialVocabularyFuture[writePartialVocabularyFuture.size() - 1] =
        writeNextPartialVocabulary(i, numFiles, actualCurrentPartialSize,
                                   std::move(oldItemPtr),
                                   std::move(localWriter), &writer);
    numFiles++;
    // Save the information how many triples this partial vocabulary actually
    // deals with we will use this later for mapping from partial to global
    // ids
    actualPartialSizes.push_back(actualCurrentPartialSize);
  }
  for (auto& future : writePartialVocabularyFuture) {
    if (future.valid()) {
      future.get();
    }
  }
  writer.wlock()->finish();
  LOG(INFO) << "Done, total number of triples read: " << i
            << " [may contain duplicates]" << std::endl;
  LOG(INFO) << "Number of QLever-internal triples created: "
            << (idTriples->size() - i) << " [may contain duplicates]"
            << std::endl;

  size_t sizeInternalVocabulary = 0;
  if (_vocabPrefixCompressed) {
    LOG(INFO) << "Merging partial vocabularies in byte order "
              << "(internal only) ..." << std::endl;
    VocabularyMerger m;
    std::ofstream compressionOutfile(_onDiskBase + TMP_BASENAME_COMPRESSION +
                                     INTERNAL_VOCAB_SUFFIX);
    AD_CHECK(compressionOutfile.is_open());
    auto internalVocabularyActionCompression =
        [&compressionOutfile](const auto& word) {
          compressionOutfile << RdfEscaping::escapeNewlinesAndBackslashes(word)
                             << '\n';
        };
    m._noIdMapsAndIgnoreExternalVocab = true;
    auto mergeResult =
        m.mergeVocabulary(_onDiskBase + TMP_BASENAME_COMPRESSION, numFiles,
                          std::less<>(), internalVocabularyActionCompression);
    sizeInternalVocabulary = mergeResult._numWordsTotal;
    LOG(INFO) << "Number of words in internal vocabulary: "
              << sizeInternalVocabulary << std::endl;
  }

  LOG(INFO) << "Merging partial vocabularies in Unicode order "
            << "(internal and external) ..." << std::endl;
  const VocabularyMerger::VocMergeRes mergeRes = [&]() {
    VocabularyMerger v;
    auto sortPred = [cmp = &(_vocab.getCaseComparator())](std::string_view a,
                                                          std::string_view b) {
      return (*cmp)(a, b, decltype(_vocab)::SortLevel::TOTAL);
    };
    auto wordWriter =
        _vocab.makeUncompressingWordWriter(_onDiskBase + INTERNAL_VOCAB_SUFFIX);
    auto internalVocabularyAction = [&wordWriter](const auto& word) {
      wordWriter.push(word.data(), word.size());
    };
    return v.mergeVocabulary(_onDiskBase, numFiles, sortPred,
                             internalVocabularyAction);
  }();
  LOG(DEBUG) << "Finished merging partial vocabularies" << std::endl;
  IndexBuilderDataAsStxxlVector res;
  res.nofWords = mergeRes._numWordsTotal;
  res.langPredLowerBound = mergeRes._langPredLowerBound;
  res.langPredUpperBound = mergeRes._langPredUpperBound;
  LOG(INFO) << "Number of words in external vocabulary: "
            << res.nofWords - sizeInternalVocabulary << std::endl;

  res.idTriples = std::move(idTriples);
  res.actualPartialSizes = std::move(actualPartialSizes);

  LOG(INFO) << "Removing temporary files ..." << std::endl;
  for (size_t n = 0; n < numFiles; ++n) {
    string partialFilename =
        _onDiskBase + PARTIAL_VOCAB_FILE_NAME + std::to_string(n);
    deleteTemporaryFile(partialFilename);
    if (_vocabPrefixCompressed) {
      partialFilename = _onDiskBase + TMP_BASENAME_COMPRESSION +
                        PARTIAL_VOCAB_FILE_NAME + std::to_string(n);
      deleteTemporaryFile(partialFilename);
    }
  }

  return res;
}

// _____________________________________________________________________________
std::unique_ptr<PsoSorter> Index::convertPartialToGlobalIds(
    TripleVec& data, const vector<size_t>& actualLinesPerPartial,
    size_t linesPerPartial) {
  LOG(INFO) << "Converting triples from local IDs to global IDs ..."
            << std::endl;
  LOG(DEBUG) << "Triples per partial vocabulary: " << linesPerPartial
             << std::endl;

  // Iterate over all partial vocabularies.
  TripleVec::bufreader_type reader(data);
  auto resultPtr = std::make_unique<PsoSorter>(STXXL_MEMORY_TO_USE);
  auto& result = *resultPtr;
  size_t i = 0;
  for (size_t partialNum = 0; partialNum < actualLinesPerPartial.size();
       partialNum++) {
    std::string mmapFilename(_onDiskBase + PARTIAL_MMAP_IDS +
                             std::to_string(partialNum));
    LOG(DEBUG) << "Reading ID map from: " << mmapFilename << std::endl;
    ad_utility::HashMap<Id, Id> idMap = IdMapFromPartialIdMapFile(mmapFilename);
    // Delete the temporary file in which we stored this map
    deleteTemporaryFile(mmapFilename);

    // update the triples for which this partial vocabulary was responsible
    for (size_t tmpNum = 0; tmpNum < actualLinesPerPartial[partialNum];
         ++tmpNum) {
      std::array<Id, 3> curTriple = *reader;
      ++reader;

      // For all triple elements find their mapping from partial to global ids.
      for (size_t k = 0; k < 3; ++k) {
        auto iterator = idMap.find(curTriple[k]);
        if (iterator == idMap.end()) {
          LOG(INFO) << "Not found in partial vocabulary: " << curTriple[k]
                    << std::endl;
          AD_CHECK(false);
        }
        // TODO<joka921> at some point we have to check for out of range.
        curTriple[k] = iterator->second;
      }

      // update the Element
      result.push(curTriple);
      ++i;
      if (i % 100'000'000 == 0) {
        LOG(INFO) << "Triples converted: " << i << std::endl;
      }
    }
  }
  LOG(INFO) << "Done, total number of triples converted: " << i << std::endl;
  return resultPtr;
}

// _____________________________________________________________________________
template <class MetaDataDispatcher, typename SortedTriples>
std::optional<std::pair<typename MetaDataDispatcher::WriteType,
                        typename MetaDataDispatcher::WriteType>>
Index::createPermutationPairImpl(const string& fileName1,
                                 const string& fileName2,
                                 SortedTriples&& sortedTriples, size_t c0,
                                 size_t c1, size_t c2,
                                 auto&&... perTripleCallbacks) {
  using MetaData = typename MetaDataDispatcher::WriteType;
  MetaData metaData1, metaData2;
  if constexpr (metaData1._isMmapBased) {
    metaData1.setup(fileName1 + MMAP_FILE_SUFFIX, ad_utility::CreateTag{});
    metaData2.setup(fileName2 + MMAP_FILE_SUFFIX, ad_utility::CreateTag{});
  }

  CompressedRelationWriter writer1{ad_utility::File(fileName1, "w")};
  CompressedRelationWriter writer2{ad_utility::File(fileName2, "w")};

  // Iterate over the vector and identify "relation" boundaries, where a
  // "relation" is the sequence of sortedTriples equal first component. For PSO
  // and POS, this is a predicate (of which "relation" is a synonym).
  LOG(INFO) << "Creating a pair of index permutations ... " << std::endl;
  size_t from = 0;
  std::optional<Id> currentRel;
  ad_utility::BufferedVector<array<Id, 2>> buffer(
      THRESHOLD_RELATION_CREATION, fileName1 + ".tmp.mmap-buffer");
  bool functional = true;
  size_t distinctCol1 = 0;
  size_t sizeOfRelation = 0;
  Id lastLhs = ID_NO_VALUE;
  uint64_t totalNumTriples = 0;
  for (auto triple : sortedTriples) {
    if (!currentRel.has_value()) {
      currentRel = triple[c0];
    }
    // Call each of the `perTripleCallbacks` for the current triple
    (..., perTripleCallbacks(triple));
    ++totalNumTriples;
    if (triple[c0] != currentRel) {
      writer1.addRelation(currentRel.value(), buffer, distinctCol1, functional);
      writeSwitchedRel(&writer2, currentRel.value(), &buffer);
      for (auto& md : writer1.getFinishedMetaData()) {
        metaData1.add(md);
      }
      for (auto& md : writer2.getFinishedMetaData()) {
        metaData2.add(md);
      }
      buffer.clear();
      distinctCol1 = 1;
      currentRel = triple[c0];
      functional = true;
    } else {
      sizeOfRelation++;
      if (triple[c1] == lastLhs) {
        functional = false;
      } else {
        distinctCol1++;
      }
    }
    buffer.push_back({triple[c1], triple[c2]});
    lastLhs = triple[c1];
  }
  if (from < totalNumTriples) {
    writer1.addRelation(currentRel.value(), buffer, distinctCol1, functional);
    writeSwitchedRel(&writer2, currentRel.value(), &buffer);
  }

  writer1.finish();
  writer2.finish();
  for (auto& md : writer1.getFinishedMetaData()) {
    metaData1.add(md);
  }
  for (auto& md : writer2.getFinishedMetaData()) {
    metaData2.add(md);
  }
  metaData1.blockData() = writer1.getFinishedBlocks();
  metaData2.blockData() = writer2.getFinishedBlocks();

  return std::make_pair(std::move(metaData1), std::move(metaData2));
}

// __________________________________________________________________________
void Index::writeSwitchedRel(CompressedRelationWriter* out, Id currentRel,
                             ad_utility::BufferedVector<array<Id, 2>>* bufPtr) {
  // sort according to the "switched" relation.
  auto& buffer = *bufPtr;

  for (auto& el : buffer) {
    std::swap(el[0], el[1]);
  }
  std::sort(buffer.begin(), buffer.end(), [](const auto& a, const auto& b) {
    return a[0] == b[0] ? a[1] < b[1] : a[0] < b[0];
  });

  Id lastLhs = std::numeric_limits<Id>::max();

  bool functional = true;
  size_t distinctC1 = 0;
  for (const auto& el : buffer) {
    if (el[0] == lastLhs) {
      functional = false;
    } else {
      distinctC1++;
    }
    lastLhs = el[0];
  }

  out->addRelation(currentRel, buffer, distinctC1, functional);
}

// ________________________________________________________________________
template <class MetaDataDispatcher, class Comparator1, class Comparator2>
std::optional<std::pair<typename MetaDataDispatcher::WriteType,
                        typename MetaDataDispatcher::WriteType>>
Index::createPermutations(
    auto&& sortedTriples,
    const PermutationImpl<Comparator1, typename MetaDataDispatcher::ReadType>&
        p1,
    const PermutationImpl<Comparator2, typename MetaDataDispatcher::ReadType>&
        p2,
    auto&&... perTripleCallbacks) {
  auto metaData = createPermutationPairImpl<MetaDataDispatcher>(
      _onDiskBase + ".index" + p1._fileSuffix,
      _onDiskBase + ".index" + p2._fileSuffix, AD_FWD(sortedTriples),
      p1._keyOrder[0], p1._keyOrder[1], p1._keyOrder[2],
      AD_FWD(perTripleCallbacks)...);

  if (metaData.has_value()) {
    auto& mdv = metaData.value();
    LOG(INFO) << "Statistics for " << p1._readableName << ": "
              << mdv.first.statistics() << std::endl;
    LOG(INFO) << "Statistics for " << p2._readableName << ": "
              << mdv.second.statistics() << std::endl;
  }

  return metaData;
}

// ________________________________________________________________________
template <class MetaDataDispatcher, class Comparator1, class Comparator2>
void Index::createPermutationPair(
    auto&& sortedTriples,
    const PermutationImpl<Comparator1, typename MetaDataDispatcher::ReadType>&
        p1,
    const PermutationImpl<Comparator2, typename MetaDataDispatcher::ReadType>&
        p2,
    auto&&... perTripleCallbacks) {
  auto metaData = createPermutations<MetaDataDispatcher>(
      AD_FWD(sortedTriples), p1, p2, AD_FWD(perTripleCallbacks)...);
  if (metaData) {
    LOG(INFO) << "Exchanging multiplicities for " << p1._readableName << " and "
              << p2._readableName << " ..." << std::endl;
    exchangeMultiplicities(&(metaData.value().first),
                           &(metaData.value().second));
    LOG(INFO) << "Writing meta data for " << p1._readableName << " and "
              << p2._readableName << " ..." << std::endl;
    ad_utility::File f1(_onDiskBase + ".index" + p1._fileSuffix, "r+");
    metaData.value().first.appendToFile(&f1);
    ad_utility::File f2(_onDiskBase + ".index" + p2._fileSuffix, "r+");
    metaData.value().second.appendToFile(&f2);
  }
}

// _________________________________________________________________________
template <class MetaData>
void Index::exchangeMultiplicities(MetaData* m1, MetaData* m2) {
  for (auto it = m1->data().begin(); it != m1->data().end(); ++it) {
    // our MetaData classes have a read-only interface because normally the
    // FuullRelationMetaData are created separately and then added and never
    // changed. This function forms an exception to this pattern
    // because calculation the 2nd column multiplicity separately for each
    // permutation is inefficient. So it is fine to use const_cast here as an
    // exception: we delibarately write to a read-only data structure and are
    // knowing what we are doing
    Id id = it.getId();
    m2->data()[id].setCol2Multiplicity(m1->data()[id].getCol1Multiplicity());
    m1->data()[id].setCol2Multiplicity(m2->data()[id].getCol1Multiplicity());
  }
}

// _____________________________________________________________________________
void Index::addPatternsToExistingIndex() {
  auto [langPredLowerBound, langPredUpperBound] = _vocab.prefix_range("@");
  // We only iterate over the SPO permutation which typically only has few
  // triples per subject, so it should be safe to not apply a memory limit here.
  AD_CHECK(false);
  ad_utility::AllocatorWithLimit<Id> allocator{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          std::numeric_limits<uint64_t>::max())};
  auto iterator = TriplesView(_SPO, allocator);
  createPatternsFromSpoTriplesView(iterator, _onDiskBase + ".index.patterns",
                                   Id::make(langPredLowerBound),
                                   Id::make(langPredUpperBound));
}

// _____________________________________________________________________________
void Index::createFromOnDiskIndex(const string& onDiskBase) {
  setOnDiskBase(onDiskBase);
  readConfiguration();
  _vocab.readFromFile(
      _onDiskBase + INTERNAL_VOCAB_SUFFIX,
      _onDiskLiterals ? _onDiskBase + EXTERNAL_VOCAB_SUFFIX : "");

  _totalVocabularySize = _vocab.size() + _vocab.getExternalVocab().size();
  LOG(DEBUG) << "Number of words in internal and external vocabulary: "
             << _totalVocabularySize << std::endl;
  _PSO.loadFromDisk(_onDiskBase);
  _POS.loadFromDisk(_onDiskBase);

  if (_loadAllPermutations) {
    _OPS.loadFromDisk(_onDiskBase);
    _OSP.loadFromDisk(_onDiskBase);
    _SPO.loadFromDisk(_onDiskBase);
    _SOP.loadFromDisk(_onDiskBase);
  } else {
    LOG(INFO) << "Only the PSO and POS permutation were loaded, SPARQL queries "
                 "with predicate variables will therefore not work"
              << std::endl;
  }

  if (_usePatterns) {
    PatternCreator::readPatternsFromFile(
        _onDiskBase + ".index.patterns",
        _fullHasPredicateMultiplicityPredicates,
        _fullHasPredicateMultiplicityEntities, _fullHasPredicateSize, _patterns,
        _hasPattern);
  }
}

// _____________________________________________________________________________
void Index::throwExceptionIfNoPatterns() const {
  if (!_usePatterns) {
    AD_THROW(ad_semsearch::Exception::CHECK_FAILED,
             "The requested feature requires a loaded patterns file ("
             "do not specify the --no-patterns option for this to work)");
  }
}

// _____________________________________________________________________________
const vector<PatternID>& Index::getHasPattern() const {
  throwExceptionIfNoPatterns();
  return _hasPattern;
}

// _____________________________________________________________________________
const CompactVectorOfStrings<Id>& Index::getHasPredicate() const {
  throwExceptionIfNoPatterns();
  return _hasPredicate;
}

// _____________________________________________________________________________
const CompactVectorOfStrings<Id>& Index::getPatterns() const {
  throwExceptionIfNoPatterns();
  return _patterns;
}

// _____________________________________________________________________________
double Index::getHasPredicateMultiplicityEntities() const {
  throwExceptionIfNoPatterns();
  return _fullHasPredicateMultiplicityEntities;
}

// _____________________________________________________________________________
double Index::getHasPredicateMultiplicityPredicates() const {
  throwExceptionIfNoPatterns();
  return _fullHasPredicateMultiplicityPredicates;
}

// _____________________________________________________________________________
size_t Index::getHasPredicateFullSize() const {
  throwExceptionIfNoPatterns();
  return _fullHasPredicateSize;
}

// _____________________________________________________________________________
size_t Index::relationCardinality(const string& relationName) const {
  if (relationName == INTERNAL_TEXT_MATCH_PREDICATE) {
    return TEXT_PREDICATE_CARDINALITY_ESTIMATE;
  }
  Id relId;
  if (getId(relationName, &relId)) {
    if (this->_PSO.metaData().col0IdExists(relId)) {
      return this->_PSO.metaData().getMetaData(relId).getNofElements();
    }
  }
  return 0;
}

// _____________________________________________________________________________
size_t Index::subjectCardinality(const string& sub) const {
  Id relId;
  if (getId(sub, &relId)) {
    if (this->_SPO.metaData().col0IdExists(relId)) {
      return this->_SPO.metaData().getMetaData(relId).getNofElements();
    }
  }
  return 0;
}

// _____________________________________________________________________________
size_t Index::objectCardinality(const string& obj) const {
  Id relId;
  // TODO<joka921> other datatypes
  if (getId(obj, &relId)) {
    if (this->_OSP.metaData().col0IdExists(relId)) {
      return this->_OSP.metaData().getMetaData(relId).getNofElements();
    }
  }
  return 0;
}

// _____________________________________________________________________________
size_t Index::sizeEstimate(const string& sub, const string& pred,
                           const string& obj) const {
  // One or two of the parameters have to be empty strings.
  // This determines the permutations to use.

  // With only one nonempty string, we can get the exact count.
  // With two, we can check if the relation is functional (return 1) or not
  // where we approximate the result size by the block size.
  if (sub.size() > 0 && pred.size() == 0 && obj.size() == 0) {
    return subjectCardinality(sub);
  }
  if (sub.size() == 0 && pred.size() > 0 && obj.size() == 0) {
    return relationCardinality(pred);
  }
  if (sub.size() == 0 && pred.size() == 0 && obj.size() > 0) {
    return objectCardinality(obj);
  }
  if (sub.size() == 0 && pred.size() == 0 && obj.size() == 0) {
    return getNofTriples();
  }
  AD_THROW(ad_semsearch::Exception::CHECK_FAILED,
           "Index::sizeEsimate called with more then one of S/P/O given. "
           "This should never be the case anymore, "
           " since for such SCANs we compute the result "
           "directly and don't need an estimate anymore!");
}

// _____________________________________________________________________________
template <class T>
void Index::writeAsciiListFile(const string& filename, const T& ids) const {
  std::ofstream f(filename);

  for (size_t i = 0; i < ids.size(); ++i) {
    f << ids[i] << ' ';
  }
  f.close();
}

template void Index::writeAsciiListFile<vector<Id>>(
    const string& filename, const vector<Id>& ids) const;

template void Index::writeAsciiListFile<vector<Score>>(
    const string& filename, const vector<Score>& ids) const;

// _____________________________________________________________________________
bool Index::isLiteral(const string& object) {
  return decltype(_vocab)::isLiteral(object);
}

// _____________________________________________________________________________
bool Index::shouldBeExternalized(const string& object) {
  return _vocab.shouldBeExternalized(object);
}

// _____________________________________________________________________________
void Index::setKbName(const string& name) {
  _POS.setKbName(name);
  _PSO.setKbName(name);
  _SOP.setKbName(name);
  _SPO.setKbName(name);
  _OPS.setKbName(name);
  _OSP.setKbName(name);
}

// ____________________________________________________________________________
void Index::setOnDiskLiterals(bool onDiskLiterals) {
  _onDiskLiterals = onDiskLiterals;
}

// ____________________________________________________________________________
void Index::setOnDiskBase(const std::string& onDiskBase) {
  _onDiskBase = onDiskBase;
}

// ____________________________________________________________________________
void Index::setKeepTempFiles(bool keepTempFiles) {
  _keepTempFiles = keepTempFiles;
}

// _____________________________________________________________________________
void Index::setUsePatterns(bool usePatterns) { _usePatterns = usePatterns; }

// _____________________________________________________________________________
void Index::setLoadAllPermutations(bool loadAllPermutations) {
  _loadAllPermutations = loadAllPermutations;
}

// ____________________________________________________________________________
void Index::setSettingsFile(const std::string& filename) {
  _settingsFileName = filename;
}

// ____________________________________________________________________________
void Index::setPrefixCompression(bool compressed) {
  _vocabPrefixCompressed = compressed;
}

// ____________________________________________________________________________
void Index::writeConfiguration() const {
  std::ofstream f(_onDiskBase + CONFIGURATION_FILE);
  AD_CHECK(f.is_open());
  f << _configurationJson;
}

// ___________________________________________________________________________
void Index::readConfiguration() {
  std::ifstream f(_onDiskBase + CONFIGURATION_FILE);
  AD_CHECK(f.is_open());
  f >> _configurationJson;
  if (_configurationJson.find("external-literals") !=
      _configurationJson.end()) {
    _onDiskLiterals =
        static_cast<bool>(_configurationJson["external-literals"]);
  }

  if (_configurationJson.find("prefixes") != _configurationJson.end()) {
    if (_configurationJson["prefixes"]) {
      vector<string> prefixes;
      std::ifstream prefixFile(_onDiskBase + PREFIX_FILE);
      AD_CHECK(prefixFile.is_open());
      for (string prefix; std::getline(prefixFile, prefix);) {
        prefixes.emplace_back(std::move(prefix));
      }
      _vocab.buildCodebookForPrefixCompression(prefixes);
    } else {
      _vocab.buildCodebookForPrefixCompression(std::vector<std::string>());
    }
  }

  if (_configurationJson.find("prefixes-external") !=
      _configurationJson.end()) {
    _vocab.initializeExternalizePrefixes(
        _configurationJson["prefixes-external"]);
  }

  if (_configurationJson.count("ignore-case")) {
    LOG(ERROR) << ERROR_IGNORE_CASE_UNSUPPORTED << '\n';
    throw std::runtime_error("Deprecated key \"ignore-case\" in index build");
  }

  if (_configurationJson.count("locale")) {
    std::string lang{_configurationJson["locale"]["language"]};
    std::string country{_configurationJson["locale"]["country"]};
    bool ignorePunctuation{_configurationJson["locale"]["ignore-punctuation"]};
    _vocab.setLocale(lang, country, ignorePunctuation);
    _textVocab.setLocale(lang, country, ignorePunctuation);
  } else {
    LOG(ERROR) << "Key \"locale\" is missing in the metadata. This is probably "
                  "and old index build that is no longer supported by QLever. "
                  "Please rebuild your index\n";
    throw std::runtime_error(
        "Missing required key \"locale\" in index build's metadata");
  }

  if (_configurationJson.find("languages-internal") !=
      _configurationJson.end()) {
    _vocab.initializeInternalizedLangs(
        _configurationJson["languages-internal"]);
  }

  if (_configurationJson.find("has-all-permutations") !=
          _configurationJson.end() &&
      _configurationJson["has-all-permutations"] == false) {
    // If the permutations simply don't exist, then we can never load them.
    _loadAllPermutations = false;
  }
}

// ___________________________________________________________________________
LangtagAndTriple Index::tripleToInternalRepresentation(TurtleTriple&& triple) {
  LangtagAndTriple result{"", {}};
  auto& resultTriple = result._triple;
  resultTriple[0] = std::move(triple._subject);
  resultTriple[1] = std::move(triple._predicate);
  // TODO<joka921> As soon as we have the "folded" Ids, we simply store the
  // numeric value.
  resultTriple[2] = triple._object.toRdfLiteral();
  for (auto& el : resultTriple) {
    auto& iriOrLiteral = std::get<TripleComponent>(el)._iriOrLiteral;
    iriOrLiteral = _vocab.getLocaleManager().normalizeUtf8(iriOrLiteral);
  }
  size_t upperBound = 3;
  auto& object = std::get<TripleComponent>(resultTriple[2])._iriOrLiteral;
  // TODO<joka921> Actually create numeric Ids here...
  if (ad_utility::isXsdValue(object)) {
    object = ad_utility::convertValueLiteralToIndexWord(object);
    upperBound = 2;
  } else if (ad_utility::isNumeric(object)) {
    object = ad_utility::convertNumericToIndexWord(object);
    upperBound = 2;
  } else if (isLiteral(object)) {
    result._langtag = decltype(_vocab)::getLanguage(object);
  }

  for (size_t k = 0; k < upperBound; ++k) {
    // If we already have an ID, we can just continue;
    if (!std::holds_alternative<TripleComponent>(resultTriple[k])) {
      continue;
    }
    auto& component = std::get<TripleComponent>(resultTriple[k]);
    if (_onDiskLiterals &&
        _vocab.shouldBeExternalized(component._iriOrLiteral)) {
      component._isExternal = true;
    }
  }
  return result;
}

// ___________________________________________________________________________
template <class Parser>
void Index::readIndexBuilderSettingsFromFile() {
  json j;  // if we have no settings, we still have to initialize some default
           // values
  if (!_settingsFileName.empty()) {
    std::ifstream f(_settingsFileName);
    AD_CHECK(f.is_open());
    f >> j;
  }

  if (j.find("prefixes-external") != j.end()) {
    _vocab.initializeExternalizePrefixes(j["prefixes-external"]);
    _configurationJson["prefixes-external"] = j["prefixes-external"];
    _onDiskLiterals = true;
    _configurationJson["external-literals"] = true;
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
    _vocab.setLocale(lang, country, ignorePunctuation);
    _textVocab.setLocale(lang, country, ignorePunctuation);
    _configurationJson["locale"]["language"] = lang;
    _configurationJson["locale"]["country"] = country;
    _configurationJson["locale"]["ignore-punctuation"] = ignorePunctuation;
  }

  if (j.find("languages-internal") != j.end()) {
    _vocab.initializeInternalizedLangs(j["languages-internal"]);
    _configurationJson["languages-internal"] = j["languages-internal"];
    _onDiskLiterals = true;
    _configurationJson["external-literals"] = true;
  }
  if (j.count("ascii-prefixes-only")) {
    if constexpr (std::is_same_v<std::decay_t<Parser>, TurtleParserAuto>) {
      bool v{j["ascii-prefixes-only"]};
      if (v) {
        LOG(INFO) << WARNING_ASCII_ONLY_PREFIXES << std::endl;
        _onlyAsciiTurtlePrefixes = true;
      } else {
        _onlyAsciiTurtlePrefixes = false;
      }
    } else {
      LOG(WARN) << "You specified the ascii-prefixes-only but a parser that is "
                   "not the Turtle stream parser. This means that this setting "
                   "is ignored."
                << std::endl;
    }
  }

  if (j.count("num-triples-per-batch")) {
    _numTriplesPerBatch = size_t{j["num-triples-per-batch"]};
    LOG(INFO)
        << "You specified \"num-triples-per-batch = " << _numTriplesPerBatch
        << "\", choose a lower value if the index builder runs out of memory"
        << std::endl;
  }

  if (j.count("parser-batch-size")) {
    _parserBatchSize = size_t{j["parser-batch-size"]};
    LOG(INFO) << "Overriding setting parser-batch-size to " << _parserBatchSize
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
    auto value = j[key];
    if (value == overflowingIntegersThrow) {
      LOG(INFO) << "Integers that cannot be represented by QLever will throw "
                   "an exception"
                << std::endl;
      _turtleParserIntegerOverflowBehavior =
          TurtleParserIntegerOverflowBehavior::Error;
    } else if (value == overflowingIntegersBecomeDoubles) {
      LOG(INFO) << "Integers that cannot be represented by QLever will be "
                   "converted to doubles"
                << std::endl;
      _turtleParserIntegerOverflowBehavior =
          TurtleParserIntegerOverflowBehavior::OverflowingToDouble;
    } else if (value == allIntegersBecomeDoubles) {
      LOG(INFO) << "All integers will be converted to doubles" << std::endl;
      _turtleParserIntegerOverflowBehavior =
          TurtleParserIntegerOverflowBehavior::OverflowingToDouble;
    } else {
      AD_CHECK(std::find(allModes.begin(), allModes.end(), value) ==
               allModes.end());
      LOG(ERROR) << "Invalid value for " << key << std::endl;
      LOG(INFO) << "The currently supported values are "
                << absl::StrJoin(allModes, ",") << std::endl;
    }
  } else {
    _turtleParserIntegerOverflowBehavior =
        TurtleParserIntegerOverflowBehavior::Error;
    LOG(INFO) << "Integers that cannot be represented by QLever will throw an "
                 "exception (this is the default behavior)"
              << std::endl;
  }
}

// ___________________________________________________________________________
std::future<void> Index::writeNextPartialVocabulary(
    size_t numLines, size_t numFiles, size_t actualCurrentPartialSize,
    std::unique_ptr<ItemMapArray> items, auto localIds,
    ad_utility::Synchronized<TripleVec::bufwriter_type>* globalWritePtr) {
  LOG(DEBUG) << "Input triples read in this section: " << numLines << std::endl;
  LOG(DEBUG)
      << "Triples processed, also counting internal triples added by QLever: "
      << actualCurrentPartialSize << std::endl;
  std::future<void> resultFuture;
  string partialFilename =
      _onDiskBase + PARTIAL_VOCAB_FILE_NAME + std::to_string(numFiles);
  string partialCompressionFilename = _onDiskBase + TMP_BASENAME_COMPRESSION +
                                      PARTIAL_VOCAB_FILE_NAME +
                                      std::to_string(numFiles);

  auto lambda = [localIds = std::move(localIds), globalWritePtr,
                 items = std::move(items), vocab = &_vocab, partialFilename,
                 partialCompressionFilename, numFiles,
                 vocabPrefixCompressed = _vocabPrefixCompressed]() mutable {
    auto vec = vocabMapsToVector(std::move(items));
    const auto identicalPred = [&c = vocab->getCaseComparator()](
                                   const auto& a, const auto& b) {
      return c(a.second.m_splitVal, b.second.m_splitVal,
               decltype(_vocab)::SortLevel::TOTAL);
    };
    LOG(TIMING) << "Start sorting of vocabulary with #elements: " << vec.size()
                << std::endl;
    sortVocabVector(&vec, identicalPred, true);
    LOG(TIMING) << "Finished sorting of vocabulary" << std::endl;
    auto mapping = createInternalMapping(&vec);
    LOG(TIMING) << "Finished creating of Mapping vocabulary" << std::endl;
    auto sz = vec.size();
    // since now adjacent duplicates also have the same Ids, it suffices to
    // compare those
    vec.erase(std::unique(vec.begin(), vec.end(),
                          [](const auto& a, const auto& b) {
                            return a.second.m_id == b.second.m_id;
                          }),
              vec.end());
    LOG(TRACE) << "Removed " << sz - vec.size()
               << " Duplicates from the local partial vocabularies\n";
    // The writing to the STXXL vector has to be done in order, to
    // make the update from local to global ids work.
    globalWritePtr->withWriteLockAndOrdered(
        [&](auto& writerPtr) {
          writeMappedIdsToExtVec(localIds, mapping, &writerPtr);
        },
        numFiles);
    writePartialVocabularyToFile(vec, partialFilename);
    if (vocabPrefixCompressed) {
      // sort according to the actual byte values
      LOG(TIMING) << "Start sorting of vocabulary for prefix compression"
                  << std::endl;
      sortVocabVector(
          &vec, [](const auto& a, const auto& b) { return a.first < b.first; },
          false);
      LOG(TIMING) << "Finished sorting of vocabulary for prefix compression"
                  << std::endl;
      LOG(TIMING) << "Remove externalized words from prefix compression"
                  << std::endl;
      std::erase_if(vec, [](const auto& a) {
        return a.second.m_splitVal.isExternalized;
      });
      writePartialVocabularyToFile(vec, partialCompressionFilename);
    }
    LOG(TIMING) << "Finished writing the partial vocabulary" << std::endl;
    vec.clear();
  };

  return std::async(std::launch::async, std::move(lambda));
}
