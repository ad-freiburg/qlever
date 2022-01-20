// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./Index.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <future>
#include <optional>
#include <stxxl/algorithm>
#include <stxxl/map>
#include <unordered_map>

#include "../parser/ParallelParseBuffer.h"
#include "../parser/TsvParser.h"
#include "../util/BatchedPipeline.h"
#include "../util/CompressionUsingZstd/ZstdWrapper.h"
#include "../util/Conversions.h"
#include "../util/HashMap.h"
#include "../util/Serializer/FileSerializer.h"
#include "../util/TupleHelpers.h"
#include "./PrefixHeuristic.h"
#include "./VocabularyGenerator.h"
#include "MetaDataIterator.h"

using std::array;

const uint32_t Index::PATTERNS_FILE_VERSION = 0;

// _____________________________________________________________________________
Index::Index()
    : _usePatterns(false),
      _maxNumPatterns(std::numeric_limits<PatternID>::max() - 2) {}

// _____________________________________________________________________________________________
template <class Parser>
VocabularyData Index::createIdTriplesAndVocab(const string& ntFile) {
  auto vocabData =
      passFileForVocabulary<Parser>(ntFile, _numTriplesPerPartialVocab);
  // first save the total number of words, this is needed to initialize the
  // dense IndexMetaData variants
  _totalVocabularySize = vocabData.nofWords;
  LOG(INFO) << "total size of vocabulary (internal and external) is "
            << _totalVocabularySize << std::endl;

  if (_onDiskLiterals) {
    _vocab.externalizeLiteralsFromTextFile(
        _onDiskBase + EXTERNAL_LITS_TEXT_FILE_NAME,
        _onDiskBase + ".literals-index");
  }
  deleteTemporaryFile(_onDiskBase + EXTERNAL_LITS_TEXT_FILE_NAME);
  // clear vocabulary to save ram (only information from partial binary files
  // used from now on). This will preserve information about externalized
  // Prefixes etc.
  _vocab.clear();
  convertPartialToGlobalIds(*vocabData.idTriples, vocabData.actualPartialSizes,
                            NUM_TRIPLES_PER_PARTIAL_VOCAB);

  return vocabData;
}

// _____________________________________________________________________________
template <class Parser>
void Index::createFromFile(const string& filename) {
  string indexFilename = _onDiskBase + ".index";
  _configurationJson["external-literals"] = _onDiskLiterals;

  initializeVocabularySettingsBuild<Parser>();

  VocabularyData vocabData;
  if constexpr (std::is_same_v<std::decay_t<Parser>, TurtleParserAuto>) {
    if (_onlyAsciiTurtlePrefixes) {
      LOG(INFO) << "Using the CTRE library for Tokenization\n";
      vocabData = createIdTriplesAndVocab<TurtleParallelParser<TokenizerCtre>>(
          filename);
    } else {
      LOG(INFO) << "Using the Google Re2 library for Tokenization\n";
      vocabData =
          createIdTriplesAndVocab<TurtleParallelParser<Tokenizer>>(filename);
    }

  } else {
    vocabData = createIdTriplesAndVocab<Parser>(filename);
  }

  // If we have no compression, this will also copy the whole vocabulary.
  // but since we expect compression to be the default case, this  should not
  // hurt.
  string vocabFile = _onDiskBase + ".vocabulary";
  string vocabFileTmp = _onDiskBase + ".vocabularyTmp";
  std::vector<string> prefixes;
  if (_vocabPrefixCompressed) {
    // We have to use the "normally" sorted vocabulary for the prefix
    // compression.
    std::string vocabFileForPrefixCalculation =
        _onDiskBase + TMP_BASENAME_COMPRESSION + ".vocabulary";
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
  LOG(INFO) << "Writing compressed vocabulary to disk" << std::endl;
  decltype(_vocab)::WordWriter wordWriter{vocabFileTmp};
  auto internalVocabularyAction = [&wordWriter](const auto& word) {
    wordWriter.push(word.data(), word.size());
  };
  auto wordReader = decltype(_vocab)::makeWordDiskIterator(vocabFile);
  Vocabulary<CompressedString, TripleComponentComparator>::prefixCompressFile(
      std::move(wordReader), prefixes, internalVocabularyAction);
  LOG(INFO) << "Finished writing compressed vocabulary" << std::endl;

  // TODO<joka921> maybe move this to its own function
  if (std::rename(vocabFileTmp.c_str(), vocabFile.c_str())) {
    LOG(INFO) << "Error: Rename the prefixed vocab file " << vocabFileTmp
              << " to " << vocabFile << " set errno to " << errno
              << ". Terminating..." << std::endl;
    AD_CHECK(false);
  }

  // Write the configuration already at this point, so we have it available in
  // case any of the permutations fail.
  writeConfiguration();

  // For the first permutation, perform a unique.
  createPermutationPair<IndexMetaDataHmapDispatcher>(&vocabData, _PSO, _POS,
                                                     PerformUnique::True);

  if (_loadAllPermutations) {
    // After the SPO permutation, create patterns if so desired.
    createPermutationPair<IndexMetaDataMmapDispatcher>(
        &vocabData, _SPO, _SOP, PerformUnique::False, _usePatterns);
    createPermutationPair<IndexMetaDataMmapDispatcher>(&vocabData, _OSP, _OPS);
    _configurationJson["has-all-permutations"] = true;
  } else {
    if (_usePatterns) {
      // The first argument means that the triples are not yet sorted according
      // to SPO.
      createPatterns(false, &vocabData);
    }
    _configurationJson["has-all-permutations"] = false;
  }
  LOG(INFO) << "Finished writing permutations" << std::endl;

  // Dump the configuration again in case the permutations have added some
  // information.
  writeConfiguration();
  LOG(INFO) << "Index build completed" << std::endl;
}

// Explicit instantiations.
template void Index::createFromFile<TsvParser>(const string& filename);
template void Index::createFromFile<TurtleStreamParser<Tokenizer>>(
    const string& filename);
template void Index::createFromFile<TurtleMmapParser<Tokenizer>>(
    const string& filename);
template void Index::createFromFile<TurtleParserAuto>(const string& filename);

// _____________________________________________________________________________
template <class Parser>
VocabularyData Index::passFileForVocabulary(const string& filename,
                                            size_t linesPerPartial) {
  auto parser = std::make_shared<Parser>(filename);
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

    std::unique_ptr<TripleVec> localIdTriples(new TripleVec());
    TripleVec::bufwriter_type localWriter(*localIdTriples);

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
          [this](Triple&& t) {
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
            localWriter << innerOpt.value();
          }
        }
        if (i % 10'000'000 == 0) {
          LOG(INFO) << "Lines (from KB-file) processed: " << i << std::endl;
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

    localWriter.finish();
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
                                   std::move(localIdTriples), &writer);
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
  LOG(INFO) << "Pass done." << endl;

  if (_vocabPrefixCompressed) {
    LOG(INFO) << "Merging temporary vocabulary for prefix compression"
              << std::endl;
    {
      VocabularyMerger m;
      std::ofstream compressionOutfile(_onDiskBase + TMP_BASENAME_COMPRESSION +
                                       ".vocabulary");
      AD_CHECK(compressionOutfile.is_open());
      auto internalVocabularyActionCompression =
          [&compressionOutfile](const auto& word) {
            compressionOutfile
                << RdfEscaping::escapeNewlinesAndBackslashes(word) << '\n';
          };
      m._noIdMapsAndIgnoreExternalVocab = true;
      m.mergeVocabulary(_onDiskBase + TMP_BASENAME_COMPRESSION, numFiles,
                        std::less<>(), internalVocabularyActionCompression);
      LOG(INFO) << "Finished merging additional vocabulary" << std::endl;
    }
  }

  LOG(INFO) << "Merging vocabulary\n";
  const VocabularyMerger::VocMergeRes mergeRes = [&]() {
    VocabularyMerger v;
    auto sortPred = [cmp = &(_vocab.getCaseComparator())](std::string_view a,
                                                          std::string_view b) {
      return (*cmp)(a, b, decltype(_vocab)::SortLevel::TOTAL);
    };
    decltype(_vocab)::WordWriter wordWriter{_onDiskBase + ".vocabulary"};
    auto internalVocabularyAction = [&wordWriter](const auto& word) {
      wordWriter.push(word.data(), word.size());
    };
    return v.mergeVocabulary(_onDiskBase, numFiles, sortPred,
                             internalVocabularyAction);
  }();
  LOG(INFO) << "Finished merging vocabulary\n";
  VocabularyData res;
  res.nofWords = mergeRes._numWordsTotal;
  res.langPredLowerBound = mergeRes._langPredLowerBound;
  res.langPredUpperBound = mergeRes._langPredUpperBound;

  res.idTriples = std::move(idTriples);
  res.actualPartialSizes = std::move(actualPartialSizes);

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
void Index::convertPartialToGlobalIds(
    TripleVec& data, const vector<size_t>& actualLinesPerPartial,
    size_t linesPerPartial) {
  LOG(INFO) << "Updating Ids in stxxl vector to global Ids.\n";

  size_t i = 0;
  // iterate over all partial vocabularies
  for (size_t partialNum = 0; partialNum < actualLinesPerPartial.size();
       partialNum++) {
    std::string mmapFilename(_onDiskBase + PARTIAL_MMAP_IDS +
                             std::to_string(partialNum));
    LOG(INFO) << "Reading IdMap from " << mmapFilename << " ...\n";
    ad_utility::HashMap<Id, Id> idMap = IdMapFromPartialIdMapFile(mmapFilename);
    LOG(INFO) << "Done reading idMap\n";
    // Delete the temporary file in which we stored this map
    deleteTemporaryFile(mmapFilename);

    // update the triples for which this partial vocabulary was responsible
    for (size_t tmpNum = 0; tmpNum < actualLinesPerPartial[partialNum];
         ++tmpNum) {
      std::array<Id, 3> curTriple = data[i];

      // for all triple elements find their mapping from partial to global ids
      ad_utility::HashMap<Id, Id>::iterator iterators[3];
      for (size_t k = 0; k < 3; ++k) {
        iterators[k] = idMap.find(curTriple[k]);
        if (iterators[k] == idMap.end()) {
          LOG(INFO) << "not found in partial Vocab: " << curTriple[k] << '\n';
          AD_CHECK(false);
        }
      }

      // update the Element
      data[i] = array<Id, 3>{
          {iterators[0]->second, iterators[1]->second, iterators[2]->second}};

      ++i;
      if (i % 100'000'000 == 0) {
        LOG(INFO) << "Lines processed: " << i << '\n';
      }
    }
    LOG(INFO) << "Lines processed: " << i << '\n';
    LOG(DEBUG)
        << "Corresponding number of statements in original knowledge base: "
        << linesPerPartial * (partialNum + 1) << '\n';
  }
  LOG(INFO) << "Pass done\n";
}

// _____________________________________________________________________________
template <class MetaDataDispatcher>
std::optional<std::pair<typename MetaDataDispatcher::WriteType,
                        typename MetaDataDispatcher::WriteType>>
Index::createPermutationPairImpl(const string& fileName1,
                                 const string& fileName2,
                                 const Index::TripleVec& vec, size_t c0,
                                 size_t c1, size_t c2) {
  using MetaData = typename MetaDataDispatcher::WriteType;
  MetaData metaData1, metaData2;
  if constexpr (metaData1._isMmapBased) {
    metaData1.setup(_totalVocabularySize,
                    CompressedRelationMetaData::emptyMetaData(),
                    fileName1 + MMAP_FILE_SUFFIX);
    metaData2.setup(_totalVocabularySize,
                    CompressedRelationMetaData::emptyMetaData(),
                    fileName2 + MMAP_FILE_SUFFIX);
  }

  if (vec.size() == 0) {
    LOG(WARN) << "Attempt to write an empty index!" << std::endl;
    return std::nullopt;
  }

  CompressedRelationWriter writer1{ad_utility::File(fileName1, "w")};
  CompressedRelationWriter writer2{ad_utility::File(fileName2, "w")};

  LOG(INFO) << "Creating a pair of on-disk index permutation of " << vec.size()
            << " elements / facts." << std::endl;
  // Iterate over the vector and identify relation boundaries
  size_t from = 0;
  Id currentRel = vec[0][c0];
  ad_utility::BufferedVector<array<Id, 2>> buffer(
      THRESHOLD_RELATION_CREATION, fileName1 + ".tmp.MmapBuffer");
  bool functional = true;
  size_t distinctCol1 = 0;
  size_t sizeOfRelation = 0;
  Id lastLhs = std::numeric_limits<Id>::max();
  for (TripleVec::bufreader_type reader(vec); !reader.empty(); ++reader) {
    if ((*reader)[c0] != currentRel) {
      writer1.addRelation(currentRel, buffer, distinctCol1, functional);
      writeSwitchedRel(&writer2, currentRel, &buffer);
      for (auto& md : writer1.getFinishedMetaData()) {
        metaData1.add(md);
      }
      for (auto& md : writer2.getFinishedMetaData()) {
        metaData2.add(md);
      }
      buffer.clear();
      distinctCol1 = 1;
      currentRel = (*reader)[c0];
      functional = true;
    } else {
      sizeOfRelation++;
      if ((*reader)[c1] == lastLhs) {
        functional = false;
      } else {
        distinctCol1++;
      }
    }
    buffer.push_back(array<Id, 2>{{(*reader)[c1], (*reader)[c2]}});
    lastLhs = (*reader)[c1];
  }
  if (from < vec.size()) {
    writer1.addRelation(currentRel, buffer, distinctCol1, functional);
    writeSwitchedRel(&writer2, currentRel, &buffer);
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

  LOG(INFO) << "Done creating index permutation." << std::endl;
  LOG(INFO) << "Calculating statistics for these permutation.\n";
  // metaData1.calculateExpensiveStatistics();
  // metaData2.calculateExpensiveStatistics();
  LOG(INFO) << "Writing statistics for this permutation:\n"
            << metaData1.statistics() << std::endl;
  LOG(INFO) << "Writing statistics for this permutation:\n"
            << metaData2.statistics() << std::endl;

  LOG(INFO) << "Permutation done." << std::endl;
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
    TripleVec* vec,
    const PermutationImpl<Comparator1, typename MetaDataDispatcher::ReadType>&
        p1,
    const PermutationImpl<Comparator2, typename MetaDataDispatcher::ReadType>&
        p2,
    PerformUnique performUnique) {
  LOG(INFO) << "Sorting for " << p1._readableName << " permutation"
            << std::endl;
  stxxl::sort(begin(*vec), end(*vec), p1._comp, STXXL_MEMORY_TO_USE);
  LOG(INFO) << "Sort done." << std::endl;

  if (performUnique == PerformUnique::True) {
    // this only has to be done for the first permutation (PSO)
    LOG(INFO) << "Removing duplicate triples as these are not supported in RDF"
              << std::endl;
    LOG(INFO) << "Size before: " << vec->size() << std::endl;
    auto last = std::unique(begin(*vec), end(*vec));
    vec->resize(size_t(last - vec->begin()));
    LOG(INFO) << "Done: unique." << std::endl;
    LOG(INFO) << "Size after: " << vec->size() << std::endl;
  }

  return createPermutationPairImpl<MetaDataDispatcher>(
      _onDiskBase + ".index" + p1._fileSuffix,
      _onDiskBase + ".index" + p2._fileSuffix, *vec, p1._keyOrder[0],
      p1._keyOrder[1], p1._keyOrder[2]);
}

// ________________________________________________________________________
template <class MetaDataDispatcher, class Comparator1, class Comparator2>
void Index::createPermutationPair(
    VocabularyData* vocabularyData,
    const PermutationImpl<Comparator1, typename MetaDataDispatcher::ReadType>&
        p1,
    const PermutationImpl<Comparator2, typename MetaDataDispatcher::ReadType>&
        p2,
    PerformUnique performUnique, bool createPatternsAfterFirst) {
  auto metaData = createPermutations<MetaDataDispatcher>(
      &(*vocabularyData->idTriples), p1, p2, performUnique);
  if (createPatternsAfterFirst) {
    // the second permutation does not alter the original triple vector,
    // so this does still work.
    createPatterns(true, vocabularyData);
  }
  if (metaData) {
    LOG(INFO) << "Exchanging Multiplicities for " << p1._readableName << " and "
              << p2._readableName << '\n';
    exchangeMultiplicities(&(metaData.value().first),
                           &(metaData.value().second));
    LOG(INFO) << "Done" << '\n';
    LOG(INFO) << "Writing MetaData for " << p1._readableName << " and "
              << p2._readableName << std::endl;
    ad_utility::File f1(_onDiskBase + ".index" + p1._fileSuffix, "r+");
    metaData.value().first.appendToFile(&f1);
    ad_utility::File f2(_onDiskBase + ".index" + p2._fileSuffix, "r+");
    metaData.value().second.appendToFile(&f2);
    LOG(INFO) << "Done" << std::endl;
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
    m2->data()[it->first].setCol2Multiplicity(
        m1->data()[it->first].getCol1Multiplicity());
    m1->data()[it->first].setCol2Multiplicity(
        m2->data()[it->first].getCol1Multiplicity());
  }
}

// _____________________________________________________________________________
void Index::addPatternsToExistingIndex() {
  auto [langPredLowerBound, langPredUpperBound] = _vocab.prefix_range("@");
  createPatternsImpl<MetaDataIterator<Permutation::SPO_T>, Permutation::SPO_T>(
      _onDiskBase + ".index.patterns", _hasPredicate, _hasPattern, _patterns,
      _fullHasPredicateMultiplicityEntities,
      _fullHasPredicateMultiplicityPredicates, _fullHasPredicateSize,
      _maxNumPatterns, langPredLowerBound, langPredUpperBound, _SPO);
}

// _____________________________________________________________________________
void Index::createPatterns(bool isSortedSPO, VocabularyData* vocabData) {
  // The first argument means that the triples are not yet sorted according
  // to SPO.
  if (isSortedSPO) {
    LOG(INFO) << "Triples are already sorted by SPO for pattern creation."
              << std::endl;
  } else {
    LOG(INFO) << "Sorting triples by SPO for pattern creation..." << std::endl;
    stxxl::sort(begin(*vocabData->idTriples), end(*vocabData->idTriples),
                SortBySPO(), STXXL_MEMORY_TO_USE);
    LOG(INFO) << "Sort done." << std::endl;
  }
  createPatternsImpl<TripleVec::bufreader_type>(
      _onDiskBase + ".index.patterns", _hasPredicate, _hasPattern, _patterns,
      _fullHasPredicateMultiplicityEntities,
      _fullHasPredicateMultiplicityPredicates, _fullHasPredicateSize,
      _maxNumPatterns, vocabData->langPredLowerBound,
      vocabData->langPredUpperBound, *vocabData->idTriples);
}

// _____________________________________________________________________________
template <typename VecReaderType, typename... Args>
void Index::createPatternsImpl(
    const string& fileName, CompactVectorOfStrings<Id>& hasPredicate,
    std::vector<PatternID>& hasPattern, CompactVectorOfStrings<Id>& patterns,
    double& fullHasPredicateMultiplicityEntities,
    double& fullHasPredicateMultiplicityPredicates,
    size_t& fullHasPredicateSize, const size_t maxNumPatterns,
    const Id langPredLowerBound, const Id langPredUpperBound,
    const Args&... vecReaderArgs) {
  IndexMetaDataHmap meta;
  using PatternsCountMap = ad_utility::HashMap<Pattern, size_t>;

  LOG(INFO) << "Creating patterns file..." << std::endl;
  VecReaderType reader(vecReaderArgs...);
  if (reader.empty()) {
    LOG(WARN) << "Triple vector was empty, no patterns created" << std::endl;
    return;
  }

  PatternsCountMap patternCounts;
  // determine the most common patterns
  Pattern pattern;

  size_t numValidPatterns = 0;
  Id currentSubj = (*reader)[0];

  for (; !reader.empty(); ++reader) {
    auto triple = *reader;
    if (triple[0] != currentSubj) {
      currentSubj = triple[0];
      numValidPatterns++;
      patternCounts[pattern]++;
      pattern.clear();
    }
    // don't list predicates twice
    if (pattern.empty() || pattern.back() != triple[1]) {
      // Ignore @..@ type language predicates
      if (triple[1] < langPredLowerBound || triple[1] >= langPredUpperBound) {
        pattern.push_back(triple[1]);
      }
    }
  }
  // process the last entry
  patternCounts[pattern]++;
  LOG(INFO) << "Counted patterns and found " << patternCounts.size()
            << " distinct patterns." << std::endl;
  LOG(INFO) << "Patterns were found for " << numValidPatterns << " entities."
            << std::endl;

  // stores patterns sorted by their number of occurences
  size_t actualNumPatterns = patternCounts.size() < maxNumPatterns
                                 ? patternCounts.size()
                                 : maxNumPatterns;
  LOG(INFO) << "Using " << actualNumPatterns << " of the "
            << patternCounts.size() << " patterns that were found in the data."
            << std::endl;
  std::vector<std::pair<Pattern, size_t>> sortedPatterns;
  sortedPatterns.reserve(actualNumPatterns);
  auto comparePatternCounts =
      [](const std::pair<Pattern, size_t>& first,
         const std::pair<Pattern, size_t>& second) -> bool {
    return first.second > second.second ||
           (first.second == second.second && first.first > second.first);
  };
  for (auto& it : patternCounts) {
    if (sortedPatterns.size() < maxNumPatterns) {
      sortedPatterns.push_back(it);
      if (sortedPatterns.size() == maxNumPatterns) {
        LOG(DEBUG) << "Sorting patterns after initial insertions." << std::endl;
        // actuall sort the sorted patterns
        std::sort(sortedPatterns.begin(), sortedPatterns.end(),
                  comparePatternCounts);
      }
    } else {
      if (comparePatternCounts(it, sortedPatterns.back())) {
        // The new element is larger than the smallest element in the vector.
        // Insert it into the correct position in the vector using binary
        // search.
        sortedPatterns.pop_back();
        auto sortedIt =
            std::lower_bound(sortedPatterns.begin(), sortedPatterns.end(), it,
                             comparePatternCounts);
        sortedPatterns.insert(sortedIt, it);
      }
    }
  }
  if (sortedPatterns.size() < maxNumPatterns) {
    LOG(DEBUG) << "Sorting patterns after all insertions." << std::endl;
    // actuall sort the sorted patterns
    std::sort(sortedPatterns.begin(), sortedPatterns.end(),
              comparePatternCounts);
  }

  LOG(DEBUG) << "Number of sorted patterns: " << sortedPatterns.size()
             << std::endl;

  // store the actual patterns
  std::vector<std::vector<Id>> buffer;
  buffer.reserve(sortedPatterns.size());
  for (const auto& p : sortedPatterns) {
    buffer.push_back(p.first._data);
  }
  patterns.build(buffer);

  std::unordered_map<Pattern, Id> patternSet;
  patternSet.reserve(sortedPatterns.size());
  for (size_t i = 0; i < sortedPatterns.size(); i++) {
    patternSet.insert(std::pair<Pattern, Id>(sortedPatterns[i].first, i));
  }

  LOG(DEBUG) << "Pattern set size: " << patternSet.size() << std::endl;

  // Associate entities with patterns if possible, store has-relation otherwise
  ad_utility::MmapVectorTmp<std::array<Id, 2>> entityHasPattern(
      fileName + ".mmap.entityHasPattern.tmp");
  ad_utility::MmapVectorTmp<std::array<Id, 2>> entityHasPredicate(
      fileName + ".mmap.entityHasPredicate.tmp");

  size_t numEntitiesWithPatterns = 0;
  size_t numEntitiesWithoutPatterns = 0;
  size_t numInvalidEntities = 0;

  // store how many entries there are in the full has-relation relation (after
  // resolving all patterns) and how may distinct elements there are (for the
  // multiplicity;
  fullHasPredicateSize = 0;
  size_t fullHasPredicateEntitiesDistinctSize = 0;
  size_t fullHasPredicatePredicatesDistinctSize = 0;
  // This vector stores if the pattern was already counted toward the distinct
  // has relation predicates size.
  std::vector<bool> haveCountedPattern(patternSet.size());

  // the input triple list is in spo order, we only need a hash map for
  // predicates
  ad_utility::HashSet<Id> predicateHashSet;

  pattern.clear();
  currentSubj = (*VecReaderType(vecReaderArgs...))[0];
  size_t patternIndex = 0;
  // Create the has-relation and has-pattern predicates
  for (VecReaderType reader2(vecReaderArgs...); !reader2.empty(); ++reader2) {
    auto triple = *reader2;
    if (triple[0] != currentSubj) {
      // we have arrived at a new entity;
      fullHasPredicateEntitiesDistinctSize++;
      auto it = patternSet.find(pattern);
      // increase the haspredicate size here as every predicate is only
      // listed once per entity (otherwise it woul always be the same as
      // vec.size()
      fullHasPredicateSize += pattern.size();
      if (it == patternSet.end()) {
        numEntitiesWithoutPatterns++;
        // The pattern does not exist, use the has-relation predicate instead
        for (size_t i = 0; i < patternIndex; i++) {
          if (predicateHashSet.find(pattern[i]) == predicateHashSet.end()) {
            predicateHashSet.insert(pattern[i]);
            fullHasPredicatePredicatesDistinctSize++;
          }
          entityHasPredicate.push_back(
              std::array<Id, 2>{currentSubj, pattern[i]});
        }
      } else {
        numEntitiesWithPatterns++;
        // The pattern does exist, add an entry to the has-pattern predicate
        entityHasPattern.push_back(std::array<Id, 2>{currentSubj, it->second});
        if (!haveCountedPattern[it->second]) {
          haveCountedPattern[it->second] = true;
          // iterate over the pattern once to
          for (size_t i = 0; i < patternIndex; i++) {
            if (predicateHashSet.find(pattern[i]) == predicateHashSet.end()) {
              predicateHashSet.insert(pattern[i]);
              fullHasPredicatePredicatesDistinctSize++;
            }
          }
        }
      }
      pattern.clear();
      currentSubj = triple[0];
      patternIndex = 0;
    }
    // don't list predicates twice
    if (patternIndex == 0 || pattern[patternIndex - 1] != (triple[1])) {
      // Ignore @..@ type language predicates
      if (triple[1] < langPredLowerBound || triple[1] >= langPredUpperBound) {
        pattern.push_back(triple[1]);
        patternIndex++;
      }
    }
  }
  // process the last element
  fullHasPredicateSize += pattern.size();
  fullHasPredicateEntitiesDistinctSize++;
  auto last = patternSet.find(pattern);
  if (last == patternSet.end()) {
    numEntitiesWithoutPatterns++;
    // The pattern does not exist, use the has-relation predicate instead
    for (size_t i = 0; i < patternIndex; i++) {
      entityHasPredicate.push_back(std::array<Id, 2>{currentSubj, pattern[i]});
      if (predicateHashSet.find(pattern[i]) == predicateHashSet.end()) {
        predicateHashSet.insert(pattern[i]);
        fullHasPredicatePredicatesDistinctSize++;
      }
    }
  } else {
    numEntitiesWithPatterns++;
    // The pattern does exist, add an entry to the has-pattern predicate
    entityHasPattern.push_back(std::array<Id, 2>{currentSubj, last->second});
    for (size_t i = 0; i < patternIndex; i++) {
      if (predicateHashSet.find(pattern[i]) == predicateHashSet.end()) {
        predicateHashSet.insert(pattern[i]);
        fullHasPredicatePredicatesDistinctSize++;
      }
    }
  }

  fullHasPredicateMultiplicityEntities =
      fullHasPredicateSize /
      static_cast<double>(fullHasPredicateEntitiesDistinctSize);
  fullHasPredicateMultiplicityPredicates =
      fullHasPredicateSize /
      static_cast<double>(fullHasPredicatePredicatesDistinctSize);

  LOG(DEBUG) << "Number of entity-has-pattern entries: "
             << entityHasPattern.size() << std::endl;
  LOG(DEBUG) << "Number of entity-has-relation entries: "
             << entityHasPredicate.size() << std::endl;

  LOG(INFO) << "Found " << patterns.size() << " distinct patterns."
            << std::endl;
  LOG(INFO) << numEntitiesWithPatterns
            << " of the databases entities have been assigned a pattern."
            << std::endl;
  LOG(INFO) << numEntitiesWithoutPatterns
            << " of the databases entities have not been assigned a pattern."
            << std::endl;
  LOG(INFO) << "Of these " << numInvalidEntities
            << " would have to large a pattern." << std::endl;

  LOG(DEBUG) << "Total number of entities: "
             << (numEntitiesWithoutPatterns + numEntitiesWithPatterns)
             << std::endl;

  LOG(DEBUG) << "Full has relation size: " << fullHasPredicateSize << std::endl;
  LOG(DEBUG) << "Full has relation entity multiplicity: "
             << fullHasPredicateMultiplicityEntities << std::endl;
  LOG(DEBUG) << "Full has relation predicate multiplicity: "
             << fullHasPredicateMultiplicityPredicates << std::endl;

  // Store all data in the file
  ad_utility::File file(fileName, "w");

  // Write a byte of ones to make it less likely that an unversioned file is
  // read as a versioned one (unversioned files begin with the id of the lowest
  // entity that has a pattern).
  // Then write the version, both multiplicitires and the full size.
  unsigned char firstByte = 255;
  file.write(&firstByte, sizeof(char));
  file.write(&PATTERNS_FILE_VERSION, sizeof(uint32_t));
  file.write(&fullHasPredicateMultiplicityEntities, sizeof(double));
  file.write(&fullHasPredicateMultiplicityPredicates, sizeof(double));
  file.write(&fullHasPredicateSize, sizeof(size_t));

  // write the entityHasPatterns vector
  size_t numHasPatterns = entityHasPattern.size();
  file.write(&numHasPatterns, sizeof(size_t));
  file.write(entityHasPattern.data(), sizeof(Id) * numHasPatterns * 2);

  // write the entityHasPredicate vector
  size_t numHasPredicatess = entityHasPredicate.size();
  file.write(&numHasPredicatess, sizeof(size_t));
  file.write(entityHasPredicate.data(), sizeof(Id) * numHasPredicatess * 2);

  // write the patterns
  // TODO<joka921> Also use the serializer interface for the patterns.
  ad_utility::serialization::FileWriteSerializer patternWriter{std::move(file)};
  patternWriter << patterns;

  LOG(INFO) << "Done creating patterns file." << std::endl;

  // create the has-relation and has-pattern lookup vectors
  if (entityHasPattern.size() > 0) {
    hasPattern.resize(entityHasPattern.back()[0] + 1);
    size_t pos = 0;
    for (size_t i = 0; i < entityHasPattern.size(); i++) {
      while (entityHasPattern[i][0] > pos) {
        hasPattern[pos] = NO_PATTERN;
        pos++;
      }
      hasPattern[pos] = entityHasPattern[i][1];
      pos++;
    }
  }

  vector<vector<Id>> hasPredicateTmp;
  if (entityHasPredicate.size() > 0) {
    hasPredicateTmp.resize(entityHasPredicate.back()[0] + 1);
    size_t pos = 0;
    for (size_t i = 0; i < entityHasPredicate.size(); i++) {
      Id current = entityHasPredicate[i][0];
      while (current > pos) {
        pos++;
      }
      while (i < entityHasPredicate.size() &&
             entityHasPredicate[i][0] == current) {
        hasPredicateTmp.back().push_back(entityHasPredicate[i][1]);
        i++;
      }
      pos++;
    }
  }
  hasPredicate.build(hasPredicateTmp);
}

// _____________________________________________________________________________
void Index::createFromOnDiskIndex(const string& onDiskBase) {
  setOnDiskBase(onDiskBase);
  readConfiguration();
  _vocab.readFromFile(_onDiskBase + ".vocabulary",
                      _onDiskLiterals ? _onDiskBase + ".literals-index" : "");

  _totalVocabularySize = _vocab.size() + _vocab.getExternalVocab().size();
  LOG(INFO) << "total vocab size is " << _totalVocabularySize << std::endl;
  _PSO.loadFromDisk(_onDiskBase);
  _POS.loadFromDisk(_onDiskBase);

  if (_loadAllPermutations) {
    _OPS.loadFromDisk(_onDiskBase);
    _OSP.loadFromDisk(_onDiskBase);
    _SPO.loadFromDisk(_onDiskBase);
    _SOP.loadFromDisk(_onDiskBase);
  } else {
    LOG(INFO)
        << "Only the PSO and POS permutation were loaded. Queries that contain "
           "predicate variables will therefore not work on this QLever "
           "instance."
        << std::endl;
  }

  if (_usePatterns) {
    // Read the pattern info from the patterns file
    std::string patternsFilePath = _onDiskBase + ".index.patterns";
    ad_utility::File patternsFile;
    patternsFile.open(patternsFilePath, "r");
    AD_CHECK(patternsFile.isOpen());
    off_t off = 0;
    unsigned char firstByte;
    patternsFile.read(&firstByte, sizeof(char), off);
    off++;
    uint32_t version;
    patternsFile.read(&version, sizeof(uint32_t), off);
    off += sizeof(uint32_t);
    if (version != PATTERNS_FILE_VERSION || firstByte != 255) {
      version = firstByte == 255 ? version : -1;
      _usePatterns = false;
      patternsFile.close();
      std::ostringstream oss;
      oss << "The patterns file " << patternsFilePath << " version of "
          << version << " does not match the programs pattern file "
          << "version of " << PATTERNS_FILE_VERSION << ". Rebuild the index"
          << " or start the query engine without pattern support." << std::endl;
      throw std::runtime_error(oss.str());
    } else {
      patternsFile.read(&_fullHasPredicateMultiplicityEntities, sizeof(double),
                        off);
      off += sizeof(double);
      patternsFile.read(&_fullHasPredicateMultiplicityPredicates,
                        sizeof(double), off);
      off += sizeof(double);
      patternsFile.read(&_fullHasPredicateSize, sizeof(size_t), off);
      off += sizeof(size_t);

      // read the entity has patterns vector
      size_t hasPatternSize;
      patternsFile.read(&hasPatternSize, sizeof(size_t), off);
      off += sizeof(size_t);
      std::vector<array<Id, 2>> entityHasPattern(hasPatternSize);
      patternsFile.read(entityHasPattern.data(),
                        hasPatternSize * sizeof(Id) * 2, off);
      off += hasPatternSize * sizeof(Id) * 2;

      // read the entity has relation vector
      size_t hasPredicateSize;
      patternsFile.read(&hasPredicateSize, sizeof(size_t), off);
      off += sizeof(size_t);
      std::vector<array<Id, 2>> entityHasPredicate(hasPredicateSize);
      patternsFile.read(entityHasPredicate.data(),
                        hasPredicateSize * sizeof(Id) * 2, off);
      off += hasPredicateSize * sizeof(Id) * 2;

      // read the patterns
      // TODO<joka921> Refactor the rest of the patterns into  the serializer
      // interface.
      patternsFile.seek(off, SEEK_SET);
      ad_utility::serialization::FileReadSerializer patternLoader{
          std::move(patternsFile)};
      patternLoader >> _patterns;

      // create the has-relation and has-pattern lookup vectors
      if (entityHasPattern.size() > 0) {
        _hasPattern.resize(entityHasPattern.back()[0] + 1);
        size_t pos = 0;
        for (size_t i = 0; i < entityHasPattern.size(); i++) {
          while (entityHasPattern[i][0] > pos) {
            _hasPattern[pos] = NO_PATTERN;
            pos++;
          }
          _hasPattern[pos] = entityHasPattern[i][1];
          pos++;
        }
      }

      vector<vector<Id>> hasPredicateTmp;
      if (entityHasPredicate.size() > 0) {
        hasPredicateTmp.resize(entityHasPredicate.back()[0] + 1);
        size_t pos = 0;
        for (size_t i = 0; i < entityHasPredicate.size(); i++) {
          Id current = entityHasPredicate[i][0];
          while (current > pos) {
            pos++;
          }
          while (i < entityHasPredicate.size() &&
                 entityHasPredicate[i][0] == current) {
            hasPredicateTmp.back().push_back(entityHasPredicate[i][1]);
            i++;
          }
          pos++;
        }
      }
      _hasPredicate.build(hasPredicateTmp);
    }
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
  if (_vocab.getId(relationName, &relId)) {
    if (this->_PSO.metaData().col0IdExists(relId)) {
      return this->_PSO.metaData().getMetaData(relId).getNofElements();
    }
  }
  return 0;
}

// _____________________________________________________________________________
size_t Index::subjectCardinality(const string& sub) const {
  Id relId;
  if (_vocab.getId(sub, &relId)) {
    if (this->_SPO.metaData().col0IdExists(relId)) {
      return this->_SPO.metaData().getMetaData(relId).getNofElements();
    }
  }
  return 0;
}

// _____________________________________________________________________________
size_t Index::objectCardinality(const string& obj) const {
  Id relId;
  if (_vocab.getId(obj, &relId)) {
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
      _vocab.initializePrefixes(prefixes);
    } else {
      _vocab.initializePrefixes(std::vector<std::string>());
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
LangtagAndTriple Index::tripleToInternalRepresentation(Triple&& tripleIn) {
  LangtagAndTriple res{"", std::move(tripleIn)};
  auto& spo = res._triple;
  for (auto& el : spo) {
    el = _vocab.getLocaleManager().normalizeUtf8(el);
  }
  size_t upperBound = 3;
  if (ad_utility::isXsdValue(spo[2])) {
    spo[2] = ad_utility::convertValueLiteralToIndexWord(spo[2]);
    upperBound = 2;
  } else if (ad_utility::isNumeric(spo[2])) {
    spo[2] = ad_utility::convertNumericToIndexWord(spo[2]);
    upperBound = 2;
  } else if (isLiteral(spo[2])) {
    res._langtag = decltype(_vocab)::getLanguage(spo[2]);
  }

  for (size_t k = 0; k < upperBound; ++k) {
    if (_onDiskLiterals && _vocab.shouldBeExternalized(spo[k])) {
      if (isLiteral(spo[k])) {
        spo[k][0] = EXTERNALIZED_LITERALS_PREFIX_CHAR;
      } else {
        AD_CHECK(spo[k][0] == '<');
        spo[k][0] = EXTERNALIZED_ENTITIES_PREFIX_CHAR;
      }
    }
  }
  return res;
}

// ___________________________________________________________________________
template <class Parser>
void Index::initializeVocabularySettingsBuild() {
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
      LOG(INFO) << "locale was not specified by the settings JSON, defaulting "
                   "to en US\n";
    }
    LOG(INFO) << "Using Locale " << lang << " " << country
              << " with ignore-punctuation: " << ignorePunctuation << '\n';

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

  if (j.count("num-triples-per-partial-vocab")) {
    _numTriplesPerPartialVocab = size_t{j["num-triples-per-partial-vocab"]};
    LOG(INFO) << "Overriding setting num-triples-per-partial-vocab to "
              << _numTriplesPerPartialVocab
              << " This might influence performance / memory usage during "
                 "index build."
              << std::endl;
  }

  if (j.count("parser-batch-size")) {
    _parserBatchSize = size_t{j["parser-batch-size"]};
    LOG(INFO) << "Overriding setting parser-batch-size to " << _parserBatchSize
              << " This might influence performance during index build."
              << std::endl;
  }
}

// ___________________________________________________________________________
std::future<void> Index::writeNextPartialVocabulary(
    size_t numLines, size_t numFiles, size_t actualCurrentPartialSize,
    std::unique_ptr<ItemMapArray> items, std::unique_ptr<TripleVec> localIds,
    ad_utility::Synchronized<TripleVec::bufwriter_type>* globalWritePtr) {
  LOG(INFO) << "Lines (from KB-file) processed: " << numLines << '\n';
  LOG(INFO) << "Actual number of Triples in this section (include "
               "langfilter triples): "
            << actualCurrentPartialSize << '\n';
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
          writeMappedIdsToExtVec(*localIds, mapping, &writerPtr);
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
      writePartialVocabularyToFile(vec, partialCompressionFilename);
    }
    LOG(TIMING) << "Finished writing the partial vocabulary" << std::endl;
    vec.clear();
  };

  return std::async(std::launch::async, std::move(lambda));
}
