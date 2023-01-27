// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors:
//   2014-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include "./IndexImpl.h"

#include <CompilationInfo.h>
#include <absl/strings/str_join.h>
#include <index/PrefixHeuristic.h>
#include <index/TriplesView.h>
#include <index/VocabularyGenerator.h>
#include <parser/ParallelParseBuffer.h>
#include <util/BatchedPipeline.h>
#include <util/CompressionUsingZstd/ZstdWrapper.h>
#include <util/HashMap.h>
#include <util/Serializer/FileSerializer.h>
#include <util/TupleHelpers.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <future>
#include <optional>
#include <stxxl/algorithm>
#include <stxxl/map>
#include <unordered_map>

using std::array;

// _____________________________________________________________________________
IndexImpl::IndexImpl() : _usePatterns(false) {}

// _____________________________________________________________________________
template <class Parser>
IndexBuilderDataAsPsoSorter IndexImpl::createIdTriplesAndVocab(
    const string& ntFile) {
  auto indexBuilderData =
      passFileForVocabulary<Parser>(ntFile, _numTriplesPerBatch);
  // first save the total number of words, this is needed to initialize the
  // dense IndexMetaData variants
  _totalVocabularySize = indexBuilderData.vocabularyMetaData_.numWordsTotal_;
  LOG(DEBUG) << "Number of words in internal and external vocabulary: "
             << _totalVocabularySize << std::endl;

  LOG(INFO) << "Converting external vocabulary to binary format ..."
            << std::endl;
  _vocab.externalizeLiteralsFromTextFile(
      _onDiskBase + EXTERNAL_LITS_TEXT_FILE_NAME,
      _onDiskBase + EXTERNAL_VOCAB_SUFFIX);
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

// Compute patterns and write them to `filename`. Triples where the predicate is
// in [langPredLowerBound, langPredUpperBound). `spoTriplesView` must be
// input-spoTriplesView and yield SPO-sorted triples of IDs.
void createPatternsFromSpoTriplesView(auto&& spoTriplesView,
                                      const std::string& filename,
                                      auto&& isInternalId) {
  PatternCreator patternCreator{filename};
  for (const auto& triple : spoTriplesView) {
    if (!std::ranges::any_of(triple, isInternalId)) {
      patternCreator.processTriple(triple);
    }
  }
  patternCreator.finish();
}

// _____________________________________________________________________________
template <class Parser>
void IndexImpl::createFromFile(const string& filename) {
  string indexFilename = _onDiskBase + ".index";

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
    auto prefixFile = ad_utility::makeOfstream(_onDiskBase + PREFIX_FILE);
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
    AD_FAIL();
  }

  // Write the configuration already at this point, so we have it available in
  // case any of the permutations fail.
  writeConfiguration();

  auto isInternalId = [&](const auto& id) {
    const auto& v = indexBuilderData.vocabularyMetaData_;
    auto isInRange = [&](const auto& range) {
      return range.begin() <= id && id < range.end();
    };
    return isInRange(v.internalEntities_) || isInRange(v.langTaggedPredicates_);
  };

  auto makeNumEntitiesCounter = [&isInternalId](size_t& numEntities,
                                                size_t idx) {
    // TODO<joka921> Make the `index` a template parameter.
    return [lastEntity = std::optional<Id>{}, &numEntities, &isInternalId,
            idx](const auto& triple) mutable {
      const auto& id = triple[idx];
      if (id != lastEntity && !std::ranges::any_of(triple, isInternalId)) {
        numEntities++;
      }
      lastEntity = id;
    };
  };

  size_t numTriplesNormal = 0;
  auto countActualTriples = [&numTriplesNormal,
                             &isInternalId](const auto& triple) mutable {
    numTriplesNormal += !std::ranges::any_of(triple, isInternalId);
  };

  StxxlSorter<SortBySPO> spoSorter{stxxlMemoryInBytes() / 5};
  auto& psoSorter = *indexBuilderData.psoSorter;
  // For the first permutation, perform a unique.
  auto uniqueSorter = ad_utility::uniqueView(psoSorter.sortedView());

  size_t numPredicatesNormal = 0;
  createPermutationPair<IndexMetaDataHmapDispatcher>(
      std::move(uniqueSorter), _PSO, _POS, spoSorter.makePushCallback(),
      makeNumEntitiesCounter(numPredicatesNormal, 1), countActualTriples);
  _configurationJson["num-predicates-normal"] = numPredicatesNormal;
  _configurationJson["num-triples-normal"] = numTriplesNormal;
  writeConfiguration();
  psoSorter.clear();

  if (_loadAllPermutations) {
    // After the SPO permutation, create patterns if so desired.
    StxxlSorter<SortByOSP> ospSorter{stxxlMemoryInBytes() / 5};
    size_t numSubjectsNormal = 0;
    auto numSubjectCounter = makeNumEntitiesCounter(numSubjectsNormal, 0);
    if (_usePatterns) {
      PatternCreator patternCreator{_onDiskBase + ".index.patterns"};
      auto pushTripleToPatterns = [&patternCreator,
                                   &isInternalId](const auto& triple) {
        if (!std::ranges::any_of(triple, isInternalId)) {
          patternCreator.processTriple(triple);
        }
      };
      createPermutationPair<IndexMetaDataMmapDispatcher>(
          spoSorter.sortedView(), _SPO, _SOP, ospSorter.makePushCallback(),
          pushTripleToPatterns, numSubjectCounter);
      patternCreator.finish();
    } else {
      createPermutationPair<IndexMetaDataMmapDispatcher>(
          spoSorter.sortedView(), _SPO, _SOP, ospSorter.makePushCallback(),
          numSubjectCounter);
    }
    spoSorter.clear();
    _configurationJson["num-subjects-normal"] = numSubjectsNormal;
    writeConfiguration();

    // For the last pair of permutations we don't need a next sorter, so we have
    // no fourth argument.
    size_t numObjectsNormal = 0;
    createPermutationPair<IndexMetaDataMmapDispatcher>(
        ospSorter.sortedView(), _OSP, _OPS,
        makeNumEntitiesCounter(numObjectsNormal, 2));
    _configurationJson["num-objects-normal"] = numObjectsNormal;
    _configurationJson["has-all-permutations"] = true;
  } else {
    if (_usePatterns) {
      createPatternsFromSpoTriplesView(spoSorter.sortedView(),
                                       _onDiskBase + ".index.patterns",
                                       isInternalId);
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
template void IndexImpl::createFromFile<TurtleStreamParser<Tokenizer>>(
    const string& filename);
template void IndexImpl::createFromFile<TurtleMmapParser<Tokenizer>>(
    const string& filename);
template void IndexImpl::createFromFile<TurtleParserAuto>(
    const string& filename);

// _____________________________________________________________________________
template <class Parser>
IndexBuilderDataAsStxxlVector IndexImpl::passFileForVocabulary(
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
        LOG(TIMING) << ad_utility::Timer::toMilliseconds(t) << " msecs"
                    << std::endl;
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
    ad_utility::Timer sortFutureTimer{ad_utility::Timer::Started};
    if (writePartialVocabularyFuture[0].valid()) {
      writePartialVocabularyFuture[0].get();
    }
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
    auto compressionOutfile = ad_utility::makeOfstream(
        _onDiskBase + TMP_BASENAME_COMPRESSION + INTERNAL_VOCAB_SUFFIX);
    auto internalVocabularyActionCompression =
        [&compressionOutfile](const auto& word) {
          compressionOutfile << RdfEscaping::escapeNewlinesAndBackslashes(word)
                             << '\n';
        };
    m._noIdMapsAndIgnoreExternalVocab = true;
    auto mergeResult =
        m.mergeVocabulary(_onDiskBase + TMP_BASENAME_COMPRESSION, numFiles,
                          std::less<>(), internalVocabularyActionCompression);
    sizeInternalVocabulary = mergeResult.numWordsTotal_;
    LOG(INFO) << "Number of words in internal vocabulary: "
              << sizeInternalVocabulary << std::endl;
  }

  LOG(INFO) << "Merging partial vocabularies in Unicode order "
            << "(internal and external) ..." << std::endl;
  const VocabularyMerger::VocabularyMetaData mergeRes = [&]() {
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
  res.vocabularyMetaData_ = mergeRes;
  LOG(INFO) << "Number of words in external vocabulary: "
            << res.vocabularyMetaData_.numWordsTotal_ - sizeInternalVocabulary
            << std::endl;

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
std::unique_ptr<PsoSorter> IndexImpl::convertPartialToGlobalIds(
    TripleVec& data, const vector<size_t>& actualLinesPerPartial,
    size_t linesPerPartial) {
  LOG(INFO) << "Converting triples from local IDs to global IDs ..."
            << std::endl;
  LOG(DEBUG) << "Triples per partial vocabulary: " << linesPerPartial
             << std::endl;

  // Iterate over all partial vocabularies.
  TripleVec::bufreader_type reader(data);
  auto resultPtr = std::make_unique<PsoSorter>(stxxlMemoryInBytes() / 5);
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
        // TODO<joka921> The Maps should actually store `VocabIndex`es
        if (curTriple[k].getDatatype() != Datatype::VocabIndex) {
          continue;
        }
        auto iterator = idMap.find(curTriple[k]);
        if (iterator == idMap.end()) {
          LOG(INFO) << "Not found in partial vocabulary: " << curTriple[k]
                    << std::endl;
          AD_FAIL();
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
IndexImpl::createPermutationPairImpl(const string& fileName1,
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
  BufferedIdTable buffer{ad_utility::BufferedVector<Id>{
      THRESHOLD_RELATION_CREATION, fileName1 + ".tmp.mmap-buffer"}};
  size_t distinctCol1 = 0;
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
      writer1.addRelation(currentRel.value(), buffer, distinctCol1);
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
    } else {
      distinctCol1 += triple[c1] != lastLhs;
    }
    buffer.push_back(std::array{triple[c1], triple[c2]});
    lastLhs = triple[c1];
  }
  if (from < totalNumTriples) {
    writer1.addRelation(currentRel.value(), buffer, distinctCol1);
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
void IndexImpl::writeSwitchedRel(CompressedRelationWriter* out, Id currentRel,
                                 BufferedIdTable* bufPtr) {
  // Sort according to the "switched" relation.
  // TODO<joka921> The swapping is rather inefficient, as we have to iterate
  // over the whole file. Maybe the `CompressedRelationWriter` should take
  // the switched relations directly.
  auto& buffer = *bufPtr;

  AD_CONTRACT_CHECK(buffer.numColumns() == 2);
  for (BufferedIdTable::row_reference row : buffer) {
    std::swap(row[0], row[1]);
  }
  std::ranges::sort(buffer, [](const auto& a, const auto& b) {
    return std::ranges::lexicographical_compare(a, b);
  });
  Id lastLhs = std::numeric_limits<Id>::max();

  size_t distinctC1 = 0;
  for (const auto& el : buffer.getColumn(0)) {
    distinctC1 += el != lastLhs;
    lastLhs = el;
  }
  out->addRelation(currentRel, buffer, distinctC1);
}

// ________________________________________________________________________
template <class MetaDataDispatcher, class Comparator1, class Comparator2>
std::optional<std::pair<typename MetaDataDispatcher::WriteType,
                        typename MetaDataDispatcher::WriteType>>
IndexImpl::createPermutations(
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
void IndexImpl::createPermutationPair(
    auto&& sortedTriples,
    const PermutationImpl<Comparator1, typename MetaDataDispatcher::ReadType>&
        p1,
    const PermutationImpl<Comparator2, typename MetaDataDispatcher::ReadType>&
        p2,
    auto&&... perTripleCallbacks) {
  auto metaData = createPermutations<MetaDataDispatcher>(
      AD_FWD(sortedTriples), p1, p2, AD_FWD(perTripleCallbacks)...);
  // Set the name of this newly created pair of `IndexMetaData` objects.
  // NOTE: When `setKbName` was called, it set the name of _PSO._meta,
  // _PSO._meta, ... which however are not used during index building.
  // `getKbName` simple reads one of these names.
  metaData.value().first.setName(getKbName());
  metaData.value().second.setName(getKbName());
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
void IndexImpl::exchangeMultiplicities(MetaData* m1, MetaData* m2) {
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
void IndexImpl::addPatternsToExistingIndex() {
  // auto [langPredLowerBound, langPredUpperBound] = _vocab.prefix_range("@");
  //  We only iterate over the SPO permutation which typically only has few
  //  triples per subject, so it should be safe to not apply a memory limit
  //  here.
  AD_FAIL();
  /*
  ad_utility::AllocatorWithLimit<Id> allocator{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          std::numeric_limits<uint64_t>::max())};
  auto iterator = TriplesView(_SPO, allocator);
  createPatternsFromSpoTriplesView(iterator, _onDiskBase + ".index.patterns",
                                   Id::makeFromVocabIndex(langPredLowerBound),
                                   Id::makeFromVocabIndex(langPredUpperBound));
                                   */
  // TODO<joka921> Remove the AD_FAIL() again.
}

// _____________________________________________________________________________
void IndexImpl::createFromOnDiskIndex(const string& onDiskBase) {
  setOnDiskBase(onDiskBase);
  readConfiguration();
  _vocab.readFromFile(_onDiskBase + INTERNAL_VOCAB_SUFFIX,
                      _onDiskBase + EXTERNAL_VOCAB_SUFFIX);

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
        _onDiskBase + ".index.patterns", _avgNumDistinctSubjectsPerPredicate,
        _avgNumDistinctPredicatesPerSubject, _numDistinctSubjectPredicatePairs,
        _patterns, _hasPattern);
  }
}

// _____________________________________________________________________________
void IndexImpl::throwExceptionIfNoPatterns() const {
  if (!_usePatterns) {
    AD_THROW(
        "The requested feature requires a loaded patterns file ("
        "do not specify the --no-patterns option for this to work)");
  }
}

// _____________________________________________________________________________
const vector<PatternID>& IndexImpl::getHasPattern() const {
  throwExceptionIfNoPatterns();
  return _hasPattern;
}

// _____________________________________________________________________________
const CompactVectorOfStrings<Id>& IndexImpl::getHasPredicate() const {
  throwExceptionIfNoPatterns();
  return _hasPredicate;
}

// _____________________________________________________________________________
const CompactVectorOfStrings<Id>& IndexImpl::getPatterns() const {
  throwExceptionIfNoPatterns();
  return _patterns;
}

// _____________________________________________________________________________
double IndexImpl::getAvgNumDistinctPredicatesPerSubject() const {
  throwExceptionIfNoPatterns();
  return _avgNumDistinctPredicatesPerSubject;
}

// _____________________________________________________________________________
double IndexImpl::getAvgNumDistinctSubjectsPerPredicate() const {
  throwExceptionIfNoPatterns();
  return _avgNumDistinctSubjectsPerPredicate;
}

// _____________________________________________________________________________
size_t IndexImpl::getNumDistinctSubjectPredicatePairs() const {
  throwExceptionIfNoPatterns();
  return _numDistinctSubjectPredicatePairs;
}

// _____________________________________________________________________________
template <class T>
void IndexImpl::writeAsciiListFile(const string& filename, const T& ids) const {
  std::ofstream f(filename);

  for (size_t i = 0; i < ids.size(); ++i) {
    f << ids[i] << ' ';
  }
  f.close();
}

template void IndexImpl::writeAsciiListFile<vector<Id>>(
    const string& filename, const vector<Id>& ids) const;

template void IndexImpl::writeAsciiListFile<vector<Score>>(
    const string& filename, const vector<Score>& ids) const;

// _____________________________________________________________________________
bool IndexImpl::isLiteral(const string& object) const {
  return decltype(_vocab)::isLiteral(object);
}

// _____________________________________________________________________________
bool IndexImpl::shouldBeExternalized(const string& object) {
  return _vocab.shouldBeExternalized(object);
}

// _____________________________________________________________________________
void IndexImpl::setKbName(const string& name) {
  _POS.setKbName(name);
  _PSO.setKbName(name);
  _SOP.setKbName(name);
  _SPO.setKbName(name);
  _OPS.setKbName(name);
  _OSP.setKbName(name);
}

// ____________________________________________________________________________
void IndexImpl::setOnDiskBase(const std::string& onDiskBase) {
  _onDiskBase = onDiskBase;
}

// ____________________________________________________________________________
void IndexImpl::setKeepTempFiles(bool keepTempFiles) {
  _keepTempFiles = keepTempFiles;
}

// _____________________________________________________________________________
void IndexImpl::setUsePatterns(bool usePatterns) { _usePatterns = usePatterns; }

// _____________________________________________________________________________
void IndexImpl::setLoadAllPermutations(bool loadAllPermutations) {
  _loadAllPermutations = loadAllPermutations;
}

// ____________________________________________________________________________
void IndexImpl::setSettingsFile(const std::string& filename) {
  _settingsFileName = filename;
}

// ____________________________________________________________________________
void IndexImpl::setPrefixCompression(bool compressed) {
  _vocabPrefixCompressed = compressed;
}

// ____________________________________________________________________________
void IndexImpl::writeConfiguration() const {
  // Copy the configuration and add the current commit hash.
  auto configuration = _configurationJson;
  configuration["git_hash"] = std::string(qlever::version::GitHash);
  auto f = ad_utility::makeOfstream(_onDiskBase + CONFIGURATION_FILE);
  f << configuration;
}

// ___________________________________________________________________________
void IndexImpl::readConfiguration() {
  auto f = ad_utility::makeIfstream(_onDiskBase + CONFIGURATION_FILE);
  f >> _configurationJson;
  if (_configurationJson.find("git_hash") != _configurationJson.end()) {
    LOG(INFO) << "The git hash used to build this index was "
              << std::string(_configurationJson["git_hash"]).substr(0, 6)
              << std::endl;
  } else {
    LOG(INFO) << "The index was built before git commit hashes were stored in "
                 "the index meta data"
              << std::endl;
  }

  if (_configurationJson.find("prefixes") != _configurationJson.end()) {
    if (_configurationJson["prefixes"]) {
      vector<string> prefixes;
      auto prefixFile = ad_utility::makeIfstream(_onDiskBase + PREFIX_FILE);
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

  auto loadRequestedDataMember = [this](std::string_view key, auto& target) {
    auto it = _configurationJson.find(key);
    if (it == _configurationJson.end()) {
      throw std::runtime_error{absl::StrCat(
          "The required key \"", key,
          "\" was not found in the `meta-data.json`. Most likely this index "
          "was built with an older version of QLever and should be rebuilt")};
    }
    target = std::decay_t<decltype(target)>{*it};
  };

  loadRequestedDataMember("num-predicates-normal", _numPredicatesNormal);
  loadRequestedDataMember("num-subjects-normal", _numSubjectsNormal);
  loadRequestedDataMember("num-objects-normal", _numObjectsNormal);
  loadRequestedDataMember("num-triples-normal", _numTriplesNormal);
}

// ___________________________________________________________________________
LangtagAndTriple IndexImpl::tripleToInternalRepresentation(
    TurtleTriple&& triple) const {
  LangtagAndTriple result{"", {}};
  auto& resultTriple = result._triple;
  resultTriple[0] = std::move(triple._subject);
  resultTriple[1] = std::move(triple._predicate);

  // If the object of the triple can be directly folded into an ID, do so. Note
  // that the actual folding is done by the `TripleComponent`.
  std::optional<Id> idIfNotString = triple._object.toValueIdIfNotString();

  // TODO<joka921> The following statement could be simplified by a helper
  // function "optionalCast";
  if (idIfNotString.has_value()) {
    resultTriple[2] = idIfNotString.value();
  } else {
    resultTriple[2] = std::move(triple._object.getString());
  }

  for (size_t i = 0; i < 3; ++i) {
    auto& el = resultTriple[i];
    if (!std::holds_alternative<PossiblyExternalizedIriOrLiteral>(el)) {
      // If we already have an ID, we can just continue;
      continue;
    }
    auto& component = std::get<PossiblyExternalizedIriOrLiteral>(el);
    auto& iriOrLiteral = component._iriOrLiteral;
    iriOrLiteral = _vocab.getLocaleManager().normalizeUtf8(iriOrLiteral);
    if (_vocab.shouldBeExternalized(iriOrLiteral)) {
      component._isExternal = true;
    }
    // Only the third element (the object) might contain a language tag.
    if (i == 2 && isLiteral(iriOrLiteral)) {
      result._langtag = decltype(_vocab)::getLanguage(iriOrLiteral);
    }
  }
  return result;
}

// ___________________________________________________________________________
template <class Parser>
void IndexImpl::readIndexBuilderSettingsFromFile() {
  json j;  // if we have no settings, we still have to initialize some default
           // values
  if (!_settingsFileName.empty()) {
    auto f = ad_utility::makeIfstream(_settingsFileName);
    f >> j;
  }

  if (j.find("prefixes-external") != j.end()) {
    _vocab.initializeExternalizePrefixes(j["prefixes-external"]);
    _configurationJson["prefixes-external"] = j["prefixes-external"];
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
      AD_CONTRACT_CHECK(std::find(allModes.begin(), allModes.end(), value) ==
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
std::future<void> IndexImpl::writeNextPartialVocabulary(
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

// ____________________________________________________________________________
IndexImpl::NumNormalAndInternal IndexImpl::numTriples() const {
  return {_numTriplesNormal, PSO()._meta.getNofTriples() - _numTriplesNormal};
}
