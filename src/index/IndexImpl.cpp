// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors:
//   2014-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include "./IndexImpl.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <future>
#include <optional>
#include <unordered_map>

#include "CompilationInfo.h"
#include "absl/strings/str_join.h"
#include "index/IndexFormatVersion.h"
#include "index/PrefixHeuristic.h"
#include "index/TriplesView.h"
#include "index/VocabularyGenerator.h"
#include "parser/ParallelParseBuffer.h"
#include "util/BatchedPipeline.h"
#include "util/CompressionUsingZstd/ZstdWrapper.h"
#include "util/HashMap.h"
#include "util/Serializer/FileSerializer.h"
#include "util/TupleHelpers.h"

using std::array;

// _____________________________________________________________________________
IndexImpl::IndexImpl(ad_utility::AllocatorWithLimit<Id> allocator)
    : allocator_{std::move(allocator)} {};

// _____________________________________________________________________________
IndexBuilderDataAsPsoSorter IndexImpl::createIdTriplesAndVocab(
    std::shared_ptr<TurtleParserBase> parser) {
  auto indexBuilderData =
      passFileForVocabulary(std::move(parser), numTriplesPerBatch_);
  // first save the total number of words, this is needed to initialize the
  // dense IndexMetaData variants
  totalVocabularySize_ = indexBuilderData.vocabularyMetaData_.numWordsTotal_;
  LOG(DEBUG) << "Number of words in internal and external vocabulary: "
             << totalVocabularySize_ << std::endl;

  LOG(INFO) << "Converting external vocabulary to binary format ..."
            << std::endl;
  vocab_.externalizeLiteralsFromTextFile(
      onDiskBase_ + EXTERNAL_LITS_TEXT_FILE_NAME,
      onDiskBase_ + EXTERNAL_VOCAB_SUFFIX);
  deleteTemporaryFile(onDiskBase_ + EXTERNAL_LITS_TEXT_FILE_NAME);
  // clear vocabulary to save ram (only information from partial binary files
  // used from now on). This will preserve information about externalized
  // Prefixes etc.
  vocab_.clear();
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
void IndexImpl::createFromFile(const string& filename) {
  LOG(INFO) << "Processing input triples from " << filename << " ..."
            << std::endl;
  string indexFilename = onDiskBase_ + ".index";

  readIndexBuilderSettingsFromFile();

  auto setTokenizer = [this,
                       &filename]<template <typename> typename ParserTemplate>()
      -> std::unique_ptr<TurtleParserBase> {
    if (onlyAsciiTurtlePrefixes_) {
      return std::make_unique<ParserTemplate<TokenizerCtre>>(filename);
    } else {
      return std::make_unique<ParserTemplate<Tokenizer>>(filename);
    }
  };

  std::unique_ptr<TurtleParserBase> parser = [&setTokenizer, this]() {
    if (useParallelParser_) {
      return setTokenizer.template operator()<TurtleParallelParser>();
    } else {
      return setTokenizer.template operator()<TurtleStreamParser>();
    }
  }();

  IndexBuilderDataAsPsoSorter indexBuilderData =
      createIdTriplesAndVocab(std::move(parser));

  // If we have no compression, this will also copy the whole vocabulary.
  // but since we expect compression to be the default case, this  should not
  // hurt.
  string vocabFile = onDiskBase_ + INTERNAL_VOCAB_SUFFIX;
  string vocabFileTmp = onDiskBase_ + ".vocabularyTmp";
  const std::vector<string>& prefixes = indexBuilderData.prefixes_;
  if (vocabPrefixCompressed_) {
    auto prefixFile = ad_utility::makeOfstream(onDiskBase_ + PREFIX_FILE);
    for (const auto& prefix : prefixes) {
      prefixFile << prefix << std::endl;
    }
  }
  configurationJson_["prefixes"] = vocabPrefixCompressed_;
  LOG(INFO) << "Writing compressed vocabulary to disk ..." << std::endl;

  vocab_.buildCodebookForPrefixCompression(prefixes);
  auto wordReader = RdfsVocabulary::makeUncompressedDiskIterator(vocabFile);
  auto wordWriter = vocab_.makeCompressedWordWriter(vocabFileTmp);
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

  StxxlSorter<SortBySPO> spoSorter{stxxlMemory().getBytes() / 5};
  auto& psoSorter = *indexBuilderData.psoSorter;
  // For the first permutation, perform a unique.
  auto uniqueSorter = ad_utility::uniqueView(psoSorter.sortedView());

  size_t numPredicatesNormal = 0;
  createPermutationPair(
      std::move(uniqueSorter), pso_, pos_, spoSorter.makePushCallback(),
      makeNumEntitiesCounter(numPredicatesNormal, 1), countActualTriples);
  configurationJson_["num-predicates-normal"] = numPredicatesNormal;
  configurationJson_["num-triples-normal"] = numTriplesNormal;
  writeConfiguration();
  psoSorter.clear();

  if (loadAllPermutations_) {
    // After the SPO permutation, create patterns if so desired.
    StxxlSorter<SortByOSP> ospSorter{stxxlMemory().getBytes() / 5};
    size_t numSubjectsNormal = 0;
    auto numSubjectCounter = makeNumEntitiesCounter(numSubjectsNormal, 0);
    if (usePatterns_) {
      PatternCreator patternCreator{onDiskBase_ + ".index.patterns"};
      auto pushTripleToPatterns = [&patternCreator,
                                   &isInternalId](const auto& triple) {
        if (!std::ranges::any_of(triple, isInternalId)) {
          patternCreator.processTriple(triple);
        }
      };
      createPermutationPair(spoSorter.sortedView(), spo_, sop_,
                            ospSorter.makePushCallback(), pushTripleToPatterns,
                            numSubjectCounter);
      patternCreator.finish();
    } else {
      createPermutationPair(spoSorter.sortedView(), spo_, sop_,
                            ospSorter.makePushCallback(), numSubjectCounter);
    }
    spoSorter.clear();
    configurationJson_["num-subjects-normal"] = numSubjectsNormal;
    writeConfiguration();

    // For the last pair of permutations we don't need a next sorter, so we have
    // no fourth argument.
    size_t numObjectsNormal = 0;
    createPermutationPair(ospSorter.sortedView(), osp_, ops_,
                          makeNumEntitiesCounter(numObjectsNormal, 2));
    configurationJson_["num-objects-normal"] = numObjectsNormal;
    configurationJson_["has-all-permutations"] = true;
  } else {
    if (usePatterns_) {
      createPatternsFromSpoTriplesView(spoSorter.sortedView(),
                                       onDiskBase_ + ".index.patterns",
                                       isInternalId);
    }
    configurationJson_["has-all-permutations"] = false;
  }
  LOG(DEBUG) << "Finished writing permutations" << std::endl;

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
          parserBatchSize_,
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
              &itemArray, linesPerPartial, &(vocab_.getCaseComparator())));

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
  std::vector<std::string> prefixes;
  if (vocabPrefixCompressed_) {
    LOG(INFO) << "Merging partial vocabularies in byte order "
              << "(internal only) ..." << std::endl;
    VocabularyMerger m;
    auto compressionOutfile = ad_utility::makeOfstream(
        onDiskBase_ + TMP_BASENAME_COMPRESSION + INTERNAL_VOCAB_SUFFIX);
    auto internalVocabularyActionCompression =
        [&compressionOutfile](const auto& word) {
          compressionOutfile << RdfEscaping::escapeNewlinesAndBackslashes(word)
                             << '\n';
        };
    m._noIdMapsAndIgnoreExternalVocab = true;
    auto mergeResult =
        m.mergeVocabulary(onDiskBase_ + TMP_BASENAME_COMPRESSION, numFiles,
                          std::less<>(), internalVocabularyActionCompression);
    sizeInternalVocabulary = mergeResult.numWordsTotal_;
    LOG(INFO) << "Number of words in internal vocabulary: "
              << sizeInternalVocabulary << std::endl;
    // Flush and close the created vocabulary, s.t. the prefix compression sees
    // all of its contents.
    compressionOutfile.close();
    // We have to use the "normally" sorted vocabulary for the prefix
    // compression.
    std::string vocabFileForPrefixCalculation =
        onDiskBase_ + TMP_BASENAME_COMPRESSION + INTERNAL_VOCAB_SUFFIX;
    prefixes = calculatePrefixes(vocabFileForPrefixCalculation,
                                 NUM_COMPRESSION_PREFIXES, 1, true);
    ad_utility::deleteFile(vocabFileForPrefixCalculation);
  }

  LOG(INFO) << "Merging partial vocabularies in Unicode order "
            << "(internal and external) ..." << std::endl;
  const VocabularyMerger::VocabularyMetaData mergeRes = [&]() {
    VocabularyMerger v;
    auto sortPred = [cmp = &(vocab_.getCaseComparator())](std::string_view a,
                                                          std::string_view b) {
      return (*cmp)(a, b, decltype(vocab_)::SortLevel::TOTAL);
    };
    auto wordWriter =
        vocab_.makeUncompressingWordWriter(onDiskBase_ + INTERNAL_VOCAB_SUFFIX);
    auto internalVocabularyAction = [&wordWriter](const auto& word) {
      wordWriter.push(word.data(), word.size());
    };
    return v.mergeVocabulary(onDiskBase_, numFiles, sortPred,
                             internalVocabularyAction);
  }();
  LOG(DEBUG) << "Finished merging partial vocabularies" << std::endl;
  IndexBuilderDataAsStxxlVector res;
  res.vocabularyMetaData_ = mergeRes;
  res.prefixes_ = std::move(prefixes);
  LOG(INFO) << "Number of words in external vocabulary: "
            << res.vocabularyMetaData_.numWordsTotal_ - sizeInternalVocabulary
            << std::endl;

  res.idTriples = std::move(idTriples);
  res.actualPartialSizes = std::move(actualPartialSizes);

  LOG(INFO) << "Removing temporary files ..." << std::endl;
  for (size_t n = 0; n < numFiles; ++n) {
    deleteTemporaryFile(absl::StrCat(onDiskBase_, PARTIAL_VOCAB_FILE_NAME, n));
    if (vocabPrefixCompressed_) {
      deleteTemporaryFile(absl::StrCat(onDiskBase_, TMP_BASENAME_COMPRESSION,
                                       PARTIAL_VOCAB_FILE_NAME, n));
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
  auto resultPtr = std::make_unique<PsoSorter>(stxxlMemory().getBytes() / 5);
  auto& result = *resultPtr;
  size_t i = 0;
  for (size_t partialNum = 0; partialNum < actualLinesPerPartial.size();
       partialNum++) {
    std::string mmapFilename =
        absl::StrCat(onDiskBase_, PARTIAL_MMAP_IDS, partialNum);
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
std::optional<std::pair<IndexImpl::IndexMetaDataMmapDispatcher::WriteType,
                        IndexImpl::IndexMetaDataMmapDispatcher::WriteType>>
IndexImpl::createPermutationPairImpl(const string& fileName1,
                                     const string& fileName2,
                                     auto&& sortedTriples, size_t c0, size_t c1,
                                     size_t c2, auto&&... perTripleCallbacks) {
  using MetaData = IndexMetaDataMmapDispatcher::WriteType;
  MetaData metaData1, metaData2;
  if constexpr (MetaData::_isMmapBased) {
    metaData1.setup(fileName1 + MMAP_FILE_SUFFIX, ad_utility::CreateTag{});
    metaData2.setup(fileName2 + MMAP_FILE_SUFFIX, ad_utility::CreateTag{});
  }

  CompressedRelationWriter writer1{ad_utility::File(fileName1, "w"),
                                   blocksizePermutationInBytes_};
  CompressedRelationWriter writer2{ad_utility::File(fileName2, "w"),
                                   blocksizePermutationInBytes_};

  // Iterate over the vector and identify "relation" boundaries, where a
  // "relation" is the sequence of sortedTriples equal first component. For PSO
  // and POS, this is a predicate (of which "relation" is a synonym).
  LOG(INFO) << "Creating a pair of index permutations ... " << std::endl;
  size_t from = 0;
  std::optional<Id> currentRel;
  BufferedIdTable buffer{
      2,
      std::array{
          ad_utility::BufferedVector<Id>{THRESHOLD_RELATION_CREATION,
                                         fileName1 + ".tmp.mmap-buffer-col0"},
          ad_utility::BufferedVector<Id>{THRESHOLD_RELATION_CREATION,
                                         fileName1 + ".tmp.mmap-buffer-col1"}}};
  size_t distinctCol1 = 0;
  Id lastLhs = ID_NO_VALUE;
  uint64_t totalNumTriples = 0;
  auto addCurrentRelation = [&metaData1, &metaData2, &writer1, &writer2,
                             &currentRel, &buffer, &distinctCol1]() {
    auto md1 = writer1.addRelation(currentRel.value(), buffer, distinctCol1);
    auto md2 = writeSwitchedRel(&writer2, currentRel.value(), &buffer);
    md1.setCol2Multiplicity(md2.getCol1Multiplicity());
    md2.setCol2Multiplicity(md1.getCol1Multiplicity());
    metaData1.add(md1);
    metaData2.add(md2);
  };
  for (auto triple : AD_FWD(sortedTriples)) {
    if (!currentRel.has_value()) {
      currentRel = triple[c0];
    }
    // Call each of the `perTripleCallbacks` for the current triple
    (..., perTripleCallbacks(triple));
    ++totalNumTriples;
    if (triple[c0] != currentRel) {
      addCurrentRelation();
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
    addCurrentRelation();
  }

  metaData1.blockData() = std::move(writer1).getFinishedBlocks();
  metaData2.blockData() = std::move(writer2).getFinishedBlocks();

  return std::make_pair(std::move(metaData1), std::move(metaData2));
}

// __________________________________________________________________________
CompressedRelationMetadata IndexImpl::writeSwitchedRel(
    CompressedRelationWriter* out, Id currentRel, BufferedIdTable* bufPtr) {
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
  return out->addRelation(currentRel, buffer, distinctC1);
}

// ________________________________________________________________________
std::optional<std::pair<IndexImpl::IndexMetaDataMmapDispatcher::WriteType,
                        IndexImpl::IndexMetaDataMmapDispatcher::WriteType>>
IndexImpl::createPermutations(auto&& sortedTriples, const Permutation& p1,
                              const Permutation& p2,
                              auto&&... perTripleCallbacks) {
  auto metaData = createPermutationPairImpl(
      onDiskBase_ + ".index" + p1.fileSuffix_,
      onDiskBase_ + ".index" + p2.fileSuffix_, AD_FWD(sortedTriples),
      p1.keyOrder_[0], p1.keyOrder_[1], p1.keyOrder_[2],
      AD_FWD(perTripleCallbacks)...);

  if (metaData.has_value()) {
    auto& mdv = metaData.value();
    LOG(INFO) << "Statistics for " << p1.readableName_ << ": "
              << mdv.first.statistics() << std::endl;
    LOG(INFO) << "Statistics for " << p2.readableName_ << ": "
              << mdv.second.statistics() << std::endl;
  }

  return metaData;
}

// ________________________________________________________________________
void IndexImpl::createPermutationPair(auto&& sortedTriples,
                                      const Permutation& p1,
                                      const Permutation& p2,
                                      auto&&... perTripleCallbacks) {
  auto metaData = createPermutations(AD_FWD(sortedTriples), p1, p2,
                                     AD_FWD(perTripleCallbacks)...);
  // Set the name of this newly created pair of `IndexMetaData` objects.
  // NOTE: When `setKbName` was called, it set the name of pso_.meta_,
  // pso_.meta_, ... which however are not used during index building.
  // `getKbName` simple reads one of these names.
  metaData.value().first.setName(getKbName());
  metaData.value().second.setName(getKbName());
  if (metaData) {
    LOG(INFO) << "Writing meta data for " << p1.readableName_ << " and "
              << p2.readableName_ << " ..." << std::endl;
    ad_utility::File f1(onDiskBase_ + ".index" + p1.fileSuffix_, "r+");
    metaData.value().first.appendToFile(&f1);
    ad_utility::File f2(onDiskBase_ + ".index" + p2.fileSuffix_, "r+");
    metaData.value().second.appendToFile(&f2);
  }
}

// _____________________________________________________________________________
void IndexImpl::addPatternsToExistingIndex() {
  // auto [langPredLowerBound, langPredUpperBound] = vocab_.prefix_range("@");
  //  We only iterate over the SPO permutation which typically only has few
  //  triples per subject, so it should be safe to not apply a memory limit
  //  here.
  AD_FAIL();
  /*
  ad_utility::AllocatorWithLimit<Id> allocator{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(
          std::numeric_limits<uint64_t>::max())};
  auto iterator = TriplesView(spo_, allocator);
  createPatternsFromSpoTriplesView(iterator, onDiskBase_ + ".index.patterns",
                                   Id::makeFromVocabIndex(langPredLowerBound),
                                   Id::makeFromVocabIndex(langPredUpperBound));
                                   */
  // TODO<joka921> Remove the AD_FAIL() again.
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

  if (usePatterns_) {
    PatternCreator::readPatternsFromFile(
        onDiskBase_ + ".index.patterns", avgNumDistinctSubjectsPerPredicate_,
        avgNumDistinctPredicatesPerSubject_, numDistinctSubjectPredicatePairs_,
        patterns_, hasPattern_);
  }
}

// _____________________________________________________________________________
void IndexImpl::throwExceptionIfNoPatterns() const {
  if (!usePatterns_) {
    AD_THROW(
        "The requested feature requires a loaded patterns file ("
        "do not specify the --no-patterns option for this to work)");
  }
}

// _____________________________________________________________________________
const vector<PatternID>& IndexImpl::getHasPattern() const {
  throwExceptionIfNoPatterns();
  return hasPattern_;
}

// _____________________________________________________________________________
const CompactVectorOfStrings<Id>& IndexImpl::getHasPredicate() const {
  throwExceptionIfNoPatterns();
  return hasPredicate_;
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
  return decltype(vocab_)::isLiteral(object);
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
void IndexImpl::setUsePatterns(bool usePatterns) { usePatterns_ = usePatterns; }

// _____________________________________________________________________________
void IndexImpl::setLoadAllPermutations(bool loadAllPermutations) {
  loadAllPermutations_ = loadAllPermutations;
}

// ____________________________________________________________________________
void IndexImpl::setSettingsFile(const std::string& filename) {
  settingsFileName_ = filename;
}

// ____________________________________________________________________________
void IndexImpl::setPrefixCompression(bool compressed) {
  vocabPrefixCompressed_ = compressed;
}

// ____________________________________________________________________________
void IndexImpl::writeConfiguration() const {
  // Copy the configuration and add the current commit hash.
  auto configuration = configurationJson_;
  configuration["git-hash"] = std::string(qlever::version::GitHash);
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
              << std::string(configurationJson_["git-hash"]).substr(0, 6)
              << std::endl;
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

  if (configurationJson_.find("prefixes") != configurationJson_.end()) {
    if (configurationJson_["prefixes"]) {
      vector<string> prefixes;
      auto prefixFile = ad_utility::makeIfstream(onDiskBase_ + PREFIX_FILE);
      for (string prefix; std::getline(prefixFile, prefix);) {
        prefixes.emplace_back(std::move(prefix));
      }
      vocab_.buildCodebookForPrefixCompression(prefixes);
    } else {
      vocab_.buildCodebookForPrefixCompression(std::vector<std::string>());
    }
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

  if (configurationJson_.find("has-all-permutations") !=
          configurationJson_.end() &&
      configurationJson_["has-all-permutations"] == false) {
    // If the permutations simply don't exist, then we can never load them.
    loadAllPermutations_ = false;
  }

  auto loadRequestedDataMember = [this](std::string_view key, auto& target) {
    auto it = configurationJson_.find(key);
    if (it == configurationJson_.end()) {
      throw std::runtime_error{absl::StrCat(
          "The required key \"", key,
          "\" was not found in the `meta-data.json`. Most likely this index "
          "was built with an older version of QLever and should be rebuilt")};
    }
    target = std::decay_t<decltype(target)>{*it};
  };

  loadRequestedDataMember("num-predicates-normal", numPredicatesNormal_);
  loadRequestedDataMember("num-subjects-normal", numSubjectsNormal_);
  loadRequestedDataMember("num-objects-normal", numObjectsNormal_);
  loadRequestedDataMember("num-triples-normal", numTriplesNormal_);
}

// ___________________________________________________________________________
LangtagAndTriple IndexImpl::tripleToInternalRepresentation(
    TurtleTriple&& triple) const {
  LangtagAndTriple result{"", {}};
  auto& resultTriple = result._triple;
  resultTriple[0] = std::move(triple.subject_);
  resultTriple[1] = std::move(triple.predicate_);

  // If the object of the triple can be directly folded into an ID, do so. Note
  // that the actual folding is done by the `TripleComponent`.
  std::optional<Id> idIfNotString = triple.object_.toValueIdIfNotString();

  // TODO<joka921> The following statement could be simplified by a helper
  // function "optionalCast";
  if (idIfNotString.has_value()) {
    resultTriple[2] = idIfNotString.value();
  } else {
    // `toRdfLiteral` handles literals as well as IRIs correctly.
    resultTriple[2] = std::move(triple.object_).toRdfLiteral();
  }

  for (size_t i = 0; i < 3; ++i) {
    auto& el = resultTriple[i];
    if (!std::holds_alternative<PossiblyExternalizedIriOrLiteral>(el)) {
      // If we already have an ID, we can just continue;
      continue;
    }
    auto& component = std::get<PossiblyExternalizedIriOrLiteral>(el);
    auto& iriOrLiteral = component._iriOrLiteral;
    iriOrLiteral = vocab_.getLocaleManager().normalizeUtf8(iriOrLiteral);
    if (vocab_.shouldBeExternalized(iriOrLiteral)) {
      component._isExternal = true;
    }
    // Only the third element (the object) might contain a language tag.
    if (i == 2 && isLiteral(iriOrLiteral)) {
      result._langtag = decltype(vocab_)::getLanguage(iriOrLiteral);
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
    auto value = j[key];
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
      AD_CONTRACT_CHECK(std::find(allModes.begin(), allModes.end(), value) ==
                        allModes.end());
      LOG(ERROR) << "Invalid value for " << key << std::endl;
      LOG(INFO) << "The currently supported values are "
                << absl::StrJoin(allModes, ",") << std::endl;
    }
  } else {
    turtleParserIntegerOverflowBehavior_ =
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
      absl::StrCat(onDiskBase_, PARTIAL_VOCAB_FILE_NAME, numFiles);
  string partialCompressionFilename = absl::StrCat(
      onDiskBase_, TMP_BASENAME_COMPRESSION, PARTIAL_VOCAB_FILE_NAME, numFiles);

  auto lambda = [localIds = std::move(localIds), globalWritePtr,
                 items = std::move(items), vocab = &vocab_, partialFilename,
                 partialCompressionFilename, numFiles,
                 vocabPrefixCompressed = vocabPrefixCompressed_]() mutable {
    auto vec = vocabMapsToVector(std::move(items));
    const auto identicalPred = [&c = vocab->getCaseComparator()](
                                   const auto& a, const auto& b) {
      return c(a.second.m_splitVal, b.second.m_splitVal,
               decltype(vocab_)::SortLevel::TOTAL);
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
        return a.second.m_splitVal.isExternalized_;
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
  return {numTriplesNormal_, PSO().meta_.getNofTriples() - numTriplesNormal_};
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
  if (hasAllPermutations()) {
    auto numActually = numSubjectsNormal_;
    return {numActually, spo_.metaData().getNofDistinctC1() - numActually};
  } else {
    AD_THROW(
        "Can only get # distinct subjects if all 6 permutations "
        "have been registered on sever start (and index build time) "
        "with the -a option.");
  }
}

// __________________________________________________________________________
Index::NumNormalAndInternal IndexImpl::numDistinctObjects() const {
  if (hasAllPermutations()) {
    auto numActually = numObjectsNormal_;
    return {numActually, osp_.metaData().getNofDistinctC1() - numActually};
  } else {
    AD_THROW(
        "Can only get # distinct objects if all 6 permutations "
        "have been registered on sever start (and index build time) "
        "with the -a option.");
  }
}

// __________________________________________________________________________
Index::NumNormalAndInternal IndexImpl::numDistinctPredicates() const {
  auto numActually = numPredicatesNormal_;
  return {numActually, pso_.metaData().getNofDistinctC1() - numActually};
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
  if (const auto& p = getPermutation(permutation);
      p.metaData().col0IdExists(id)) {
    return p.metaData().getMetaData(id).getNofElements();
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
bool IndexImpl::getId(const string& element, Id* id) const {
  // TODO<joka921> we should parse doubles correctly in the SparqlParser and
  // then return the correct ids here or somewhere else.
  VocabIndex vocabId;
  auto success = getVocab().getId(element, &vocabId);
  *id = Id::makeFromVocabIndex(vocabId);
  return success;
}

// ___________________________________________________________________________
std::pair<Id, Id> IndexImpl::prefix_range(const std::string& prefix) const {
  // TODO<joka921> Do we need prefix ranges for numbers?
  auto [begin, end] = vocab_.prefix_range(prefix);
  return {Id::makeFromVocabIndex(begin), Id::makeFromVocabIndex(end)};
}

// _____________________________________________________________________________
vector<float> IndexImpl::getMultiplicities(
    const TripleComponent& key, Permutation::Enum permutation) const {
  const auto& p = getPermutation(permutation);
  std::optional<Id> keyId = key.toValueId(getVocab());
  vector<float> res;
  if (keyId.has_value() && p.meta_.col0IdExists(keyId.value())) {
    auto metaData = p.meta_.getMetaData(keyId.value());
    res.push_back(metaData.getCol1Multiplicity());
    res.push_back(metaData.getCol2Multiplicity());
  } else {
    res.push_back(1);
    res.push_back(1);
  }
  return res;
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
  return {m[p.keyOrder_[0]], m[p.keyOrder_[1]], m[p.keyOrder_[2]]};
}

// _____________________________________________________________________________
IdTable IndexImpl::scan(
    const TripleComponent& col0String,
    std::optional<std::reference_wrapper<const TripleComponent>> col1String,
    const Permutation::Enum& permutation,
    ad_utility::SharedConcurrentTimeoutTimer timer) const {
  std::optional<Id> col0Id = col0String.toValueId(getVocab());
  std::optional<Id> col1Id =
      col1String.has_value() ? col1String.value().get().toValueId(getVocab())
                             : std::nullopt;
  if (!col0Id.has_value() || (col1String.has_value() && !col1Id.has_value())) {
    size_t numColumns = col1String.has_value() ? 1 : 2;
    return IdTable{numColumns, allocator_};
  }
  return scan(col0Id.value(), col1Id, permutation, timer);
}
// _____________________________________________________________________________
IdTable IndexImpl::scan(Id col0Id, std::optional<Id> col1Id,
                        Permutation::Enum p,
                        ad_utility::SharedConcurrentTimeoutTimer timer) const {
  return getPermutation(p).scan(col0Id, col1Id, timer);
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
  return p.getResultSizeOfScan(col0Id.value(), col1Id.value());
}

// _____________________________________________________________________________
void IndexImpl::deleteTemporaryFile(const string& path) {
  if (!keepTempFiles_) {
    ad_utility::deleteFile(path);
  }
}
