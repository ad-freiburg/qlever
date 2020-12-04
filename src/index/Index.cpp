// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <future>
#include <optional>
#include <stxxl/algorithm>
#include <stxxl/map>
#include <unordered_map>

#include "../parser/NTriplesParser.h"
#include "../parser/ParallelParseBuffer.h"
#include "../parser/TsvParser.h"
#include "../util/BatchedPipeline.h"
#include "../util/Conversions.h"
#include "../util/HashMap.h"
#include "../util/TupleHelpers.h"
#include "./Index.h"
#include "./PrefixHeuristic.h"
#include "./VocabularyGenerator.h"
#include "MetaDataIterator.h"

using std::array;

// _____________________________________________________________________________
Index::Index() : _usePatterns(false) {}

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
  if constexpr (std::is_same_v<std::decay_t<Parser>, TurtleParserDummy>) {
    if (_onlyAsciiTurtlePrefixes) {
      LOG(INFO) << "Using the CTRE library for Tokenization\n";
      vocabData =
          createIdTriplesAndVocab<TurtleStreamParser<TokenizerCtre>>(filename);
    } else {
      LOG(INFO) << "Using the Google Re2 library for Tokenization\n";
      vocabData =
          createIdTriplesAndVocab<TurtleStreamParser<Tokenizer>>(filename);
    }

  } else {
    vocabData = createIdTriplesAndVocab<Parser>(filename);
  }

  // also perform unique for first permutation
  createPermutationPair<IndexMetaDataHmapDispatcher>(&vocabData, _PSO, _POS,
                                                     true);
  if (_usePatterns) {
    _patternIndex.generatePredicateLocalNamespace(&vocabData);
  }

  // also create Patterns after the Spo permutation if specified
  createPermutationPair<IndexMetaDataMmapDispatcher>(&vocabData, _SPO, _SOP,
                                                     false);
  if (_usePatterns) {
    // After creating the permutation pair for SPO and SOP the vocabData will
    // be sorted by the SPO columns.
    _patternIndex.createPatterns(&vocabData, _onDiskBase);
  }
  createPermutationPair<IndexMetaDataMmapDispatcher>(&vocabData, _OSP, _OPS);

  // if we have no compression, this will also copy the whole vocabulary.
  // but since we expect compression to be the default case, this  should not
  // hurt
  string vocabFile = _onDiskBase + ".vocabulary";
  string vocabFileTmp = _onDiskBase + ".vocabularyTmp";
  std::vector<string> prefixes;
  if (_vocabPrefixCompressed) {
    // we have to use the "normally" sorted vocabulary for the prefix
    // compression;
    std::string vocabFileForPrefixCalculation =
        _onDiskBase + TMP_BASENAME_COMPRESSION + ".vocabulary";
    prefixes = calculatePrefixes(vocabFileForPrefixCalculation,
                                 NUM_COMPRESSION_PREFIXES, 1, true);
    std::ofstream prefixFile(_onDiskBase + PREFIX_FILE);
    AD_CHECK(prefixFile.is_open());
    for (const auto& prefix : prefixes) {
      prefixFile << prefix << '\n';
    }
  }
  _configurationJson["prefixes"] = _vocabPrefixCompressed;
  Vocabulary<CompressedString, TripleComponentComparator>::prefixCompressFile(
      vocabFile, vocabFileTmp, prefixes);

  // TODO<joka921> maybe move this to its own function
  if (std::rename(vocabFileTmp.c_str(), vocabFile.c_str())) {
    LOG(INFO) << "Error: Rename the prefixed vocab file " << vocabFileTmp
              << " to " << vocabFile << " set errno to " << errno
              << ". Terminating...\n";
    AD_CHECK(false);
  }
  writeConfiguration();
}

// explicit instantiations
template void Index::createFromFile<TsvParser>(const string& filename);
template void Index::createFromFile<NTriplesParser>(const string& filename);
template void Index::createFromFile<TurtleStreamParser<Tokenizer>>(
    const string& filename);
template void Index::createFromFile<TurtleMmapParser<Tokenizer>>(
    const string& filename);
template void Index::createFromFile<TurtleParserDummy>(const string& filename);

// _____________________________________________________________________________
template <class Parser>
VocabularyData Index::passFileForVocabulary(const string& filename,
                                            size_t linesPerPartial) {
  auto parser = std::make_shared<Parser>(filename);
  std::unique_ptr<TripleVec> idTriples(new TripleVec());
  TripleVec::bufwriter_type writer(*idTriples);
  bool parserExhausted = false;

  size_t i = 0;
  // already count the numbers of triples that will be used for the language
  // filter
  size_t numFiles = 0;

  // we add extra triples
  std::vector<size_t> actualPartialSizes;

  std::future<void> sortFuture;
  while (!parserExhausted) {
    size_t actualCurrentPartialSize = 0;

    std::unique_ptr<TripleVec> localIdTriples(new TripleVec());
    TripleVec::bufwriter_type localWriter(*localIdTriples);

    std::array<ItemMapManager, NUM_PARALLEL_ITEM_MAPS> itemArray;

    {
      auto p = ad_pipeline::setupParallelPipeline<1, NUM_PARALLEL_ITEM_MAPS>(
          _parserBatchSize,
          // when called, returns an optional to the next triple. If
          // <linexPerPartial> triples were parsed, return std::nullopt. when
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
        if (i % 10000000 == 0) {
          LOG(INFO) << "Lines (from KB-file) processed: " << i << '\n';
        }
      }
      LOG(INFO) << "WaitTimes for Pipeline in msecs\n";
      for (const auto& t : p.getWaitingTime()) {
        LOG(INFO) << t << " msecs\n";
      }
    }

    localWriter.finish();
    // wait until sorting the last partial vocabulary has finished
    // to control the number of threads and the amount of memory used at the
    // same time. typically sorting is finished before we reach again here so
    // it is not a bottleneck.
    if (sortFuture.valid()) {
      sortFuture.get();
    }
    std::array<ItemMap, NUM_PARALLEL_ITEM_MAPS> convertedMaps;
    for (size_t j = 0; j < NUM_PARALLEL_ITEM_MAPS; ++j) {
      convertedMaps[j] = std::move(itemArray[j]).moveMap();
    }
    auto oldItemPtr =
        std::make_shared<const std::array<ItemMap, NUM_PARALLEL_ITEM_MAPS>>(
            std::move(convertedMaps));
    sortFuture = writeNextPartialVocabulary(
        i, numFiles, actualCurrentPartialSize, oldItemPtr,
        std::move(localIdTriples), &writer);
    numFiles++;
    // Save the information how many triples this partial vocabulary actually
    // deals with we will use this later for mapping from partial to global
    // ids
    actualPartialSizes.push_back(actualCurrentPartialSize);
  }
  if (sortFuture.valid()) {
    sortFuture.get();
  }
  writer.finish();
  LOG(INFO) << "Pass done." << endl;

  if (_vocabPrefixCompressed) {
    LOG(INFO) << "Merging temporary vocabulary for prefix compression";
    {
      VocabularyMerger m;
      m.mergeVocabulary(_onDiskBase + TMP_BASENAME_COMPRESSION, numFiles,
                        std::less<>());
      LOG(INFO) << "Finished merging additional Vocabulary.";
    }
  }

  LOG(INFO) << "Merging vocabulary\n";
  const VocabularyMerger::VocMergeRes mergeRes = [&]() {
    VocabularyMerger v;
    auto sortPred = [cmp = &(_vocab.getCaseComparator())](std::string_view a,
                                                          std::string_view b) {
      return (*cmp)(a, b, decltype(_vocab)::SortLevel::TOTAL);
    };

    return v.mergeVocabulary(_onDiskBase, numFiles, sortPred);
  }();
  LOG(INFO) << "Finished Merging Vocabulary.\n";
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
    LOG(INFO) << "Lines processed: " << i << '\n';
    LOG(INFO) << "Corresponding number of statements in original knowledgeBase:"
              << linesPerPartial * partialNum << '\n';

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
      if (i % 10000000 == 0) {
        LOG(INFO) << "Lines processed: " << i << '\n';
      }
    }
  }
  LOG(INFO) << "Lines processed: " << i << '\n';
  LOG(INFO) << "Pass done.\n";
}

pair<FullRelationMetaData, BlockBasedRelationMetaData> Index::writeSwitchedRel(
    ad_utility::File* out, off_t lastOffset, Id currentRel,
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

  return writeRel(*out, lastOffset, currentRel, buffer, distinctC1, functional);
}

// _____________________________________________________________________________
template <class MetaDataDispatcher>
std::optional<std::pair<typename MetaDataDispatcher::WriteType,
                        typename MetaDataDispatcher::WriteType>>
Index::createPermutationPairImpl(const string& fileName1,
                                 const string& fileName2,
                                 const Index::TripleVec& vec, size_t c0,
                                 size_t c1, size_t c2) {
  typename MetaDataDispatcher::WriteType metaData1;
  typename MetaDataDispatcher::WriteType metaData2;
  if constexpr (metaData1._isMmapBased) {
    metaData1.setup(_totalVocabularySize, FullRelationMetaData::empty,
                    fileName1 + MMAP_FILE_SUFFIX);
    metaData2.setup(_totalVocabularySize, FullRelationMetaData::empty,
                    fileName2 + MMAP_FILE_SUFFIX);
  }

  if (vec.size() == 0) {
    LOG(WARN) << "Attempt to write an empty index!" << std::endl;
    return std::nullopt;
  }
  ad_utility::File out1(fileName1, "w");
  ad_utility::File out2(fileName2, "w");

  LOG(INFO) << "Creating a pair of on-disk index permutation of " << vec.size()
            << " elements / facts." << std::endl;
  // Iterate over the vector and identify relation boundaries
  size_t from = 0;
  Id currentRel = vec[0][c0];
  off_t lastOffset1 = 0;
  off_t lastOffset2 = 0;
  ad_utility::BufferedVector<array<Id, 2>> buffer(
      THRESHOLD_RELATION_CREATION, fileName1 + ".tmp.MmapBuffer");
  bool functional = true;
  size_t distinctC1 = 1;
  size_t sizeOfRelation = 0;
  Id lastLhs = std::numeric_limits<Id>::max();
  for (TripleVec::bufreader_type reader(vec); !reader.empty(); ++reader) {
    if ((*reader)[c0] != currentRel) {
      auto md = writeRel(out1, lastOffset1, currentRel, buffer, distinctC1,
                         functional);
      metaData1.add(md.first, md.second);
      auto md2 = writeSwitchedRel(&out2, lastOffset2, currentRel, &buffer);
      metaData2.add(md2.first, md2.second);
      buffer.clear();
      distinctC1 = 1;
      lastOffset1 = metaData1.getOffsetAfter();
      lastOffset2 = metaData2.getOffsetAfter();
      currentRel = (*reader)[c0];
      functional = true;
    } else {
      sizeOfRelation++;
      if ((*reader)[c1] == lastLhs) {
        functional = false;
      } else {
        distinctC1++;
      }
    }
    buffer.push_back(array<Id, 2>{{(*reader)[c1], (*reader)[c2]}});
    lastLhs = (*reader)[c1];
  }
  if (from < vec.size()) {
    auto md =
        writeRel(out1, lastOffset1, currentRel, buffer, distinctC1, functional);
    metaData1.add(md.first, md.second);

    auto md2 = writeSwitchedRel(&out2, lastOffset2, currentRel, &buffer);
    metaData2.add(md2.first, md2.second);
  }

  LOG(INFO) << "Done creating index permutation." << std::endl;
  LOG(INFO) << "Calculating statistics for these permutation.\n";
  metaData1.calculateExpensiveStatistics();
  metaData2.calculateExpensiveStatistics();
  LOG(INFO) << "Writing statistics for this permutation:\n"
            << metaData1.statistics() << std::endl;
  LOG(INFO) << "Writing statistics for this permutation:\n"
            << metaData2.statistics() << std::endl;

  out1.close();
  out2.close();
  LOG(INFO) << "Permutation done.\n";
  return std::make_pair(std::move(metaData1), std::move(metaData2));
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
    bool performUnique) {
  LOG(INFO) << "Sorting for " << p1._readableName << " permutation..."
            << std::endl;
  stxxl::sort(begin(*vec), end(*vec), p1._comp, STXXL_MEMORY_TO_USE);
  LOG(INFO) << "Sort done." << std::endl;

  if (performUnique) {
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
    VocabularyData* vocabData,
    const PermutationImpl<Comparator1, typename MetaDataDispatcher::ReadType>&
        p1,
    const PermutationImpl<Comparator2, typename MetaDataDispatcher::ReadType>&
        p2,
    bool performUnique) {
  auto metaData = createPermutations<MetaDataDispatcher>(
      &(*vocabData->idTriples), p1, p2, performUnique);
  if (metaData) {
    LOG(INFO) << "Exchanging Multiplicities for " << p1._readableName << " and "
              << p2._readableName << '\n';
    exchangeMultiplicities(&(metaData.value().first),
                           &(metaData.value().second));
    LOG(INFO) << "Done" << '\n';
    LOG(INFO) << "Writing MetaData for " << p1._readableName << " and "
              << p2._readableName << '\n';
    ad_utility::File f1(_onDiskBase + ".index" + p1._fileSuffix, "r+");
    metaData.value().first.appendToFile(&f1);
    ad_utility::File f2(_onDiskBase + ".index" + p2._fileSuffix, "r+");
    metaData.value().second.appendToFile(&f2);
    LOG(INFO) << "Done" << '\n';
  }
}

// _________________________________________________________________________
template <class MetaData>
void Index::exchangeMultiplicities(MetaData* m1, MetaData* m2) {
  for (auto it = m1->data().begin(); it != m1->data().end(); ++it) {
    const FullRelationMetaData& constRmd = it->second;
    // our MetaData classes have a read-only interface because normally the
    // FuullRelationMetaData are created separately and then added and never
    // changed. This function forms an exception to this pattern
    // because calculation the 2nd column multiplicity separately for each
    // permutation is inefficient. So it is fine to use const_cast here as an
    // exception: we delibarately write to a read-only data structure and are
    // knowing what we are doing
    FullRelationMetaData& rmd = const_cast<FullRelationMetaData&>(constRmd);
    m2->data()[it->first].setCol2LogMultiplicity(rmd.getCol1LogMultiplicity());
    rmd.setCol2LogMultiplicity(m2->data()[it->first].getCol1LogMultiplicity());
  }
}

// _____________________________________________________________________________
void Index::addPatternsToExistingIndex() {
  auto [langPredLowerBound, langPredUpperBound] = _vocab.prefix_range("@");

  _patternIndex.generatePredicateLocalNamespaceFromExistingIndex(
      langPredLowerBound, langPredUpperBound, _PSO._meta);
  _patternIndex.createPatternsFromExistingIndex(langPredLowerBound,
                                                langPredUpperBound, _SPO._meta,
                                                _SPO._file, _onDiskBase);
}

// _____________________________________________________________________________
pair<FullRelationMetaData, BlockBasedRelationMetaData> Index::writeRel(
    ad_utility::File& out, off_t currentOffset, Id relId,
    const ad_utility::BufferedVector<array<Id, 2>>& data, size_t distinctC1,
    bool functional) {
  LOG(TRACE) << "Writing a relation ...\n";
  AD_CHECK_GT(data.size(), 0);
  LOG(TRACE) << "Calculating multiplicities ...\n";
  double multC1 = functional ? 1.0 : data.size() / double(distinctC1);
  // Dummy value that will be overwritten later
  double multC2 = 42.42;
  LOG(TRACE) << "Done calculating multiplicities.\n";
  FullRelationMetaData rmd(
      relId, currentOffset, data.size(), multC1, multC2, functional,
      !functional && data.size() > USE_BLOCKS_INDEX_SIZE_TRESHOLD);

  // Write the full pair index.
  out.write(data.data(), data.size() * 2 * sizeof(Id));
  pair<FullRelationMetaData, BlockBasedRelationMetaData> ret;
  ret.first = rmd;

  if (functional) {
    writeFunctionalRelation(data, ret);
  } else {
    writeNonFunctionalRelation(out, data, ret);
  };
  LOG(TRACE) << "Done writing relation.\n";
  return ret;
}

// _____________________________________________________________________________
void Index::writeFunctionalRelation(
    const BufferedVector<array<Id, 2>>& data,
    pair<FullRelationMetaData, BlockBasedRelationMetaData>& rmd) {
  // Only has to do something if there are blocks.
  if (rmd.first.hasBlocks()) {
    LOG(TRACE) << "Writing part for functional relation ...\n";
    // Do not write extra LHS and RHS lists.
    rmd.second._startRhs =
        rmd.first._startFullIndex + rmd.first.getNofBytesForFulltextIndex();
    // Since the relation is functional, there are no lhs lists and thus this
    // is trivial.
    rmd.second._offsetAfter = rmd.second._startRhs;
    // Create the block data for the meta data.
    // Blocks are offsets into the full pair index for functional relations.
    size_t nofDistinctLhs = 0;
    Id lastLhs = std::numeric_limits<Id>::max();
    for (size_t i = 0; i < data.size(); ++i) {
      if (data[i][0] != lastLhs) {
        if (nofDistinctLhs % DISTINCT_LHS_PER_BLOCK == 0) {
          rmd.second._blocks.emplace_back(BlockMetaData(
              data[i][0], rmd.first._startFullIndex + i * 2 * sizeof(Id)));
        }
        ++nofDistinctLhs;
      }
    }
  }
}

// _____________________________________________________________________________
void Index::writeNonFunctionalRelation(
    ad_utility::File& out, const BufferedVector<array<Id, 2>>& data,
    pair<FullRelationMetaData, BlockBasedRelationMetaData>& rmd) {
  // Only has to do something if there are blocks.
  if (rmd.first.hasBlocks()) {
    LOG(TRACE) << "Writing part for non-functional relation ...\n";
    // Make a pass over the data and extract a RHS list for each LHS.
    // Prepare both in buffers.
    // TODO: add compression - at least to RHS.
    pair<Id, off_t>* bufLhs = new pair<Id, off_t>[data.size()];
    Id* bufRhs = new Id[data.size()];
    size_t nofDistinctLhs = 0;
    Id lastLhs = std::numeric_limits<Id>::max();
    size_t nofRhsDone = 0;
    for (; nofRhsDone < data.size(); ++nofRhsDone) {
      if (data[nofRhsDone][0] != lastLhs) {
        bufLhs[nofDistinctLhs++] =
            pair<Id, off_t>(data[nofRhsDone][0], nofRhsDone * sizeof(Id));
        lastLhs = data[nofRhsDone][0];
      }
      bufRhs[nofRhsDone] = data[nofRhsDone][1];
    }

    // Go over the Lhs data once more and adjust the offsets.
    off_t startRhs = rmd.first.getStartOfLhs() +
                     nofDistinctLhs * (sizeof(Id) + sizeof(off_t));

    for (size_t i = 0; i < nofDistinctLhs; ++i) {
      bufLhs[i].second += startRhs;
    }

    // Write to file.
    out.write(bufLhs, nofDistinctLhs * (sizeof(Id) + sizeof(off_t)));
    out.write(bufRhs, data.size() * sizeof(Id));

    // Update meta data.
    rmd.second._startRhs = startRhs;
    rmd.second._offsetAfter =
        startRhs + rmd.first.getNofElements() * sizeof(Id);

    // Create the block data for the FullRelationMetaData.
    // Block are offsets into the LHS list for non-functional relations.
    for (size_t i = 0; i < nofDistinctLhs; ++i) {
      if (i % DISTINCT_LHS_PER_BLOCK == 0) {
        rmd.second._blocks.emplace_back(BlockMetaData(
            bufLhs[i].first,
            rmd.first.getStartOfLhs() + i * (sizeof(Id) + sizeof(off_t))));
      }
    }
    delete[] bufLhs;
    delete[] bufRhs;
  }
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
  _OPS.loadFromDisk(_onDiskBase);
  _OSP.loadFromDisk(_onDiskBase);
  _SPO.loadFromDisk(_onDiskBase);
  _SOP.loadFromDisk(_onDiskBase);

  if (_usePatterns) {
    // Read the pattern info from the patterns file
    _patternIndex.loadPatternIndex(_onDiskBase);
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
void Index::scanFunctionalRelation(const pair<off_t, size_t>& blockOff,
                                   Id lhsId, ad_utility::File& indexFile,
                                   IdTable* result) const {
  if (blockOff.second == 0) {
    // TODO<joka921> this check should be in the callers, but for that I want to
    // refactor the code duplication there first
    // nothing to do if the result is empty
    return;
  }
  LOG(TRACE) << "Scanning functional relation ...\n";
  WidthTwoList block;
  block.resize(blockOff.second / (2 * sizeof(Id)));
  indexFile.read(block.data(), blockOff.second, blockOff.first);
  auto it = std::lower_bound(
      block.begin(), block.end(), lhsId,
      [](const array<Id, 2>& elem, Id key) { return elem[0] < key; });
  if ((*it)[0] == lhsId) {
    result->push_back({(*it)[1]});
  }
  LOG(TRACE) << "Read " << result->size() << " RHS.\n";
}

// _____________________________________________________________________________
void Index::scanNonFunctionalRelation(const pair<off_t, size_t>& blockOff,
                                      const pair<off_t, size_t>& followBlock,
                                      Id lhsId, ad_utility::File& indexFile,
                                      off_t upperBound, IdTable* result) const {
  LOG(TRACE) << "Scanning non-functional relation ...\n";

  if (blockOff.second == 0) {
    // TODO<joka921> this check should be in the callers, but for that I want to
    // refactor the code duplication there first
    // nothing to do if the result is empty
    return;
  }
  vector<pair<Id, off_t>> block;
  block.resize(blockOff.second / (sizeof(Id) + sizeof(off_t)));
  indexFile.read(block.data(), blockOff.second, blockOff.first);
  auto it = std::lower_bound(
      block.begin(), block.end(), lhsId,
      [](const pair<Id, off_t>& elem, Id key) { return elem.first < key; });
  if (it->first == lhsId) {
    size_t nofBytes = 0;
    if ((it + 1) != block.end()) {
      LOG(TRACE) << "Obtained upper bound from same block!\n";
      nofBytes = static_cast<size_t>((it + 1)->second - it->second);
    } else {
      // Look at the follow block to determine the upper bound / nofBytes.
      if (followBlock.first == blockOff.first) {
        LOG(TRACE) << "Last block of relation, using rel upper bound!\n";
        nofBytes = static_cast<size_t>(upperBound - it->second);
      } else {
        LOG(TRACE) << "Special case: extra scan of follow block!\n";
        pair<Id, off_t> follower;
        indexFile.read(&follower, sizeof(follower), followBlock.first);
        nofBytes = static_cast<size_t>(follower.second - it->second);
      }
    }
    result->resize(nofBytes / sizeof(Id));
    indexFile.read(result->data(), nofBytes, it->second);
  } else {
    LOG(TRACE) << "Could not find LHS in block. Result will be empty.\n";
  }
}

// _____________________________________________________________________________
size_t Index::relationCardinality(const string& relationName) const {
  if (relationName == INTERNAL_TEXT_MATCH_PREDICATE) {
    return TEXT_PREDICATE_CARDINALITY_ESTIMATE;
  }
  Id relId;
  if (_vocab.getId(relationName, &relId)) {
    if (this->_PSO.metaData().relationExists(relId)) {
      return this->_PSO.metaData().getRmd(relId).getNofElements();
    }
  }
  return 0;
}

// _____________________________________________________________________________
size_t Index::subjectCardinality(const string& sub) const {
  Id relId;
  if (_vocab.getId(sub, &relId)) {
    if (this->_SPO.metaData().relationExists(relId)) {
      return this->_SPO.metaData().getRmd(relId).getNofElements();
    }
  }
  return 0;
}

// _____________________________________________________________________________
size_t Index::objectCardinality(const string& obj) const {
  Id relId;
  if (_vocab.getId(obj, &relId)) {
    if (this->_OSP.metaData().relationExists(relId)) {
      return this->_OSP.metaData().getRmd(relId).getNofElements();
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
const PatternIndex& Index::getPatternIndex() const {
  throwExceptionIfNoPatterns();
  return _patternIndex;
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
    _onDiskLiterals = _configurationJson["external-literals"];
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
    std::string lang = _configurationJson["locale"]["language"];
    std::string country = _configurationJson["locale"]["country"];
    bool ignorePunctuation = _configurationJson["locale"]["ignore-punctuation"];
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
      lang = j["locale"]["language"];
      country = j["locale"]["country"];
      ignorePunctuation = j["locale"]["ignore-punctuation"];
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
    if constexpr (std::is_same_v<std::decay_t<Parser>, TurtleParserDummy>) {
      bool v = j["ascii-prefixes-only"];
      if (v) {
        LOG(WARN) << WARNING_ASCII_ONLY_PREFIXES;
        _onlyAsciiTurtlePrefixes = true;
      } else {
        _onlyAsciiTurtlePrefixes = false;
      }
    } else {
      LOG(WARN) << "You specified the ascii-prefixes-only but a parser that is "
                   "not the Turtle stream parser. This means that this setting "
                   "is ignored\n";
    }
  }

  if (j.count("num-triples-per-partial-vocab")) {
    _numTriplesPerPartialVocab = j["num-triples-per-partial-vocab"];
    LOG(INFO) << "Overriding setting num-triples-per-partial-vocab to "
              << _numTriplesPerPartialVocab
              << " This might influence performance / memory usage during "
                 "index build\n";
  }

  if (j.count("parser-batch-size")) {
    _parserBatchSize = j["parser-batch-size"];
    LOG(INFO) << "Overriding setting parser-batch-size to " << _parserBatchSize
              << " This might influence performance during index build\n";
  }
}

// ___________________________________________________________________________
std::future<void> Index::writeNextPartialVocabulary(
    size_t numLines, size_t numFiles, size_t actualCurrentPartialSize,
    std::shared_ptr<const std::array<ItemMap, NUM_PARALLEL_ITEM_MAPS>> items,
    std::unique_ptr<TripleVec> localIds,
    TripleVec::bufwriter_type* globalWritePtr) {
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

  auto lambda = [localIds = std::move(localIds), globalWritePtr, items,
                 vocab = &_vocab, partialFilename, partialCompressionFilename,
                 vocabPrefixCompressed = _vocabPrefixCompressed]() {
    auto vec = vocabMapsToVector(items);
    const auto identicalPred = [& c = vocab->getCaseComparator()](
                                   const auto& a, const auto& b) {
      return c(a.second.m_splitVal, b.second.m_splitVal,
               decltype(_vocab)::SortLevel::TOTAL);
    };
    LOG(INFO) << "Start sorting of vocabulary with #elements: " << vec.size()
              << std::endl;
    sortVocabVector(&vec, identicalPred, true);
    LOG(INFO) << "Finished sorting of vocabulary" << std::endl;
    auto mapping = createInternalMapping(&vec);
    LOG(INFO) << "Finished creating of Mapping vocabulary" << std::endl;
    auto sz = vec.size();
    // since now adjacent duplicates also have the same Ids, it suffices to
    // compare those
    vec.erase(std::unique(vec.begin(), vec.end(),
                          [](const auto& a, const auto& b) {
                            return a.second.m_id == b.second.m_id;
                          }),
              vec.end());
    LOG(INFO) << "Removed " << sz - vec.size()
              << " Duplicates from the local partial vocabularies\n";
    writeMappedIdsToExtVec(*localIds, mapping, globalWritePtr);
    writePartialVocabularyToFile(vec, partialFilename);
    if (vocabPrefixCompressed) {
      // sort according to the actual byte values
      LOG(INFO) << "Start sorting of vocabulary for prefix compression"
                << std::endl;
      sortVocabVector(
          &vec, [](const auto& a, const auto& b) { return a.first < b.first; },
          false);
      LOG(INFO) << "Finished sorting of vocabulary for prefix compression"
                << std::endl;
      writePartialVocabularyToFile(vec, partialCompressionFilename);
    }
    LOG(INFO) << "Finished writing the partial vocabulary" << std::endl;
  };

  return std::async(std::launch::async, std::move(lambda));
}
