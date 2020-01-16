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
#include "../util/Conversions.h"
#include "../util/HashMap.h"
#include "./Index.h"
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
  initializeVocabularySettingsBuild();

  auto vocabData =
      passFileForVocabulary<Parser>(ntFile, NUM_TRIPLES_PER_PARTIAL_VOCAB);
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

  auto vocabData = createIdTriplesAndVocab<Parser>(filename);

  // also perform unique for first permutation
  createPermutationPair<IndexMetaDataHmapDispatcher>(&vocabData, _PSO, _POS,
                                                     true);
  // also create Patterns after the Spo permutation if specified
  createPermutationPair<IndexMetaDataMmapDispatcher>(&vocabData, _SPO, _SOP,
                                                     false, _usePatterns);
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
template void Index::createFromFile<TurtleStreamParser>(const string& filename);
template void Index::createFromFile<TurtleMmapParser>(const string& filename);

// _____________________________________________________________________________
template <class Parser>
VocabularyData Index::passFileForVocabulary(const string& filename,
                                            size_t linesPerPartial) {
  ParallelParseBuffer<Parser> p(PARSER_BATCH_SIZE, filename);
  size_t i = 0;
  // already count the numbers of triples that will be used for the language
  // filter
  size_t numFiles = 0;

  // we add extra triples
  std::vector<size_t> actualPartialSizes;
  size_t actualCurrentPartialSize = 0;
  std::unique_ptr<TripleVec> idTriples(new TripleVec());
  TripleVec::bufwriter_type writer(*idTriples);

  ItemMap items;

  // insert the special  ql:langtag predicate into all partial vocabularies
  auto langPredId = assignNextId(&items, LANGUAGE_PREDICATE);
  std::array<string, 3> spo;

  std::pair<std::future<void>, std::future<void>> sortFutures;
  while (true) {
    auto opt = p.getTriple();
    if (!opt) {
      break;
    }
    auto& spo = opt.value();
    auto langtag = tripleToInternalRepresentation(&spo);
    std::array<Id, 3> spoIds;
    // immediately get or reuse ids for the elements of the triple
    // these are sorted by order of appearance and only valid in the current
    // partial vocabulary
    for (size_t k = 0; k < 3; ++k) {
      spoIds[k] = assignNextId(&items, spo[k]);
    }
    writer << array<Id, 3>{{spoIds[0], spoIds[1], spoIds[2]}};
    actualCurrentPartialSize++;

    // if we have a language-tagged object we also add the special triples for
    // the language filter
    if (!langtag.empty()) {
      auto langTagId =
          assignNextId(&items, ad_utility::convertLangtagToEntityUri(langtag));
      auto langTaggedPredId = assignNextId(
          &items,
          ad_utility::convertToLanguageTaggedPredicate(spo[1], langtag));
      writer << array<Id, 3>{{spoIds[0], langTaggedPredId, spoIds[2]}};
      writer << array<Id, 3>{{spoIds[2], langPredId, langTagId}};
      actualCurrentPartialSize += 2;
    }

    ++i;
    if (i % 10000000 == 0) {
      LOG(INFO) << "Lines (from KB-file) processed: " << i << '\n';
    }
    if (i % linesPerPartial == 0) {
      // wait until sorting the last partial vocabulary has finished
      // to control the number of threads and the amount of memory used at the
      // same time. typically sorting is finished before we reach again here so
      // it is not a bottleneck.
      if (sortFutures.first.valid()) {
        sortFutures.first.get();
      }
      if (sortFutures.second.valid()) {
        sortFutures.second.get();
      }

      auto oldItemPtr = std::make_shared<const ItemMap>(std::move(items));
      sortFutures = writeNextPartialVocabulary(
          i, numFiles, actualCurrentPartialSize, oldItemPtr);
      numFiles++;
      // Save the information how many triples this partial vocabulary actually
      // deals with we will use this later for mapping from partial to global
      // ids
      actualPartialSizes.push_back(actualCurrentPartialSize);
      // reset data structures for next partial vocab
      actualCurrentPartialSize = 0;
      items.clear();
      // insert the special predicate into all partial vocabularies
      langPredId = assignNextId(&items, LANGUAGE_PREDICATE);
    }
  }
  // deal with remainder
  if (!items.empty()) {
    if (sortFutures.first.valid()) {
      sortFutures.first.get();
    }
    if (sortFutures.second.valid()) {
      sortFutures.second.get();
    }
    auto oldItemPtr = std::make_shared<const ItemMap>(std::move(items));
    sortFutures = writeNextPartialVocabulary(
        i, numFiles, actualCurrentPartialSize, oldItemPtr);
    numFiles++;
    actualPartialSizes.push_back(actualCurrentPartialSize);
    if (sortFutures.first.valid()) {
      sortFutures.first.get();
    }
    if (sortFutures.second.valid()) {
      sortFutures.second.get();
    }
  }
  writer.finish();
  LOG(INFO) << "Pass done." << endl;

  if (_vocabPrefixCompressed) {
    LOG(INFO) << "Merging temporary vocabulary for prefix compression";
    {
      VocabularyMerger m;
      m.mergeVocabulary(_onDiskBase + TMP_BASENAME_COMPRESSION, numFiles,
                        std::less<std::string>());
      LOG(INFO) << "Finished merging additional Vocabulary.";
    }
  }

  LOG(INFO) << "Merging vocabulary\n";
  VocabularyMerger::VocMergeRes mergeRes;
  {
    VocabularyMerger v;
    auto identicalPred = [c = _vocab.getCaseComparator()](const auto& a,
                                                          const auto& b) {
      return c(a, b, decltype(c)::Level::IDENTICAL);
    };
    mergeRes = v.mergeVocabulary(_onDiskBase, numFiles, identicalPred);
    LOG(INFO) << "Finished Merging Vocabulary.\n";
  }
  VocabularyData res;
  res.nofWords = mergeRes._numWordsTotal;
  res.langPredLowerBound = mergeRes._langPredLowerBound;
  res.langPredUpperBound = mergeRes._langPredUpperBound;

  res.idTriples = std::move(idTriples);
  res.actualPartialSizes = std::move(actualPartialSizes);

  for (size_t i = 0; i < numFiles; ++i) {
    string partialFilename =
        _onDiskBase + PARTIAL_VOCAB_FILE_NAME + std::to_string(i);
    deleteTemporaryFile(partialFilename);
  }
  if (_vocabPrefixCompressed) {
    string partialFilename = _onDiskBase + TMP_BASENAME_COMPRESSION +
                             PARTIAL_VOCAB_FILE_NAME + std::to_string(i);
    deleteTemporaryFile(partialFilename);
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
    bool performUnique, bool createPatternsAfterFirst) {
  auto metaData = createPermutations<MetaDataDispatcher>(
      &(*vocabData->idTriples), p1, p2, performUnique);
  if (createPatternsAfterFirst) {
    // the second permutation does not alter the original triple vector,
    // so this does still work.
    createPatterns(true, vocabData);
  }
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
  createPatternsImpl<MetaDataIterator<IndexMetaDataMmapView>,
                     IndexMetaDataMmapView, ad_utility::File>(
      _onDiskBase + ".index.patterns", _hasPredicate, _hasPattern, _patterns,
      _fullHasPredicateMultiplicityEntities,
      _fullHasPredicateMultiplicityPredicates, _fullHasPredicateSize,
      _maxNumPatterns, langPredLowerBound, langPredUpperBound, _SPO.metaData(),
      _SPO._file);
}

// _____________________________________________________________________________
void Index::createPatterns(bool vecAlreadySorted, VocabularyData* vocabData) {
  if (vecAlreadySorted) {
    LOG(INFO) << "Vector already sorted for pattern creation." << std::endl;
  } else {
    LOG(INFO) << "Sorting for pattern creation..." << std::endl;
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
void Index::createPatternsImpl(const string& fileName,
                               CompactStringVector<Id, Id>& hasPredicate,
                               std::vector<PatternID>& hasPattern,
                               CompactStringVector<size_t, Id>& patterns,
                               double& fullHasPredicateMultiplicityEntities,
                               double& fullHasPredicateMultiplicityPredicates,
                               size_t& fullHasPredicateSize,
                               const size_t maxNumPatterns,
                               const Id langPredLowerBound,
                               const Id langPredUpperBound,
                               const Args&... vecReaderArgs) {
  IndexMetaDataHmap meta;
  typedef std::unordered_map<Pattern, size_t> PatternsCountMap;

  LOG(INFO) << "Creating patterns file..." << std::endl;
  VecReaderType reader(vecReaderArgs...);
  if (reader.empty()) {
    LOG(WARN) << "Triple vector was empty, no patterns created" << std::endl;
    return;
  }

  PatternsCountMap patternCounts;
  // determine the most common patterns
  Pattern pattern;

  size_t patternIndex = 0;
  size_t numValidPatterns = 0;
  Id currentSubj = (*reader)[0];

  for (; !reader.empty(); ++reader) {
    auto triple = *reader;
    if (triple[0] != currentSubj) {
      currentSubj = triple[0];
      numValidPatterns++;
      auto it = patternCounts.find(pattern);
      if (it == patternCounts.end()) {
        patternCounts.insert(std::pair<Pattern, size_t>(pattern, size_t(1)));
      } else {
        (*it).second++;
      }
      pattern.clear();
      patternIndex = 0;
    }

    // don't list predicates twice
    if (patternIndex == 0 || pattern[patternIndex - 1] != triple[1]) {
      // Ignore @..@ type language predicates
      if (triple[1] < langPredLowerBound || triple[1] >= langPredUpperBound) {
        pattern.push_back(triple[1]);
        patternIndex++;
      }
    }
  }
  // process the last entry
  auto it = patternCounts.find(pattern);
  if (it == patternCounts.end()) {
    patternCounts.insert(std::pair<Pattern, size_t>(pattern, size_t(1)));
  } else {
    (*it).second++;
  }
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
  patternIndex = 0;
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
  patterns.write(file);
  file.close();

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
      _patterns.load(patternsFile, off);

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
const CompactStringVector<Id, Id>& Index::getHasPredicate() const {
  throwExceptionIfNoPatterns();
  return _hasPredicate;
}

// _____________________________________________________________________________
const CompactStringVector<size_t, Id>& Index::getPatterns() const {
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
string Index::tripleToInternalRepresentation(array<string, 3>* triplePtr) {
  auto& spo = *triplePtr;
  for (auto& el : spo) {
    el = _vocab.getLocaleManager().normalizeUtf8(el);
  }
  size_t upperBound = 3;
  string langtag;
  if (ad_utility::isXsdValue(spo[2])) {
    spo[2] = ad_utility::convertValueLiteralToIndexWord(spo[2]);
    upperBound = 2;
  } else if (isLiteral(spo[2])) {
    langtag = decltype(_vocab)::getLanguage(spo[2]);
  }

  for (size_t k = 0; k < upperBound; ++k) {
    if (_onDiskLiterals && _vocab.shouldBeExternalized(spo[k])) {
      spo[k] = EXTERNALIZED_LITERALS_PREFIX + spo[k];
    }
  }
  return langtag;
}

// ___________________________________________________________________________
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
}

// ___________________________________________________________________________
Id Index::assignNextId(Index::ItemMap* mapPtr, const string& key) {
  ItemMap& map = *mapPtr;
  if (!map.count(key)) {
    Id res = map.size();
    map[key] = map.size();
    return res;
  } else {
    return map[key];
  }
}

// ___________________________________________________________________________
pair<std::future<void>, std::future<void>> Index::writeNextPartialVocabulary(
    size_t numLines, size_t numFiles, size_t actualCurrentPartialSize,
    std::shared_ptr<const Index::ItemMap> items) {
  LOG(INFO) << "Lines (from KB-file) processed: " << numLines << '\n';
  LOG(INFO) << "Actual number of Triples in this section (include "
               "langfilter triples): "
            << actualCurrentPartialSize << '\n';
  std::future<void> fut1, fut2;
  string partialFilename =
      _onDiskBase + PARTIAL_VOCAB_FILE_NAME + std::to_string(numFiles);

  LOG(INFO) << "writing partial vocabulary to " << partialFilename << std::endl;
  LOG(INFO) << "it contains " << items->size() << " elements\n";
  auto identicalPred = [c = _vocab.getCaseComparator()](const auto& a,
                                                        const auto& b) {
    return c(a, b, decltype(c)::Level::IDENTICAL);
  };
  fut1 = std::async([&items, partialFilename, comp = identicalPred]() {
    writePartialIdMapToBinaryFileForMerging(items, partialFilename, comp);
  });

  if (_vocabPrefixCompressed) {
    // we also have to create the "ordinary" vocabulary order to make the
    // prefix compression work
    string partialTmpFilename = _onDiskBase + TMP_BASENAME_COMPRESSION +
                                PARTIAL_VOCAB_FILE_NAME +
                                std::to_string(numFiles);
    LOG(INFO) << "writing partial temporary vocabulary to "
              << partialTmpFilename << std::endl;
    LOG(INFO) << "it contains " << items->size() << " elements\n";
    fut2 = std::async([&items, partialTmpFilename]() {
      writePartialIdMapToBinaryFileForMerging(items, partialTmpFilename,
                                              std::less<std::string>());
    });
  }
  return {std::move(fut1), std::move(fut2)};
}
