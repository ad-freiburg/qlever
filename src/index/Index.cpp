// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./Index.h"
#include <algorithm>
#include <cmath>
#include <optional>
#include <cstdio>
#include <stxxl/algorithm>
#include <stxxl/map>
#include <unordered_set>
#include "../parser/NTriplesParser.h"
#include "../parser/TsvParser.h"
#include "../util/Conversions.h"
#include "./PrefixHeuristic.h"
#include "./VocabularyGenerator.h"

using std::array;

const uint32_t Index::PATTERNS_FILE_VERSION = 0;

// _____________________________________________________________________________
Index::Index()
    : _usePatterns(false),
      _maxNumPatterns(std::numeric_limits<PatternID>::max() - 2) {}

// _____________________________________________________________________________________________
template <class Parser>
Index::ExtVec Index::createExtVecAndVocab(const string& filename) {
  initializeVocabularySettingsBuild();

  auto linesAndWords =
      passFileForVocabulary<Parser>(filename, NUM_TRIPLES_PER_PARTIAL_VOCAB);
  size_t nofLines = linesAndWords.nofLines;
  // first save the total number of words, this is needed to initialize the
  // dense IndexMetaData variants
  _totalVocabularySize = linesAndWords.nofWords;

  if (_onDiskLiterals) {
    _vocab.externalizeLiteralsFromTextFile(
        _onDiskBase + EXTERNAL_LITS_TEXT_FILE_NAME,
        _onDiskBase + ".literals-index");
  }
  // clear vocabulary to save ram (only information from partial binary files
  // used from now on). This will preserve information about externalized
  // Prefixes etc.
  _vocab.clear();
  ExtVec idTriples(nofLines);
  passFileIntoIdVector<Parser>(filename, idTriples,
                               NUM_TRIPLES_PER_PARTIAL_VOCAB);

  if (!_keepTempFiles) {
    // remove temporary files only used during index creation
    LOG(INFO) << "Removing temporary files (partial vocabulary and external "
                 "text file...\n";

    // TODO: using system and rm is not really elegant nor portable.
    // use std::filesystem as soon as QLever is ported to C++17
    string removeCommand1 =
        "rm -- " + _onDiskBase + EXTERNAL_LITS_TEXT_FILE_NAME;
    bool w1 = system(removeCommand1.c_str());
    string removeCommand2 =
        "rm -- " + _onDiskBase + PARTIAL_VOCAB_FILE_NAME + "*";
    bool w2 = system(removeCommand2.c_str());
    if (w1 || w2) {
      LOG(INFO)
          << "Warning. Deleting of temporary files probably not successful\n";
    } else {
      LOG(INFO) << "Done.\n";
    }
  } else {
    LOG(INFO) << "Keeping temporary files (partial vocabulary and external "
                 "text file...\n";
  }
  return idTriples;
}

// _____________________________________________________________________________
template <class Parser>
void Index::createFromFile(const string& filename, bool allPermutations) {
  string indexFilename = _onDiskBase + ".index";
  _configurationJson["external-literals"] = _onDiskLiterals;

  ExtVec idTriples = createExtVecAndVocab<Parser>(filename);

  // if we have no compression, this will also copy the whole vocabulary.
  // but since we expect compression to be the default case, this  should not
  // hurt
  string vocabFile = _onDiskBase + ".vocabulary";
  string vocabFileTmp = _onDiskBase + ".vocabularyTmp";
  std::vector<string> prefixes;
  if (_vocabPrefixCompressed) {
    prefixes = calculatePrefixes(vocabFile, NUM_COMPRESSION_PREFIXES, 1, true);
  }
  _configurationJson["prefixes"] =
      Vocabulary<CompressedString>::prefixCompressFile(vocabFile, vocabFileTmp,
                                                       prefixes);
  // TODO<joka921> maybe move this to its own function
  if (std::rename(vocabFileTmp.c_str(), vocabFile.c_str())) {
    LOG(INFO) << "Error: Rename the prefixed vocab file " << vocabFileTmp
              << " to " << vocabFile << " set errno to " << errno
              << ". Terminating...\n";
    AD_CHECK(false);
  }
  // also perform unique for first permutation

  createPermutationPair<IndexMetaDataHmap>(&idTriples, Permutation::Pso,
                                           Permutation::Pos, true);
  if (allPermutations) {
    // also create Patterns after the Spo permutation
    createPermutationPair<IndexMetaDataMmap>(&idTriples, Permutation::Spo,
                                             Permutation::Sop, false, true);
    createPermutationPair<IndexMetaDataMmap>(&idTriples, Permutation::Osp,
                                             Permutation::Ops);
  } else if (_usePatterns) {
    // vector is not yet sorted
    createPatterns(false, &idTriples);
  }
  writeConfigurationFile();
}

// explicit instantiations
template void Index::createFromFile<TsvParser>(const string& filename,
                                               bool allPermutations);
template void Index::createFromFile<NTriplesParser>(const string& filename,
                                                    bool allPermutations);

// _____________________________________________________________________________
template <class Parser>
LinesAndWords Index::passFileForVocabulary(const string& filename,
                                           size_t linesPerPartial) {
  array<string, 3> spo;
  Parser p(filename);
  ad_utility::HashSet<string> items;
  size_t i = 0;
  size_t numFiles = 0;
  while (p.getLine(spo)) {
    tripleToInternalRepresentation(&spo);
    for (size_t k = 0; k < 3; ++k) {
      items.insert(spo[k]);
    }

    ++i;
    if (i % 10000000 == 0) {
      LOG(INFO) << "Lines processed: " << i << '\n';
    }

    if (i % linesPerPartial == 0) {
      LOG(INFO) << "Lines processed: " << i << '\n';
      string partialFilename =
          _onDiskBase + PARTIAL_VOCAB_FILE_NAME + std::to_string(numFiles);
      Vocabulary<string> vocab;
      vocab.createFromSet(items);
      LOG(INFO) << "writing partial vocabular to " << partialFilename
                << std::endl;
      vocab.writeToBinaryFileForMerging(partialFilename);
      LOG(INFO) << "Done\n";
      items.clear();
      numFiles++;
    }
  }
  // write Remainder
  //
  LOG(INFO) << "Lines processed: " << i << '\n';
  if (items.size() > 0) {
    // write remainder
    string partialFilename =
        _onDiskBase + PARTIAL_VOCAB_FILE_NAME + std::to_string(numFiles);
    LOG(INFO) << "writing partial vocabular to " << partialFilename
              << std::endl;
    Vocabulary<string> vocab;
    vocab.createFromSet(items);
    vocab.writeToBinaryFileForMerging(partialFilename);
    items.clear();
    numFiles++;
  }
  LOG(INFO) << "Merging vocabulary\n";
  LinesAndWords res;
  res.nofWords = mergeVocabulary(_onDiskBase, numFiles);
  LOG(INFO) << "Pass done.\n";
  res.nofLines = i;
  return res;
}

// _____________________________________________________________________________
template <class Parser>
void Index::passFileIntoIdVector(const string& filename, ExtVec& data,
                                 size_t linesPerPartial) {
  LOG(INFO) << "Making pass over NTriples " << filename
            << " and creating stxxl vector.\n";
  array<string, 3> spo;
  Parser p(filename);
  std::string vocabFilename(_onDiskBase + PARTIAL_VOCAB_FILE_NAME +
                            std::to_string(0));
  LOG(INFO) << "Reading partial vocab from " << vocabFilename << " ...\n";
  google::sparse_hash_map<string, Id> vocabMap =
      vocabMapFromPartialIndexedFile(vocabFilename);
  LOG(INFO) << "done reading partial vocab\n";
  size_t i = 0;
  size_t numFiles = 0;
  // write using vector_bufwriter
  ExtVec::bufwriter_type writer(data);
  while (p.getLine(spo)) {
    tripleToInternalRepresentation(&spo);

    bool broken = false;
    for (size_t k = 0; k < 3; ++k) {
      if (vocabMap.find(spo[k]) == vocabMap.end()) {
        LOG(INFO) << "not found in partial Vocab: " << spo[k] << '\n';
        broken = true;
      }
    }
    if (broken) continue;
    writer << array<Id, 3>{{vocabMap.find(spo[0])->second,
                            vocabMap.find(spo[1])->second,
                            vocabMap.find(spo[2])->second}};
    ++i;
    if (i % 100000 == 0) {
      LOG(INFO) << "Lines processed: " << i << '\n';
    }

    if (i % linesPerPartial == 0) {
      numFiles++;
      LOG(INFO) << "Lines processed: " << i << '\n';
      vocabFilename =
          _onDiskBase + PARTIAL_VOCAB_FILE_NAME + std::to_string(numFiles);
      LOG(INFO) << "Reading partial vocab from " << vocabFilename << " ...\n";
      vocabMap = vocabMapFromPartialIndexedFile(vocabFilename);
      LOG(INFO) << "done reading partial vocab\n";
    }
  }
  writer.finish();
  LOG(INFO) << "Pass done.\n";
}

// _____________________________________________________________________________
template <class MetaData>
std::optional<MetaData> Index::createPermutationImpl(const string& fileName,
                                                     Index::ExtVec const& vec,
                                                     size_t c0, size_t c1,
                                                     size_t c2) {
  MetaData metaData;
  if constexpr (metaData._isMmapBased) {
    metaData.setup(_totalVocabularySize, FullRelationMetaData::empty,
                   fileName + MMAP_FILE_SUFFIX);
  }

  if (vec.size() == 0) {
    LOG(WARN) << "Attempt to write an empty index!" << std::endl;
    return std::nullopt;
  }
  ad_utility::File out(fileName.c_str(), "w");
  LOG(INFO) << "Creating an on-disk index permutation of " << vec.size()
            << " elements / facts." << std::endl;
  // Iterate over the vector and identify relation boundaries
  size_t from = 0;
  Id currentRel = vec[0][c0];
  off_t lastOffset = 0;
  ad_utility::MmapVector<array<Id, 2>> buffer(0, fileName + ".tmp.MmapBuffer");
  bool functional = true;
  size_t distinctC1 = 0;
  size_t sizeOfRelation = 0;
  Id lastLhs = std::numeric_limits<Id>::max();
  for (ExtVec::bufreader_type reader(vec); !reader.empty(); ++reader) {
    if ((*reader)[c0] != currentRel) {
      auto md =
          writeRel(out, lastOffset, currentRel, buffer, distinctC1, functional);
      metaData.add(md.first, md.second);
      buffer.clear();
      distinctC1 = 0;
      lastOffset = metaData.getOffsetAfter();
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
        writeRel(out, lastOffset, currentRel, buffer, distinctC1, functional);
    metaData.add(md.first, md.second);
  }

  LOG(INFO) << "Done creating index permutation." << std::endl;
  LOG(INFO) << "Calculating statistics for this permutation.\n";
  metaData.calculateExpensiveStatistics();
  LOG(INFO) << "Writing statistics for this permutation:\n"
            << metaData.statistics() << std::endl;

  out.close();
  LOG(INFO) << "Permutation done.\n";
  return std::move(metaData);
}

// ________________________________________________________________________
template <class MetaData, class Comparator>
std::optional<MetaData> Index::createPermutation(
    ExtVec* vec, const Permutation::PermutationImpl<Comparator>& p,
    bool performUnique) {
  LOG(INFO) << "Sorting for " << p._readableName << " permutation..."
            << std::endl;
  stxxl::sort(begin(*vec), end(*vec), p._comp, STXXL_MEMORY_TO_USE);
  LOG(INFO) << "Sort done." << std::endl;

  if (performUnique) {
    // this only has to be done for the first permutation (PSO)
    LOG(INFO) << "Performing unique to ensure RDF semantics..." << std::endl;
    LOG(INFO) << "Size before: " << vec->size() << std::endl;
    auto last = std::unique(begin(*vec), end(*vec));
    vec->resize(size_t(last - vec->begin()));
    LOG(INFO) << "Done: unique." << std::endl;
    LOG(INFO) << "Size after: " << vec->size() << std::endl;
  }

  return createPermutationImpl<MetaData>(_onDiskBase + ".index" + p._fileSuffix,
                                         *vec, p._keyOrder[0], p._keyOrder[1],
                                         p._keyOrder[2]);
}

// ________________________________________________________________________
template <class MetaData, class Comparator1, class Comparator2>
void Index::createPermutationPair(
    ExtVec* vec, const Permutation::PermutationImpl<Comparator1>& p1,
    const Permutation::PermutationImpl<Comparator2>& p2, bool performUnique,
    bool createPatternsAfterFirst) {
  auto m1 = createPermutation<MetaData>(vec, p1, performUnique);
  if (createPatternsAfterFirst) {
    createPatterns(true, vec);
  }
  auto m2 = createPermutation<MetaData>(vec, p2, false);
  if (m1 && m2) {
    LOG(INFO) << "Exchanging Multiplicities for " << p1._readableName << " and "
              << p2._readableName << '\n';
    exchangeMultiplicities(&m1.value(), &m2.value());
    LOG(INFO) << "Done" << '\n';
    LOG(INFO) << "Writing MetaData for " << p1._readableName << " and "
              << p2._readableName << '\n';
    ad_utility::File f1(_onDiskBase + ".index" + p1._fileSuffix, "r+");
    m1.value().appendToFile(&f1);
    ad_utility::File f2(_onDiskBase + ".index" + p2._fileSuffix, "r+");
    m2.value().appendToFile(&f2);
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

// ____________________________________________________________________________
void Index::createPatterns(bool vecAlreadySorted, ExtVec* idTriples) {
  if (vecAlreadySorted) {
    LOG(INFO) << "Vector already sorted for pattern creation." << std::endl;
  } else {
    LOG(INFO) << "Sorting for pattern creation..." << std::endl;
    stxxl::sort(begin(*idTriples), end(*idTriples), SortBySPO(),
                STXXL_MEMORY_TO_USE);
    LOG(INFO) << "Sort done." << std::endl;
  }
  createPatternsImpl(_onDiskBase + ".index.patterns", *idTriples, _hasPredicate,
                     _hasPattern, _patterns,
                     _fullHasPredicateMultiplicityEntities,
                     _fullHasPredicateMultiplicityPredicates,
                     _fullHasPredicateSize, _maxNumPatterns);
}

// _____________________________________________________________________________
void Index::createPatternsImpl(const string& fileName, const ExtVec& vec,
                               CompactStringVector<Id, Id>& hasPredicate,
                               std::vector<PatternID>& hasPattern,
                               CompactStringVector<size_t, Id>& patterns,
                               double& fullHasPredicateMultiplicityEntities,
                               double& fullHasPredicateMultiplicityPredicates,
                               size_t& fullHasPredicateSize,
                               size_t maxNumPatterns) {
  if (vec.size() == 0) {
    LOG(WARN) << "Attempt to write an empty index!" << std::endl;
    return;
  }
  IndexMetaDataHmap meta;
  typedef std::unordered_map<Pattern, size_t> PatternsCountMap;

  LOG(INFO) << "Creating patterns file..." << std::endl;
  PatternsCountMap patternCounts;  //(4096 * 16, 4096 * 16);

  // determine the most common patterns
  Pattern pattern;

  size_t patternIndex = 0;
  Id currentRel;
  currentRel = vec[0][0];
  bool isValidPattern = true;
  size_t numInvalidPatterns = 0;
  size_t numValidPatterns = 0;

  for (ExtVec::bufreader_type reader(vec); !reader.empty(); ++reader) {
    if ((*reader)[0] != currentRel) {
      currentRel = (*reader)[0];
      if (isValidPattern) {
        numValidPatterns++;
        auto it = patternCounts.find(pattern);
        if (it == patternCounts.end()) {
          patternCounts.insert(std::pair<Pattern, size_t>(pattern, size_t(1)));
        } else {
          (*it).second++;
        }
      } else {
        numInvalidPatterns++;
      }
      isValidPattern = true;
      pattern.clear();
      patternIndex = 0;
    }
    // don't list predicates twice
    if (patternIndex == 0 || pattern[patternIndex - 1] != ((*reader)[1])) {
      pattern.push_back((*reader)[1]);
      patternIndex++;
    }
  }
  // process the last entry
  if (isValidPattern) {
    auto it = patternCounts.find(pattern);
    if (it == patternCounts.end()) {
      patternCounts.insert(std::pair<Pattern, size_t>(pattern, size_t(1)));
    } else {
      (*it).second++;
    }
  }
  LOG(INFO) << "Counted patterns and found " << patternCounts.size()
            << " distinct patterns." << std::endl;
  LOG(INFO) << "Patterns where found for " << numValidPatterns << " entities."
            << std::endl;
  LOG(INFO) << "Discarded the patterns of " << numInvalidPatterns
            << " entities"
               " because they were to large."
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
  for (auto& it : patternCounts) {
    if (sortedPatterns.size() < maxNumPatterns) {
      sortedPatterns.push_back(it);
      if (sortedPatterns.size() == maxNumPatterns) {
        LOG(DEBUG) << "Sorting patterns after initial insertions." << std::endl;
        // actuall sort the sorted patterns
        std::sort(sortedPatterns.begin(), sortedPatterns.end(),
                  [](const std::pair<Pattern, size_t>& first,
                     const std::pair<Pattern, size_t>& second) -> bool {
                    return first.second > second.second;
                  });
      }
    } else {
      if (it.second > sortedPatterns.back().second) {
        // The new element is larger than the smallest element in the vector.
        // Insert it into the correct position in the vector using binary
        // search.
        sortedPatterns.pop_back();
        auto sortedIt =
            std::lower_bound(sortedPatterns.begin(), sortedPatterns.end(), it,
                             [](const std::pair<Pattern, size_t>& p1,
                                const std::pair<Pattern, size_t>& p2) -> bool {
                               return p1.second > p2.second;
                             });
        sortedPatterns.insert(sortedIt, it);
      }
    }
  }
  if (sortedPatterns.size() < maxNumPatterns) {
    LOG(DEBUG) << "Sorting patterns after all insertions." << std::endl;
    // actuall sort the sorted patterns
    std::sort(sortedPatterns.begin(), sortedPatterns.end(),
              [](const std::pair<Pattern, size_t>& first,
                 const std::pair<Pattern, size_t>& second) -> bool {
                return first.second > second.second;
              });
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
  std::vector<std::array<Id, 2>> entityHasPattern;
  std::vector<std::array<Id, 2>> entityHasPredicate;

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
  currentRel = vec[0][0];
  patternIndex = 0;
  // Create the has-relation and has-pattern predicates
  for (ExtVec::bufreader_type reader2(vec); !reader2.empty(); ++reader2) {
    if ((*reader2)[0] != currentRel) {
      // we have arrived at a new entity;
      fullHasPredicateEntitiesDistinctSize++;
      std::unordered_map<Pattern, Id>::iterator it;
      if (isValidPattern) {
        it = patternSet.find(pattern);
      } else {
        it = patternSet.end();
        numInvalidEntities++;
      }
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
              std::array<Id, 2>{currentRel, pattern[i]});
        }
      } else {
        numEntitiesWithPatterns++;
        // The pattern does exist, add an entry to the has-pattern predicate
        entityHasPattern.push_back(std::array<Id, 2>{currentRel, it->second});
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
      currentRel = (*reader2)[0];
      patternIndex = 0;
      isValidPattern = true;
    }
    // don't list predicates twice
    if (patternIndex == 0 || pattern[patternIndex - 1] != ((*reader2)[1])) {
      pattern.push_back((*reader2)[1]);
      patternIndex++;
    }
  }
  // process the last element
  fullHasPredicateSize += pattern.size();
  fullHasPredicateEntitiesDistinctSize++;
  std::unordered_map<Pattern, Id>::iterator it;
  if (isValidPattern) {
    it = patternSet.find(pattern);
  } else {
    it = patternSet.end();
  }
  if (it == patternSet.end()) {
    numEntitiesWithoutPatterns++;
    // The pattern does not exist, use the has-relation predicate instead
    for (size_t i = 0; i < patternIndex; i++) {
      entityHasPredicate.push_back(std::array<Id, 2>{currentRel, pattern[i]});
      if (predicateHashSet.find(pattern[i]) == predicateHashSet.end()) {
        predicateHashSet.insert(pattern[i]);
        fullHasPredicatePredicatesDistinctSize++;
      }
    }
  } else {
    numEntitiesWithPatterns++;
    // The pattern does exist, add an entry to the has-pattern predicate
    entityHasPattern.push_back(std::array<Id, 2>{currentRel, it->second});
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
  ad_utility::File file(fileName.c_str(), "w");

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
    const ad_utility::MmapVector<array<Id, 2>>& data, size_t distinctC1,
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
    const MmapVector<array<Id, 2>>& data,
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
    ad_utility::File& out, const MmapVector<array<Id, 2>>& data,
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
void Index::createFromOnDiskIndex(const string& onDiskBase,
                                  bool allPermutations) {
  setOnDiskBase(onDiskBase);
  readConfigurationFile();
  _vocab.readFromFile(_onDiskBase + ".vocabulary",
                      _onDiskLiterals ? _onDiskBase + ".literals-index" : "");
  auto psoName = string(_onDiskBase + ".index.pso");
  _psoFile.open(psoName, "r");
  auto posName = string(_onDiskBase + ".index.pos");
  _posFile.open(posName, "r");
  AD_CHECK(_psoFile.isOpen() && _posFile.isOpen());
  // PSO

  _psoMeta.readFromFile(&_psoFile);
  LOG(INFO) << "Registered PSO permutation: " << _psoMeta.statistics()
            << std::endl;
  // POS
  _posMeta.readFromFile(&_posFile);
  LOG(INFO) << "Registered POS permutation: " << _posMeta.statistics()
            << std::endl;
  if (allPermutations) {
    // TODO<joka921> also refactor this, similar to createFromFile
    LOG(INFO) << "Setting up MmapBasedPermutations\n";
    _spoMeta.setup(onDiskBase + ".index.spo" + MMAP_FILE_SUFFIX,
                   ad_utility::ReuseTag(), ad_utility::AccessPattern::Random);
    _sopMeta.setup(onDiskBase + ".index.sop" + MMAP_FILE_SUFFIX,
                   ad_utility::ReuseTag(), ad_utility::AccessPattern::Random);
    _opsMeta.setup(onDiskBase + ".index.ops" + MMAP_FILE_SUFFIX,
                   ad_utility::ReuseTag(), ad_utility::AccessPattern::Random);
    _ospMeta.setup(onDiskBase + ".index.osp" + MMAP_FILE_SUFFIX,
                   ad_utility::ReuseTag(), ad_utility::AccessPattern::Random);
    LOG(INFO) << "Done\n";
    // TODO<joka921>: Refactor (upcoming PR): there is so  so much code
    // duplication in here
    auto spoName = string(_onDiskBase + ".index.spo");
    _spoFile.open(spoName, "r");
    auto sopName = string(_onDiskBase + ".index.sop");
    _sopFile.open(sopName, "r");
    auto ospName = string(_onDiskBase + ".index.osp");
    _ospFile.open(ospName, "r");
    auto opsName = string(_onDiskBase + ".index.ops");
    _opsFile.open(opsName, "r");
    AD_CHECK(_spoFile.isOpen() && _sopFile.isOpen() && _ospFile.isOpen() &&
             _opsFile.isOpen());
    // SPO
    _spoMeta.readFromFile(&_spoFile);
    LOG(INFO) << "Registered SPO permutation: " << _spoMeta.statistics()
              << std::endl;
    // SOP
    _sopMeta.readFromFile(&_sopFile);
    LOG(INFO) << "Registered SOP permutation: " << _sopMeta.statistics()
              << std::endl;
    // OSP
    _ospMeta.readFromFile(&_ospFile);
    LOG(INFO) << "Registered OSP permutation: " << _ospMeta.statistics()
              << std::endl;
    // OPS
    _opsMeta.readFromFile(&_opsFile);
    LOG(INFO) << "Registered OPS permutation: " << _opsMeta.statistics()
              << std::endl;
  }
  if (_usePatterns) {
    // Read the pattern info from the patterns file
    std::string patternsFilePath = _onDiskBase + ".index.patterns";
    ad_utility::File patternsFile;
    patternsFile.open(patternsFilePath.c_str(), "r");
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
bool Index::ready() const { return _psoFile.isOpen() && _posFile.isOpen(); }

// _____________________________________________________________________________
void Index::openFileHandles() {
  AD_CHECK(_onDiskBase.size() > 0);
  _psoFile.open((_onDiskBase + ".index.pso").c_str(), "r");
  _posFile.open((_onDiskBase + ".index.pos").c_str(), "r");
  if (ad_utility::File::exists(_onDiskBase + ".index.spo")) {
    _spoFile.open((_onDiskBase + ".index.spo").c_str(), "r");
  }
  if (ad_utility::File::exists(_onDiskBase + ".index.sop")) {
    _sopFile.open((_onDiskBase + ".index.sop").c_str(), "r");
  }
  if (ad_utility::File::exists(_onDiskBase + ".index.osp")) {
    _ospFile.open((_onDiskBase + ".index.osp").c_str(), "r");
  }
  if (ad_utility::File::exists(_onDiskBase + ".index.ops")) {
    _opsFile.open((_onDiskBase + ".index.ops").c_str(), "r");
  }
  AD_CHECK(_psoFile.isOpen());
  AD_CHECK(_posFile.isOpen());
}

// _____________________________________________________________________________
void Index::scanPSO(const string& predicate, WidthTwoList* result) const {
  LOG(DEBUG) << "Performing PSO scan for full relation: " << predicate << "\n";
  Id relId;
  if (_vocab.getId(predicate, &relId)) {
    LOG(TRACE) << "Successfully got key ID.\n";
    scanPSO(relId, result);
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanPSO(const string& predicate, const string& subject,
                    WidthOneList* result) const {
  LOG(DEBUG) << "Performing PSO scan of relation " << predicate
             << " with fixed subject: " << subject << "...\n";
  Id relId;
  Id subjId;
  if (_vocab.getId(predicate, &relId) && _vocab.getId(subject, &subjId)) {
    if (_psoMeta.relationExists(relId)) {
      auto rmd = _psoMeta.getRmd(relId);
      if (rmd.hasBlocks()) {
        pair<off_t, size_t> blockOff =
            rmd._rmdBlocks->getBlockStartAndNofBytesForLhs(subjId);
        // Functional relations have blocks point into the pair index,
        // non-functional relations have them point into lhs lists
        if (rmd.isFunctional()) {
          scanFunctionalRelation(blockOff, subjId, _psoFile, result);
        } else {
          pair<off_t, size_t> block2 =
              rmd._rmdBlocks->getFollowBlockForLhs(subjId);
          scanNonFunctionalRelation(blockOff, block2, subjId, _psoFile,
                                    rmd._rmdBlocks->_offsetAfter, result);
        }
      } else {
        // If we don't have blocks, scan the whole relation and filter /
        // restrict.
        WidthTwoList fullRelation;
        fullRelation.resize(rmd.getNofElements());
        _psoFile.read(fullRelation.data(),
                      rmd.getNofElements() * 2 * sizeof(Id),
                      rmd._rmdPairs._startFullIndex);
        getRhsForSingleLhs(fullRelation, subjId, result);
      }
    } else {
      LOG(DEBUG) << "No such relation.\n";
    }
  } else {
    LOG(DEBUG) << "No such subject.\n";
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanPOS(const string& predicate, WidthTwoList* result) const {
  LOG(DEBUG) << "Performing POS scan for full relation: " << predicate << "\n";
  Id relId;
  if (_vocab.getId(predicate, &relId)) {
    LOG(TRACE) << "Successfully got key ID.\n";
    scanPOS(relId, result);
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanPOS(const string& predicate, const string& object,
                    WidthOneList* result) const {
  LOG(DEBUG) << "Performing POS scan of relation " << predicate
             << " with fixed object: " << object << "...\n";
  Id relId;
  Id objId;
  if (_vocab.getId(predicate, &relId) && _vocab.getId(object, &objId)) {
    if (_posMeta.relationExists(relId)) {
      auto rmd = _posMeta.getRmd(relId);
      if (rmd.hasBlocks()) {
        pair<off_t, size_t> blockOff =
            rmd._rmdBlocks->getBlockStartAndNofBytesForLhs(objId);
        // Functional relations have blocks point into the pair index,
        // non-functional relations have them point into lhs lists
        if (rmd.isFunctional()) {
          scanFunctionalRelation(blockOff, objId, _posFile, result);
        } else {
          pair<off_t, size_t> block2 =
              rmd._rmdBlocks->getFollowBlockForLhs(objId);
          scanNonFunctionalRelation(blockOff, block2, objId, _posFile,
                                    rmd._rmdBlocks->_offsetAfter, result);
        }
      } else {
        // If we don't have blocks, scan the whole relation and filter /
        // restrict.
        WidthTwoList fullRelation;
        fullRelation.resize(rmd.getNofElements());
        _posFile.read(fullRelation.data(),
                      rmd.getNofElements() * 2 * sizeof(Id),
                      rmd._rmdPairs._startFullIndex);
        getRhsForSingleLhs(fullRelation, objId, result);
      }
    } else {
      LOG(DEBUG) << "No such relation.\n";
    }
  } else {
    LOG(DEBUG) << "No such object.\n";
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanSOP(const string& subject, const string& object,
                    WidthOneList* result) const {
  if (!_sopFile.isOpen()) {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "Cannot use predicate variables without the required "
             "index permutations. Build an index with option -a "
             "to use this feature.");
  }
  LOG(DEBUG) << "Performing SOP scan of list for " << subject
             << " with fixed object: " << object << "...\n";
  Id relId;
  Id objId;
  if (_vocab.getId(subject, &relId) && _vocab.getId(object, &objId)) {
    if (_sopMeta.relationExists(relId)) {
      auto rmd = _sopMeta.getRmd(relId);
      if (rmd.hasBlocks()) {
        pair<off_t, size_t> blockOff =
            rmd._rmdBlocks->getBlockStartAndNofBytesForLhs(objId);
        // Functional relations have blocks point into the pair index,
        // non-functional relations have them point into lhs lists
        if (rmd.isFunctional()) {
          scanFunctionalRelation(blockOff, objId, _sopFile, result);
        } else {
          pair<off_t, size_t> block2 =
              rmd._rmdBlocks->getFollowBlockForLhs(objId);
          scanNonFunctionalRelation(blockOff, block2, objId, _sopFile,
                                    rmd._rmdBlocks->_offsetAfter, result);
        }
      } else {
        // If we don't have blocks, scan the whole relation and filter /
        // restrict.
        WidthTwoList fullRelation;
        fullRelation.resize(rmd.getNofElements());
        _sopFile.read(fullRelation.data(),
                      rmd.getNofElements() * 2 * sizeof(Id),
                      rmd._rmdPairs._startFullIndex);
        getRhsForSingleLhs(fullRelation, objId, result);
      }
    } else {
      LOG(DEBUG) << "No such relation.\n";
    }
  } else {
    LOG(DEBUG) << "No such object.\n";
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanSPO(const string& subject, WidthTwoList* result) const {
  if (!_spoFile.isOpen()) {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "Cannot use predicate variables without the required "
             "index permutations. Build an index with option -a "
             "to use this feature.");
  }
  LOG(DEBUG) << "Performing SPO scan for full list for: " << subject << "\n";
  Id relId;
  if (_vocab.getId(subject, &relId)) {
    LOG(TRACE) << "Successfully got key ID.\n";
    scanSPO(relId, result);
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanSOP(const string& subject, WidthTwoList* result) const {
  if (!_sopFile.isOpen()) {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "Cannot use predicate variables without the required "
             "index permutations. Build an index with option -a "
             "to use this feature.");
  }
  LOG(DEBUG) << "Performing SOP scan for full list for: " << subject << "\n";
  Id relId;
  if (_vocab.getId(subject, &relId)) {
    LOG(TRACE) << "Successfully got key ID.\n";
    scanSOP(relId, result);
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanOPS(const string& object, WidthTwoList* result) const {
  if (!_opsFile.isOpen()) {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "Cannot use predicate variables without the required "
             "index permutations. Build an index with option -a "
             "to use this feature.");
  }
  LOG(DEBUG) << "Performing OPS scan for full list for: " << object << "\n";
  Id relId;
  if (_vocab.getId(object, &relId)) {
    LOG(TRACE) << "Successfully got key ID.\n";
    scanOPS(relId, result);
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanOSP(const string& object, WidthTwoList* result) const {
  if (!_ospFile.isOpen()) {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "Cannot use predicate variables without the required "
             "index permutations. Build an index with option -a "
             "to use this feature.");
  }
  LOG(DEBUG) << "Performing OSP scan for full list for: " << object << "\n";
  Id relId;
  if (_vocab.getId(object, &relId)) {
    LOG(TRACE) << "Successfully got key ID.\n";
    scanOSP(relId, result);
  }
  LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
}

// _____________________________________________________________________________
void Index::scanPSO(Id predicate, Index::WidthTwoList* result) const {
  if (_psoMeta.relationExists(predicate)) {
    const FullRelationMetaData& rmd = _psoMeta.getRmd(predicate)._rmdPairs;
    result->reserve(rmd.getNofElements() + 2);
    result->resize(rmd.getNofElements());
    _psoFile.read(result->data(), rmd.getNofElements() * 2 * sizeof(Id),
                  rmd._startFullIndex);
  }
}

// _____________________________________________________________________________
void Index::scanPOS(Id predicate, Index::WidthTwoList* result) const {
  if (_posMeta.relationExists(predicate)) {
    const FullRelationMetaData& rmd = _posMeta.getRmd(predicate)._rmdPairs;
    result->reserve(rmd.getNofElements() + 2);
    result->resize(rmd.getNofElements());
    _posFile.read(result->data(), rmd.getNofElements() * 2 * sizeof(Id),
                  rmd._startFullIndex);
  }
}

// _____________________________________________________________________________
void Index::scanSPO(Id subject, Index::WidthTwoList* result) const {
  if (_spoMeta.relationExists(subject)) {
    const FullRelationMetaData& rmd = _spoMeta.getRmd(subject)._rmdPairs;
    result->reserve(rmd.getNofElements() + 2);
    result->resize(rmd.getNofElements());
    _spoFile.read(result->data(), rmd.getNofElements() * 2 * sizeof(Id),
                  rmd._startFullIndex);
  }
}

// _____________________________________________________________________________
void Index::scanSOP(Id subject, Index::WidthTwoList* result) const {
  if (_sopMeta.relationExists(subject)) {
    const FullRelationMetaData& rmd = _sopMeta.getRmd(subject)._rmdPairs;
    result->reserve(rmd.getNofElements() + 2);
    result->resize(rmd.getNofElements());
    _sopFile.read(result->data(), rmd.getNofElements() * 2 * sizeof(Id),
                  rmd._startFullIndex);
  }
}

// _____________________________________________________________________________
void Index::scanOSP(Id object, Index::WidthTwoList* result) const {
  if (_ospMeta.relationExists(object)) {
    const FullRelationMetaData& rmd = _ospMeta.getRmd(object)._rmdPairs;
    result->reserve(rmd.getNofElements() + 2);
    result->resize(rmd.getNofElements());
    _ospFile.read(result->data(), rmd.getNofElements() * 2 * sizeof(Id),
                  rmd._startFullIndex);
  }
}

// _____________________________________________________________________________
void Index::scanOPS(Id object, Index::WidthTwoList* result) const {
  if (_opsMeta.relationExists(object)) {
    const FullRelationMetaData& rmd = _opsMeta.getRmd(object)._rmdPairs;
    result->reserve(rmd.getNofElements() + 2);
    result->resize(rmd.getNofElements());
    _opsFile.read(result->data(), rmd.getNofElements() * 2 * sizeof(Id),
                  rmd._startFullIndex);
  }
}

// _____________________________________________________________________________
void Index::throwExceptionIfNoPatterns() const {
  if (!_usePatterns) {
    AD_THROW(ad_semsearch::Exception::CHECK_FAILED,
             "The requested feature requires a loaded patterns file (Use the "
             "-P or --patterns option when running the executable).");
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
                                   WidthOneList* result) const {
  LOG(TRACE) << "Scanning functional relation ...\n";
  WidthTwoList block;
  block.resize(blockOff.second / (2 * sizeof(Id)));
  indexFile.read(block.data(), blockOff.second, blockOff.first);
  auto it = std::lower_bound(
      block.begin(), block.end(), lhsId,
      [](const array<Id, 2>& elem, Id key) { return elem[0] < key; });
  if ((*it)[0] == lhsId) {
    result->push_back(array<Id, 1>{(*it)[1]});
  }
  LOG(TRACE) << "Read " << result->size() << " RHS.\n";
}

// _____________________________________________________________________________
void Index::scanNonFunctionalRelation(const pair<off_t, size_t>& blockOff,
                                      const pair<off_t, size_t>& followBlock,
                                      Id lhsId, ad_utility::File& indexFile,
                                      off_t upperBound,
                                      Index::WidthOneList* result) const {
  LOG(TRACE) << "Scanning non-functional relation ...\n";
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
    result->reserve((nofBytes / sizeof(Id)) + 2);
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
    if (this->_psoMeta.relationExists(relId)) {
      return this->_psoMeta.getRmd(relId).getNofElements();
    }
  }
  return 0;
}

// _____________________________________________________________________________
size_t Index::subjectCardinality(const string& sub) const {
  Id relId;
  if (_vocab.getId(sub, &relId)) {
    if (this->_spoMeta.relationExists(relId)) {
      return this->_spoMeta.getRmd(relId).getNofElements();
    }
  }
  return 0;
}

// _____________________________________________________________________________
size_t Index::objectCardinality(const string& obj) const {
  Id relId;
  if (_vocab.getId(obj, &relId)) {
    if (this->_ospMeta.relationExists(relId)) {
      return this->_ospMeta.getRmd(relId).getNofElements();
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
  std::ofstream f(filename.c_str());

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
  return Vocabulary<string>::isLiteral(object);
}

// _____________________________________________________________________________
bool Index::shouldBeExternalized(const string& object) {
  return _vocab.shouldBeExternalized(object);
}

// _____________________________________________________________________________
vector<float> Index::getPSOMultiplicities(const string& key) const {
  Id keyId;
  vector<float> res;
  if (_vocab.getId(key, &keyId) && _psoMeta.relationExists(keyId)) {
    auto rmd = _psoMeta.getRmd(keyId);
    auto logM1 = rmd.getCol1LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM1)));
    auto logM2 = rmd.getCol2LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM2)));
  } else {
    res.push_back(1);
    res.push_back(1);
  }
  return res;
}

// _____________________________________________________________________________
vector<float> Index::getPOSMultiplicities(const string& key) const {
  Id keyId;
  vector<float> res;
  if (_vocab.getId(key, &keyId) && _posMeta.relationExists(keyId)) {
    auto rmd = _posMeta.getRmd(keyId);
    auto logM1 = rmd.getCol1LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM1)));
    auto logM2 = rmd.getCol2LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM2)));
  } else {
    res.push_back(1);
    res.push_back(1);
  }
  return res;
}

// _____________________________________________________________________________
vector<float> Index::getSPOMultiplicities(const string& key) const {
  Id keyId;
  vector<float> res;
  if (_vocab.getId(key, &keyId) && _spoMeta.relationExists(keyId)) {
    auto rmd = _spoMeta.getRmd(keyId);
    auto logM1 = rmd.getCol1LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM1)));
    auto logM2 = rmd.getCol2LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM2)));
  } else {
    res.push_back(1);
    res.push_back(1);
  }
  return res;
}

// _____________________________________________________________________________
vector<float> Index::getSOPMultiplicities(const string& key) const {
  Id keyId;
  vector<float> res;
  if (_vocab.getId(key, &keyId) && _sopMeta.relationExists(keyId)) {
    auto rmd = _sopMeta.getRmd(keyId);
    auto logM1 = rmd.getCol1LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM1)));
    auto logM2 = rmd.getCol2LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM2)));
  } else {
    res.push_back(1);
    res.push_back(1);
  }
  return res;
}

// _____________________________________________________________________________
vector<float> Index::getOSPMultiplicities(const string& key) const {
  Id keyId;
  vector<float> res;
  if (_vocab.getId(key, &keyId) && _ospMeta.relationExists(keyId)) {
    auto rmd = _ospMeta.getRmd(keyId);
    auto logM1 = rmd.getCol1LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM1)));
    auto logM2 = rmd.getCol2LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM2)));
  } else {
    res.push_back(1);
    res.push_back(1);
  }
  return res;
}

// _____________________________________________________________________________
vector<float> Index::getOPSMultiplicities(const string& key) const {
  Id keyId;
  vector<float> res;
  if (_vocab.getId(key, &keyId) && _opsMeta.relationExists(keyId)) {
    auto rmd = _opsMeta.getRmd(keyId);
    auto logM1 = rmd.getCol1LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM1)));
    auto logM2 = rmd.getCol2LogMultiplicity();
    res.push_back(static_cast<float>(pow(2, logM2)));
  } else {
    res.push_back(1);
    res.push_back(1);
  }
  return res;
}

// _____________________________________________________________________________
vector<float> Index::getSPOMultiplicities() const {
  return vector<float>{
      {static_cast<float>(getNofTriples() / getNofSubjects()),
       static_cast<float>(getNofTriples() / getNofPredicates()),
       static_cast<float>(getNofTriples() / getNofObjects())}};
}

// _____________________________________________________________________________
vector<float> Index::getSOPMultiplicities() const {
  return vector<float>{
      {static_cast<float>(getNofTriples() / getNofSubjects()),
       static_cast<float>(getNofTriples() / getNofObjects()),
       static_cast<float>(getNofTriples() / getNofPredicates())}};
}

// _____________________________________________________________________________
vector<float> Index::getPSOMultiplicities() const {
  return vector<float>{
      {static_cast<float>(getNofTriples() / getNofPredicates()),
       static_cast<float>(getNofTriples() / getNofSubjects()),
       static_cast<float>(getNofTriples() / getNofObjects())}};
}

// _____________________________________________________________________________
vector<float> Index::getPOSMultiplicities() const {
  return vector<float>{
      {static_cast<float>(getNofTriples() / getNofPredicates()),
       static_cast<float>(getNofTriples() / getNofObjects()),
       static_cast<float>(getNofTriples() / getNofSubjects())}};
}

// _____________________________________________________________________________
vector<float> Index::getOSPMultiplicities() const {
  return vector<float>{
      {static_cast<float>(getNofTriples() / getNofObjects()),
       static_cast<float>(getNofTriples() / getNofSubjects()),
       static_cast<float>(getNofTriples() / getNofPredicates())}};
}

// _____________________________________________________________________________
vector<float> Index::getOPSMultiplicities() const {
  return vector<float>{
      {static_cast<float>(getNofTriples() / getNofObjects()),
       static_cast<float>(getNofTriples() / getNofPredicates()),
       static_cast<float>(getNofTriples() / getNofSubjects())}};
}

// _____________________________________________________________________________
void Index::setKbName(const string& name) {
  _psoMeta.setName(name);
  _posMeta.setName(name);
  _spoMeta.setName(name);
  _sopMeta.setName(name);
  _ospMeta.setName(name);
  _opsMeta.setName(name);
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
void Index::writeConfigurationFile() const {
  std::ofstream f(_onDiskBase + CONFIGURATION_FILE);
  AD_CHECK(f.is_open());
  f << _configurationJson;
}

// ___________________________________________________________________________
void Index::readConfigurationFile() {
  std::ifstream f(_onDiskBase + CONFIGURATION_FILE);
  AD_CHECK(f.is_open());
  f >> _configurationJson;
  if (_configurationJson.find("external-literals") !=
      _configurationJson.end()) {
    _onDiskLiterals = _configurationJson["external-literals"];
  }

  if (_configurationJson.find("prefixes") != _configurationJson.end()) {
    _vocab.initializeRestartPrefixes(_configurationJson["prefixes"]);
  }

  if (_configurationJson.find("prefixes-external") !=
      _configurationJson.end()) {
    _vocab.initializeExternalizePrefixes(
        _configurationJson["prefixes-external"]);
  }
}

// ___________________________________________________________________________
void Index::tripleToInternalRepresentation(array<string, 3>* triplePtr) {
  auto& spo = *triplePtr;
  size_t upperBound = 3;
  if (ad_utility::isXsdValue(spo[2])) {
    spo[2] = ad_utility::convertValueLiteralToIndexWord(spo[2]);
    upperBound = 2;
  }
  for (size_t k = 0; k < upperBound; ++k) {
    if (_onDiskLiterals && _vocab.shouldBeExternalized(spo[k])) {
      spo[k] = string({EXTERNALIZED_LITERALS_PREFIX}) + spo[k];
    }
  }
}

// ___________________________________________________________________________
void Index::initializeVocabularySettingsBuild() {
  if (_settingsFileName == "") {
    return;
  }
  std::ifstream f(_settingsFileName);
  AD_CHECK(f.is_open());
  json j;
  f >> j;

  if (j.find("prefixes-external") != j.end()) {
    _vocab.initializeExternalizePrefixes(j["prefixes-external"]);
    _configurationJson["prefixes-external"] = j["prefixes-external"];
  }
}
