#include "PatternIndex.h"

#include "IndexMetaData.h"

const uint32_t PatternIndex::PATTERNS_FILE_VERSION = 1;

// TODO: stxxl::vector<array<Id, 3>> should be Index::TripleVec but that
// creates a circular dependency
using TripleVec = stxxl::vector<array<Id, 3>>;

PatternIndex::PatternIndex()
    : _maxNumPatterns(std::numeric_limits<PatternID>::max() - 2),
      _initialized(false) {}

// _____________________________________________________________________________
void PatternIndex::generatePredicateLocalNamespace(VocabularyData* vocabData) {
  // Create a new namespace for predicates containing only predicates.
  // This will be significantly smaller than the global namespace which
  // also contains subjects and objects, and allows for shrinking
  // the pattern trick data.
  createPredicateIdsImpl<TripleVec::bufreader_type>(
      &_predicate_local_to_global_ids, vocabData->langPredLowerBound,
      vocabData->langPredUpperBound, *vocabData->idTriples);
  // Compute the global to local mapping from the local to global mapping
  _predicate_global_to_local_ids.reserve(_predicate_local_to_global_ids.size());
  for (size_t i = 0; i < _predicate_local_to_global_ids.size(); ++i) {
    _predicate_global_to_local_ids.try_emplace(
        _predicate_local_to_global_ids[i], i);
  }
}

// _____________________________________________________________________________
std::shared_ptr<PatternContainer> PatternIndex::getPatternData() {
  return _pattern_container;
}

// _____________________________________________________________________________
std::shared_ptr<const PatternContainer> PatternIndex::getPatternData() const {
  return _pattern_container;
}

// _____________________________________________________________________________
const std::vector<Id> PatternIndex::getPredicateGlobalIds() const {
  return _predicate_local_to_global_ids;
}

// _____________________________________________________________________________
double PatternIndex::getHasPredicateMultiplicityEntities() const {
  throwExceptionIfNotInitialized();
  return _fullHasPredicateMultiplicityEntities;
}

// _____________________________________________________________________________
double PatternIndex::getHasPredicateMultiplicityPredicates() const {
  throwExceptionIfNotInitialized();
  return _fullHasPredicateMultiplicityPredicates;
}

// _____________________________________________________________________________
size_t PatternIndex::getHasPredicateFullSize() const {
  throwExceptionIfNotInitialized();
  return _fullHasPredicateSize;
}

// _____________________________________________________________________________
void PatternIndex::createPatterns(VocabularyData* vocabData,
                                  const std::string& filename_base) {
  // Determine the number of bytes required for the predicate local namespace
  size_t num_bytes_predicate_id = 0;
  size_t num_predicate_ids = _predicate_local_to_global_ids.size();
  while (num_predicate_ids > 0) {
    num_predicate_ids = num_predicate_ids >> 8;
    num_bytes_predicate_id++;
  }

  std::string patterns_file_name = filename_base + ".index.patterns";

  if (num_bytes_predicate_id <= 1) {
    std::shared_ptr<PatternContainerImpl<uint8_t>> pattern_data =
        std::make_shared<PatternContainerImpl<uint8_t>>();
    createPatternsImpl<uint8_t, TripleVec::bufreader_type>(
        patterns_file_name, pattern_data, _predicate_local_to_global_ids,
        _predicate_global_to_local_ids, _fullHasPredicateMultiplicityEntities,
        _fullHasPredicateMultiplicityPredicates, _fullHasPredicateSize,
        _maxNumPatterns, vocabData->langPredLowerBound,
        vocabData->langPredUpperBound, *vocabData->idTriples);
    _pattern_container = pattern_data;
  } else if (num_bytes_predicate_id <= 2) {
    std::shared_ptr<PatternContainerImpl<uint16_t>> pattern_data =
        std::make_shared<PatternContainerImpl<uint16_t>>();
    createPatternsImpl<uint16_t, TripleVec::bufreader_type>(
        patterns_file_name, pattern_data, _predicate_local_to_global_ids,
        _predicate_global_to_local_ids, _fullHasPredicateMultiplicityEntities,
        _fullHasPredicateMultiplicityPredicates, _fullHasPredicateSize,
        _maxNumPatterns, vocabData->langPredLowerBound,
        vocabData->langPredUpperBound, *vocabData->idTriples);
    _pattern_container = pattern_data;
  } else if (num_bytes_predicate_id <= 4) {
    std::shared_ptr<PatternContainerImpl<uint32_t>> pattern_data =
        std::make_shared<PatternContainerImpl<uint32_t>>();
    createPatternsImpl<uint32_t, TripleVec::bufreader_type>(
        patterns_file_name, pattern_data, _predicate_local_to_global_ids,
        _predicate_global_to_local_ids, _fullHasPredicateMultiplicityEntities,
        _fullHasPredicateMultiplicityPredicates, _fullHasPredicateSize,
        _maxNumPatterns, vocabData->langPredLowerBound,
        vocabData->langPredUpperBound, *vocabData->idTriples);
    _pattern_container = pattern_data;
  } else if (num_bytes_predicate_id <= 8) {
    std::shared_ptr<PatternContainerImpl<uint64_t>> pattern_data =
        std::make_shared<PatternContainerImpl<uint64_t>>();
    createPatternsImpl<uint64_t, TripleVec::bufreader_type>(
        patterns_file_name, pattern_data, _predicate_local_to_global_ids,
        _predicate_global_to_local_ids, _fullHasPredicateMultiplicityEntities,
        _fullHasPredicateMultiplicityPredicates, _fullHasPredicateSize,
        _maxNumPatterns, vocabData->langPredLowerBound,
        vocabData->langPredUpperBound, *vocabData->idTriples);
    _pattern_container = pattern_data;
  } else {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "The index contains more than 2**64 predicates.");
  }
  _initialized = true;
}

// _____________________________________________________________________________
void PatternIndex::loadPatternIndex(const std::string& filename_base) {
  std::string patternsFilePath = filename_base + ".index.patterns";
  ad_utility::File patternsFile;
  patternsFile.open(patternsFilePath, "r");
  AD_CHECK(patternsFile.isOpen());
  uint8_t firstByte;
  patternsFile.readOrThrow(&firstByte, sizeof(uint8_t));
  uint32_t version;
  patternsFile.readOrThrow(&version, sizeof(uint32_t));
  if (version != PATTERNS_FILE_VERSION || firstByte != 255) {
    version = firstByte == 255 ? version : -1;
    patternsFile.close();
    std::ostringstream oss;
    oss << "The patterns file " << patternsFilePath << " version of " << version
        << " does not match the programs pattern file "
        << "version of " << PATTERNS_FILE_VERSION << ". Rebuild the index"
        << " or start the query engine without pattern support." << std::endl;
    throw std::runtime_error(oss.str());
  }
  patternsFile.readOrThrow(&_fullHasPredicateMultiplicityEntities,
                           sizeof(double));
  patternsFile.readOrThrow(&_fullHasPredicateMultiplicityPredicates,
                           sizeof(double));
  patternsFile.readOrThrow(&_fullHasPredicateSize, sizeof(size_t));

  // read the mapping from predicate local ids to global ids
  uint64_t predicate_local_ns_size;
  patternsFile.readOrThrow(&predicate_local_ns_size, sizeof(uint64_t));
  std::cout << "Got " << predicate_local_ns_size << " distinct predicates"
            << std::endl;
  _predicate_local_to_global_ids.resize(predicate_local_ns_size);
  patternsFile.readOrThrow(_predicate_local_to_global_ids.data(),
                           predicate_local_ns_size * sizeof(Id));

  size_t num_bytes_predicate_id = 0;
  size_t num_predicate_ids = _predicate_local_to_global_ids.size();
  while (num_predicate_ids > 0) {
    num_predicate_ids = num_predicate_ids >> 8;
    num_bytes_predicate_id++;
  }
  std::cout << "Using " << num_bytes_predicate_id << " bytes per predicate"
            << std::endl;
  if (num_bytes_predicate_id <= 1) {
    _pattern_container = loadPatternData<uint8_t>(&patternsFile);
  } else if (num_bytes_predicate_id <= 2) {
    _pattern_container = loadPatternData<uint16_t>(&patternsFile);
  } else if (num_bytes_predicate_id <= 4) {
    _pattern_container = loadPatternData<uint32_t>(&patternsFile);
  } else if (num_bytes_predicate_id <= 8) {
    _pattern_container = loadPatternData<uint64_t>(&patternsFile);
  } else {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "The index contains more than 2**64 predicates.");
  }
  _initialized = true;
}

// _____________________________________________________________________________
template <typename PredicateId, typename VecReaderType, typename... Args>
void PatternIndex::createPatternsImpl(
    const string& fileName,
    std::shared_ptr<PatternContainerImpl<PredicateId>> pattern_data,
    const std::vector<Id>& predicate_global_id,
    const ad_utility::HashMap<Id, size_t>& predicate_local_id,
    double& fullHasPredicateMultiplicityEntities,
    double& fullHasPredicateMultiplicityPredicates,
    size_t& fullHasPredicateSize, const size_t maxNumPatterns,
    const Id langPredLowerBound, const Id langPredUpperBound,
    const Args&... vecReaderArgs) {
  IndexMetaDataHmap meta;
  typedef ad_utility::HashMap<Pattern<PredicateId>, size_t,
                              PatternHash<PredicateId>>
      PatternsCountMap;
  typedef ad_utility::HashMap<Pattern<PredicateId>, PatternID,
                              PatternHash<PredicateId>>
      PatternIdMap;
  // Used for the entityHasPattern relation
  // This struct is 16 bytes, not 12, due to padding (at least on gcc 10)
  struct SubjectPatternPair {
    Id subject;
    PatternID pattern;
  };

  LOG(INFO) << "Creating patterns file..." << std::endl;
  VecReaderType reader(vecReaderArgs...);
  if (reader.empty()) {
    LOG(WARN) << "Triple vector was empty, no patterns created" << std::endl;
    return;
  }

  PatternsCountMap patternCounts;
  // determine the most common patterns
  Pattern<PredicateId> pattern;

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
        patternCounts.insert(
            std::pair<Pattern<PredicateId>, size_t>(pattern, size_t(1)));
      } else {
        (*it).second++;
      }
      pattern.clear();
      patternIndex = 0;
    }

    // don't list predicates twice
    if (patternIndex == 0 || pattern[patternIndex - 1] !=
                                 predicate_local_id.find(triple[1])->second) {
      // Ignore @..@ type language predicates
      if (triple[1] < langPredLowerBound || triple[1] >= langPredUpperBound) {
        pattern.push_back(predicate_local_id.find(triple[1])->second);
        patternIndex++;
      }
    }
  }
  // process the last entry
  auto it = patternCounts.find(pattern);
  if (it == patternCounts.end()) {
    patternCounts.insert(
        std::pair<Pattern<PredicateId>, size_t>(pattern, size_t(1)));
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
  std::vector<std::pair<Pattern<PredicateId>, size_t>> sortedPatterns;
  sortedPatterns.reserve(actualNumPatterns);
  auto comparePatternCounts =
      [](const std::pair<Pattern<PredicateId>, size_t>& first,
         const std::pair<Pattern<PredicateId>, size_t>& second) -> bool {
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
  std::vector<std::vector<PredicateId>> buffer;
  buffer.reserve(sortedPatterns.size());
  for (const auto& p : sortedPatterns) {
    buffer.push_back(p.first._data);
  }
  pattern_data->patterns().build(buffer);

  PatternIdMap patternSet;
  patternSet.reserve(sortedPatterns.size());
  for (size_t i = 0; i < sortedPatterns.size(); i++) {
    patternSet.try_emplace(sortedPatterns[i].first, i);
  }

  LOG(DEBUG) << "Pattern set size: " << patternSet.size() << std::endl;

  // Associate entities with patterns if possible, store has-relation otherwise
  ad_utility::MmapVectorTmp<SubjectPatternPair> entityHasPattern(
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
      // listed once per entity (otherwise it would always be the same as
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
        entityHasPattern.push_back(SubjectPatternPair{currentSubj, it->second});
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
    if (patternIndex == 0 || pattern[patternIndex - 1] !=
                                 predicate_local_id.find(triple[1])->second) {
      // Ignore @..@ type language predicates
      if (triple[1] < langPredLowerBound || triple[1] >= langPredUpperBound) {
        pattern.push_back(predicate_local_id.find(triple[1])->second);
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
    entityHasPattern.push_back(SubjectPatternPair{currentSubj, last->second});
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

  LOG(INFO) << "Found " << pattern_data->patterns().size()
            << " distinct patterns." << std::endl;
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
  uint8_t firstByte = 255;
  file.write(&firstByte, sizeof(uint8_t));
  file.write(&PATTERNS_FILE_VERSION, sizeof(uint32_t));
  file.write(&fullHasPredicateMultiplicityEntities, sizeof(double));
  file.write(&fullHasPredicateMultiplicityPredicates, sizeof(double));
  file.write(&fullHasPredicateSize, sizeof(size_t));

  // Write the mapping from the predicate local to the global namespace
  uint64_t local_predicate_ns_size = predicate_global_id.size();
  std::cout << "Got " << local_predicate_ns_size << " distinct predicates"
            << std::endl;
  std::cout << "Using " << sizeof(PredicateId) << " bytes per predicate"
            << std::endl;
  file.write(&local_predicate_ns_size, sizeof(uint64_t));
  file.write(predicate_global_id.data(),
             predicate_global_id.size() * sizeof(Id));

  // write the entityHasPatterns vector
  size_t numHasPatterns = entityHasPattern.size();
  std::cout << "has pattern size " << numHasPatterns << std::endl;
  file.write(&numHasPatterns, sizeof(size_t));

  // The SubjectPatternPair struct contains padding which increases the size
  // of this part of the file by a third. To avoid that we pack a chunk of
  // SubjectPatternPairs densely, and then write that densely packed chunk
  // into the file.
  constexpr size_t CHUNK_SIZE = 2048;
  char chunk[CHUNK_SIZE * sizeof(uint64_t) * sizeof(uint32_t)];
  size_t num_chunks = entityHasPattern.size() / CHUNK_SIZE;
  if (entityHasPattern.size() % CHUNK_SIZE != 0) {
    num_chunks++;
  }
  std::cout << "num chunks " << num_chunks << std::endl;
  for (size_t chunk_idx = 0; chunk_idx < num_chunks; chunk_idx++) {
    size_t offset = chunk_idx * CHUNK_SIZE;
    size_t count = std::min(CHUNK_SIZE, entityHasPattern.size() - offset);
    // Copy the data skipping the struct padding
    for (size_t i = 0; i < count; ++i) {
      std::memcpy(chunk + (i * 12), &entityHasPattern[offset + i].subject, 12);
    }
    // write a chunk to the file
    file.write(chunk, count * (sizeof(uint64_t) + sizeof(uint32_t)));
  }

  // write the entityHasPredicate vector
  size_t numHasPredicates = entityHasPredicate.size();
  std::cout << "hasPredicatesize" << numHasPredicates << std::endl;
  file.write(&numHasPredicates, sizeof(size_t));
  static_assert(sizeof(std::array<Id, 2>) == sizeof(Id) * 2,
                "std::array<Id, 2> has padding, unable to serialize and "
                "deserialize the hasPredicate vector.");
  file.write(entityHasPredicate.data(), sizeof(Id) * numHasPredicates * 2);

  // write the patterns
  pattern_data->patterns().write(file);
  file.close();

  LOG(INFO) << "Done creating patterns file." << std::endl;

  // create the has-relation and has-pattern lookup vectors
  if (entityHasPattern.size() > 0) {
    std::vector<PatternID>& hasPattern = pattern_data->hasPattern();
    hasPattern.resize(entityHasPattern.back().subject + 1);
    size_t pos = 0;
    for (size_t i = 0; i < entityHasPattern.size(); i++) {
      while (entityHasPattern[i].subject > pos) {
        hasPattern[pos] = NO_PATTERN;
        pos++;
      }
      hasPattern[pos] = entityHasPattern[i].pattern;
      pos++;
    }
  }

  vector<vector<PredicateId>> hasPredicateTmp;
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
  pattern_data->hasPredicate().build(hasPredicateTmp);
}

// _____________________________________________________________________________
template <typename VecReaderType, typename... Args>
void PatternIndex::createPredicateIdsImpl(std::vector<Id>* predicateIds,
                                          const Id langPredLowerBound,
                                          const Id langPredUpperBound,
                                          const Args&... vecReaderArgs) {
  VecReaderType reader(vecReaderArgs...);
  if (reader.empty()) {
    LOG(WARN) << "Triple vector was empty, no patterns created" << std::endl;
    return;
  }

  Id currentPred = ID_NO_VALUE;

  // Iterate all triples in POS (or PSO) sorting order. Add every distinct
  // non language predicate to the predicateIds vector, therefore assigning
  // a predicate namespace id to it via its position in the vector.
  for (; !reader.empty(); ++reader) {
    Id predicate = (*reader)[1];
    if (predicate != currentPred) {
      currentPred = predicate;
      if (predicate < langPredLowerBound || predicate >= langPredUpperBound) {
        // The predicate is not a language predicate, add it to the ids
        predicateIds->push_back(predicate);
      }
    }
  }
}

// _____________________________________________________________________________
template <typename PredicateId>
std::shared_ptr<PatternContainerImpl<PredicateId>>
PatternIndex::loadPatternData(ad_utility::File* file) {
  // Used for the entityHasPattern relation
  // This struct is 16 bytes, not 12, due to padding (at least on gcc 10)
  struct SubjectPatternPair {
    Id subject;
    PatternID pattern;
  };

  std::shared_ptr<PatternContainerImpl<PredicateId>> pattern_data =
      std::make_shared<PatternContainerImpl<PredicateId>>();

  // read the entity has patterns vector
  size_t hasPatternSize;
  file->readOrThrow(&hasPatternSize, sizeof(size_t));
  std::cout << "has pattern size: " << hasPatternSize << std::endl;
  std::vector<SubjectPatternPair> entityHasPattern(hasPatternSize);

  // To avoid the padding of SubjectPatternPairs the data is written densely.
  // Consult the createPatternsImpl function where the data is written for
  // more details.
  constexpr size_t CHUNK_SIZE = 2048;
  char chunk[CHUNK_SIZE * sizeof(uint64_t) * sizeof(uint32_t)];
  size_t num_chunks = entityHasPattern.size() / CHUNK_SIZE;
  if (entityHasPattern.size() % CHUNK_SIZE != 0) {
    num_chunks++;
  }
  std::cout << "num chunks " << num_chunks << std::endl;
  for (size_t chunk_idx = 0; chunk_idx < num_chunks; chunk_idx++) {
    size_t offset = chunk_idx * CHUNK_SIZE;
    size_t count = std::min(CHUNK_SIZE, entityHasPattern.size() - offset);

    // Read a chunk
    file->readOrThrow(chunk, count * (sizeof(uint64_t) + sizeof(uint32_t)));
    // decode the chunk
    for (size_t i = 0; i < count; ++i) {
      std::memcpy(&entityHasPattern[offset + i].subject, chunk + (i * 12), 12);
    }
  }

  // read the entity has predicate vector
  size_t hasPredicateSize;
  file->readOrThrow(&hasPredicateSize, sizeof(size_t));
  std::cout << "hasPredicatesize" << hasPredicateSize << std::endl;
  std::vector<array<Id, 2>> entityHasPredicate(hasPredicateSize);
  file->readOrThrow(entityHasPredicate.data(),
                    hasPredicateSize * sizeof(Id) * 2);

  // readOrThrow the patterns
  pattern_data->patterns().load(file);

  // create the has-relation and has-pattern lookup vectors
  if (entityHasPattern.size() > 0) {
    std::vector<PatternID>& has_pattern = pattern_data->hasPattern();
    has_pattern.resize(entityHasPattern.back().subject + 1);
    size_t pos = 0;
    for (size_t i = 0; i < entityHasPattern.size(); i++) {
      while (entityHasPattern[i].subject > pos) {
        has_pattern[pos] = NO_PATTERN;
        pos++;
      }
      has_pattern[pos] = entityHasPattern[i].pattern;
      pos++;
    }
  }

  vector<vector<PredicateId>> hasPredicateTmp;
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
  pattern_data->hasPredicate().build(hasPredicateTmp);
  return pattern_data;
}

// _____________________________________________________________________________
void PatternIndex::throwExceptionIfNotInitialized() const {
  if (!_initialized) {
    AD_THROW(ad_semsearch::Exception::CHECK_FAILED,
             "The requested feature requires a loaded patterns file ("
             "do not specify the --no-patterns option for this to work)");
  }
}
