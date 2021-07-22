#include "PatternIndex.h"

#include "MetaDataIterator.h"

const uint32_t PatternIndex::PATTERNS_FILE_VERSION = 1;

// TODO: stxxl::vector<array<Id, 3>> should be Index::TripleVec but that
// creates a circular dependency
using TripleVec = stxxl::vector<array<Id, 3>>;

PatternIndex::PatternIndex() : _initialized(false) {}

// _____________________________________________________________________________
void PatternIndex::generatePredicateLocalNamespace(VocabularyData* vocabData) {
  // Create a new namespace for predicates containing only predicates.
  // This will be significantly smaller than the global namespace which
  // also contains subjects and objects, and allows for shrinking
  // the pattern trick data.

  // This is not inside a templated method as the PSO metadata is based upon
  // HashMaps which need to be treated differently
  TripleVec::bufreader_type reader(*vocabData->idTriples);
  if (reader.empty()) {
    LOG(WARN) << "Triple vector was empty, no patterns created" << std::endl;
    return;
  }

  Id currentPred = ID_NO_VALUE;

  Id langPredLowerBound = vocabData->langPredLowerBound;
  Id langPredUpperBound = vocabData->langPredUpperBound;

  // Iterate all triples in POS (or PSO) sorting order. Add every distinct
  // non language predicate to the predicateIds vector, therefore assigning
  // a predicate namespace id to it via its position in the vector.
  for (; !reader.empty(); ++reader) {
    Id predicate = (*reader)[1];
    if (predicate != currentPred) {
      currentPred = predicate;
      if (predicate < langPredLowerBound || predicate >= langPredUpperBound) {
        // The predicate is not a language predicate, add it to the ids
        _predicate_local_to_global_ids.push_back(predicate);
      }
    }
  }

  // Compute the global to local mapping from the local to global mapping
  _predicate_global_to_local_ids.reserve(_predicate_local_to_global_ids.size());
  for (size_t i = 0; i < _predicate_local_to_global_ids.size(); ++i) {
    _predicate_global_to_local_ids.try_emplace(
        _predicate_local_to_global_ids[i], i);
  }
}

// _____________________________________________________________________________
void PatternIndex::generatePredicateLocalNamespaceFromExistingIndex(
    Id langPredLowerBound, Id langPredUpperBound,
    IndexMetaDataHmap& meta_data) {
  // This is not inside a templated method as the PSO metadata is based upon
  // HashMaps which need to be treated differently

  // Iterate the hash map mapping predicates to metadata
  for (const auto& triple_it : meta_data.data()) {
    Id predicate = triple_it.first;
    if (predicate < langPredLowerBound || predicate >= langPredUpperBound) {
      _predicate_local_to_global_ids.push_back(predicate);
    }
  }

  // The sorting ensures that the namespace generated during and after index
  // creation are identical. It is currently not strictly speaking required,
  // but is also not that expensive (as the number of predicates tends to be
  // small), and prevents nasty bugs appearing only if the namespace was
  // generated from an existing index.
  std::sort(_predicate_local_to_global_ids.begin(),
            _predicate_local_to_global_ids.end());

  // Compute the global to local mapping from the local to global mapping
  _predicate_global_to_local_ids.reserve(_predicate_local_to_global_ids.size());
  for (size_t i = 0; i < _predicate_local_to_global_ids.size(); ++i) {
    _predicate_global_to_local_ids.try_emplace(
        _predicate_local_to_global_ids[i], i);
  }
}

// _____________________________________________________________________________
PatternContainer& PatternIndex::getPatternData() { return _pattern_container; }

// _____________________________________________________________________________
const PatternContainer& PatternIndex::getPatternData() const {
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

  auto createPatternLambda = [&]<typename T>(T) {
    // TODO<joka921>: make the createImpl directly return a value
    _pattern_container = createPatternsImpl<T, TripleVec::bufreader_type>(
        patterns_file_name, vocabData->langPredLowerBound,
        vocabData->langPredUpperBound, *vocabData->idTriples);
  };

  if (num_bytes_predicate_id <= 1) {
    createPatternLambda(uint8_t{});
  } else if (num_bytes_predicate_id <= 2) {
    createPatternLambda(uint16_t{});
  } else if (num_bytes_predicate_id <= 4) {
    createPatternLambda(uint32_t{});
  } else if (num_bytes_predicate_id <= 8) {
    createPatternLambda(uint64_t{});
  } else {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "The index contains more than 2**64 predicates.");
  }
  _initialized = true;
}

// _____________________________________________________________________________
void PatternIndex::createPatternsFromExistingIndex(
    Id langPredLowerBound, Id langPredUpperBound,
    IndexMetaDataMmapView& meta_data, ad_utility::File& file,
    const std::string& filename_base) {
  size_t num_bytes_predicate_id = 0;
  size_t num_predicate_ids = _predicate_local_to_global_ids.size();
  while (num_predicate_ids > 0) {
    num_predicate_ids = num_predicate_ids >> 8;
    num_bytes_predicate_id++;
  }

  std::string patterns_file_name = filename_base + ".index.patterns";

  auto createPatternsLambda = [&]<typename T>(T) {
    _pattern_container =
        createPatternsImpl<T, MetaDataIterator<IndexMetaDataMmapView>,
                           IndexMetaDataMmapView, ad_utility::File>(
            patterns_file_name, langPredLowerBound, langPredUpperBound,
            meta_data, file);
  };

  if (num_bytes_predicate_id <= 1) {
    createPatternsLambda(uint8_t{});
  } else if (num_bytes_predicate_id <= 2) {
    createPatternsLambda(uint16_t{});
  } else if (num_bytes_predicate_id <= 4) {
    createPatternsLambda(uint32_t{});
  } else if (num_bytes_predicate_id <= 8) {
    createPatternsLambda(uint64_t{});
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
    // TODO<joka921> make the loadPatternData also return a value;o
    _pattern_container = std::move(*loadPatternData<uint8_t>(&patternsFile));
  } else if (num_bytes_predicate_id <= 2) {
    _pattern_container = std::move(*loadPatternData<uint32_t>(&patternsFile));
  } else if (num_bytes_predicate_id <= 4) {
    _pattern_container = std::move(*loadPatternData<uint32_t>(&patternsFile));
  } else if (num_bytes_predicate_id <= 8) {
    _pattern_container = std::move(*loadPatternData<uint64_t>(&patternsFile));
  } else {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "The index contains more than 2**64 predicates.");
  }
  _initialized = true;
}

// _____________________________________________________________________________
template <typename PredicateId, typename VecReaderType, typename... Args>
PatternContainerImpl<PredicateId> PatternIndex::createPatternsImpl(
    const string& fileName, const Id langPredLowerBound,
    const Id langPredUpperBound, Args&... vecReaderArgs) {
  IndexMetaDataHmap meta;

  struct PatternIdAndCount {
    PatternID id;
    size_t count;
  };

  typedef ad_utility::HashMap<Pattern<PredicateId>, PatternIdAndCount>
      PatternsCountMap;
  typedef ad_utility::HashMap<Pattern<PredicateId>, PatternID> PatternIdMap;
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
    return {};
  }

  PatternsCountMap patternCounts;
  // determine the most common patterns
  Pattern<PredicateId> pattern;
  size_t numEntitiesWithPatterns = 0;
  size_t fullHasPredicatePredicatesDistinctSize = 0;

  // Associate entities with patterns if possible, store has-relation otherwise
  ad_utility::MmapVectorTmp<SubjectPatternPair> entityHasPattern(
      fileName + ".mmap.entityHasPattern.tmp");

  size_t numValidPatterns = 0;
  Id currentSubj = (*reader)[0];

  auto pushPattern = [&] {
    numEntitiesWithPatterns++;
    numValidPatterns++;
    if (!patternCounts.contains(pattern)) {
      patternCounts[pattern] =
          PatternIdAndCount{static_cast<PatternID>(patternCounts.size()), 0};
      fullHasPredicatePredicatesDistinctSize += pattern.size();
    }
    entityHasPattern.push_back(
        SubjectPatternPair{currentSubj, patternCounts[pattern].id});
    patternCounts[pattern].count++;
    pattern.clear();
  };

  for (; !reader.empty(); ++reader) {
    auto triple = *reader;
    if (triple[0] != currentSubj) {
      pushPattern();
      currentSubj = triple[0];
    }

    // Ignore @..@ type language predicates
    if (triple[1] >= langPredLowerBound && triple[1] < langPredUpperBound) {
      continue;
    }

    AD_CHECK(_predicate_global_to_local_ids.contains(triple[1]));
    auto localId = _predicate_global_to_local_ids[triple[1]];
    // don't list predicates twice
    if (pattern.empty() || pattern.back() != localId) {
      pattern.push_back(localId);
    }
  }
  // process the last entry
  pushPattern();
  LOG(INFO) << "Counted patterns and found " << patternCounts.size()
            << " distinct patterns." << std::endl;
  LOG(INFO) << "Patterns were found for " << numValidPatterns << " entities."
            << std::endl;

  const size_t maxNumPatterns = std::numeric_limits<PatternID>::max() - 2;
  if (patternCounts.size() > maxNumPatterns) {
    throw std::runtime_error(
        "Need more than " + std::to_string(maxNumPatterns) +
        " patterns, which is currently not supported. Please contact the "
        "developers of QLever about this.");
  }

  std::vector<std::pair<Pattern<PredicateId>, PatternIdAndCount>>
      sortedPatterns;
  sortedPatterns.reserve(patternCounts.size());
  sortedPatterns.insert(sortedPatterns.end(),
                        std::make_move_iterator(patternCounts.begin()),
                        std::make_move_iterator(patternCounts.end()));

  auto cmp = [](const auto& a, const auto& b) {
    return a.second.id < b.second.id;
  };
  std::sort(sortedPatterns.begin(), sortedPatterns.end(), cmp);

  // store the actual patterns
  std::vector<std::vector<PredicateId>> buffer;
  buffer.reserve(sortedPatterns.size());
  for (const auto& p : sortedPatterns) {
    // TODO<joka921> can we move here?
    buffer.push_back(p.first._data);
  }
  PatternContainerImpl<PredicateId> pattern_data;
  pattern_data.patterns().build(buffer);

  // store how many entries there are in the full has-relation relation (after
  // resolving all patterns) and how may distinct elements there are (for the
  // multiplicity;
  _fullHasPredicateSize = 0;
  size_t fullHasPredicateEntitiesDistinctSize = 0;

  _fullHasPredicateMultiplicityEntities =
      _fullHasPredicateSize /
      static_cast<double>(fullHasPredicateEntitiesDistinctSize);
  _fullHasPredicateMultiplicityPredicates =
      _fullHasPredicateSize /
      static_cast<double>(fullHasPredicatePredicatesDistinctSize);

  LOG(DEBUG) << "Number of entity-has-pattern entries: "
             << entityHasPattern.size() << std::endl;

  LOG(INFO) << "Found " << pattern_data.patterns().size()
            << " distinct patterns." << std::endl;
  LOG(INFO) << numEntitiesWithPatterns
            << " of the databases entities have been assigned a pattern."
            << std::endl;

  LOG(DEBUG) << "Full has relation size: " << _fullHasPredicateSize
             << std::endl;
  LOG(DEBUG) << "Full has relation entity multiplicity: "
             << _fullHasPredicateMultiplicityEntities << std::endl;
  LOG(DEBUG) << "Full has relation predicate multiplicity: "
             << _fullHasPredicateMultiplicityPredicates << std::endl;

  // Store all data in the file
  ad_utility::File file(fileName, "w");

  // Write a byte of ones to make it less likely that an unversioned file is
  // read as a versioned one (unversioned files begin with the id of the lowest
  // entity that has a pattern).
  // Then write the version, both multiplicitires and the full size.
  uint8_t firstByte = 255;
  file.write(&firstByte, sizeof(uint8_t));
  file.write(&PATTERNS_FILE_VERSION, sizeof(uint32_t));
  file.write(&_fullHasPredicateMultiplicityEntities, sizeof(double));
  file.write(&_fullHasPredicateMultiplicityPredicates, sizeof(double));
  file.write(&_fullHasPredicateSize, sizeof(size_t));

  // Write the mapping from the predicate local to the global namespace
  uint64_t local_predicate_ns_size = _predicate_local_to_global_ids.size();
  std::cout << "Got " << local_predicate_ns_size << " distinct predicates"
            << std::endl;
  std::cout << "Using " << sizeof(PredicateId) << " bytes per predicate"
            << std::endl;
  file.write(&local_predicate_ns_size, sizeof(uint64_t));
  file.write(_predicate_local_to_global_ids.data(),
             _predicate_local_to_global_ids.size() * sizeof(Id));

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

  // write the patterns
  pattern_data.patterns().write(file);
  file.close();

  LOG(INFO) << "Done creating patterns file." << std::endl;

  // TODO: We can perform this setup directly above without the detour
  // of the MMAP-Vector. then we can fully initialize this pattern_data
  // and use automatic serialization to get rid of a lot of code.
  // create the has-relation and has-pattern lookup vectors
  if (entityHasPattern.size() > 0) {
    std::vector<PatternID>& hasPattern = pattern_data.hasPattern();
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

  return pattern_data;
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
