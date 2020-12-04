#include "PatternIndex.h"
#include "MetaDataIterator.h"

const uint32_t PatternIndex::PATTERNS_FILE_VERSION = 1;
const uint32_t PatternIndex::NAMESPACE_FILE_VERSION = 0;

const std::string PatternIndex::PATTERNS_NAMESPACE_SUFFIX =
    ".index.patterns.pred_namespace";
const std::string PatternIndex::PATTERNS_SUBJECTS_SUFFIX = ".index.patterns";
const std::string PatternIndex::PATTERNS_OBJECTS_SUFFIX =
    ".index.patterns.objects";

// TODO: stxxl::vector<array<Id, 3>> should be Index::TripleVec but that
// creates a circular dependency
using TripleVec = stxxl::vector<array<Id, 3>>;

PatternIndex::PatternIndex()
    : _maxNumPatterns(std::numeric_limits<PatternID>::max() - 2),
      _num_bytes_per_predicate(0),
      _initialized(false) {}

// _____________________________________________________________________________
void PatternIndex::generatePredicateLocalNamespace(
    VocabularyData* vocabData, const std::string& filename_base) {
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

  writeNamespaceFile(filename_base + PATTERNS_NAMESPACE_SUFFIX,
                     _predicate_local_to_global_ids);
}

// _____________________________________________________________________________
void PatternIndex::generatePredicateLocalNamespaceFromExistingIndex(
    Id langPredLowerBound, Id langPredUpperBound, IndexMetaDataHmap& meta_data,
    const std::string& filename_base) {
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

  // Compute the global to local mapping from the local to global mapping
  _predicate_global_to_local_ids.reserve(_predicate_local_to_global_ids.size());
  for (size_t i = 0; i < _predicate_local_to_global_ids.size(); ++i) {
    _predicate_global_to_local_ids.try_emplace(
        _predicate_local_to_global_ids[i], i);
  }

  writeNamespaceFile(filename_base + PATTERNS_NAMESPACE_SUFFIX,
                     _predicate_local_to_global_ids);
}

// _____________________________________________________________________________
std::shared_ptr<PatternContainer> PatternIndex::getSubjectPatternData() {
  return _subjects_pattern_data;
}

// _____________________________________________________________________________
std::shared_ptr<const PatternContainer> PatternIndex::getSubjectPatternData()
    const {
  return _subjects_pattern_data;
}

// _____________________________________________________________________________
const PatternIndex::PatternMetaData& PatternIndex::getSubjectMetaData() const {
  return _subject_meta_data;
};

// _____________________________________________________________________________

std::shared_ptr<PatternContainer> PatternIndex::getObjectPatternData() {
  return _objects_pattern_data;
}

// _____________________________________________________________________________
std::shared_ptr<const PatternContainer> PatternIndex::getObjectPatternData()
    const {
  return _objects_pattern_data;
}

// _____________________________________________________________________________
const PatternIndex::PatternMetaData& PatternIndex::getObjectMetaData() const {
  return _object_meta_data;
}

// _____________________________________________________________________________
const std::vector<Id> PatternIndex::getPredicateGlobalIds() const {
  return _predicate_local_to_global_ids;
}

// _____________________________________________________________________________
void PatternIndex::createPatterns(VocabularyData* vocabData,
                                  const std::string& filename_base) {
  std::string patterns_file_name = filename_base + PATTERNS_SUBJECTS_SUFFIX;

  callCreatePatternsImpl<HasRelationType::SUBJECT, TripleVec::bufreader_type>(
      _num_bytes_per_predicate, patterns_file_name, &_subjects_pattern_data,
      _predicate_global_to_local_ids, &_subject_meta_data, _maxNumPatterns,
      vocabData->langPredLowerBound, vocabData->langPredUpperBound,
      *vocabData->idTriples);

  _initialized = true;
}

// _____________________________________________________________________________
void PatternIndex::createPatternsForObjects(VocabularyData* vocabData,
                                            const std::string& filename_base) {
  std::string patterns_file_name = filename_base + PATTERNS_OBJECTS_SUFFIX;

  callCreatePatternsImpl<HasRelationType::OBJECT, TripleVec::bufreader_type>(
      _num_bytes_per_predicate, patterns_file_name, &_objects_pattern_data,
      _predicate_global_to_local_ids, &_object_meta_data, _maxNumPatterns,
      vocabData->langPredLowerBound, vocabData->langPredUpperBound,
      *vocabData->idTriples);

  _initialized = true;
}

// _____________________________________________________________________________
void PatternIndex::createPatternsFromExistingIndex(
    Id langPredLowerBound, Id langPredUpperBound,
    IndexMetaDataMmapView& meta_data, ad_utility::File& file,
    const std::string& filename_base) {
  std::string patterns_file_name = filename_base + PATTERNS_SUBJECTS_SUFFIX;

  callCreatePatternsImpl<HasRelationType::SUBJECT,
                         MetaDataIterator<IndexMetaDataMmapView>,
                         IndexMetaDataMmapView, ad_utility::File>(
      _num_bytes_per_predicate, patterns_file_name, &_subjects_pattern_data,
      _predicate_global_to_local_ids, &_subject_meta_data, _maxNumPatterns,
      langPredLowerBound, langPredUpperBound, meta_data, file);

  _initialized = true;
}

// _____________________________________________________________________________
void PatternIndex::createPatternsForObjectsFromExistingIndex(
    Id langPredLowerBound, Id langPredUpperBound,
    IndexMetaDataMmapView& meta_data, ad_utility::File& file,
    const std::string& filename_base) {
  std::string patterns_file_name = filename_base + PATTERNS_OBJECTS_SUFFIX;

  callCreatePatternsImpl<HasRelationType::OBJECT,
                         MetaDataIterator<IndexMetaDataMmapView>,
                         IndexMetaDataMmapView, ad_utility::File>(
      _num_bytes_per_predicate, patterns_file_name, &_objects_pattern_data,
      _predicate_global_to_local_ids, &_object_meta_data, _maxNumPatterns,
      langPredLowerBound, langPredUpperBound, meta_data, file);

  _initialized = true;
}

// _____________________________________________________________________________
void PatternIndex::loadPatternIndex(const std::string& filename_base) {
  loadNamespaceFile(filename_base + PATTERNS_NAMESPACE_SUFFIX,
                    &_predicate_local_to_global_ids);

  // determine the number of bytes used for predicate ids
  _num_bytes_per_predicate = 0;
  size_t num_predicate_ids = _predicate_local_to_global_ids.size();
  while (num_predicate_ids > 0) {
    num_predicate_ids = num_predicate_ids >> 8;
    _num_bytes_per_predicate++;
  }
  std::cout << "Using " << _num_bytes_per_predicate << " bytes per predicate"
            << std::endl;

  std::string patternsFilePath = filename_base + PATTERNS_SUBJECTS_SUFFIX;
  loadPatternFile(patternsFilePath, _num_bytes_per_predicate,
                  &_subject_meta_data, &_subjects_pattern_data);

  patternsFilePath = filename_base + PATTERNS_OBJECTS_SUFFIX;
  loadPatternFile(patternsFilePath, _num_bytes_per_predicate,
                  &_object_meta_data, &_objects_pattern_data);
  _initialized = true;
}

// _____________________________________________________________________________
void PatternIndex::loadPatternFile(const std::string& path,
                                   size_t num_bytes_per_predicate,
                                   PatternMetaData* meta_data_storage,
                                   std::shared_ptr<PatternContainer>* storage) {
  ad_utility::File patternsFile;
  patternsFile.open(path, "r");
  AD_CHECK(patternsFile.isOpen());
  uint8_t firstByte;
  patternsFile.readOrThrow(&firstByte, sizeof(uint8_t));
  uint32_t version;
  patternsFile.readOrThrow(&version, sizeof(uint32_t));
  if (version != PATTERNS_FILE_VERSION || firstByte != 255) {
    version = firstByte == 255 ? version : -1;
    patternsFile.close();
    std::ostringstream oss;
    oss << "The patterns file " << path << " version of " << version
        << " does not match the programs pattern file "
        << "version of " << PATTERNS_FILE_VERSION << ". Rebuild the index"
        << " or start the query engine without pattern support." << std::endl;
    throw std::runtime_error(oss.str());
  }
  patternsFile.readOrThrow(
      &meta_data_storage->fullHasPredicateMultiplicityEntities, sizeof(double));
  patternsFile.readOrThrow(
      &meta_data_storage->fullHasPredicateMultiplicityPredicates,
      sizeof(double));
  patternsFile.readOrThrow(&meta_data_storage->fullHasPredicateSize,
                           sizeof(size_t));

  if (num_bytes_per_predicate <= 1) {
    *storage = loadPatternData<uint8_t>(&patternsFile);
  } else if (num_bytes_per_predicate <= 2) {
    *storage = loadPatternData<uint16_t>(&patternsFile);
  } else if (num_bytes_per_predicate <= 4) {
    *storage = loadPatternData<uint32_t>(&patternsFile);
  } else if (num_bytes_per_predicate <= 8) {
    *storage = loadPatternData<uint64_t>(&patternsFile);
  } else {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "The index contains more than 2**64 predicates.");
  }
}

// _____________________________________________________________________________
void PatternIndex::loadNamespaceFile(const std::string& path,
                                     std::vector<Id>* namespace_storage) {
  ad_utility::File namespaceFile;
  namespaceFile.open(path, "r");

  uint32_t file_version;
  namespaceFile.readOrThrow(&file_version, sizeof(uint32_t));
  if (file_version != NAMESPACE_FILE_VERSION) {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "The pattern trick namespace file " + path + " has version " +
                 std::to_string(file_version) +
                 " but the executable was built with version " +
                 std::to_string(NAMESPACE_FILE_VERSION));
  }

  // read the mapping from predicate local ids to global ids
  uint64_t predicate_local_ns_size;
  namespaceFile.readOrThrow(&predicate_local_ns_size, sizeof(uint64_t));
  std::cout << "Got " << predicate_local_ns_size << " distinct predicates"
            << std::endl;
  namespace_storage->resize(predicate_local_ns_size);
  namespaceFile.readOrThrow(namespace_storage->data(),
                            predicate_local_ns_size * sizeof(Id));
  namespaceFile.close();
}

// _____________________________________________________________________________
void PatternIndex::writeNamespaceFile(const std::string& path,
                                      const vector<Id>& ns) {
  ad_utility::File namespaceFile;
  namespaceFile.open(path, "w");

  namespaceFile.write(&NAMESPACE_FILE_VERSION, sizeof(uint32_t));

  // read the mapping from predicate local ids to global ids
  uint64_t predicate_local_ns_size = ns.size();
  namespaceFile.write(&predicate_local_ns_size, sizeof(uint64_t));
  namespaceFile.write(ns.data(), predicate_local_ns_size * sizeof(Id));
  namespaceFile.close();
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
template <HasRelationType TYPE, typename VecReaderType, typename... Args>
void PatternIndex::callCreatePatternsImpl(
    size_t num_bytes_per_predicate_id, const string& fileName,
    std::shared_ptr<PatternContainer>* pattern_data,
    const ad_utility::HashMap<Id, size_t>& predicate_local_id,
    PatternMetaData* metadata, const size_t maxNumPatterns,
    const Id langPredLowerBound, const Id langPredUpperBound,
    Args&... vecReaderArgs) {
  if (num_bytes_per_predicate_id <= 1) {
    std::shared_ptr<PatternContainerImpl<uint8_t>> pattern_data_impl =
        std::make_shared<PatternContainerImpl<uint8_t>>();
    createPatternsImpl<uint8_t, TYPE, VecReaderType, Args...>(
        fileName, pattern_data_impl, predicate_local_id, metadata,
        maxNumPatterns, langPredLowerBound, langPredUpperBound,
        vecReaderArgs...);
    *pattern_data = pattern_data_impl;
  } else if (num_bytes_per_predicate_id <= 2) {
    std::shared_ptr<PatternContainerImpl<uint16_t>> pattern_data_impl =
        std::make_shared<PatternContainerImpl<uint16_t>>();
    createPatternsImpl<uint16_t, TYPE, VecReaderType, Args...>(
        fileName, pattern_data_impl, predicate_local_id, metadata,
        maxNumPatterns, langPredLowerBound, langPredUpperBound,
        vecReaderArgs...);
    *pattern_data = pattern_data_impl;
  } else if (num_bytes_per_predicate_id <= 4) {
    std::shared_ptr<PatternContainerImpl<uint32_t>> pattern_data_impl =
        std::make_shared<PatternContainerImpl<uint32_t>>();
    createPatternsImpl<uint32_t, TYPE, VecReaderType, Args...>(
        fileName, pattern_data_impl, predicate_local_id, metadata,
        maxNumPatterns, langPredLowerBound, langPredUpperBound,
        vecReaderArgs...);
    *pattern_data = pattern_data_impl;
  } else if (num_bytes_per_predicate_id <= 8) {
    std::shared_ptr<PatternContainerImpl<uint64_t>> pattern_data_impl =
        std::make_shared<PatternContainerImpl<uint64_t>>();
    createPatternsImpl<uint64_t, TYPE, VecReaderType, Args...>(
        fileName, pattern_data_impl, predicate_local_id, metadata,
        maxNumPatterns, langPredLowerBound, langPredUpperBound,
        vecReaderArgs...);
    *pattern_data = pattern_data_impl;
  } else {
    AD_THROW(ad_semsearch::Exception::BAD_INPUT,
             "The index contains more than 2**64 predicates.");
  }
};

// _____________________________________________________________________________
template <typename PredicateId, HasRelationType TYPE, typename VecReaderType,
          typename... Args>
void PatternIndex::createPatternsImpl(
    const string& fileName,
    std::shared_ptr<PatternContainerImpl<PredicateId>> pattern_data,
    const ad_utility::HashMap<Id, size_t>& predicate_local_id,
    PatternMetaData* metadata, const size_t maxNumPatterns,
    const Id langPredLowerBound, const Id langPredUpperBound,
    Args&... vecReaderArgs) {
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

  constexpr size_t SUBJECT_COL = HasRelationTypeData<TYPE>::SUBJECT_COL;

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
  Id currentSubj = (*reader)[SUBJECT_COL];

  for (; !reader.empty(); ++reader) {
    auto triple = *reader;
    if (triple[SUBJECT_COL] != currentSubj) {
      currentSubj = triple[SUBJECT_COL];
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
  metadata->fullHasPredicateSize = 0;
  size_t fullHasPredicateEntitiesDistinctSize = 0;
  size_t fullHasPredicatePredicatesDistinctSize = 0;
  // This vector stores if the pattern was already counted toward the distinct
  // has relation predicates size.
  std::vector<bool> haveCountedPattern(patternSet.size());

  // the input triple list is in spo order, we only need a hash map for
  // predicates
  ad_utility::HashSet<Id> predicateHashSet;

  pattern.clear();
  currentSubj = (*VecReaderType(vecReaderArgs...))[SUBJECT_COL];
  patternIndex = 0;
  // Create the has-relation and has-pattern predicates
  for (VecReaderType reader2(vecReaderArgs...); !reader2.empty(); ++reader2) {
    auto triple = *reader2;
    if (triple[SUBJECT_COL] != currentSubj) {
      // we have arrived at a new entity;
      fullHasPredicateEntitiesDistinctSize++;
      auto it = patternSet.find(pattern);
      // increase the haspredicate size here as every predicate is only
      // listed once per entity (otherwise it would always be the same as
      // vec.size()
      metadata->fullHasPredicateSize += pattern.size();
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
      currentSubj = triple[SUBJECT_COL];
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
  metadata->fullHasPredicateSize += pattern.size();
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

  metadata->fullHasPredicateMultiplicityEntities =
      metadata->fullHasPredicateSize /
      static_cast<double>(fullHasPredicateEntitiesDistinctSize);
  metadata->fullHasPredicateMultiplicityPredicates =
      metadata->fullHasPredicateSize /
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

  LOG(DEBUG) << "Full has relation size: " << metadata->fullHasPredicateSize
             << std::endl;
  LOG(DEBUG) << "Full has relation entity multiplicity: "
             << metadata->fullHasPredicateMultiplicityEntities << std::endl;
  LOG(DEBUG) << "Full has relation predicate multiplicity: "
             << metadata->fullHasPredicateMultiplicityPredicates << std::endl;

  // Store all data in the file
  ad_utility::File file(fileName, "w");

  // Write a byte of ones to make it less likely that an unversioned file is
  // read as a versioned one (unversioned files begin with the id of the lowest
  // entity that has a pattern).
  // Then write the version, both multiplicitires and the full size.
  uint8_t firstByte = 255;
  file.write(&firstByte, sizeof(uint8_t));
  file.write(&PATTERNS_FILE_VERSION, sizeof(uint32_t));
  file.write(&metadata->fullHasPredicateMultiplicityEntities, sizeof(double));
  file.write(&metadata->fullHasPredicateMultiplicityPredicates, sizeof(double));
  file.write(&metadata->fullHasPredicateSize, sizeof(size_t));

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
void PatternIndex::throwExceptionIfNotInitialized() const {
  if (!_initialized) {
    AD_THROW(ad_semsearch::Exception::CHECK_FAILED,
             "The requested feature requires a loaded patterns file ("
             "do not specify the --no-patterns option for this to work)");
  }
}
