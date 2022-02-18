//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_PATTERNCREATOR_H
#define QLEVER_PATTERNCREATOR_H

#include "../global/Constants.h"
#include "../util/MmapVector.h"

class PatternCreator {
 private:
  std::string _fileName;
  std::vector<PatternID>& _hasPattern;
  CompactVectorOfStrings<Id>& _patterns;
  double& _fullHasPredicateMultiplicityEntities;
  double& _fullHasPredicateMultiplicityPredicates;
  size_t& _fullHasPredicateSize;
  const Id _langPredLowerBound;
  const Id _langPredUpperBound;

  using PatternsCountMap = ad_utility::HashMap<Pattern, size_t>;
  PatternsCountMap _patternCounts;
  Pattern _pattern;
  size_t _numValidPatterns = 0;
  std::optional<Id> currentSubj;

  // Associate entities with patterns.
  ad_utility::MmapVectorTmp<std::array<Id, 2>> _entityHasPattern;
  size_t _numEntitiesWithPatterns = 0;
  size_t _numEntitiesWithoutPatterns = 0;
  size_t _numInvalidEntities = 0;

  size_t _fullHasPredicateEntitiesDistinctSize = 0;

 public:
  PatternCreator(const string& fileName, std::vector<PatternID>& hasPattern,
                 CompactVectorOfStrings<Id>& patterns,
                 double& fullHasPredicateMultiplicityEntities,
                 double& fullHasPredicateMultiplicityPredicates,
                 size_t& fullHasPredicateSize, const Id langPredLowerBound,
                 const Id langPredUpperBound)
      : _fileName{fileName},
        _hasPattern{hasPattern},
        _patterns{patterns},
        _fullHasPredicateMultiplicityEntities{
            fullHasPredicateMultiplicityEntities},
        _fullHasPredicateMultiplicityPredicates{
            fullHasPredicateMultiplicityPredicates},
        _fullHasPredicateSize{fullHasPredicateSize},
        _langPredLowerBound{langPredLowerBound},
        _langPredUpperBound{langPredUpperBound},
        _entityHasPattern{fileName + ".mmap.entityHasPattern.tmp"} {
    LOG(INFO) << "Computing predicate patterns ..." << std::endl;
    // store how many entries there are in the full has-relation relation (after
    // resolving all patterns) and how may distinct elements there are (for the
    // multiplicity;
    _fullHasPredicateSize = 0;
  }

  void subjectAction(const auto& pattern, const auto& subject) {
    _numValidPatterns++;
    _patternCounts[pattern]++;
    _fullHasPredicateEntitiesDistinctSize++;
    auto it = _patternCounts.find(pattern);
    AD_CHECK(it != _patternCounts.end());
    // increase the haspredicate size here as every predicate is only
    // listed once per entity (otherwise it woul always be the same as
    // vec.size()
    _fullHasPredicateSize += pattern.size();
    _numEntitiesWithPatterns++;
    // The pattern does exist, add an entry to the has-pattern predicate
    _entityHasPattern.push_back(std::array<Id, 2>{subject, it->second});
  }

  void pushTriple(std::array<Id, 3> triple) {
    if (!currentSubj.has_value()) {
      // This is the first triple
      currentSubj = triple[0];
    } else if (triple[0] != currentSubj) {
      // we have arrived at a new entity;
      currentSubj = triple[0];
      subjectAction(_pattern, *currentSubj);
      _pattern.clear();
    }
    // don't list predicates twice
    if (_pattern.empty() || _pattern.back() != triple[1]) {
      // Ignore @..@ type language predicates
      if (triple[1] < _langPredLowerBound || triple[1] >= _langPredUpperBound) {
        _pattern.push_back(triple[1]);
      }
    }
  }
  void finish() {
    // Don't forget to process the last entry.
    subjectAction(_pattern, *currentSubj);

    LOG(DEBUG) << "Number of distinct pattens: " << _patternCounts.size()
               << std::endl;
    LOG(DEBUG) << "Number of entities for which a pattern was found: "
               << _numValidPatterns << std::endl;

    // store the actual patterns
    std::vector<std::vector<Id>> buffer;
    buffer.reserve(_patternCounts.size());
    for (const auto& p : _patternCounts) {
      buffer.push_back(p.first._data);
    }
    _patterns.build(buffer);

    _fullHasPredicateMultiplicityEntities =
        _fullHasPredicateSize /
        static_cast<double>(_fullHasPredicateEntitiesDistinctSize);
    LOG(DEBUG) << "Number of entity-has-pattern entries: "
               << _entityHasPattern.size() << std::endl;

    LOG(INFO) << "Number of patterns: " << _patterns.size() << std::endl;
    LOG(INFO) << "Number of subjects with pattern: " << _numEntitiesWithPatterns
              << (_numEntitiesWithoutPatterns == 0 ? " [all]" : "")
              << std::endl;
    if (_numEntitiesWithoutPatterns > 0) {
      LOG(INFO) << "Number of subjects without pattern: "
                << _numEntitiesWithoutPatterns << std::endl;
      LOG(INFO) << "Of these " << _numInvalidEntities
                << " would have to large a pattern." << std::endl;
    }

    LOG(INFO) << "Total number of distinct subject-predicate pairs: "
              << _fullHasPredicateSize << std::endl;
    LOG(INFO) << "Average number of predicates per subject: " << std::fixed
              << std::setprecision(1) << _fullHasPredicateMultiplicityEntities
              << std::endl;
    // Store all data in the file
    ad_utility::File file(_fileName, "w");

    // Write a byte of ones to make it less likely that an unversioned file is
    // read as a versioned one (unversioned files begin with the id of the
    // lowest entity that has a pattern). Then write the version, both
    // multiplicitires and the full size.
    unsigned char firstByte = 255;
    file.write(&firstByte, sizeof(char));
    file.write(&PATTERNS_FILE_VERSION, sizeof(uint32_t));
    file.write(&_fullHasPredicateMultiplicityEntities, sizeof(double));
    file.write(&_fullHasPredicateMultiplicityPredicates, sizeof(double));
    file.write(&_fullHasPredicateSize, sizeof(size_t));

    // write the entityHasPatterns vector
    size_t numHasPatterns = _entityHasPattern.size();
    file.write(&numHasPatterns, sizeof(size_t));
    file.write(_entityHasPattern.data(), sizeof(Id) * numHasPatterns * 2);

    // write the entityHasPredicate vector
    // TODO<joka921> The 0 is needed, because the reading of the pattern still
    // expects the "hasPredicates" vector.
    size_t numHasPredicatess = 0;
    file.write(&numHasPredicatess, sizeof(size_t));

    // write the patterns
    // TODO<joka921> Also use the serializer interface for the patterns.
    ad_utility::serialization::FileWriteSerializer patternWriter{
        std::move(file)};
    patternWriter << _patterns;

    // create the has-relation and has-pattern lookup vectors
    if (_entityHasPattern.size() > 0) {
      _hasPattern.resize(_entityHasPattern.back()[0] + 1);
      size_t pos = 0;
      for (size_t i = 0; i < _entityHasPattern.size(); i++) {
        while (_entityHasPattern[i][0] > pos) {
          _hasPattern[pos] = NO_PATTERN;
          pos++;
        }
        _hasPattern[pos] = _entityHasPattern[i][1];
        pos++;
      }
    }
  }
};
#endif  // QLEVER_PATTERNCREATOR_H
