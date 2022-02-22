//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_PATTERNCREATOR_H
#define QLEVER_PATTERNCREATOR_H

#include "../global/Constants.h"
#include "../util/MmapVector.h"
#include "../util/Serializer/SerializeVector.h"

// TODO<joka921> Refactor this, s.t. this is not global.
static constexpr const char* _subjectToPatternSuffix = ".subjectsToPatterns";
struct PatternStatistics {
 public:
  uint64_t _numDistinctSubjectPredicate;
  double _averageNumPredicatesPerSubject;
  double _averageNumSubjectsPerPredicate;

 public:
  PatternStatistics() = default;
  PatternStatistics(uint64_t numDistinctSubjectPredicate,
                    uint64_t numDistinctSubjects,
                    uint64_t numDistinctPredicates)
      : _numDistinctSubjectPredicate{numDistinctSubjectPredicate},
        _averageNumPredicatesPerSubject{
            static_cast<double>(_numDistinctSubjectPredicate) /
            numDistinctSubjects},
        _averageNumSubjectsPerPredicate{
            static_cast<double>(_numDistinctSubjectPredicate) /
            numDistinctPredicates} {}

  template <typename Serializer>
  friend void serialize(Serializer& serializer, PatternStatistics& s) {
    unsigned char firstByte = 255;
    serializer | firstByte;
    auto version = PATTERNS_FILE_VERSION;
    serializer | version;
    if (version != PATTERNS_FILE_VERSION || firstByte != 255) {
      version = firstByte == 255 ? version : -1;
      std::ostringstream oss;
      oss << "The patterns file version of " << version
          << " does not match the programs pattern file "
          << "version of " << PATTERNS_FILE_VERSION << ". Rebuild the index"
          << " or start the query engine without pattern support." << std::endl;
      throw std::runtime_error(std::move(oss).str());
    }

    serializer | s._averageNumPredicatesPerSubject;
    serializer | s._averageNumSubjectsPerPredicate;
    serializer | s._numDistinctSubjectPredicate;
  }
};

inline auto noPredicatesIgnored = [](const auto&&...) { return false; };
template <typename IsPredicateIgnored = decltype(noPredicatesIgnored)>
class PatternCreator {
 private:
  std::string _fileName;
  // store how many entries there are in the full has-relation relation (after
  // resolving all patterns)
  IsPredicateIgnored _isPredicateIgnored;

  struct PatternIdAndCount {
    PatternID _patternId = 0;
    uint64_t _count = 0;
  };
  using PatternsCountMap = ad_utility::HashMap<Pattern, PatternIdAndCount>;
  PatternsCountMap _patternCounts;
  Pattern _currentPattern;

  Id _nextUnassignedSubject = 0;
  std::optional<Id> _currentSubject;

  // Associate entities with patterns.
  ad_utility::serialization::VectorIncrementalSerializer<
      PatternID, ad_utility::serialization::FileWriteSerializer>
      _subjectToPatternSerializer;

  ad_utility::HashSet<uint64_t> _alreadyCountedPredicates;
  uint64_t _numberOfDistinctPredicates = 0;
  uint64_t _numberOfDistinctSubjects = 0;
  uint64_t _numberOfDistinctSubjectPredicate = 0;

 public:
  PatternCreator(const string& fileName,
                 IsPredicateIgnored isPredicateIgnored = IsPredicateIgnored{})
      : _fileName{fileName},
        _isPredicateIgnored{std::move(isPredicateIgnored)},
        _subjectToPatternSerializer{{fileName + _subjectToPatternSuffix}} {
    LOG(INFO) << "Computing predicate patterns ..." << std::endl;
  }

  void subjectAction(const auto& pattern, const auto& subject) {
    _numberOfDistinctSubjects++;
    _numberOfDistinctSubjectPredicate += pattern.size();
    Id patternId;
    auto it = _patternCounts.find(pattern);
    if (it == _patternCounts.end()) {
      patternId = _patternCounts.size();
      _patternCounts[pattern] =
          PatternIdAndCount{static_cast<PatternID>(patternId), 1ul};

      // Count the total number of distinct predicates that appear in the
      // patterns
      for (auto predicate : pattern) {
        if (!_alreadyCountedPredicates.contains(predicate)) {
          _numberOfDistinctPredicates++;
          _alreadyCountedPredicates.insert(predicate);
        }
      }
    } else {
      patternId = it->second._patternId;
      it->second._count++;
    }

    // The pattern does exist, add an entry to the has-pattern predicate
    while (_nextUnassignedSubject < subject) {
      _subjectToPatternSerializer.push(NO_PATTERN);
      _nextUnassignedSubject++;
    }
    _subjectToPatternSerializer.push(patternId);
    _nextUnassignedSubject++;
  }

  void pushTriple(std::array<Id, 3> triple) {
    if (_isPredicateIgnored(triple[1])) {
      return;
    }

    if (!_currentSubject.has_value()) {
      // This is the first triple
      _currentSubject = triple[0];
    } else if (triple[0] != _currentSubject) {
      // we have arrived at a new entity;
      subjectAction(_currentPattern, *_currentSubject);
      _currentSubject = triple[0];
      _currentPattern.clear();
    }
    // don't list predicates twice
    if (_currentPattern.empty() || _currentPattern.back() != triple[1]) {
      _currentPattern.push_back(triple[1]);
    }
  }

  void finish() {
    // Don't forget to process the last entry.
    subjectAction(_currentPattern, *_currentSubject);

    // The mapping from subjects to patterns is already written to disk at this
    // point.
    _subjectToPatternSerializer.finish();

    // store the actual patterns. They are stored in a hash map, so we have to
    // first order them by their patternId.
    std::vector<std::pair<Pattern, PatternIdAndCount>> orderedPatterns;
    orderedPatterns.insert(orderedPatterns.end(), _patternCounts.begin(),
                           _patternCounts.end());
    std::sort(orderedPatterns.begin(), orderedPatterns.end(),
              [](const auto& a, const auto& b) {
                return a.second._patternId < b.second._patternId;
              });
    std::vector<std::vector<Id>> buffer;
    buffer.reserve(orderedPatterns.size());
    for (const auto& p : orderedPatterns) {
      buffer.push_back(p.first._data);
    }
    CompactVectorOfStrings<Id> _patterns;
    _patterns.build(buffer);
    // Store all data in the file
    ad_utility::serialization::FileWriteSerializer patternWriter{_fileName};

    PatternStatistics patternStatistics(_numberOfDistinctSubjectPredicate,
                                        _numberOfDistinctSubjects,
                                        _numberOfDistinctPredicates);
    patternWriter << patternStatistics;

    // Write the patterns
    patternWriter << _patterns;

    // The rest is only statistics.
    printStatistics(_patterns);
  }

  void printStatistics(const auto& patterns) const {
    LOG(DEBUG) << "Number of distinct pattens: " << _patternCounts.size()
               << std::endl;
    uint64_t numberOfSubjects =
        std::accumulate(_patternCounts.begin(), _patternCounts.end(), 0ul,
                        [](auto cur, const auto& patternAndCount) {
                          return cur + patternAndCount.second._count;
                        });
    LOG(DEBUG) << "Number of subjects for which a pattern was found: "
               << numberOfSubjects << std::endl;

    LOG(INFO) << "Number of patterns: " << patterns.size() << std::endl;

    LOG(INFO) << "Total number of distinct subject-predicate pairs: "
              << _numberOfDistinctSubjectPredicate << std::endl;
  }
};
struct PatternReader {
  static void readPatternsFromFile(
      const std::string& filename, auto& _fullHasPredicateMultiplicityEntities,
      auto& _fullHasPredicateMultiplicityPredicates,
      auto& _fullHasPredicateSize, auto& _patterns, auto& _hasPattern) {
    // Read the pattern info from the patterns file.
    LOG(INFO) << "Reading patterns from file " << filename << " ..."
              << std::endl;

    ad_utility::serialization::FileReadSerializer patternReader(filename);
    PatternStatistics statistics;
    patternReader >> statistics;
    patternReader >> _patterns;

    _fullHasPredicateSize = statistics._numDistinctSubjectPredicate;
    _fullHasPredicateMultiplicityEntities =
        statistics._averageNumPredicatesPerSubject;
    _fullHasPredicateMultiplicityPredicates =
        statistics._averageNumSubjectsPerPredicate;

    ad_utility::serialization::FileReadSerializer subjectToPatternReader(
        filename + _subjectToPatternSuffix);
    subjectToPatternReader >> _hasPattern;
  }
};
#endif  // QLEVER_PATTERNCREATOR_H
