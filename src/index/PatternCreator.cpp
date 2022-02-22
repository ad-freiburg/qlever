//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "./PatternCreator.h"

// _________________________________________________________________________
void PatternCreator::pushTriple(std::array<Id, 3> triple) {
  if (!_currentSubject.has_value()) {
    // This is the first triple
    _currentSubject = triple[0];
  } else if (triple[0] != _currentSubject) {
    // we have arrived at a new entity;
    finishSubject(_currentPattern, _currentSubject.value());
    _currentSubject = triple[0];
    _currentPattern.clear();
  }
  // Don't list predicates twice in the same pattern.
  if (_currentPattern.empty() || _currentPattern.back() != triple[1]) {
    _currentPattern.push_back(triple[1]);
  }
}

// ________________________________________________________________________________
void PatternCreator::finishSubject(const Pattern& pattern, const Id& subject) {
  _numberOfDistinctSubjects++;
  _numberOfDistinctSubjectPredicate += pattern.size();
  PatternID patternId;
  auto it = _patternCounts.find(pattern);
  if (it == _patternCounts.end()) {
    // This is a new pattern, assign a new pattern ID and a count of 1
    patternId = static_cast<PatternID>(_patternCounts.size());
    _patternCounts[pattern] = PatternIdAndCount{patternId, 1ul};

    // Count the total number of distinct predicates that appear in the
    // pattern and have not been counted before.
    for (auto predicate : pattern) {
      _distinctPredicates.insert(predicate);
    }
  } else {
    // We have already seen the same pattern for a previous subject, reuse the
    // id and increase the count.
    patternId = it->second._patternId;
    it->second._count++;
  }

  // The mapping from subjects to patterns is a vector of pattern IDs. We have
  // to assign the ID NO_PATTERN to all the possible subjects that have no
  // triple.
  while (_nextUnassignedSubject < subject) {
    _subjectToPatternSerializer.push(NO_PATTERN);
    _nextUnassignedSubject++;
  }

  // Write the subject-pattern mapping for this subject.
  _subjectToPatternSerializer.push(patternId);
  _nextUnassignedSubject++;
}

// ____________________________________________________________________________
void PatternCreator::finish() {
  if (_isFinished) {
    return;
  }
  _isFinished = true;

  // Write the pattern of the last subject.
  finishSubject(_currentPattern, *_currentSubject);

  // The mapping from subjects to patterns is already written to disk at this
  // point.
  _subjectToPatternSerializer.finish();

  // Store all data in the file
  ad_utility::serialization::FileWriteSerializer patternSerializer{_filename};

  PatternStatistics patternStatistics(_numberOfDistinctSubjectPredicate,
                                      _numberOfDistinctSubjects,
                                      _distinctPredicates.size());
  patternSerializer << patternStatistics;

  // Store the actual patterns ordered by their pattern ID. They are currently
  // stored in a hash map, so we first have to sort them.
  std::vector<std::pair<Pattern, PatternIdAndCount>> orderedPatterns;
  orderedPatterns.insert(orderedPatterns.end(), _patternCounts.begin(),
                         _patternCounts.end());
  std::sort(orderedPatterns.begin(), orderedPatterns.end(),
            [](const auto& a, const auto& b) {
              return a.second._patternId < b.second._patternId;
            });
  CompactVectorOfStrings<Id>::Writer patternWriter{
      std::move(patternSerializer).file()};
  for (const auto& p : orderedPatterns) {
    patternWriter.push(p.first.data(), p.first.size());
  }
  patternWriter.finish();

  // Print some statistics for the log of the index builder.
  printStatistics(orderedPatterns.size());
}

// ____________________________________________________________________________
void PatternCreator::readPatternsFromFile(
    const std::string& filename, double& averageNumSubjectsPerPredicate,
    double& averageNumPredicatesPerSubject,
    uint64_t& numDistinctSubjectPredicatePairs,
    CompactVectorOfStrings<Id>& patterns,
    std::vector<PatternID>& subjectToPattern) {
  // Read the pattern info from the patterns file.
  LOG(INFO) << "Reading patterns from file " << filename << " ..." << std::endl;

  // First read the statistics and the patterns
  ad_utility::serialization::FileReadSerializer patternReader(filename);
  PatternStatistics statistics;
  patternReader >> statistics;
  patternReader >> patterns;

  numDistinctSubjectPredicatePairs = statistics._numDistinctSubjectPredicate;
  averageNumSubjectsPerPredicate = statistics._avgNumSubjectsPerPredicate;
  averageNumPredicatesPerSubject = statistics._avgNumPredicatesPerSubject;

  // Read the subjectToPatternMap
  ad_utility::serialization::FileReadSerializer subjectToPatternReader(
      filename + _subjectToPatternSuffix);
  subjectToPatternReader >> subjectToPattern;
}

// ____________________________________________________________________________
void PatternCreator::printStatistics(uint64_t numPatterns) const {
  LOG(DEBUG) << "Number of distinct pattens: " << _patternCounts.size()
             << std::endl;
  LOG(DEBUG) << "Number of subjects for which a pattern was found: "
             << _numberOfDistinctSubjects << std::endl;

  LOG(INFO) << "Number of patterns: " << numPatterns << std::endl;

  LOG(INFO) << "Total number of distinct subject-predicate pairs: "
            << _numberOfDistinctSubjectPredicate << std::endl;
}
