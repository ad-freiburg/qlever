//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "./PatternCreator.h"

// _________________________________________________________________________
void PatternCreator::processTriple(std::array<Id, 3> triple) {
  if (!_currentSubjectIndex.has_value()) {
    // This is the first triple
    _currentSubjectIndex = VocabIndex::make(triple[0].get());
  } else if (triple[0].get() != _currentSubjectIndex.value().get()) {
    // New subject.
    finishSubject(_currentSubjectIndex.value(), _currentPattern);
    _currentSubjectIndex->get() = triple[0].get();
    _currentPattern.clear();
  }
  // Don't list predicates twice in the same pattern.
  if (_currentPattern.empty() || _currentPattern.back() != triple[1]) {
    _currentPattern.push_back(triple[1]);
  }
}

// ________________________________________________________________________________
void PatternCreator::finishSubject(VocabIndex subjectIndex,
                                   const Pattern& pattern) {
  _numDistinctSubjects++;
  _numDistinctSubjectPredicatePairs += pattern.size();
  PatternID patternId;
  auto it = _patternToIdAndCount.find(pattern);
  if (it == _patternToIdAndCount.end()) {
    // This is a new pattern, assign a new pattern ID and a count of 1.
    patternId = static_cast<PatternID>(_patternToIdAndCount.size());
    _patternToIdAndCount[pattern] = PatternIdAndCount{patternId, 1ul};

    // Count the total number of distinct predicates that appear in the
    // pattern and have not been counted before.
    for (auto predicate : pattern) {
      _distinctPredicates.insert(predicate);
    }
  } else {
    // We have already seen the same pattern for a previous subject ID, reuse
    // the ID and increase the count.
    patternId = it->second._patternId;
    it->second._count++;
  }

  // The mapping from subjects to patterns is a vector of pattern IDs. We have
  // to assign the ID NO_PATTERN to all the possible subjects that have no
  // triple.
  while (_nextUnassignedSubjectIndex < subjectIndex) {
    _subjectToPatternSerializer.push(NO_PATTERN);
    _nextUnassignedSubjectIndex = _nextUnassignedSubjectIndex.incremented();
  }

  // Write the subjectIndex-pattern mapping for this subjectIndex.
  _subjectToPatternSerializer.push(patternId);
  _nextUnassignedSubjectIndex = _nextUnassignedSubjectIndex.incremented();
}

// ____________________________________________________________________________
void PatternCreator::finish() {
  if (_isFinished) {
    return;
  }
  _isFinished = true;

  // Write the pattern of the last subject.
  finishSubject(_currentSubjectIndex.value(), _currentPattern);

  // The mapping from subjects to patterns is already written to disk at this
  // point.
  _subjectToPatternSerializer.finish();

  // Store all data in the file
  ad_utility::serialization::FileWriteSerializer patternSerializer{
      std::move(_subjectToPatternSerializer).serializer()};

  PatternStatistics patternStatistics(_numDistinctSubjectPredicatePairs,
                                      _numDistinctSubjects,
                                      _distinctPredicates.size());
  patternSerializer << patternStatistics;

  // Store the actual patterns ordered by their pattern ID. They are currently
  // stored in a hash map, so we first have to sort them.
  std::vector<std::pair<Pattern, PatternIdAndCount>> orderedPatterns;
  orderedPatterns.insert(orderedPatterns.end(), _patternToIdAndCount.begin(),
                         _patternToIdAndCount.end());
  std::sort(orderedPatterns.begin(), orderedPatterns.end(),
            [](const auto& a, const auto& b) {
              return a.second._patternId < b.second._patternId;
            });
  CompactVectorOfStrings<Pattern::value_type>::Writer patternWriter{
      std::move(patternSerializer).file()};
  for (const auto& p : orderedPatterns) {
    patternWriter.push(p.first.data(), p.first.size());
  }
  patternWriter.finish();

  // Print some statistics for the log of the index builder.
  printStatistics(patternStatistics);
}

// ____________________________________________________________________________
void PatternCreator::readPatternsFromFile(
    const std::string& filename, double& avgNumSubjectsPerPredicate,
    double& avgNumPredicatesPerSubject,
    uint64_t& numDistinctSubjectPredicatePairs,
    CompactVectorOfStrings<Id>& patterns,
    std::vector<PatternID>& subjectToPattern) {
  // Read the pattern info from the patterns file.
  LOG(INFO) << "Reading patterns from file " << filename << " ..." << std::endl;

  // Read the subjectToPatternMap.
  ad_utility::serialization::FileReadSerializer patternReader(filename);

  // Read the statistics and the patterns.
  patternReader >> subjectToPattern;
  PatternStatistics statistics;
  patternReader >> statistics;
  patternReader >> patterns;

  numDistinctSubjectPredicatePairs =
      statistics._numDistinctSubjectPredicatePairs;
  avgNumSubjectsPerPredicate = statistics._avgNumSubjectsPerPredicate;
  avgNumPredicatesPerSubject = statistics._avgNumPredicatesPerSubject;
}

// ____________________________________________________________________________
void PatternCreator::printStatistics(
    PatternStatistics patternStatistics) const {
  LOG(INFO) << "Number of distinct patterns: " << _patternToIdAndCount.size()
            << std::endl;
  LOG(INFO) << "Number of subjects with pattern: " << _numDistinctSubjects
            << " [all]" << std::endl;
  LOG(INFO) << "Total number of distinct subject-predicate pairs: "
            << _numDistinctSubjectPredicatePairs << std::endl;
  LOG(INFO) << "Average number of predicates per subject: " << std::fixed
            << std::setprecision(1)
            << patternStatistics._avgNumPredicatesPerSubject << std::endl;
  LOG(INFO) << "Average number of subjects per predicate: " << std::fixed
            << std::setprecision(0)
            << patternStatistics._avgNumSubjectsPerPredicate << std::endl;
}
