//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/PatternCreator.h"

#include "global/SpecialIds.h"

static const Id hasPatternId = qlever::specialIds.at(HAS_PATTERN_PREDICATE);
static const Id hasPredicateId = qlever::specialIds.at(HAS_PREDICATE_PREDICATE);

// _________________________________________________________________________
void PatternCreator::processTriple(std::array<Id, 3> triple,
                                   bool ignoreForPatterns) {
  _tripleBuffer.emplace_back(triple, ignoreForPatterns);
  if (ignoreForPatterns) {
    return;
  }
  if (!_currentSubjectIndex.has_value()) {
    // This is the first triple
    _currentSubjectIndex = triple[0].getVocabIndex();
  } else if (triple[0].getVocabIndex() != _currentSubjectIndex) {
    // New subject.
    finishSubject(_currentSubjectIndex.value(), _currentPattern);
    _currentSubjectIndex = triple[0].getVocabIndex();
    _currentPattern.clear();
  }
  // Don't list predicates twice in the same pattern.
  if (_currentPattern.empty() || _currentPattern.back() != triple[1]) {
    _currentPattern.push_back(triple[1]);
    // This is wasteful and currently not needed. If we use those lines, then we
    // get a fully materialized `has-predicate` relation.
    /*
    _additionalTriplesPsoSorter.push(
        std::array{Id::makeFromVocabIndex(_currentSubjectIndex.value()),
                   hasPredicateId, triple[1]});
                   */
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

  _additionalTriplesPsoSorter.push(
      std::array{Id::makeFromVocabIndex(subjectIndex), hasPatternId,
                 Id::makeFromInt(patternId)});
  std::ranges::for_each(_tripleBuffer, [this, patternId](const auto& t) {
    const auto& [s, p, o] = t.first;
    _fullPsoSorter.push(std::array{
        s, p, o, Id::makeFromInt(t.second ? NO_PATTERN : patternId)});
  });
  _tripleBuffer.clear();
}

// ____________________________________________________________________________
void PatternCreator::finish() {
  if (_isFinished) {
    return;
  }
  _isFinished = true;

  // Write the pattern of the last subject.
  if (_currentSubjectIndex.has_value()) {
    finishSubject(_currentSubjectIndex.value(), _currentPattern);
  }

  // Store all data in the file
  PatternStatistics patternStatistics(_numDistinctSubjectPredicatePairs,
                                      _numDistinctSubjects,
                                      _distinctPredicates.size());
  _patternSerializer << patternStatistics;

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
      std::move(_patternSerializer).file()};
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
    CompactVectorOfStrings<Id>& patterns) {
  // Read the pattern info from the patterns file.
  LOG(INFO) << "Reading patterns from file " << filename << " ..." << std::endl;

  // Read the subjectToPatternMap.
  ad_utility::serialization::FileReadSerializer patternReader(filename);

  // Read the statistics and the patterns.
  PatternStatistics statistics;
  patternReader >> statistics;
  patternReader >> patterns;

  numDistinctSubjectPredicatePairs =
      statistics._numDistinctSubjectPredicatePairs;
  avgNumSubjectsPerPredicate = statistics._avgNumDistinctSubjectsPerPredicate;
  avgNumPredicatesPerSubject = statistics._avgNumDistinctPredicatesPerSubject;
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
            << patternStatistics._avgNumDistinctPredicatesPerSubject
            << std::endl;
  LOG(INFO) << "Average number of subjects per predicate: " << std::fixed
            << std::setprecision(0)
            << patternStatistics._avgNumDistinctSubjectsPerPredicate
            << std::endl;
}
