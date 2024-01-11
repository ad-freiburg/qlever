//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/PatternCreator.h"

#include "global/SpecialIds.h"

static const Id hasPatternId = qlever::specialIds.at(HAS_PATTERN_PREDICATE);

// _________________________________________________________________________
void PatternCreatorNew::processTriple(std::array<Id, 3> triple,
                                      bool ignoreForPatterns) {
  if (ignoreForPatterns) {
    tripleBuffer_.emplace_back(triple, ignoreForPatterns);
    return;
  }
  if (!currentSubjectIndex_.has_value()) {
    // This is the first triple
    currentSubjectIndex_ = triple[0].getVocabIndex();
  } else if (triple[0].getVocabIndex() != currentSubjectIndex_) {
    // New subject.
    finishSubject(currentSubjectIndex_.value(), currentPattern_);
    currentSubjectIndex_ = triple[0].getVocabIndex();
    currentPattern_.clear();
  }
  tripleBuffer_.emplace_back(triple, ignoreForPatterns);
  // Don't list predicates twice in the same pattern.
  if (currentPattern_.empty() || currentPattern_.back() != triple[1]) {
    currentPattern_.push_back(triple[1]);
  }
}

// ________________________________________________________________________________
void PatternCreatorNew::finishSubject(VocabIndex subjectIndex,
                                      const Pattern& pattern) {
  numDistinctSubjects_++;
  numDistinctSubjectPredicatePairs_ += pattern.size();
  PatternID patternId;
  auto it = patternToIdAndCount_.find(pattern);
  if (it == patternToIdAndCount_.end()) {
    // This is a new pattern, assign a new pattern ID and a count of 1.
    patternId = static_cast<PatternID>(patternToIdAndCount_.size());
    patternToIdAndCount_[pattern] = PatternIdAndCount{patternId, 1UL};

    // Count the total number of distinct predicates that appear in the
    // pattern and have not been counted before.
    for (auto predicate : pattern) {
      distinctPredicates_.insert(predicate);
    }
  } else {
    // We have already seen the same pattern for a previous subject ID, reuse
    // the ID and increase the count.
    patternId = it->second.patternId_;
    it->second.count_++;
  }

  auto additionalTriple = std::array{Id::makeFromVocabIndex(subjectIndex),
                                     hasPatternId, Id::makeFromInt(patternId)};
  tripleSorter_.hasPatternPredicateSortedByPSO_->push(additionalTriple);
  auto curSubject = Id::makeFromVocabIndex(currentSubjectIndex_.value());
  std::ranges::for_each(tripleBuffer_, [this, patternId,
                                        &curSubject](const auto& t) {
    const auto& [s, p, o] = t.triple_;
    // It might happen that the `tripleBuffer_` contains different subjects
    // which are purely internal and therefore have no pattern.
    auto actualPatternId =
        Id::makeFromInt(curSubject != s ? NO_PATTERN : patternId);
    AD_CORRECTNESS_CHECK(curSubject == s || t.isInternal_);
    ospSorterTriplesWithPattern().push(std::array{s, p, o, actualPatternId});
  });
  tripleBuffer_.clear();
}

// ____________________________________________________________________________
void PatternCreatorNew::finish() {
  if (isFinished_) {
    return;
  }
  isFinished_ = true;

  // Write the pattern of the last subject.
  if (currentSubjectIndex_.has_value()) {
    finishSubject(currentSubjectIndex_.value(), currentPattern_);
  }

  // Store all data in the file
  PatternStatistics patternStatistics(numDistinctSubjectPredicatePairs_,
                                      numDistinctSubjects_,
                                      distinctPredicates_.size());
  patternSerializer_ << patternStatistics;

  // Store the actual patterns ordered by their pattern ID. They are currently
  // stored in a hash map, so we first have to sort them.
  // TODO<C++23> Use `ranges::to<vector>`.
  std::vector<std::pair<Pattern, PatternIdAndCount>> orderedPatterns{
      patternToIdAndCount_.begin(), patternToIdAndCount_.end()};
  std::ranges::sort(orderedPatterns, std::less<>{},
                    [](const auto& a) { return a.second.patternId_; });
  CompactVectorOfStrings<Pattern::value_type>::Writer patternWriter{
      std::move(patternSerializer_).file()};
  for (const auto& pattern : orderedPatterns | std::views::keys) {
    patternWriter.push(pattern.data(), pattern.size());
  }
  patternWriter.finish();

  // Print some statistics for the log of the index builder.
  printStatistics(patternStatistics);
}

// ____________________________________________________________________________
void PatternCreatorNew::readPatternsFromFile(
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
      statistics.numDistinctSubjectPredicatePairs_;
  avgNumSubjectsPerPredicate = statistics.avgNumDistinctSubjectsPerPredicate_;
  avgNumPredicatesPerSubject = statistics.avgNumDistinctPredicatesPerSubject_;
}

// ____________________________________________________________________________
void PatternCreatorNew::printStatistics(
    PatternStatistics patternStatistics) const {
  LOG(INFO) << "Number of distinct patterns: " << patternToIdAndCount_.size()
            << std::endl;
  LOG(INFO) << "Number of subjects with pattern: " << numDistinctSubjects_
            << " [all]" << std::endl;
  LOG(INFO) << "Total number of distinct subject-predicate pairs: "
            << numDistinctSubjectPredicatePairs_ << std::endl;
  LOG(INFO) << "Average number of predicates per subject: " << std::fixed
            << std::setprecision(1)
            << patternStatistics.avgNumDistinctPredicatesPerSubject_
            << std::endl;
  LOG(INFO) << "Average number of subjects per predicate: " << std::fixed
            << std::setprecision(0)
            << patternStatistics.avgNumDistinctSubjectsPerPredicate_
            << std::endl;
}

// All the legacy code of the old pattern stuff.
// _________________________________________________________________________
void PatternCreator::processTriple(std::array<Id, 3> triple) {
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
    _patternToIdAndCount[pattern] = PatternIdAndCount{patternId, 1UL};

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
  if (_currentSubjectIndex.has_value()) {
    finishSubject(_currentSubjectIndex.value(), _currentPattern);
  }

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
      statistics.numDistinctSubjectPredicatePairs_;
  avgNumSubjectsPerPredicate = statistics.avgNumDistinctSubjectsPerPredicate_;
  avgNumPredicatesPerSubject = statistics.avgNumDistinctPredicatesPerSubject_;
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
            << patternStatistics.avgNumDistinctPredicatesPerSubject_
            << std::endl;
  LOG(INFO) << "Average number of subjects per predicate: " << std::fixed
            << std::setprecision(0)
            << patternStatistics.avgNumDistinctSubjectsPerPredicate_
            << std::endl;
}
