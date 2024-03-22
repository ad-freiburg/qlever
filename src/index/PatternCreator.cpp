//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "index/PatternCreator.h"

#include "global/SpecialIds.h"

static const Id hasPatternId = qlever::specialIds.at(HAS_PATTERN_PREDICATE);

// _________________________________________________________________________
void PatternCreator::processTriple(std::array<Id, 3> triple,
                                   bool ignoreForPatterns) {
  if (ignoreForPatterns) {
    tripleBuffer_.emplace_back(triple, ignoreForPatterns);
    return;
  }
  if (!currentSubject_.has_value()) {
    // This is the first triple.
    currentSubject_ = triple[0];
  } else if (triple[0] != currentSubject_) {
    // New subject.
    finishSubject(currentSubject_.value(), currentPattern_);
    currentSubject_ = triple[0];
    currentPattern_.clear();
  }
  tripleBuffer_.emplace_back(triple, ignoreForPatterns);
  // Don't list predicates twice in the same pattern.
  if (currentPattern_.empty() || currentPattern_.back() != triple[1]) {
    currentPattern_.push_back(triple[1]);
  }
}

// ________________________________________________________________________________
void PatternCreator::finishSubject(Id subject, const Pattern& pattern) {
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

  auto additionalTriple =
      std::array{subject, hasPatternId, Id::makeFromInt(patternId)};
  tripleSorter_.hasPatternPredicateSortedByPSO_->push(additionalTriple);
  auto curSubject = currentSubject_.value();
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
void PatternCreator::finish() {
  if (isFinished_) {
    return;
  }
  isFinished_ = true;

  // Write the pattern of the last subject.
  if (currentSubject_.has_value()) {
    finishSubject(currentSubject_.value(), currentPattern_);
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
      statistics.numDistinctSubjectPredicatePairs_;
  avgNumSubjectsPerPredicate = statistics.avgNumDistinctSubjectsPerPredicate_;
  avgNumPredicatesPerSubject = statistics.avgNumDistinctPredicatesPerSubject_;
}

// ____________________________________________________________________________
void PatternCreator::printStatistics(
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
