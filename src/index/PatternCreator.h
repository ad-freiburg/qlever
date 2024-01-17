//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

/**
 * \file Contains functionality to create triples that are sorted by SPO, write
 *       these patterns to disk, and to read them from disk.
 */

#ifndef QLEVER_PATTERNCREATOR_H
#define QLEVER_PATTERNCREATOR_H

#include "engine/idTable/CompressedExternalIdTable.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "global/Pattern.h"
#include "index/StxxlSortFunctors.h"
#include "util/BufferedVector.h"
#include "util/ExceptionHandling.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"
#include "util/TypeTraits.h"

/// Several statistics for the patterns, as well as the functionality to
/// serialize them.
struct PatternStatistics {
 public:
  // The number of distinct subject-predicate pairs contained in the patterns.
  uint64_t numDistinctSubjectPredicatePairs_;
  // The average number of distinct predicates per subject.
  double avgNumDistinctPredicatesPerSubject_;
  // The average number of distinct subjects per predicate.
  double avgNumDistinctSubjectsPerPredicate_;

  /// Uninitialized default construction, necessary for the serialization to
  /// work.
  PatternStatistics() = default;

  /// Construct from the number of distinct subject-predicate pairs, the number
  /// of distinct subjects, and the number of distinct predicates. The average
  /// statistics are calculated inside this constructor.
  PatternStatistics(uint64_t numDistinctSubjectPredicate,
                    uint64_t numDistinctSubjects,
                    uint64_t numDistinctPredicates)
      : numDistinctSubjectPredicatePairs_{numDistinctSubjectPredicate},
        avgNumDistinctPredicatesPerSubject_{
            static_cast<double>(numDistinctSubjectPredicatePairs_) /
            static_cast<double>(numDistinctSubjects)},
        avgNumDistinctSubjectsPerPredicate_{
            static_cast<double>(numDistinctSubjectPredicatePairs_) /
            static_cast<double>(numDistinctPredicates)} {}

  /// Symmetric serialization.
  AD_SERIALIZE_FRIEND_FUNCTION(PatternStatistics) {
    serializer | arg.avgNumDistinctPredicatesPerSubject_;
    serializer | arg.avgNumDistinctSubjectsPerPredicate_;
    serializer | arg.numDistinctSubjectPredicatePairs_;
  }
};

/// Handle the creation and serialization of the patterns to and from disk.
/// Reading the patterns from disk is done via the static function
/// `readPatternsFromFile`. To create patterns, a `PatternCreator` object has to
/// be constructed, followed by one call to `processTriple` for each SPO triple.
/// The final writing to disk can be done explicitly by the `finish()` function,
/// but is also performed implicitly by the destructor.
/// The mapping from subjects to pattern indices (has-pattern) and the full
/// mapping from subjects to predicates (has-predicate) is not written to disk,
/// but stored in a STXXL sorter which then has to be used to build an index for
/// these predicates.
class PatternCreatorNew {
 public:
  using PSOSorter = ad_utility::CompressedExternalIdTableSorter<SortByPSO, 3>;
  using OSPSorter4Cols =
      ad_utility::CompressedExternalIdTableSorter<SortByOSP, 4>;

  // Combine all the triples that this pattern creator creates.
  struct TripleSorter {
    std::unique_ptr<PSOSorter> hasPatternPredicateSortedByPSO_;
    std::unique_ptr<OSPSorter4Cols> triplesWithSubjectPatternsSortedByOsp_;
  };

 private:
  // The file to which the patterns will be written.
  std::string filename_;

  // Store the ID of a pattern, and the number of distinct subjects it occurs
  // with.
  struct PatternIdAndCount {
    PatternID patternId_ = 0;
    uint64_t count_ = 0;
  };
  using PatternToIdAndCount = ad_utility::HashMap<Pattern, PatternIdAndCount>;
  PatternToIdAndCount patternToIdAndCount_;

  // Between the calls to `processTriple` we have to remember the current
  // subject (the subject of the last triple for which `processTriple` was
  // called).
  std::optional<VocabIndex> currentSubjectIndex_;
  // The pattern of `currentSubjectIndex_`. This might still be incomplete,
  // because more triples with the same subject might be pushed.
  Pattern currentPattern_;

  ad_utility::serialization::FileWriteSerializer patternSerializer_;

  // Store the additional triples that are created by the pattern mechanism for
  // the `has-pattern` and `has-predicate` predicates.
  struct TripleAndIsInternal {
    std::array<Id, 3> triple_;
    bool isInternal_;
  };
  ad_utility::BufferedVector<TripleAndIsInternal> tripleBuffer_;
  TripleSorter tripleSorter_;

  // The predicates which have already occured in one of the patterns. Needed to
  // count the number of distinct predicates.
  ad_utility::HashSet<Pattern::value_type> distinctPredicates_;

  // The number of distinct subjects and distinct subject-predicate pairs.
  uint64_t numDistinctSubjects_ = 0;
  uint64_t numDistinctSubjectPredicatePairs_ = 0;

  // True if `finish()` was already called.
  bool isFinished_ = false;

 public:
  // The patterns will be written to files starting with `basename`.
  explicit PatternCreatorNew(const string& basename,
                             ad_utility::MemorySize memoryLimit)
      : filename_{basename},
        patternSerializer_{{basename}},
        tripleBuffer_(100'000, basename + ".tripleBufferForPatterns.dat"),
        tripleSorter_{
            std::make_unique<PSOSorter>(
                basename + ".additionalTriples.pso.dat", memoryLimit / 2,
                ad_utility::makeUnlimitedAllocator<Id>()),
            std::make_unique<OSPSorter4Cols>(
                basename + ".withPatterns.osp.dat", memoryLimit / 2,
                ad_utility::makeUnlimitedAllocator<Id>())} {
    LOG(DEBUG) << "Computing predicate patterns ..." << std::endl;
  }

  // This function has to be called for all the triples in the SPO permutation
  // The `triple` must be >= all previously pushed triples wrt the SPO
  // permutation.
  void processTriple(std::array<Id, 3> triple, bool ignoreForPatterns);

  // Write the patterns to disk after all triples have been pushed. Calls to
  // `processTriple` after calling `finish` lead to undefined behavior. Note
  // that the destructor also calls `finish` to give the `PatternCreatorNew`
  // proper RAII semantics.
  void finish();

  // Destructor implicitly calls `finish`.
  ~PatternCreatorNew() {
    ad_utility::terminateIfThrows([this]() { finish(); },
                                  "Finishing the underlying file of a "
                                  "`PatternCreatorNew` during destruction.");
  }

  // Read the patterns from the files with the given `basename`. The patterns
  // must have been written to files with this `basename` using
  // `PatternCreatorNew`. The patterns and all their statistics will be written
  // to the various arguments.
  static void readPatternsFromFile(const std::string& filename,
                                   double& avgNumSubjectsPerPredicate,
                                   double& avgNumPredicatesPerSubject,
                                   uint64_t& numDistinctSubjectPredicatePairs,
                                   CompactVectorOfStrings<Id>& patterns);

  // Move out the sorted triples after finishing creating the patterns.
  TripleSorter&& getTripleSorter() && {
    finish();
    return std::move(tripleSorter_);
  }

 private:
  void finishSubject(VocabIndex subjectIndex, const Pattern& pattern);

  void printStatistics(PatternStatistics patternStatistics) const;

  auto& ospSorterTriplesWithPattern() {
    return *tripleSorter_.triplesWithSubjectPatternsSortedByOsp_;
  }
};

// The old version of the pattern creator.
class PatternCreator {
 private:
  // The file to which the patterns will be written.
  std::string _filename;

  // Store the Id of a pattern, and the number of distinct subjects it occurs
  // with.
  struct PatternIdAndCount {
    PatternID _patternId = 0;
    uint64_t _count = 0;
  };
  using PatternToIdAndCount = ad_utility::HashMap<Pattern, PatternIdAndCount>;
  PatternToIdAndCount _patternToIdAndCount;

  // Between the calls to `processTriple` we have to remember the current
  // subject (the subject of the last triple for which `processTriple` was
  // called).
  std::optional<VocabIndex> _currentSubjectIndex;
  // The pattern of `currentSubjectIndex_`. This might still be incomplete,
  // because more triples with the same subject might be pushed.
  Pattern _currentPattern;

  // The lowest subject Id for which we have not yet finished and written the
  // pattern.
  VocabIndex _nextUnassignedSubjectIndex = VocabIndex::make(0);

  // Directly serialize the mapping from subjects to patterns to disk.
  ad_utility::serialization::VectorIncrementalSerializer<
      PatternID, ad_utility::serialization::FileWriteSerializer>
      _subjectToPatternSerializer;

  // The predicates which have already occured in one of the patterns. Needed to
  // count the number of distinct predicates.
  ad_utility::HashSet<Pattern::value_type> _distinctPredicates;

  // The number of distinct subjects and distinct subject-predicate pairs.
  uint64_t _numDistinctSubjects = 0;
  uint64_t _numDistinctSubjectPredicatePairs = 0;

  // True if `finish()` was already called.
  bool _isFinished = false;

 public:
  // The patterns will be written to `filename` as well as to other filenames
  // which have `filename` as a prefix.
  explicit PatternCreator(const string& filename)
      : _filename{filename}, _subjectToPatternSerializer{{filename}} {
    LOG(DEBUG) << "Computing predicate patterns ..." << std::endl;
  }

  // This function has to be called for all the triples in the SPO permutation
  // \param triple Must be >= all previously pushed triples wrt the SPO
  // permutation.
  void processTriple(std::array<Id, 3> triple);

  // Write the patterns to disk after all triples have been pushed. Calls to
  // `processTriple` after calling `finish` lead to undefined behavior. Note
  // that the constructor also calls `finish` to give the `PatternCreator`
  // proper RAII semantics.
  void finish();

  // Destructor implicitly calls `finish`
  ~PatternCreator() {
    ad_utility::terminateIfThrows([this]() { finish(); },
                                  "Finishing the underlying file of a "
                                  "`PatternCreator` during destruction.");
  }

  // Read the patterns from `filename`. The patterns must have been written to
  // this file using a `PatternCreator`. The patterns and all their statistics
  // will be written to the various arguments.
  static void readPatternsFromFile(const std::string& filename,
                                   double& avgNumSubjectsPerPredicate,
                                   double& avgNumPredicatesPerSubject,
                                   uint64_t& numDistinctSubjectPredicatePairs,
                                   CompactVectorOfStrings<Id>& patterns,
                                   std::vector<PatternID>& subjectToPattern);

 private:
  void finishSubject(VocabIndex subjectIndex, const Pattern& pattern);
  void printStatistics(PatternStatistics patternStatistics) const;
};
#endif  // QLEVER_PATTERNCREATOR_H
