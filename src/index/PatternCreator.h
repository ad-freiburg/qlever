//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

/**
 * \file Contains functionality to create triples that are sorted by SPO, write
 *       these patterns to disk, and to read them from disk.
 */

#ifndef QLEVER_PATTERNCREATOR_H
#define QLEVER_PATTERNCREATOR_H

#include "../global/Constants.h"
#include "../global/Id.h"
#include "../global/Pattern.h"
#include "../util/MmapVector.h"
#include "../util/Serializer/SerializeVector.h"

/// Several statistics for the patterns, as well as the functionality to
/// serialize them.
struct PatternStatistics {
 public:
  // The number of distinct subject-predicate pairs contained in the patterns.
  uint64_t _numDistinctSubjectPredicatePairs;
  // The average number of distinct predicates per subject.
  double _avgNumPredicatesPerSubject;
  // The average number of distinct subjects per predicate.
  double _avgNumSubjectsPerPredicate;

  /// Uninitialized default construction, necessary for the serialization to
  /// work.
  PatternStatistics() = default;

  /// Construct from the number of distinct subject-predicate pairs, the number
  /// of distinct subjects, and the number of distinct predicates. The average
  /// statistics are calculated inside this constructor.
  PatternStatistics(uint64_t numDistinctSubjectPredicate,
                    uint64_t numDistinctSubjects,
                    uint64_t numDistinctPredicates)
      : _numDistinctSubjectPredicatePairs{numDistinctSubjectPredicate},
        _avgNumPredicatesPerSubject{
            static_cast<double>(_numDistinctSubjectPredicatePairs) /
            static_cast<double>(numDistinctSubjects)},
        _avgNumSubjectsPerPredicate{
            static_cast<double>(_numDistinctSubjectPredicatePairs) /
            static_cast<double>(numDistinctPredicates)} {}

  /// Symmetric serialization.
  template <typename Serializer>
  friend void serialize(Serializer& serializer, PatternStatistics& s) {
    serializer | s._avgNumPredicatesPerSubject;
    serializer | s._avgNumSubjectsPerPredicate;
    serializer | s._numDistinctSubjectPredicatePairs;
  }
};

/// Handle the creation and serialization of the patterns to and from disk.
/// Reading the patterns from disk is done via the static function
/// `readPatternsFromFile`. To create patterns, a `PatternCreator` object has to
/// be constructed, followed by one call to `processTriple` for each SPO triple.
/// The final writing to disk can be done explicitly by the `finish()` function,
/// but is also performed implicitly by the destructor.
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
  std::optional<Id> _currentSubjectId;
  // The pattern of `_currentSubjectId`. This might still be incomplete, because
  // more triples with the same subject might be pushed.
  Pattern _currentPattern;

  // The lowest subject Id for which we have not yet finished and written the
  // pattern.
  Id _nextUnassignedSubjectId = 0;

  // Directly serialize the mapping from subjects to patterns to disk.
  ad_utility::serialization::VectorIncrementalSerializer<
      PatternID, ad_utility::serialization::FileWriteSerializer>
      _subjectToPatternSerializer;

  // The predicates which have already occured in one of the patterns. Needed to
  // count the number of distinct predicates.
  ad_utility::HashSet<uint64_t> _distinctPredicates;

  // The number of distinct subjects and distinct subject-predicate pairs.
  uint64_t _numDistinctSubjects = 0;
  uint64_t _numDistinctSubjectPredicatePairs = 0;

  // True if `finish()` was already called.
  bool _isFinished = false;

 public:
  /// The patterns will be written to `filename` as well as to other filenames
  /// which have `filename` as a prefix.
  explicit PatternCreator(const string& filename)
      : _filename{filename}, _subjectToPatternSerializer{{filename}} {
    LOG(DEBUG) << "Computing predicate patterns ..." << std::endl;
  }

  /// This function has to be called for all the triples in the SPO permutation
  /// \param triple Must be >= all previously pushed triples wrt the SPO
  /// permutation.
  void processTriple(std::array<Id, 3> triple);

  /// Write the patterns to disk after all triples have been pushed. Calls to
  /// `processTriple` after calling `finish` lead to undefined behavior. Note
  /// that the constructor also calls `finish` to give the `PatternCreator`
  /// proper RAII semantics.
  void finish();

  /// Destructor implicitly calls `finish`
  ~PatternCreator() { finish(); }

  /// Read the patterns from `filename`. The patterns must have been written to
  /// this file using a `PatternCreator`. The patterns and all their statistics
  /// will be written to the various arguments.
  /// TODO<joka921> The storage of the pattern will change soon, so we have
  /// chosen an interface here that requires as little change as possible in the
  /// `Index` class.
  static void readPatternsFromFile(const std::string& filename,
                                   double& avgNumSubjectsPerPredicate,
                                   double& avgNumPredicatesPerSubject,
                                   uint64_t& numDistinctSubjectPredicatePairs,
                                   CompactVectorOfStrings<Id>& patterns,
                                   std::vector<PatternID>& subjectToPattern);

 private:
  void finishSubject(const Id& subjectId, const Pattern& pattern);
  void printStatistics(PatternStatistics patternStatistics) const;
};
#endif  // QLEVER_PATTERNCREATOR_H
