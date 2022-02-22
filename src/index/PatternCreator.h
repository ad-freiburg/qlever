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
  uint64_t _numDistinctSubjectPredicate;
  // The average number of distinct predicates per subject.
  double _avgNumPredicatesPerSubject;
  // The average number of distinct subjects per predicate.
  double _avgNumSubjectsPerPredicate;

 public:
  /// Uninitialized default construction, necessary for the serialization to
  /// work.
  PatternStatistics() = default;

  /// Construct from the number of distinct subject-predicate pairs, the number
  /// of distinct subjects, and the number of distinct predicates. The average
  /// statistics are calculated inside this constructor.
  PatternStatistics(uint64_t numDistinctSubjectPredicate,
                    uint64_t numDistinctSubjects,
                    uint64_t numDistinctPredicates)
      : _numDistinctSubjectPredicate{numDistinctSubjectPredicate},
        _avgNumPredicatesPerSubject{
            static_cast<double>(_numDistinctSubjectPredicate) /
            static_cast<double>(numDistinctSubjects)},
        _avgNumSubjectsPerPredicate{
            static_cast<double>(_numDistinctSubjectPredicate) /
            static_cast<double>(numDistinctPredicates)} {}

  /// Serialization. Also serializes and checks a magic byte and the
  /// `PATTERNS_FILE_VERSION` to detect old versions of the patterns files which
  /// are not compatible anymore.
  template <typename Serializer>
  friend void serialize(Serializer& serializer, PatternStatistics& s) {
    // First serialize and check the magic byte and the version.
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

    // Now serialize the actual members.
    serializer | s._avgNumPredicatesPerSubject;
    serializer | s._avgNumSubjectsPerPredicate;
    serializer | s._numDistinctSubjectPredicate;
  }
};

/// Handle the creation and serialization of the patterns to and from disk.
/// Reading the patterns from disk is done via the static function
/// `readPatternsFromFile`. To create patterns, a `PatternCreator` object has to
/// be constructed, followed by one call to `pushTriple` for each SPO triple.
/// The final writing to disk can be done explicitly by the `finish()` function,
/// but is also performed implicitly by the destructor.
class PatternCreator {
 private:
  static constexpr const char* _subjectToPatternSuffix = ".subjectsToPatterns";
  // The file to which the patterns will be written.
  std::string _filename;

  // Store the Id of a pattern, and the number of distinct subjects it occurs
  // with.
  struct PatternIdAndCount {
    PatternID _patternId = 0;
    uint64_t _count = 0;
  };
  using PatternsCountMap = ad_utility::HashMap<Pattern, PatternIdAndCount>;
  PatternsCountMap _patternCounts;

  // Between the calls to `pushTriple` we have to remember the current subject
  // (the subject of the last triple for which `pushTriple` was called).
  std::optional<Id> _currentSubject;
  // The pattern of `_currentSubject`. This might still be incomplete, because
  // more triples with the same subject might be pushed.
  Pattern _currentPattern;

  // The lowest subject Id for which we have not yet finished and written the
  // pattern.
  Id _nextUnassignedSubject = 0;

  // Directly serialize the mapping from subjects to patterns to disk.
  ad_utility::serialization::VectorIncrementalSerializer<
      PatternID, ad_utility::serialization::FileWriteSerializer>
      _subjectToPatternSerializer;

  // The predicates which have already occured in one of the patterns. Needed to
  // count the number of distinct predicates.
  ad_utility::HashSet<uint64_t> _distinctPredicates;

  // The number of distinct subjects, and distinct subject-predicate pairs.
  uint64_t _numberOfDistinctSubjects = 0;
  uint64_t _numberOfDistinctSubjectPredicate = 0;

  // True if `finish()` was already called.
  bool _isFinished = false;

 public:
  /// The patterns will be written to `filename` as well as to other filenames
  /// which have `filename` as a prefix.
  explicit PatternCreator(const string& filename)
      : _filename{filename},
        _subjectToPatternSerializer{{filename + _subjectToPatternSuffix}} {
    LOG(INFO) << "Computing predicate patterns ..." << std::endl;
  }

  /// This function has to be called for all the triples in the SPO permutation
  /// \param triple Must be >= all previously pushed triples wrt the SPO
  /// permutation.
  void pushTriple(std::array<Id, 3> triple);

  /// Write the patterns to disk after all triples have been pushed. Calls to
  /// `pushTriple` after calling `finish` lead to undefined behavior. Note that
  /// the constructor also calls `finish` to give the `PatternCreator` proper
  /// RAII semantics.
  void finish();

  /// Destructor implicitly calls `finish`
  ~PatternCreator() { finish(); }

  ///  Read the patterns from `filename`. The patterns must have been written to
  ///  this file using a `PatternCreator`. The patterns and all their statistics
  ///  will be written to the various arguments.
  /// TODO<joka921> The storage of the pattern will change soon, so we have
  /// chosen an interface here that requires as little change as possible in the
  /// `Index` class.
  static void readPatternsFromFile(const std::string& filename,
                                   double& averageNumSubjectsPerPredicate,
                                   double& averageNumPredicatesPerSubject,
                                   uint64_t& numDistinctSubjectPredicatePairs,
                                   CompactVectorOfStrings<Id>& patterns,
                                   std::vector<PatternID>& subjectToPattern);

 private:
  void finishSubject(const Pattern& pattern, const Id& subject);
  void printStatistics(uint64_t numPatterns) const;
};
#endif  // QLEVER_PATTERNCREATOR_H
