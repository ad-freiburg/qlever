// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2014-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)
#pragma once

#include <util/json.h>
#include <string>
#include <vector>
#include <optional>
#include <index/CompressedString.h>
#include <index/StringSortComparator.h>
#include <index/Vocabulary.h>
#include <global/Id.h>
#include <parser/TripleComponent.h>
#include <parser/TurtleParser.h>
#include <array>

// Forward declarations.
class IdTable;
class TextBlockMetaData;

// an empty Tag type.
class TurtleParserAuto{};

using json = nlohmann::json;

enum struct Permutations {
  PSO, POS, SPO, SOP, OPS, OSP
};
class Index {
 public:
  /// Forbid copy and assignment.
  Index& operator=(const Index&) = delete;
  Index(const Index&) = delete;

  /// Allow move construction, which is mostly used in unit tests.
  Index(Index&&) noexcept = default;

  Index();


  // Creates an index from a file. Parameter Parser must be able to split the
  // file's format into triples.
  // Will write vocabulary and on-disk index data.
  // !! The index can not directly be used after this call, but has to be setup
  // by createFromOnDiskIndex after this call.
  template <class Parser>
  void createFromFile(const std::string& filename);

  void addPatternsToExistingIndex();

  // Creates an index object from an on disk index that has previously been
  // constructed. Read necessary meta data into memory and opens file handles.
  void createFromOnDiskIndex(const std::string& onDiskBase);

  // Adds a text index to a complete KB index. First reads the given context
  // file (if file name not empty), then adds words from literals (if true).
  void addTextFromContextFile(const std::string& contextFile,
                              bool addWordsFromLiterals);

  // Build docsDB file from given file (one text record per line).
  void buildDocsDB(const std::string& docsFile);

  // Adds text index from on disk index that has previously been constructed.
  // Read necessary meta data into memory and opens file handles.
  void addTextFromOnDiskIndex();

  using Vocab = Vocabulary<CompressedString, TripleComponentComparator> ;
  const Vocab& getVocab() const;
  Vocab& getNonConstVocabForTesting();

  using TextVocab = Vocabulary<std::string, SimpleStringComparator>;
  const TextVocab& getTextVocab() const;

  // --------------------------------------------------------------------------
  //  -- RETRIEVAL ---
  // --------------------------------------------------------------------------
  typedef std::vector<std::array<Id, 1>> WidthOneList;
  typedef std::vector<std::array<Id, 2>> WidthTwoList;
  typedef std::vector<std::array<Id, 3>> WidthThreeList;
  typedef std::vector<std::array<Id, 4>> WidthFourList;
  typedef std::vector<std::array<Id, 5>> WidthFiveList;
  typedef std::vector<vector<Id>> VarWidthList;

  // --------------------------------------------------------------------------
  // RDF RETRIEVAL
  // --------------------------------------------------------------------------
  size_t relationCardinality(const std::string& relationName) const;

  size_t subjectCardinality(const TripleComponent& sub) const;

  size_t objectCardinality(const TripleComponent& obj) const;

  // TODO<joka921> Once we have an overview over the folding this logic should
  // probably not be in the index class.
  std::optional<std::string> idToOptionalString(Id id) const;

  bool getId(const std::string& element, Id* id) const;

  std::pair<Id, Id> prefix_range(const std::string& prefix) const;

  const vector<PatternID>& getHasPattern() const;
  const CompactVectorOfStrings<Id>& getHasPredicate() const;
  const CompactVectorOfStrings<Id>& getPatterns() const;
  /**
   * @return The multiplicity of the Entites column (0) of the full has-relation
   *         relation after unrolling the patterns.
   */
  double getHasPredicateMultiplicityEntities() const;

  /**
   * @return The multiplicity of the Predicates column (0) of the full
   * has-relation relation after unrolling the patterns.
   */
  double getHasPredicateMultiplicityPredicates() const;

  /**
   * @return The size of the full has-relation relation after unrolling the
   *         patterns.
   */
  size_t getHasPredicateFullSize() const;

  // --------------------------------------------------------------------------
  // TEXT RETRIEVAL
  // --------------------------------------------------------------------------
  std::string_view wordIdToString(WordIndex wordIndex) const;

  size_t getSizeEstimate(const std::string& words) const;

  void getContextListForWords(const std::string& words, IdTable* result) const;

  void getECListForWordsOneVar(const std::string& words, size_t limit,
                               IdTable* result) const;

  // With two or more variables.
  void getECListForWords(const std::string& words, size_t nofVars, size_t limit,
                         IdTable* result) const;

  // With filtering. Needs many template instantiations but
  // only nofVars truly makes a difference. Others are just data types
  // of result tables.
  void getFilteredECListForWords(const std::string& words, const IdTable& filter,
                                 size_t filterColumn, size_t nofVars,
                                 size_t limit, IdTable* result) const;

  // Special cast with a width-one filter.
  void getFilteredECListForWordsWidthOne(const std::string& words,
                                         const IdTable& filter, size_t nofVars,
                                         size_t limit, IdTable* result) const;

  void getContextEntityScoreListsForWords(const std::string& words,
                                          vector<TextRecordIndex>& cids,
                                          vector<Id>& eids,
                                          vector<Score>& scores) const;

  template <size_t I>
  void getECListForWordsAndSingleSub(const std::string& words,
                                     const vector<std::array<Id, I>>& subres,
                                     size_t subResMainCol, size_t limit,
                                     vector<std::array<Id, 3 + I>>& res) const;

  void getECListForWordsAndTwoW1Subs(const std::string& words,
                                     const vector<std::array<Id, 1>> subres1,
                                     const vector<std::array<Id, 1>> subres2,
                                     size_t limit,
                                     vector<std::array<Id, 5>>& res) const;

  void getECListForWordsAndSubtrees(
      const std::string& words,
      const vector<ad_utility::HashMap<Id, vector<vector<Id>>>>& subResVecs,
      size_t limit, vector<vector<Id>>& res) const;

  void getWordPostingsForTerm(const std::string& term, vector<TextRecordIndex>& cids,
                              vector<Score>& scores) const;

  void getEntityPostingsForTerm(const std::string& term,
                                vector<TextRecordIndex>& cids, vector<Id>& eids,
                                vector<Score>& scores) const;

  std::string getTextExcerpt(TextRecordIndex cid) const;

  // Only for debug reasons and external encoding tests.
  // Supply an empty vector to dump all lists above a size threshold.
  void dumpAsciiLists(const vector<std::string>& lists, bool decodeGapsFreq) const;

  void dumpAsciiLists(const TextBlockMetaData& tbmd) const;

  float getAverageNofEntityContexts() const;

  void setKbName(const std::string& name);

  void setTextName(const std::string& name);

  void setUsePatterns(bool usePatterns);

  void setLoadAllPermutations(bool loadAllPermutations);

  void setKeepTempFiles(bool keepTempFiles);

  uint64_t& stxxlMemoryInBytes();
  const uint64_t& stxxlMemoryInBytes() const;

  void setOnDiskBase(const std::string& onDiskBase);

  void setSettingsFile(const std::string& filename);

  void setPrefixCompression(bool compressed);

  void setNumTriplesPerBatch(uint64_t numTriplesPerBatch);

  const std::string& getTextName() const;

  const std::string& getKbName() const;

  size_t getNofTriples() const;

  size_t getNofTextRecords() const;
  size_t getNofWordPostings() const;
  size_t getNofEntityPostings() const;

  size_t getNofSubjects() const;

  size_t getNofObjects() const;

  size_t getNofPredicates() const;

  bool hasAllPermutations() const;

  // _____________________________________________________________________________
  vector<float> getMultiplicities(const TripleComponent& key,
                                  Permutations permutation) const;

  // ___________________________________________________________________
  vector<float> getMultiplicities(Permutations p) const;

  /**
   * @brief Perform a scan for one key i.e. retrieve all YZ from the XYZ
   * permutation for a specific key value of X
   * @tparam Permutation The permutations Index::POS()... have different types
   * @param key The key (in Id space) for which to search, e.g. fixed value for
   * O in OSP permutation.
   * @param result The Id table to which we will write. Must have 2 columns.
   * @param p The Permutation to use (in particularly POS(), SOP,... members of
   * Index class).
   */
  void scan(Id key, IdTable* result, Permutations p,
            ad_utility::SharedConcurrentTimeoutTimer timer = nullptr) const;

  /**
   * @brief Perform a scan for one key i.e. retrieve all YZ from the XYZ
   * permutation for a specific key value of X
   * @tparam Permutation The permutations Index::POS()... have different types
   * @param key The key (as a raw string that is yet to be transformed to index
   * space) for which to search, e.g. fixed value for O in OSP permutation.
   * @param result The Id table to which we will write. Must have 2 columns.
   * @param p The Permutation to use (in particularly POS(), SOP,... members of
   * Index class).
   */
  void scan(const TripleComponent& key, IdTable* result, const Permutations& p,
            ad_utility::SharedConcurrentTimeoutTimer timer = nullptr) const;

  /**
   * @brief Perform a scan for two keys i.e. retrieve all Z from the XYZ
   * permutation for specific key values of X and Y.
   * @tparam Permutation The permutations Index::POS()... have different types
   * @param col0String The first key (as a raw string that is yet to be
   * transformed to index space) for which to search, e.g. fixed value for O in
   * OSP permutation.
   * @param col1String The second key (as a raw string that is yet to be
   * transformed to index space) for which to search, e.g. fixed value for S in
   * OSP permutation.
   * @param result The Id table to which we will write. Must have 2 columns.
   * @param p The Permutation to use (in particularly POS(), SOP,... members of
   * Index class).
   */
  // _____________________________________________________________________________
  void scan(const TripleComponent& col0String,
            const TripleComponent& col1String, IdTable* result,
            Permutations p,
            ad_utility::SharedConcurrentTimeoutTimer timer = nullptr) const;

  // Count the number of "QLever-internal" triples (predicate ql:langtag or
  // predicate starts with @) and all other triples (that were actually part of
  // the input).
  std::pair<size_t, size_t> getNumTriplesActuallyAndAdded() const;
};
