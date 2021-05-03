// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include "../engine/IdTable.h"
#include "../global/Pattern.h"
#include "./Vocabulary.h"

// Forward declaration for Pimpl idiom
class IndexImpl;

/**
 * Used as a Template Argument to the createFromFile method, when we do not yet
 * know, which Tokenizer Specialization of the TurtleParser we are going to use
 */
class TurtleParserDummy {};

class Index {
 public:
  // Forbid copy and assignment
  Index& operator=(const Index&) = delete;

  Index(const Index&) = delete;

  Index();
  ~Index();

  enum class Permutation { POS, PSO, SPO, SOP, OPS, OSP };

  // Creates an index from a file. Parameter Parser must be able to split the
  // file's format into triples.
  // Will write vocabulary and on-disk index data.
  // !! The index can not directly be used after this call, but has to be setup
  // by createFromOnDiskIndex after this call.
  template <class Parser>
  void createFromFile(const string& filename);

  void addPatternsToExistingIndex();

  // Creates an index object from an on disk index
  // that has previously been constructed.
  // Read necessary meta data into memory and opens file handles.
  void createFromOnDiskIndex(const string& onDiskBase);

  // Adds a text index to a fully initialized KB index.
  // Reads a context file and builds the index for the first time.
  void addTextFromContextFile(const string& contextFile);

  void buildDocsDB(const string& docsFile);

  // Adds text index from on disk index that has previously been constructed.
  // Read necessary meta data into memory and opens file handles.
  void addTextFromOnDiskIndex();

  [[nodiscard]] const RdfsVocabulary& getVocab() const;

  [[nodiscard]] const TextVocabulary& getTextVocab() const;

  // --------------------------------------------------------------------------
  //  -- RETRIEVAL ---
  // --------------------------------------------------------------------------
  typedef vector<array<Id, 1>> WidthOneList;
  typedef vector<array<Id, 2>> WidthTwoList;
  typedef vector<array<Id, 3>> WidthThreeList;
  typedef vector<array<Id, 4>> WidthFourList;
  typedef vector<array<Id, 5>> WidthFiveList;
  typedef vector<vector<Id>> VarWidthList;

  // --------------------------------------------------------------------------
  // RDF RETRIEVAL
  // --------------------------------------------------------------------------
  [[nodiscard]] size_t relationCardinality(const string& relationName) const;

  [[nodiscard]] size_t subjectCardinality(const string& sub) const;

  [[nodiscard]] size_t objectCardinality(const string& obj) const;

  [[nodiscard]] size_t sizeEstimate(const string& sub, const string& pred,
                                    const string& obj) const;

  [[nodiscard]] std::optional<string> idToOptionalString(Id id) const;

  [[nodiscard]] const vector<PatternID>& getHasPattern() const;
  [[nodiscard]] const CompactStringVector<Id, Id>& getHasPredicate() const;
  [[nodiscard]] const CompactStringVector<size_t, Id>& getPatterns() const;
  /**
   * @return The multiplicity of the Entites column (0) of the full has-relation
   *         relation after unrolling the patterns.
   */
  [[nodiscard]] double getHasPredicateMultiplicityEntities() const;

  /**
   * @return The multiplicity of the Predicates column (0) of the full
   * has-relation relation after unrolling the patterns.
   */
  [[nodiscard]] double getHasPredicateMultiplicityPredicates() const;

  /**
   * @return The size of the full has-relation relation after unrolling the
   *         patterns.
   */
  [[nodiscard]] size_t getHasPredicateFullSize() const;

  // --------------------------------------------------------------------------
  // TEXT RETRIEVAL
  // --------------------------------------------------------------------------
  const string& wordIdToString(Id id) const;

  size_t getSizeEstimate(const string& words) const;

  void getContextListForWords(const string& words, IdTable* result) const;

  void getECListForWordsOneVar(const string& words, size_t limit,
                               IdTable* result) const;

  // With two or more variables.
  void getECListForWords(const string& words, size_t nofVars, size_t limit,
                         IdTable* result) const;

  // With filtering. Needs many template instantiations but
  // only nofVars truly makes a difference. Others are just data types
  // of result tables.
  void getFilteredECListForWords(const string& words, const IdTable& filter,
                                 size_t filterColumn, size_t nofVars,
                                 size_t limit, IdTable* result) const;

  // Special cast with a width-one filter.
  void getFilteredECListForWordsWidthOne(const string& words,
                                         const IdTable& filter, size_t nofVars,
                                         size_t limit, IdTable* result) const;

  void getContextEntityScoreListsForWords(const string& words, vector<Id>& cids,
                                          vector<Id>& eids,
                                          vector<Score>& scores) const;

  template <size_t I>
  void getECListForWordsAndSingleSub(const string& words,
                                     const vector<array<Id, I>>& subres,
                                     size_t subResMainCol, size_t limit,
                                     vector<array<Id, 3 + I>>& res) const;

  void getECListForWordsAndTwoW1Subs(const string& words,
                                     const vector<array<Id, 1>>& subres1,
                                     const vector<array<Id, 1>>& subres2,
                                     size_t limit,
                                     vector<array<Id, 5>>& res) const;

  void getECListForWordsAndSubtrees(
      const string& words,
      const vector<ad_utility::HashMap<Id, vector<vector<Id>>>>& subResVecs,
      size_t limit, vector<vector<Id>>& res) const;

  void getWordPostingsForTerm(const string& term, vector<Id>& cids,
                              vector<Score>& scores) const;

  void getEntityPostingsForTerm(const string& term, vector<Id>& cids,
                                vector<Id>& eids, vector<Score>& scores) const;

  string getTextExcerpt(Id cid) const;

  float getAverageNofEntityContexts() const;

  void setKbName(const string& name);

  void setTextName(const string& name);

  void setUsePatterns(bool usePatterns);

  void setOnDiskLiterals(bool onDiskLiterals);

  void setKeepTempFiles(bool keepTempFiles);

  void setOnDiskBase(const std::string& onDiskBase);

  void setSettingsFile(const std::string& filename);

  void setPrefixCompression(bool compressed);

  const string& getTextName() const;

  const string& getKbName() const;

  size_t getNofTriples() const;

  size_t getNofTextRecords() const;
  size_t getNofWordPostings() const;
  size_t getNofEntityPostings() const;

  size_t getNofSubjects() const;

  size_t getNofObjects() const;

  size_t getNofPredicates() const;

  bool hasAllPermutations() const;

  vector<float> getMultiplicities(const string& key, const Permutation p) const;
  vector<float> getMultiplicities(const Permutation p) const;

  // Only for debug reasons and external encoding tests.
  // Supply an empty vector to dump all lists above a size threshold.
  void dumpAsciiLists(const vector<string>& lists, bool decodeGapsFreq) const;

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
  void scan(Id key, IdTable* result, const Permutation& p,
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
  void scan(const string& key, IdTable* result, const Permutation& p,
            ad_utility::SharedConcurrentTimeoutTimer timer = nullptr) const;

  /**
   * @brief Perform a scan for two keys i.e. retrieve all Z from the XYZ
   * permutation for specific key values of X and Y.
   * @tparam Permutation The permutations Index::POS()... have different types
   * @param keyFirst The first key (as a raw string that is yet to be
   * transformed to index space) for which to search, e.g. fixed value for O in
   * OSP permutation.
   * @param keySecond The second key (as a raw string that is yet to be
   * transformed to index space) for which to search, e.g. fixed value for S in
   * OSP permutation.
   * @param result The Id table to which we will write. Must have 2 columns.
   * @param p The Permutation to use (in particularly POS(), SOP,... members of
   * Index class).
   */
  // _____________________________________________________________________________
  void scan(const string& keyFirst, const string& keySecond, IdTable* result,
            const Permutation& p) const;

 private:
  std::unique_ptr<IndexImpl> _pimpl;
};
