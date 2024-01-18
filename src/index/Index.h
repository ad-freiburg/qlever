// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2014-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)
#pragma once

#include <array>
#include <optional>
#include <string>
#include <vector>

#include "global/Id.h"
#include "index/CompressedString.h"
#include "index/Permutation.h"
#include "index/StringSortComparator.h"
#include "index/Vocabulary.h"
#include "parser/TripleComponent.h"
#include "util/CancellationHandle.h"

// Forward declarations.
class IdTable;
class TextBlockMetaData;
class IndexImpl;

class Index {
 private:
  // Pimpl to reduce compile times.
  std::unique_ptr<IndexImpl> pimpl_;

 public:
  // Alongside the actual knowledge graph QLever stores additional triples
  // for optimized query processing. This struct is used to report various
  // statistics (number of triples, distinct number of subjects, etc.) for which
  // the value differs when you also consider the added triples.
  struct NumNormalAndInternal {
    size_t normal_{};
    size_t internal_{};
    size_t normalAndInternal_() const { return normal_ + internal_; }
    bool operator==(const NumNormalAndInternal&) const = default;
  };

  // Store all information about possible search results from the text index in
  // one place.
  // Every vector is either empty or has the same size as the others.
  struct WordEntityPostings {
    // Stores the index of the TextRecord of each result.
    vector<TextRecordIndex> cids_;
    // For every instance should wids_.size() never be < 1.
    // For prefix-queries stores for each term and result the index of
    // the Word the prefixed-word was completed to.
    vector<vector<WordIndex>> wids_ = {{}};
    // Stores the index of the entity of each result.
    vector<Id> eids_;
    // Stores for each result how often an entity
    // appears in its associated TextRecord.
    vector<Score> scores_;
  };

  /// Forbid copy and assignment.
  Index& operator=(const Index&) = delete;
  Index(const Index&) = delete;

  /// Allow move construction, which is mostly used in unit tests.
  Index(Index&&) noexcept;

  explicit Index(ad_utility::AllocatorWithLimit<Id> allocator);
  ~Index();

  // Get underlying access to the Pimpl where necessary.
  const IndexImpl& getPimpl() const { return *pimpl_; }

  // Create an index from a file. Will write vocabulary and on-disk index data.
  // NOTE: The index can not directly be used after this call, but has to be
  // setup by `createFromOnDiskIndex` after this call.
  void createFromFile(const std::string& filename);

  // Create an index object from an on-disk index that has previously been
  // constructed using the `createFromFile` method which is typically called via
  // `IndexBuilderMain`. Read necessary metadata into memory and open file
  // handles.
  void createFromOnDiskIndex(const std::string& onDiskBase);

  // Add a text index to a complete KB index. First read the given context
  // file (if file name not empty), then add words from literals (if true).
  void addTextFromContextFile(const std::string& contextFile,
                              bool addWordsFromLiterals);

  // Build docsDB file from given file (one text record per line).
  void buildDocsDB(const std::string& docsFile);

  // Add text index from on-disk index that has previously been constructed.
  // Read necessary metadata into memory and open file handles.
  void addTextFromOnDiskIndex();

  using Vocab =
      Vocabulary<CompressedString, TripleComponentComparator, VocabIndex>;
  [[nodiscard]] const Vocab& getVocab() const;
  Vocab& getNonConstVocabForTesting();

  using TextVocab =
      Vocabulary<std::string, SimpleStringComparator, WordVocabIndex>;
  [[nodiscard]] const TextVocab& getTextVocab() const;

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
  [[nodiscard]] size_t getCardinality(const TripleComponent& comp,
                                      Permutation::Enum permutation) const;
  [[nodiscard]] size_t getCardinality(Id id,
                                      Permutation::Enum permutation) const;

  // TODO<joka921> Once we have an overview over the folding this logic should
  // probably not be in the index class.
  [[nodiscard]] std::optional<std::string> idToOptionalString(
      VocabIndex id) const;
  [[nodiscard]] std::optional<std::string> idToOptionalString(
      WordVocabIndex id) const;

  bool getId(const std::string& element, Id* id) const;

  [[nodiscard]] std::pair<Id, Id> prefix_range(const std::string& prefix) const;

  [[nodiscard]] const vector<PatternID>& getHasPattern() const;
  [[nodiscard]] const CompactVectorOfStrings<Id>& getHasPredicate() const;
  [[nodiscard]] const CompactVectorOfStrings<Id>& getPatterns() const;
  /**
   * @return The multiplicity of the entites column (0) of the full has-relation
   *         relation after unrolling the patterns.
   */
  [[nodiscard]] double getAvgNumDistinctPredicatesPerSubject() const;

  /**
   * @return The multiplicity of the predicates column (0) of the full
   * has-relation relation after unrolling the patterns.
   */
  [[nodiscard]] double getAvgNumDistinctSubjectsPerPredicate() const;

  /**
   * @return The size of the full has-relation relation after unrolling the
   *         patterns.
   */
  [[nodiscard]] size_t getNumDistinctSubjectPredicatePairs() const;

  // --------------------------------------------------------------------------
  // TEXT RETRIEVAL
  // --------------------------------------------------------------------------
  [[nodiscard]] std::string_view wordIdToString(WordIndex wordIndex) const;

  [[nodiscard]] size_t getSizeOfTextBlockForWord(const std::string& word) const;

  [[nodiscard]] size_t getSizeOfTextBlockForEntities(
      const std::string& word) const;

  [[nodiscard]] size_t getSizeEstimate(const std::string& words) const;

  void getContextListForWords(const std::string& words, IdTable* result) const;

  void getECListForWordsOneVar(const std::string& words, size_t limit,
                               IdTable* result) const;

  // With two or more variables.
  void getECListForWords(const std::string& words, size_t nofVars, size_t limit,
                         IdTable* result) const;

  // With filtering. Needs many template instantiations but
  // only nofVars truly makes a difference. Others are just data types
  // of result tables.
  void getFilteredECListForWords(const std::string& words,
                                 const IdTable& filter, size_t filterColumn,
                                 size_t nofVars, size_t limit,
                                 IdTable* result) const;

  // Special cast with a width-one filter.
  void getFilteredECListForWordsWidthOne(const std::string& words,
                                         const IdTable& filter, size_t nofVars,
                                         size_t limit, IdTable* result) const;

  WordEntityPostings getContextEntityScoreListsForWords(
      const std::string& words) const;

  IdTable getWordPostingsForTerm(
      const std::string& term,
      const ad_utility::AllocatorWithLimit<Id>& allocator) const;

  WordEntityPostings getEntityPostingsForTerm(const std::string& term) const;

  IdTable getEntityMentionsForWord(
      const string& term,
      const ad_utility::AllocatorWithLimit<Id>& allocator) const;

  size_t getIndexOfBestSuitedElTerm(const vector<string>& terms) const;

  [[nodiscard]] std::string getTextExcerpt(TextRecordIndex cid) const;

  // Only for debug reasons and external encoding tests.
  // Supply an empty vector to dump all lists above a size threshold.
  void dumpAsciiLists(const vector<std::string>& lists,
                      bool decodeGapsFreq) const;

  void dumpAsciiLists(const TextBlockMetaData& tbmd) const;

  [[nodiscard]] float getAverageNofEntityContexts() const;

  void setKbName(const std::string& name);

  void setTextName(const std::string& name);

  bool& usePatterns();

  bool& loadAllPermutations();

  void setKeepTempFiles(bool keepTempFiles);

  ad_utility::MemorySize& memoryLimitIndexBuilding();
  const ad_utility::MemorySize& memoryLimitIndexBuilding() const;

  ad_utility::MemorySize& blocksizePermutationsPerColumn();

  void setOnDiskBase(const std::string& onDiskBase);

  void setSettingsFile(const std::string& filename);

  void setPrefixCompression(bool compressed);

  void setNumTriplesPerBatch(uint64_t numTriplesPerBatch);

  const std::string& getTextName() const;

  const std::string& getKbName() const;

  NumNormalAndInternal numTriples() const;

  size_t getNofTextRecords() const;
  size_t getNofWordPostings() const;
  size_t getNofEntityPostings() const;

  NumNormalAndInternal numDistinctSubjects() const;
  NumNormalAndInternal numDistinctObjects() const;
  NumNormalAndInternal numDistinctPredicates() const;

  bool hasAllPermutations() const;

  // _____________________________________________________________________________
  vector<float> getMultiplicities(const TripleComponent& key,
                                  Permutation::Enum permutation) const;

  // ___________________________________________________________________
  vector<float> getMultiplicities(Permutation::Enum p) const;

  /**
   * @brief Perform a scan for one or two keys i.e. retrieve all YZ from the XYZ
   * permutation for specific key values of X if `col1String` is `nullopt`, and
   * all Z for the given XY if `col1String` is specified.
   * @tparam Permutation The permutations Index::POS()... have different types
   * @param col0String The first key (as a raw string that is yet to be
   * transformed to index space) for which to search, e.g. fixed value for O in
   * OSP permutation.
   * @param col1String The second key (as a raw string that is yet to be
   * transformed to index space) for which to search, e.g. fixed value for S in
   * OSP permutation.
   * @param result The Id table to which we will write. Must have 2 columns.
   * @param p The Permutation::Enum to use (in particularly POS(), SOP,...
   * members of Index class).
   */
  IdTable scan(
      const TripleComponent& col0String,
      std::optional<std::reference_wrapper<const TripleComponent>> col1String,
      Permutation::Enum p, Permutation::ColumnIndicesRef additionalColumns,
      ad_utility::SharedCancellationHandle cancellationHandle) const;

  // Similar to the overload of `scan` above, but the keys are specified as IDs.
  IdTable scan(Id col0Id, std::optional<Id> col1Id, Permutation::Enum p,
               Permutation::ColumnIndicesRef additionalColumns,
               ad_utility::SharedCancellationHandle cancellationHandle) const;

  // Similar to the previous overload of `scan`, but only get the exact size of
  // the scan result.
  size_t getResultSizeOfScan(const TripleComponent& col0String,
                             const TripleComponent& col1String,
                             const Permutation::Enum& permutation) const;

  // Get access to the implementation. This should be used rarely as it
  // requires including the rather expensive `IndexImpl.h` header
  IndexImpl& getImpl() { return *pimpl_; }
  [[nodiscard]] const IndexImpl& getImpl() const { return *pimpl_; }
};
