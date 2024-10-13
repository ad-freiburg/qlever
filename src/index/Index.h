// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2014-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)
#pragma once

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
#include "util/json.h"

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
    size_t normal{};
    size_t internal{};
    size_t normalAndInternal_() const { return normal + internal; }
    bool operator==(const NumNormalAndInternal&) const = default;
    static NumNormalAndInternal fromNormalAndTotal(size_t normal,
                                                   size_t total) {
      AD_CONTRACT_CHECK(total >= normal);
      return {normal, total - normal};
    }
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(NumNormalAndInternal, normal, internal);
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
  enum class Filetype { Turtle, NQuad };
  void createFromFile(const std::string& filename,
                      Filetype filetype = Filetype::Turtle);

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
  // RDF RETRIEVAL
  // --------------------------------------------------------------------------
  [[nodiscard]] size_t getCardinality(const TripleComponent& comp,
                                      Permutation::Enum permutation) const;
  [[nodiscard]] size_t getCardinality(Id id,
                                      Permutation::Enum permutation) const;

  // TODO<joka921> Once we have an overview over the folding this logic should
  // probably not be in the index class.
  std::string indexToString(VocabIndex id) const;
  std::string_view indexToString(WordVocabIndex id) const;

  [[nodiscard]] Vocab::PrefixRanges prefixRanges(std::string_view prefix) const;

  [[nodiscard]] const CompactVectorOfStrings<Id>& getPatterns() const;
  /**
   * @return The multiplicity of the entities column (0) of the full
   * has-relation relation after unrolling the patterns.
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

  IdTable getWordPostingsForTerm(
      const std::string& term,
      const ad_utility::AllocatorWithLimit<Id>& allocator) const;

  IdTable getEntityMentionsForWord(
      const string& term,
      const ad_utility::AllocatorWithLimit<Id>& allocator) const;

  size_t getIndexOfBestSuitedElTerm(const vector<string>& terms) const;

  [[nodiscard]] std::string getTextExcerpt(TextRecordIndex cid) const;

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

  void setNumTriplesPerBatch(uint64_t numTriplesPerBatch);

  const std::string& getTextName() const;

  const std::string& getKbName() const;

  const std::string& getIndexId() const;

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
  IdTable scan(const ScanSpecificationAsTripleComponent& scanSpecification,
               Permutation::Enum p,
               Permutation::ColumnIndicesRef additionalColumns,
               const ad_utility::SharedCancellationHandle& cancellationHandle,
               const LimitOffsetClause& limitOffset = {}) const;

  // Similar to the overload of `scan` above, but the keys are specified as IDs.
  IdTable scan(const ScanSpecification& scanSpecification, Permutation::Enum p,
               Permutation::ColumnIndicesRef additionalColumns,
               const ad_utility::SharedCancellationHandle& cancellationHandle,
               const LimitOffsetClause& limitOffset = {}) const;

  // Similar to the previous overload of `scan`, but only get the exact size of
  // the scan result.
  size_t getResultSizeOfScan(const ScanSpecification& scanSpecification,
                             const Permutation::Enum& permutation) const;

  // Get access to the implementation. This should be used rarely as it
  // requires including the rather expensive `IndexImpl.h` header
  IndexImpl& getImpl() { return *pimpl_; }
  [[nodiscard]] const IndexImpl& getImpl() const { return *pimpl_; }
};
