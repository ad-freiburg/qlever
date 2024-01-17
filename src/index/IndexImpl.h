// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2014-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)
#pragma once

#include <engine/ResultTable.h>
#include <global/Pattern.h>
#include <global/SpecialIds.h>
#include <index/CompressedRelation.h>
#include <index/ConstantsIndexBuilding.h>
#include <index/DocsDB.h>
#include <index/Index.h>
#include <index/IndexBuilderTypes.h>
#include <index/IndexMetaData.h>
#include <index/PatternCreator.h>
#include <index/Permutation.h>
#include <index/StxxlSortFunctors.h>
#include <index/TextMetaData.h>
#include <index/Vocabulary.h>
#include <index/VocabularyGenerator.h>
#include <parser/ContextFileParser.h>
#include <parser/TripleComponent.h>
#include <parser/TurtleParser.h>
#include <util/BackgroundStxxlSorter.h>
#include <util/BufferedVector.h>
#include <util/CompressionUsingZstd/ZstdWrapper.h>
#include <util/File.h>
#include <util/Forward.h>
#include <util/HashMap.h>
#include <util/MmapVector.h>
#include <util/json.h>

#include <array>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <stxxl/sorter>
#include <stxxl/stream>
#include <stxxl/vector>
#include <vector>

#include "engine/idTable/CompressedExternalIdTable.h"
#include "util/CancellationHandle.h"
#include "util/MemorySize/MemorySize.h"

using ad_utility::BufferedVector;
using ad_utility::MmapVector;
using ad_utility::MmapVectorView;
using std::array;
using std::shared_ptr;
using std::string;
using std::tuple;
using std::vector;

using json = nlohmann::json;

static constexpr size_t NumColumnsIndexBuilding = 3;
template <typename Comparator, size_t I = NumColumnsIndexBuilding>
using ExternalSorter =
    ad_utility::CompressedExternalIdTableSorter<Comparator, I>;

// The Order in which the permutations are created during the index building.
using FirstPermutation = SortBySPO;
using FirstPermutationSorter = ExternalSorter<FirstPermutation>;
using SecondPermutation = SortByOSP;
using ThirdPermutation = SortByPSO;

// Several data that are passed along between different phases of the
// index builder.
struct IndexBuilderDataBase {
  VocabularyMerger::VocabularyMetaData vocabularyMetaData_;
  // The prefixes that are used for the prefix compression.
  std::vector<std::string> prefixes_;
};

// All the data from IndexBuilderDataBase and a stxxl::vector of (unsorted) ID
// triples.
struct IndexBuilderDataAsStxxlVector : IndexBuilderDataBase {
  using TripleVec = ad_utility::CompressedExternalIdTable<3>;
  // All the triples as Ids.
  std::unique_ptr<TripleVec> idTriples;
  // The number of triples for each partial vocabulary. This also depends on the
  // number of additional language filter triples.
  std::vector<size_t> actualPartialSizes;
};

// All the data from IndexBuilderDataBase and a ExternalSorter that stores all
// ID triples sorted by the first permutation.
struct IndexBuilderDataAsFirstPermutationSorter : IndexBuilderDataBase {
  using SorterPtr =
      std::unique_ptr<ad_utility::CompressedExternalIdTableSorterTypeErased>;
  SorterPtr sorter_;
  IndexBuilderDataAsFirstPermutationSorter(const IndexBuilderDataBase& base,
                                           SorterPtr sorter)
      : IndexBuilderDataBase{base}, sorter_{std::move(sorter)} {}
  IndexBuilderDataAsFirstPermutationSorter() = default;
};

class IndexImpl {
 public:
  using TripleVec = ad_utility::CompressedExternalIdTable<3>;
  // Block Id, Context Id, Word Id, Score, entity
  using TextVec = stxxl::vector<
      tuple<TextBlockIndex, TextRecordIndex, WordOrEntityIndex, Score, bool>>;
  using Posting = std::tuple<TextRecordIndex, WordIndex, Score>;

  struct IndexMetaDataMmapDispatcher {
    using WriteType = IndexMetaDataMmap;
    using ReadType = IndexMetaDataMmapView;
  };

  using NumNormalAndInternal = Index::NumNormalAndInternal;

  // Private data members.
 private:
  string onDiskBase_;
  string settingsFileName_;
  bool onlyAsciiTurtlePrefixes_ = false;
  bool useParallelParser_ = true;
  TurtleParserIntegerOverflowBehavior turtleParserIntegerOverflowBehavior_ =
      TurtleParserIntegerOverflowBehavior::Error;
  bool turtleParserSkipIllegalLiterals_ = false;
  bool keepTempFiles_ = false;
  ad_utility::MemorySize memoryLimitIndexBuilding_ =
      DEFAULT_MEMORY_LIMIT_INDEX_BUILDING;
  ad_utility::MemorySize blocksizePermutationPerColumn_ =
      UNCOMPRESSED_BLOCKSIZE_COMPRESSED_METADATA_PER_COLUMN;
  json configurationJson_;
  Index::Vocab vocab_;
  size_t totalVocabularySize_ = 0;
  bool vocabPrefixCompressed_ = true;
  Index::TextVocab textVocab_;

  TextMetaData textMeta_;
  DocsDB docsDB_;
  vector<WordIndex> blockBoundaries_;
  off_t currenttOffset_;
  mutable ad_utility::File textIndexFile_;

  // If false, only PSO and POS permutations are loaded and expected.
  bool loadAllPermutations_ = true;

  // Pattern trick data
  bool usePatterns_ = false;
  double avgNumDistinctPredicatesPerSubject_;
  double avgNumDistinctSubjectsPerPredicate_;
  uint64_t numDistinctSubjectPredicatePairs_;

  size_t parserBatchSize_ = PARSER_BATCH_SIZE;
  size_t numTriplesPerBatch_ = NUM_TRIPLES_PER_PARTIAL_VOCAB;

  // These statistics all do *not* include the triples that are added by
  // QLever for more efficient query processing.
  size_t numSubjectsNormal_ = 0;
  size_t numPredicatesNormal_ = 0;
  size_t numObjectsNormal_ = 0;
  size_t numTriplesNormal_ = 0;
  /**
   * @brief Maps pattern ids to sets of predicate ids.
   */
  CompactVectorOfStrings<Id> patterns_;
  /**
   * @brief Maps entity ids to pattern ids.
   */
  std::vector<PatternID> hasPattern_;
  /**
   * @brief Maps entity ids to sets of predicate ids
   */
  CompactVectorOfStrings<Id> hasPredicate_;

  ad_utility::AllocatorWithLimit<Id> allocator_;

  // TODO: make those private and allow only const access
  // instantiations for the six permutations used in QLever.
  // They simplify the creation of permutations in the index class.
  Permutation pos_{Permutation::Enum::POS, allocator_};
  Permutation pso_{Permutation::Enum::PSO, allocator_};
  Permutation sop_{Permutation::Enum::SOP, allocator_};
  Permutation spo_{Permutation::Enum::SPO, allocator_};
  Permutation ops_{Permutation::Enum::OPS, allocator_};
  Permutation osp_{Permutation::Enum::OSP, allocator_};

 public:
  explicit IndexImpl(ad_utility::AllocatorWithLimit<Id> allocator);

  // Forbid copying.
  IndexImpl& operator=(const IndexImpl&) = delete;
  IndexImpl(const IndexImpl&) = delete;
  // Moving is currently not supported, because several of the members use
  // mutexes internally. It is also not needed.
  IndexImpl& operator=(IndexImpl&&) = delete;
  IndexImpl(IndexImpl&&) = delete;

  const auto& POS() const { return pos_; }
  auto& POS() { return pos_; }
  const auto& PSO() const { return pso_; }
  auto& PSO() { return pso_; }
  const auto& SPO() const { return spo_; }
  auto& SPO() { return spo_; }
  const auto& SOP() const { return sop_; }
  auto& SOP() { return sop_; }
  const auto& OPS() const { return ops_; }
  auto& OPS() { return ops_; }
  const auto& OSP() const { return osp_; }
  auto& OSP() { return osp_; }

  // For a given `Permutation::Enum` (e.g. `PSO`) return the corresponding
  // `Permutation` object by reference (`pso_`).
  Permutation& getPermutation(Permutation::Enum p);
  const Permutation& getPermutation(Permutation::Enum p) const;

  // Creates an index from a file. Parameter Parser must be able to split the
  // file's format into triples.
  // Will write vocabulary and on-disk index data.
  // !! The index can not directly be used after this call, but has to be setup
  // by createFromOnDiskIndex after this call.
  void createFromFile(const string& filename);

  // Creates an index object from an on disk index that has previously been
  // constructed. Read necessary meta data into memory and opens file handles.
  void createFromOnDiskIndex(const string& onDiskBase);

  // Adds a text index to a complete KB index. First reads the given context
  // file (if file name not empty), then adds words from literals (if true).
  void addTextFromContextFile(const string& contextFile,
                              bool addWordsFromLiterals);

  // Build docsDB file from given file (one text record per line).
  void buildDocsDB(const string& docsFile) const;

  // Adds text index from on disk index that has previously been constructed.
  // Read necessary meta data into memory and opens file handles.
  void addTextFromOnDiskIndex();

  const auto& getVocab() const { return vocab_; };
  auto& getNonConstVocabForTesting() { return vocab_; }

  const auto& getTextVocab() const { return textVocab_; };

  // --------------------------------------------------------------------------
  //  -- RETRIEVAL ---
  // --------------------------------------------------------------------------

  // --------------------------------------------------------------------------
  // RDF RETRIEVAL
  // --------------------------------------------------------------------------

  // __________________________________________________________________________
  NumNormalAndInternal numDistinctSubjects() const;

  // __________________________________________________________________________
  NumNormalAndInternal numDistinctObjects() const;

  // __________________________________________________________________________
  NumNormalAndInternal numDistinctPredicates() const;

  // __________________________________________________________________________
  NumNormalAndInternal numDistinctCol0(Permutation::Enum permutation) const;

  // ___________________________________________________________________________
  size_t getCardinality(Id id, Permutation::Enum permutation) const;

  // ___________________________________________________________________________
  size_t getCardinality(const TripleComponent& comp,
                        Permutation::Enum permutation) const;

  // TODO<joka921> Once we have an overview over the folding this logic should
  // probably not be in the index class.
  std::optional<string> idToOptionalString(VocabIndex id) const;

  std::optional<string> idToOptionalString(WordVocabIndex id) const;

  // ___________________________________________________________________________
  bool getId(const string& element, Id* id) const;

  // ___________________________________________________________________________
  std::pair<Id, Id> prefix_range(const std::string& prefix) const;

  const vector<PatternID>& getHasPattern() const;
  const CompactVectorOfStrings<Id>& getHasPredicate() const;
  const CompactVectorOfStrings<Id>& getPatterns() const;
  /**
   * @return The multiplicity of the Entites column (0) of the full has-relation
   *         relation after unrolling the patterns.
   */
  double getAvgNumDistinctPredicatesPerSubject() const;

  /**
   * @return The multiplicity of the Predicates column (0) of the full
   * has-relation relation after unrolling the patterns.
   */
  double getAvgNumDistinctSubjectsPerPredicate() const;

  /**
   * @return The size of the full has-relation relation after unrolling the
   *         patterns.
   */
  size_t getNumDistinctSubjectPredicatePairs() const;

  // --------------------------------------------------------------------------
  // TEXT RETRIEVAL
  // --------------------------------------------------------------------------
  std::string_view wordIdToString(WordIndex wordIndex) const;

  size_t getSizeOfTextBlockForEntities(const string& words) const;

  // Returns the size of the whole textblock. If the word is very long or not
  // prefixed then only a small number of words actually match. So the final
  // result is much smaller.
  // Note that as a cost estimate the estimation is correct. Because we always
  // have to read the complete block and then filter by the actually needed
  // words.
  // TODO: improve size estimate by adding a correction factor.
  size_t getSizeOfTextBlockForWord(const string& words) const;

  size_t getSizeEstimate(const string& words) const;

  void callFixedGetContextListForWords(const string& words,
                                       IdTable* result) const;

  template <int WIDTH>
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

  Index::WordEntityPostings getContextEntityScoreListsForWords(
      const string& words) const;

  // Does the same as getWordPostingsForTerm but returns a
  // WordEntityPosting. Sorted by textRecord.
  Index::WordEntityPostings getWordPostingsForTermWep(
      const string& wordOrPrefix) const;

  // Returns a set of [textRecord, term] pairs where the term is contained in
  // the textRecord. The term can be either the wordOrPrefix itself or a word
  // that has wordOrPrefix as a prefix. Returned IdTable has columns:
  // textRecord, word. Sorted by textRecord.
  IdTable getWordPostingsForTerm(
      const string& wordOrPrefix,
      const ad_utility::AllocatorWithLimit<Id>& allocator) const;

  Index::WordEntityPostings getEntityPostingsForTerm(const string& term) const;

  // Returns a set of textRecords and their corresponding entities and
  // scores. Each textRecord contains its corresponding entity and the term.
  // Returned IdTable has columns: textRecord, entity, score. Sorted by
  // textRecord.
  // NOTE: This returns a superset because it contains the whole block and
  // unfitting words are filtered out later by the join with the
  // TextIndexScanForWords operation.
  IdTable getEntityMentionsForWord(
      const string& term,
      const ad_utility::AllocatorWithLimit<Id>& allocator) const;

  size_t getIndexOfBestSuitedElTerm(const vector<string>& terms) const;

  Index::WordEntityPostings readWordClWep(const TextBlockMetaData& tbmd) const;

  IdTable readWordCl(const TextBlockMetaData& tbmd,
                     const ad_utility::AllocatorWithLimit<Id>& allocator) const;

  Index::WordEntityPostings readWordEntityClWep(
      const TextBlockMetaData& tbmd) const;

  IdTable readWordEntityCl(
      const TextBlockMetaData& tbmd,
      const ad_utility::AllocatorWithLimit<Id>& allocator) const;

  string getTextExcerpt(TextRecordIndex cid) const {
    if (cid.get() >= docsDB_._size) {
      return "";
    }
    return docsDB_.getTextExcerpt(cid);
  }

  float getAverageNofEntityContexts() const {
    return textMeta_.getAverageNofEntityContexts();
  };

  void setKbName(const string& name);

  void setTextName(const string& name);

  bool& usePatterns();

  bool& loadAllPermutations();

  void setKeepTempFiles(bool keepTempFiles);

  ad_utility::MemorySize& memoryLimitIndexBuilding() {
    return memoryLimitIndexBuilding_;
  }
  const ad_utility::MemorySize& memoryLimitIndexBuilding() const {
    return memoryLimitIndexBuilding_;
  }

  ad_utility::MemorySize& blocksizePermutationPerColumn() {
    return blocksizePermutationPerColumn_;
  }

  void setOnDiskBase(const std::string& onDiskBase);

  void setSettingsFile(const std::string& filename);

  void setPrefixCompression(bool compressed);

  void setNumTriplesPerBatch(uint64_t numTriplesPerBatch) {
    numTriplesPerBatch_ = numTriplesPerBatch;
  }

  const string& getTextName() const { return textMeta_.getName(); }

  const string& getKbName() const { return pso_.metaData().getName(); }

  size_t getNofTextRecords() const { return textMeta_.getNofTextRecords(); }
  size_t getNofWordPostings() const { return textMeta_.getNofWordPostings(); }
  size_t getNofEntityPostings() const {
    return textMeta_.getNofEntityPostings();
  }

  bool hasAllPermutations() const { return SPO().isLoaded_; }

  // _____________________________________________________________________________
  vector<float> getMultiplicities(const TripleComponent& key,
                                  Permutation::Enum permutation) const;

  // ___________________________________________________________________
  vector<float> getMultiplicities(Permutation::Enum permutation) const;

  // _____________________________________________________________________________
  IdTable scan(
      const TripleComponent& col0String,
      std::optional<std::reference_wrapper<const TripleComponent>> col1String,
      const Permutation::Enum& permutation,
      Permutation::ColumnIndicesRef additionalColumns,
      ad_utility::SharedCancellationHandle cancellationHandle) const;

  // _____________________________________________________________________________
  IdTable scan(Id col0Id, std::optional<Id> col1Id, Permutation::Enum p,
               Permutation::ColumnIndicesRef additionalColumns,
               ad_utility::SharedCancellationHandle cancellationHandle) const;

  // _____________________________________________________________________________
  size_t getResultSizeOfScan(const TripleComponent& col0,
                             const TripleComponent& col1,
                             const Permutation::Enum& permutation) const;

 private:
  // Private member functions

  // Create Vocabulary and directly write it to disk. Create TripleVec with all
  // the triples converted to id space. This Vec can be used for creating
  // permutations. Member vocab_ will be empty after this because it is not
  // needed for index creation once the TripleVec is set up and it would be a
  // waste of RAM.
  IndexBuilderDataAsFirstPermutationSorter createIdTriplesAndVocab(
      std::shared_ptr<TurtleParserBase> parser);

  // ___________________________________________________________________
  IndexBuilderDataAsStxxlVector passFileForVocabulary(
      std::shared_ptr<TurtleParserBase> parser, size_t linesPerPartial);

  /**
   * @brief Everything that has to be done when we have seen all the triples
   * that belong to one partial vocabulary, including Log output used inside
   * passFileForVocabulary
   *
   * @param numLines How many Lines from the KB have we already parsed (only for
   * Logging)
   * @param numFiles How many partial vocabularies have we seen before/which is
   * the index of the voc we are going to write
   * @param actualCurrentPartialSize How many triples belong to this partition
   * (including extra langfilter triples)
   * @param items Contains our unsorted vocabulary. Maps words to their local
   * ids within this vocabulary.
   */
  std::future<void> writeNextPartialVocabulary(
      size_t numLines, size_t numFiles, size_t actualCurrentPartialSize,
      std::unique_ptr<ItemMapArray> items, auto localIds,
      ad_utility::Synchronized<std::unique_ptr<TripleVec>>* globalWritePtr);

  //  Apply the prefix compression to the internal vocabulary. Is called by
  //  `createFromFile` after the vocabularies have been created and merged.
  void compressInternalVocabularyIfSpecified(
      const std::vector<std::string>& prefixes);

  // Return a Turtle parser that parses the given file. The parser will be
  // configured to either parse in parallel or not, and to either use the
  // CTRE-based relaxed parser or not, depending on the settings of the
  // corresponding member variables.
  std::unique_ptr<TurtleParserBase> makeTurtleParser(
      const std::string& filename);

  std::unique_ptr<ad_utility::CompressedExternalIdTableSorterTypeErased>
  convertPartialToGlobalIds(TripleVec& data,
                            const vector<size_t>& actualLinesPerPartial,
                            size_t linesPerPartial);

  // Generator that returns all words in the given context file (if not empty)
  // and then all words in all literals (if second argument is true).
  //
  // TODO: So far, this is limited to the internal vocabulary (still in the
  // testing phase, once it works, it should be easy to include the IRIs and
  // literals from the external vocabulary as well).
  cppcoro::generator<ContextFileParser::Line> wordsInTextRecords(
      const std::string& contextFile, bool addWordsFromLiterals);

  size_t processWordsForVocabulary(const string& contextFile,
                                   bool addWordsFromLiterals);

  void processWordsForInvertedLists(const string& contextFile,
                                    bool addWordsFromLiterals, TextVec& vec);

  // TODO<joka921> Get rid of the `numColumns` by including them into the
  // `sortedTriples` argument.
  std::pair<IndexMetaDataMmapDispatcher::WriteType,
            IndexMetaDataMmapDispatcher::WriteType>
  createPermutationPairImpl(size_t numColumns, const string& fileName1,
                            const string& fileName2, auto&& sortedTriples,
                            std::array<size_t, 3> permutation,
                            auto&&... perTripleCallbacks);

  // _______________________________________________________________________
  // Create a pair of permutations. Only works for valid pairs (PSO-POS,
  // OSP-OPS, SPO-SOP).  First creates the permutation and then exchanges the
  // multiplicities and also writes the MetaData to disk. So we end up with
  // fully functional permutations.
  // performUnique must be set for the first pair created using vec to enforce
  // RDF standard (no duplicate triples).
  // createPatternsAfterFirst is only valid when  the pair is SPO-SOP because
  // the SPO permutation is also needed for patterns (see usage in
  // IndexImpl::createFromFile function)

  void createPermutationPair(size_t numColumns, auto&& sortedTriples,
                             const Permutation& p1, const Permutation& p2,
                             auto&&... perTripleCallbacks);

  // wrapper for createPermutation that saves a lot of code duplications
  // Writes the permutation that is specified by argument permutation
  // performs std::unique on arg vec iff arg performUnique is true (normally
  // done for first permutation that is created using vec).
  // Will sort vec.
  // returns the MetaData (MmapBased or HmapBased) for this relation.
  // Careful: only multiplicities for first column is valid after call, need to
  // call exchangeMultiplicities as done by createPermutationPair
  // the optional is std::nullopt if vec and thus the index is empty
  std::pair<IndexMetaDataMmapDispatcher::WriteType,
            IndexMetaDataMmapDispatcher::WriteType>
  createPermutations(size_t numColumns, auto&& sortedTriples,
                     const Permutation& p1, const Permutation& p2,
                     auto&&... perTripleCallbacks);

  void createTextIndex(const string& filename, const TextVec& vec);

  ContextListMetaData writePostings(ad_utility::File& out,
                                    const vector<Posting>& postings,
                                    bool skipWordlistIfAllTheSame);

  void openTextFileHandle();

  void addContextToVector(TextVec::bufwriter_type& writer,
                          TextRecordIndex context,
                          const ad_utility::HashMap<WordIndex, Score>& words,
                          const ad_utility::HashMap<Id, Score>& entities);

  template <typename T, typename MakeFromUint64t>
  vector<T> readGapComprList(size_t nofElements, off_t from, size_t nofBytes,
                             MakeFromUint64t makeFromUint64t) const;

  template <typename T, typename MakeFromUint64t = std::identity>
  vector<T> readFreqComprList(
      size_t nofElements, off_t from, size_t nofBytes,
      MakeFromUint64t makeFromUint = MakeFromUint64t{}) const;

  // Get the metadata for the block from the text index that contains the
  // `word`. Also works for prefixes that are terminated with `PREFIX_CHAR` like
  // "astro*". Returns `nullopt` if no suitable block was found because no
  // matching word is contained in the text index. Some additional information
  // is also returned that is often required by the calling functions:
  // `hasToBeFiltered_` is true iff `word` is NOT the only word in the text
  // block, and additional filtering is thus required. `idRange_` is the range
  // `[first, last]` of the `WordVocabIndex`es that correspond to the word
  // (which might also be a prefix, thus it is a range).
  struct TextBlockMetadataAndWordInfo {
    TextBlockMetaData tbmd_;
    bool hasToBeFiltered_;
    IdRange<WordVocabIndex> idRange_;
  };
  std::optional<TextBlockMetadataAndWordInfo>
  getTextBlockMetadataForWordOrPrefix(const std::string& word) const;

  /// Calculate the block boundaries for the text index. The boundary of a
  /// block is the index in the `textVocab_` of the last word that belongs
  /// to this block.
  /// This implementation takes a reference to an `IndexImpl` and a callable,
  /// that is called once for each blockBoundary, with the `size_t`
  /// blockBoundary as a parameter. Internally uses
  /// `calculateBlockBoundariesImpl`.
  template <typename I, typename BlockBoundaryAction>
  static void calculateBlockBoundariesImpl(
      I&& index, const BlockBoundaryAction& blockBoundaryAction);

  /// Calculate the block boundaries for the text index, and store them in the
  /// blockBoundaries_ member.
  void calculateBlockBoundaries();

  TextBlockIndex getWordBlockId(WordIndex wordIndex) const;

  //! Writes a list of elements (have to be able to be cast to unit64_t)
  //! to file.
  //! Returns the number of bytes written.
  template <class Numeric>
  size_t writeList(Numeric* data, size_t nofElements,
                   ad_utility::File& file) const;

  // TODO<joka921> understand what the "codes" are, are they better just ints?
  typedef ad_utility::HashMap<WordIndex, CompressionCode> WordToCodeMap;
  typedef ad_utility::HashMap<Score, Score> ScoreCodeMap;
  typedef vector<CompressionCode> WordCodebook;
  typedef vector<Score> ScoreCodebook;

  //! Creates codebooks for lists that are supposed to be entropy encoded.
  void createCodebooks(const vector<Posting>& postings,
                       WordToCodeMap& wordCodemap, WordCodebook& wordCodebook,
                       ScoreCodeMap& scoreCodemap,
                       ScoreCodebook& scoreCodebook) const;

  template <class T>
  size_t writeCodebook(const vector<T>& codebook, ad_utility::File& file) const;

  // FRIEND TESTS
  friend class IndexTest_createFromTsvTest_Test;
  friend class IndexTest_createFromOnDiskIndexTest_Test;
  friend class CreatePatternsFixture_createPatterns_Test;

  template <class T>
  void writeAsciiListFile(const string& filename, const T& ids) const;

  bool isLiteral(const string& object) const;

 public:
  LangtagAndTriple tripleToInternalRepresentation(TurtleTriple&& triple) const;

 private:
  /**
   * @brief Throws an exception if no patterns are loaded. Should be called from
   *        whithin any index method that returns data requiring the patterns
   *        file.
   */
  void throwExceptionIfNoPatterns() const;

  void writeConfiguration() const;
  void readConfiguration();

  // initialize the index-build-time settings for the vocabulary
  void readIndexBuilderSettingsFromFile();

  /**
   * Delete a temporary file unless the keepTempFiles_ flag is set
   * @param path
   */
  void deleteTemporaryFile(const string& path);

 public:
  // Count the number of "QLever-internal" triples (predicate ql:langtag or
  // predicate starts with @) and all other triples (that were actually part of
  // the input).
  NumNormalAndInternal numTriples() const;

  // The index contains several triples that are not part of the "actual"
  // knowledge graph, but are added by QLever for internal reasons (e.g. for an
  // efficient implementation of language filters). For a given
  // `Permutation::Enum`, returns the following `std::pair`: First: A
  // `vector<pair<Id, Id>>` that denotes ranges in the first column
  //        of the permutation that imply that a triple is added. For example
  //        in the `SPO` and `SOP` permutation a literal subject means that the
  //        triple was added (literals are not legal subjects in RDF), so the
  //        pair `(idOfFirstLiteral, idOfLastLiteral + 1)` will be contained
  //        in the vector.
  // Second: A lambda that checks for a triple *that is not already excluded
  //         by the ignored ranges from the first argument* whether it still
  //         is an added triple. For example in the `Sxx` and `Oxx` permutation
  //         a triple where the predicate starts with '@' (instead of the usual
  //         '<' is an added triple from the language filter implementation.
  // Note: A triple from a given permutation is an added triple if and only if
  //       it's first column is contained in any of the ranges from `first` OR
  //       the lambda `second` returns true for that triple.
  // For example usages see `IndexScan.cpp` (the implementation of the full
  // index scan) and `GroupBy.cpp`.
  auto getIgnoredIdRanges(const Permutation::Enum permutation) const {
    std::vector<std::pair<Id, Id>> ignoredRanges;
    ignoredRanges.emplace_back(qlever::getBoundsForSpecialIds());

    auto literalRange = getVocab().prefix_range("\"");
    auto taggedPredicatesRange = getVocab().prefix_range("@");
    auto internalEntitiesRange =
        getVocab().prefix_range(INTERNAL_ENTITIES_URI_PREFIX);

    auto pushIgnoredRange = [&ignoredRanges](const auto& range) {
      ignoredRanges.emplace_back(Id::makeFromVocabIndex(range.first),
                                 Id::makeFromVocabIndex(range.second));
    };
    pushIgnoredRange(internalEntitiesRange);
    using enum Permutation::Enum;
    if (permutation == SPO || permutation == SOP) {
      pushIgnoredRange(literalRange);
    } else if (permutation == PSO || permutation == POS) {
      pushIgnoredRange(taggedPredicatesRange);
    }

    // A lambda that checks whether the `predicateId` is an internal ID like
    // `ql:has-pattern` or `@en@rdfs:label`.
    auto isInternalPredicateId = [internalEntitiesRange,
                                  taggedPredicatesRange](Id predicateId) {
      if (predicateId.getDatatype() == Datatype::Undefined) {
        return true;
      }
      AD_CORRECTNESS_CHECK(predicateId.getDatatype() == Datatype::VocabIndex);
      auto idx = predicateId.getVocabIndex();
      auto isInRange = [idx](const auto& range) {
        return range.first <= idx && idx < range.second;
      };
      return (isInRange(internalEntitiesRange) ||
              isInRange(taggedPredicatesRange));
    };

    auto isTripleIgnored = [permutation,
                            isInternalPredicateId](const auto& triple) {
      // TODO<joka921, everybody in the future>:
      // A lot of code (especially for statistical queries in `GroupBy.cpp` and
      // the pattern trick) relies on this function being a noop for the `PSO`
      // and `POS` permutations, meaning that it suffices to check the
      // `ignoredRanges` for them. Should this ever change (which means that we
      // add internal triples that use predicates that are actually contained in
      // the knowledge graph), then all the code that uses this function has to
      // be thoroughly reviewed.
      if (permutation == SPO || permutation == OPS) {
        // Predicates are always entities from the vocabulary.
        return isInternalPredicateId(triple[1]);
      } else if (permutation == SOP || permutation == OSP) {
        return isInternalPredicateId(triple[2]);
      }
      return false;
    };

    return std::pair{std::move(ignoredRanges), std::move(isTripleIgnored)};
  }
  using BlocksOfTriples = cppcoro::generator<IdTableStatic<0>>;

  // Functions to create the pairs of permutations during the index build. Each
  // of them takes the following arguments:
  // * `isQleverInternalId` a callable that takes an `Id` and returns true iff
  //    the corresponding IRI was internally added by QLever and not part of the
  //    knowledge graph.
  // * `sortedInput`  The input, must be sorted by the first permutation in the
  //    function name.
  // * `nextSorter` A callback that is invoked for each row in each of the
  //    blocks in the input. Typically used to set up the sorting for the
  //    subsequent pair of permutations.

  // Create the SPO and SOP permutations. Additionally, count the number of
  // distinct actual (not internal) subjects in the input and write it to the
  // metadata. Also builds the patterns if specified.
  template <typename... NextSorter>
  requires(sizeof...(NextSorter) <= 1)
  std::optional<PatternCreatorNew::TripleSorter> createSPOAndSOP(
      size_t numColumns, auto& isInternalId, BlocksOfTriples sortedTriples,
      NextSorter&&... nextSorter);
  // Create the OSP and OPS permutations. Additionally, count the number of
  // distinct objects and write it to the metadata.
  template <typename... NextSorter>
  requires(sizeof...(NextSorter) <= 1)
  void createOSPAndOPS(size_t numColumns, auto& isInternalId,
                       BlocksOfTriples sortedTriples,
                       NextSorter&&... nextSorter);

  // Create the PSO and POS permutations. Additionally, count the number of
  // distinct predicates and the number of actual triples and write them to the
  // metadata.
  template <typename... NextSorter>
  requires(sizeof...(NextSorter) <= 1)
  void createPSOAndPOS(size_t numColumns, auto& isInternalId,
                       BlocksOfTriples sortedTriples,
                       NextSorter&&... nextSorter);

  // Set up one of the permutation sorters with the appropriate memory limit.
  // The `permutationName` is used to determine the filename and must be unique
  // for each call during one index build.
  template <typename Comparator, size_t N = NumColumnsIndexBuilding>
  ExternalSorter<Comparator, N> makeSorter(
      std::string_view permutationName) const;
  // Same as the same function, but return a `unique_ptr`.
  template <typename Comparator, size_t N = NumColumnsIndexBuilding>
  std::unique_ptr<ExternalSorter<Comparator, N>> makeSorterPtr(
      std::string_view permutationName) const;
  // The common implementation of the above two functions.
  template <typename Comparator, size_t N, bool returnPtr>
  auto makeSorterImpl(std::string_view permutationName) const;

  // Aliases for the three functions above that should be consistently used.
  // They assert that the order of the permutations as communicated by the
  // function names are consistent with the aliases for the sorters, i.e. that
  // `createFirstPermutationPair` corresponds to the `FirstPermutation`.

  // The `createFirstPermutationPair` has a special implementation for the case
  // of only two permutations (where we have to build the Pxx permutations). In
  // all other cases the Sxx permutations are built first because we need the
  // patterns.
  std::optional<PatternCreatorNew::TripleSorter> createFirstPermutationPair(
      auto&&... args) {
    static_assert(std::is_same_v<FirstPermutation, SortBySPO>);
    static_assert(std::is_same_v<SecondPermutation, SortByOSP>);
    if (loadAllPermutations()) {
      return createSPOAndSOP(AD_FWD(args)...);
    } else {
      createPSOAndPOS(AD_FWD(args)...);
      return std::nullopt;
    }
  }

  void createSecondPermutationPair(auto&&... args) {
    static_assert(std::is_same_v<SecondPermutation, SortByOSP>);
    return createOSPAndOPS(AD_FWD(args)...);
  }

  void createThirdPermutationPair(auto&&... args) {
    static_assert(std::is_same_v<ThirdPermutation, SortByPSO>);
    return createPSOAndPOS(AD_FWD(args)...);
  }

  // Build the OSP and OPS permutations from the output of the `PatternCreator`.
  // The permutations will have two additional columns: The subject pattern of
  // the subject (which is already created by the `PatternCreator`) and the
  // subject pattern of the object (which is created by this function). Return
  // these five columns sorted by PSO, to be used as an input for building the
  // PSO and POS permutations.
  std::unique_ptr<ExternalSorter<SortByPSO, 5>> buildOspWithPatterns(
      PatternCreatorNew::TripleSorter sortersFromPatternCreator,
      auto isQLeverInternalId);
};
