// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2014-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_INDEX_INDEXIMPL_H
#define QLEVER_SRC_INDEX_INDEXIMPL_H

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "backports/algorithm.h"
#include "engine/Result.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "global/Pattern.h"
#include "global/SpecialIds.h"
#include "index/CompressedRelation.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/DeltaTriples.h"
#include "index/DocsDB.h"
#include "index/EncodedIriManager.h"
#include "index/ExternalSortFunctors.h"
#include "index/Index.h"
#include "index/IndexBuilderTypes.h"
#include "index/IndexMetaData.h"
#include "index/PatternCreator.h"
#include "index/Permutation.h"
#include "index/TextMetaData.h"
#include "index/TextScoring.h"
#include "index/Vocabulary.h"
#include "index/VocabularyMerger.h"
#include "parser/RdfParser.h"
#include "parser/TripleComponent.h"
#include "util/BufferedVector.h"
#include "util/File.h"
#include "util/Forward.h"
#include "util/MemorySize/MemorySize.h"
#include "util/json.h"

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
  ad_utility::vocabulary_merger::VocabularyMetaData vocabularyMetaData_;
};

// All the data from IndexBuilderDataBase and (unsorted) external ID triples.
struct IndexBuilderDataAsExternalVector : IndexBuilderDataBase {
  using TripleVec = ad_utility::CompressedExternalIdTable<4>;
  // All the triples as Ids.
  std::unique_ptr<TripleVec> idTriples;
  // The number of triples for each partial vocabulary. This also depends on the
  // number of additional language filter triples.
  std::vector<size_t> actualPartialSizes;
};

// Store the "normal" triples sorted by the first permutation, together with
// the additional "internal" triples, sorted by PSO.
struct FirstPermutationSorterAndInternalTriplesAsPso {
  using SorterPtr =
      std::unique_ptr<ad_utility::CompressedExternalIdTableSorterTypeErased>;
  SorterPtr firstPermutationSorter_;
  std::unique_ptr<ExternalSorter<SortByPSO, NumColumnsIndexBuilding>>
      internalTriplesPso_;
};
// All the data from IndexBuilderDataBase and a ExternalSorter that stores all
// ID triples sorted by the first permutation.
struct IndexBuilderDataAsFirstPermutationSorter : IndexBuilderDataBase {
  using SorterPtr = FirstPermutationSorterAndInternalTriplesAsPso;
  SorterPtr sorter_;
  IndexBuilderDataAsFirstPermutationSorter(const IndexBuilderDataBase& base,
                                           SorterPtr sorter)
      : IndexBuilderDataBase{base}, sorter_{std::move(sorter)} {}
  IndexBuilderDataAsFirstPermutationSorter() = default;
};

class IndexImpl {
 public:
  using TextScoringMetric = qlever::TextScoringMetric;
  using TripleVec =
      ad_utility::CompressedExternalIdTable<NumColumnsIndexBuilding>;
  // Block Id, isEntity, Context Id, Word Id, Score
  using TextVec = ad_utility::CompressedExternalIdTableSorter<SortText, 5>;

  struct IndexMetaDataMmapDispatcher {
    using WriteType = IndexMetaDataMmap;
    using ReadType = IndexMetaDataMmapView;
  };

  using NumNormalAndInternal = Index::NumNormalAndInternal;

  // Private data members.
 protected:
  std::string onDiskBase_;
  std::string settingsFileName_;
  bool onlyAsciiTurtlePrefixes_ = false;
  // Note: `std::nullopt` means `not specified by the user`.
  std::optional<bool> useParallelParser_ = std::nullopt;
  TurtleParserIntegerOverflowBehavior turtleParserIntegerOverflowBehavior_ =
      TurtleParserIntegerOverflowBehavior::Error;
  bool turtleParserSkipIllegalLiterals_ = false;
  bool keepTempFiles_ = false;
  ad_utility::MemorySize memoryLimitIndexBuilding_ =
      DEFAULT_MEMORY_LIMIT_INDEX_BUILDING;
  ad_utility::MemorySize parserBufferSize_ = DEFAULT_PARSER_BUFFER_SIZE;
  ad_utility::MemorySize blocksizePermutationPerColumn_ =
      UNCOMPRESSED_BLOCKSIZE_COMPRESSED_METADATA_PER_COLUMN;
  nlohmann::json configurationJson_;
  Index::Vocab vocab_;
  Index::TextVocab textVocab_;
  EncodedIriManager encodedIriManager_;
  ScoreData scoreData_;

  TextMetaData textMeta_;
  DocsDB docsDB_;
  std::vector<WordIndex> blockBoundaries_;
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

  NumNormalAndInternal numSubjects_;
  NumNormalAndInternal numPredicates_;
  NumNormalAndInternal numObjects_;
  NumNormalAndInternal numTriples_;
  std::string indexId_;
  std::string gitShortHash_ = "git short hash not set";

  // Keeps track of the number of nonLiteral contexts in the index this is used
  // in the test retrieval of the texts. This only works reliably if the
  // wordsFile.tsv starts with contextId 1 and is continuous.
  size_t nofNonLiteralsInTextIndex_;

  TextScoringMetric textScoringMetric_;
  std::pair<float, float> bAndKParamForTextScoring_;

  // Global static pointers to the currently active index and comparator.
  // Those are used to compare LocalVocab entries with each other as well as
  // with Vocab entries.
  static inline const IndexImpl* globalSingletonIndex_ = nullptr;
  static inline const TripleComponentComparator* globalSingletonComparator_ =
      nullptr;
  /**
   * @brief Maps pattern ids to sets of predicate ids.
   */
  CompactVectorOfStrings<Id> patterns_;
  ad_utility::AllocatorWithLimit<Id> allocator_;

  // TODO: make those private and allow only const access
  // instantiations for the six permutations used in QLever.
  // They simplify the creation of permutations in the index class.
  using PermutationPtr = std::shared_ptr<Permutation>;
  PermutationPtr pos_{
      std::make_shared<Permutation>(Permutation::Enum::POS, allocator_)};
  PermutationPtr pso_{
      std::make_shared<Permutation>(Permutation::Enum::PSO, allocator_)};
  PermutationPtr sop_{
      std::make_shared<Permutation>(Permutation::Enum::SOP, allocator_)};
  PermutationPtr spo_{
      std::make_shared<Permutation>(Permutation::Enum::SPO, allocator_)};
  PermutationPtr ops_{
      std::make_shared<Permutation>(Permutation::Enum::OPS, allocator_)};
  PermutationPtr osp_{
      std::make_shared<Permutation>(Permutation::Enum::OSP, allocator_)};

  // During the index building, store the IDs of the `ql:has-pattern` predicate
  // and of `ql:default-graph` as they are required to add additional triples
  // after the creation of the vocabulary is finished.
  std::optional<Id> idOfHasPatternDuringIndexBuilding_;
  std::optional<Id> idOfInternalGraphDuringIndexBuilding_;

  // The vocabulary type that is used (only relevant during index building).
  ad_utility::VocabularyType vocabularyTypeForIndexBuilding_{
      ad_utility::VocabularyType::Enum::OnDiskCompressed};

  // BlankNodeManager, initialized during `readConfiguration`
  std::unique_ptr<ad_utility::BlankNodeManager> blankNodeManager_{nullptr};

  std::optional<DeltaTriplesManager> deltaTriples_;

 public:
  explicit IndexImpl(ad_utility::AllocatorWithLimit<Id> allocator);

  // Forbid copying.
  IndexImpl& operator=(const IndexImpl&) = delete;
  IndexImpl(const IndexImpl&) = delete;
  // Moving is currently not supported, because several of the members use
  // mutexes internally. It is also not needed.
  IndexImpl& operator=(IndexImpl&&) = delete;
  IndexImpl(IndexImpl&&) = delete;

  const auto& POS() const { return *pos_; }
  auto& POS() { return *pos_; }
  const auto& PSO() const { return *pso_; }
  auto& PSO() { return *pso_; }
  const auto& SPO() const { return *spo_; }
  auto& SPO() { return *spo_; }
  const auto& SOP() const { return *sop_; }
  auto& SOP() { return *sop_; }
  const auto& OPS() const { return *ops_; }
  auto& OPS() { return *ops_; }
  const auto& OSP() const { return *osp_; }
  auto& OSP() { return *osp_; }

  static const IndexImpl& staticGlobalSingletonIndex() {
    AD_CORRECTNESS_CHECK(globalSingletonIndex_ != nullptr);
    return *globalSingletonIndex_;
  }

  static const TripleComponentComparator& staticGlobalSingletonComparator() {
    AD_CORRECTNESS_CHECK(globalSingletonComparator_ != nullptr);
    return *globalSingletonComparator_;
  }

  void setGlobalIndexAndComparatorOnlyForTesting() const {
    globalSingletonIndex_ = this;
    globalSingletonComparator_ = &vocab_.getCaseComparator();
  }

  // For a given `Permutation::Enum` (e.g. `PSO`) return the corresponding
  // `Permutation` object by reference or shared pointer (`pso_`).
  Permutation& getPermutation(Permutation::Enum p);
  const Permutation& getPermutation(Permutation::Enum p) const;
  PermutationPtr getPermutationPtr(Permutation::Enum p);
  std::shared_ptr<const Permutation> getPermutationPtr(
      Permutation::Enum p) const;

  // Creates an index from a given set of input files. Will write vocabulary and
  // on-disk index data.
  // !! The index can not directly be used after this call, but has to be setup
  // by createFromOnDiskIndex after this call.
  void createFromFiles(std::vector<Index::InputFileSpecification> files);

  // Creates an index object from an on disk index that has previously been
  // constructed. Read necessary meta data into memory and opens file handles.
  void createFromOnDiskIndex(const std::string& onDiskBase,
                             bool persistUpdatesOnDisk);

  // Adds text index from on disk index that has previously been constructed.
  // Read necessary meta data into memory and opens file handles.
  void addTextFromOnDiskIndex();

  const auto& getVocab() const { return vocab_; };
  auto& getNonConstVocabForTesting() { return vocab_; }

  const auto& getTextVocab() const { return textVocab_; };

  const auto& getScoreData() const { return scoreData_; }

  ad_utility::BlankNodeManager* getBlankNodeManager() const;

  DeltaTriplesManager& deltaTriplesManager() { return deltaTriples_.value(); }
  const DeltaTriplesManager& deltaTriplesManager() const {
    return deltaTriples_.value();
  }

  const auto& encodedIriManager() const { return encodedIriManager_; }

  // Set the prefixes of the IRIs that will be encoded directly into
  // the `Id`; see `EncodedIriManager` for details.
  void setPrefixesForEncodedValues(
      std::vector<std::string> prefixesWithoutAngleBrackets);

  // Set the vocabulary type; see `ad_utility::VocabularyType` for details.
  void setVocabularyTypeForIndexBuilding(ad_utility::VocabularyType type) {
    vocabularyTypeForIndexBuilding_ = type;
    configurationJson_["vocabulary-type"] = type;
  }

  // __________________________________________________________________________
  NumNormalAndInternal numDistinctSubjects() const;

  // __________________________________________________________________________
  NumNormalAndInternal numDistinctObjects() const;

  // __________________________________________________________________________
  NumNormalAndInternal numDistinctPredicates() const;

  // __________________________________________________________________________
  NumNormalAndInternal numDistinctCol0(Permutation::Enum permutation) const;

  // ___________________________________________________________________________
  size_t getCardinality(Id id, Permutation::Enum permutation,
                        const LocatedTriplesState&) const;

  // ___________________________________________________________________________
  size_t getCardinality(const TripleComponent& comp,
                        Permutation::Enum permutation,
                        const LocatedTriplesState& locatedTriplesState) const;

  // ___________________________________________________________________________
  RdfsVocabulary::AccessReturnType indexToString(VocabIndex id) const;

  // ___________________________________________________________________________
  TextVocabulary::AccessReturnType indexToString(WordVocabIndex id) const;

 public:
  // ___________________________________________________________________________
  Index::Vocab::PrefixRanges prefixRanges(std::string_view prefix) const;

  const CompactVectorOfStrings<Id>& getPatterns() const;
  /**
   * @return The multiplicity of the Entities column (0) of the full
   * has-relation relation after unrolling the patterns.
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

  // This struct is used to retrieve text blocks.
  struct TextBlockMetadataAndWordInfo {
    TextBlockMetadataAndWordInfo(
        const TextBlockMetaData& tbmd,
        const IdRange<WordVocabIndex>& includingIdRange)
        : tbmd_{tbmd},
          optIdRange_{
              computeHasToBeFiltered(includingIdRange)
                  ? std::optional<IdRange<WordVocabIndex>>{includingIdRange}
                  : std::nullopt} {}
    // The TextBlockMetaData has the information on where the blocks boundaries
    // and internal boundaries are. It is necessary to retrieve either the
    // context list or entity list of a text block.
    const TextBlockMetaData tbmd_;

    // Returns true if the text block contains entries outside of the requested
    // range
    bool hasToBeFiltered() const { return optIdRange_.has_value(); };

    // The id range of the prefix or word used to retrieve the text block(s). It
    // is only set if computeHasToBeFiltered was determined to be true during
    // construction.
    // Note: This range is inclusive so it is [lowerId, upperId],
    // NOT [lowerId, upperId)
    const std::optional<IdRange<WordVocabIndex>> optIdRange_;

    // Returns true if the text block contains entries outside of the requested
    // range
    bool computeHasToBeFiltered(
        const IdRange<WordVocabIndex>& includingIdRange) const {
      return !(tbmd_._firstWordId >= includingIdRange.first().get() &&
               tbmd_._lastWordId <= includingIdRange.last().get());
    }
  };

  std::string_view wordIdToString(WordIndex wordIndex) const;

  /**
   *
   * @param word: The word used to do the entity- or wordscan
   * @param textScanMode: If set to WordScan the contextLists are looked at, if
   *                      set to EntityScan the entityLists are looked at.
   * @return Sum of context list sizes or entity list sizes of all touched
   *         blocks. If 'forWord' is true and the word is not a prefix only a
   *         small number of words actually match. So the final result is much
   *         smaller. If 'forWord' is false and the word is a prefix then the
   *         final result can be a little smaller since duplicate entities are
   *         filtered out. For details see documentation of
   *         mergeTextBlockResults.
   * @note As a cost estimate the estimation is correct. Because we always have
   *       to read the complete blocks and then (if needed) filter by the
   *       actual wordId range.
   *       TODO: improve size estimate by adding a correction factor.
   */
  size_t getSizeOfTextBlocksSum(const std::string& word,
                                TextScanMode textScanMode) const;

  // Returns a set of [textRecord, term] pairs where the term is contained in
  // the textRecord. The term can be either the wordOrPrefix itself or a word
  // that has wordOrPrefix as a prefix. Returned IdTable has columns:
  // textRecord, word. Sorted by textRecord.
  IdTable getWordPostingsForTerm(
      const std::string& wordOrPrefix,
      const ad_utility::AllocatorWithLimit<Id>& allocator) const;

  // Returns a set of textRecords and their corresponding entities and
  // scores. Each textRecord contains its corresponding entity and the term.
  // Returned IdTable has columns: textRecord, entity, score. Sorted by
  // textRecord.
  // NOTE: This returns a superset because it contains the whole block and
  // unfitting words are filtered out later by the join with the
  // TextIndexScanForWords operation.
  IdTable getEntityMentionsForWord(
      const std::string& term,
      const ad_utility::AllocatorWithLimit<Id>& allocator) const;

  size_t getIndexOfBestSuitedElTerm(
      const std::vector<std::string>& terms) const;

  std::string getTextExcerpt(TextRecordIndex cid) const {
    if (cid.get() >= docsDB_._size) {
      return "";
    }
    return docsDB_.getTextExcerpt(cid);
  }

  float getAverageNofEntityContexts() const {
    return textMeta_.getAverageNofEntityContexts();
  };

  void setKbName(const std::string& name);

  void setTextName(const std::string& name);

  bool& usePatterns();

  bool& loadAllPermutations();

  void setKeepTempFiles(bool keepTempFiles);

  ad_utility::MemorySize& memoryLimitIndexBuilding() {
    return memoryLimitIndexBuilding_;
  }
  const ad_utility::MemorySize& memoryLimitIndexBuilding() const {
    return memoryLimitIndexBuilding_;
  }

  ad_utility::MemorySize& parserBufferSize() { return parserBufferSize_; }
  const ad_utility::MemorySize& parserBufferSize() const {
    return parserBufferSize_;
  }

  ad_utility::MemorySize& blocksizePermutationPerColumn() {
    return blocksizePermutationPerColumn_;
  }

  void setOnDiskBase(const std::string& onDiskBase);

  void setSettingsFile(const std::string& filename);

  void setNumTriplesPerBatch(uint64_t numTriplesPerBatch) {
    numTriplesPerBatch_ = numTriplesPerBatch;
  }

  const std::string& getTextName() const { return textMeta_.getName(); }
  const std::string& getKbName() const { return PSO().getKbName(); }
  const std::string& getOnDiskBase() const { return onDiskBase_; }
  const std::string& getIndexId() const { return indexId_; }
  const std::string& getGitShortHash() const { return gitShortHash_; }

  size_t getNofTextRecords() const { return textMeta_.getNofTextRecords(); }
  size_t getNofWordPostings() const { return textMeta_.getNofWordPostings(); }
  size_t getNofEntityPostings() const {
    return textMeta_.getNofEntityPostings();
  }
  size_t getNofNonLiteralsInTextIndex() const {
    return nofNonLiteralsInTextIndex_;
  }

  bool hasAllPermutations() const { return SPO().isLoaded(); }

  // ___________________________________________________________________________
  std::vector<float> getMultiplicities(
      const TripleComponent& key, const Permutation& permutation,
      const LocatedTriplesState& locatedTriplesState) const;

  // ___________________________________________________________________________
  std::vector<float> getMultiplicities(const Permutation& permutation) const;

  // _____________________________________________________________________________
  size_t getResultSizeOfScan(
      const ScanSpecification& scanSpecification,
      const Permutation::Enum& permutation,
      const LocatedTriplesState& locatedTriplesState) const;

 protected:
  // Private member functions

  // Create Vocabulary and directly write it to disk. Create TripleVec with all
  // the triples converted to id space. This Vec can be used for creating
  // permutations. Member vocab_ will be empty after this because it is not
  // needed for index creation once the TripleVec is set up and it would be a
  // waste of RAM.
  IndexBuilderDataAsFirstPermutationSorter createIdTriplesAndVocab(
      std::shared_ptr<RdfParserBase> parser);

  // ___________________________________________________________________
  IndexBuilderDataAsExternalVector passFileForVocabulary(
      std::shared_ptr<RdfParserBase> parser, size_t linesPerPartial);

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

  // Return a Turtle parser that parses the given file. The parser will be
  // configured to either parse in parallel or not, and to either use the
  // CTRE-based relaxed parser or not, depending on the settings of the
  // corresponding member variables.
  std::unique_ptr<RdfParserBase> makeRdfParser(
      const std::vector<Index::InputFileSpecification>& files) const;

  template <typename Func>
  FirstPermutationSorterAndInternalTriplesAsPso convertPartialToGlobalIds(
      TripleVec& data, const std::vector<size_t>& actualLinesPerPartial,
      size_t linesPerPartial, Func isQLeverInternalTriple);

  // TODO<joka921> Get rid of the `numColumns` by including them into the
  // `sortedTriples` argument.
  template <typename T, typename... Callbacks>
  std::tuple<size_t, IndexMetaDataMmapDispatcher::WriteType,
             IndexMetaDataMmapDispatcher::WriteType>
  createPermutationPairImpl(size_t numColumns, const std::string& fileName1,
                            const std::string& fileName2, T&& sortedTriples,
                            Permutation::KeyOrder permutation,
                            Callbacks&&... perTripleCallbacks);

  // _______________________________________________________________________
  // Create a pair of permutations. Only works for valid pairs (PSO-POS,
  // OSP-OPS, SPO-SOP).  First creates the permutation and then exchanges the
  // multiplicities and also writes the MetaData to disk. So we end up with
  // fully functional permutations.
  //
  // TODO: The rest of this comment looks outdated.
  //
  // performUnique must be set for the first pair created using vec to enforce
  // RDF standard (no duplicate triples).
  // createPatternsAfterFirst is only valid when  the pair is SPO-SOP because
  // the SPO permutation is also needed for patterns (see usage in
  // IndexImpl::createFromFile function)

  template <typename SortedTriplesType, typename... CallbackTypes>
  [[nodiscard]] size_t createPermutationPair(
      size_t numColumns, SortedTriplesType&& sortedTriples,
      const Permutation& p1, const Permutation& p2,
      CallbackTypes&&... perTripleCallbacks);

  // wrapper for createPermutation that saves a lot of code duplications
  // Writes the permutation that is specified by argument permutation
  // performs std::unique on arg vec iff arg performUnique is true (normally
  // done for first permutation that is created using vec).
  // Will sort vec.
  // returns the MetaData (MmapBased or HmapBased) for this relation.
  // Careful: only multiplicities for first column is valid after call, need to
  // call exchangeMultiplicities as done by createPermutationPair
  // the optional is std::nullopt if vec and thus the index is empty
  template <typename T, typename... Callbacks>
  std::tuple<size_t, IndexMetaDataMmapDispatcher::WriteType,
             IndexMetaDataMmapDispatcher::WriteType>
  createPermutations(size_t numColumns, T&& sortedTriples,
                     const Permutation& p1, const Permutation& p2,
                     Callbacks&&... perTripleCallbacks);

  void openTextFileHandle();

  // Get the metadata for the block from the text index that contains the
  // `word`. Also works for prefixes that are terminated with `PREFIX_CHAR` like
  // "astro*". Returns `nullopt` if no suitable block was found because no
  // matching word is contained in the text index. Some additional information
  // is also returned that is often required by the calling functions:
  // `hasToBeFiltered()` is true iff TextBlockMetadataAndWordInfo contains
  // entries outside of the idRange. `optIdRange_` is the range
  // `[first, last]` of the `WordVocabIndex`es that correspond to the word. It
  // is only set iff hasToBeFiltered() is true since no IdRange is needed else.
  // (`word` might also be a prefix, thus it is a range).
  std::vector<TextBlockMetadataAndWordInfo> getTextBlockMetadataForWordOrPrefix(
      const std::string& word) const;

  // Same as public getSizeOfTextBlocksSum method but works on metadata objects
  // instead of word or prefix. The public method uses this method internally.
  static size_t getSizeOfTextBlocksSum(
      const std::vector<IndexImpl::TextBlockMetadataAndWordInfo>& tbmds,
      TextScanMode textScanMode);

  /**
   * @brief This method is used to combine the IdTables of multiple blocks
   * returned from a word or entity scan into one IdTable.
   * @param reader: The reader is the function used to read the blocks from disk
   * @param tbmds: The tbmds are all TextBlockMetadataAndWordInfo returned by
   *               the getTextBlockMetadaForWordOrPrefix function
   * @param allocator: The allocator is used to create the result IdTable.
   * @param textScanMode:
   *        if WordScan: The contextLists of the blocks are read and filtered
   *              given the respective wordId range. The wordId range was
   *              previously calculated and saved in the
   *              TextBlockMetadataAndWordInfo.Those filtered IdTables are then
   *              merged into one. Sorted by TextRecordIndex (contextId).
   *         if EntityScan: The entityLists of the blocks are read and NOT
   *              filtered. During the merging exact duplicate entries are
   *              removed. Duplicates can occur since the same entity in the
   *              same text record is saved to all words occurring in this text
   *              record. It is checked that no duplicates occur in one block
   *              but when combining multiple blocks they have to be accounted
   *              for.
   */
  IdTable mergeTextBlockResults(
      absl::FunctionRef<IdTable(const TextBlockMetaData&,
                                const ad_utility::AllocatorWithLimit<ValueId>&,
                                const ad_utility::File&, TextScoringMetric)>
          reader,
      const std::vector<TextBlockMetadataAndWordInfo>& tbmds,
      const ad_utility::AllocatorWithLimit<Id>& allocator,
      TextScanMode textScanMode) const;

  TextBlockIndex getWordBlockId(WordIndex wordIndex) const;

  // FRIEND TESTS
  friend class IndexTest_createFromTsvTest_Test;
  friend class IndexTest_createFromOnDiskIndexTest_Test;
  friend class CreatePatternsFixture_createPatterns_Test;

  bool isLiteral(std::string_view object) const;

 public:
  LangtagAndTriple tripleToInternalRepresentation(TurtleTriple&& triple) const;

 protected:
  /**
   * @brief Throws an exception if no patterns are loaded. Should be called from
   *        within any index method that returns data requiring the patterns
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
  void deleteTemporaryFile(const std::string& path);

  // Return the filename where the patterns are stored.
  std::string getPatternFilename() const;

 public:
  // Count the number of "QLever-internal" triples (predicate ql:langtag or
  // predicate starts with @) and all other triples (that were actually part of
  // the input).
  NumNormalAndInternal numTriples() const;

  using BlocksOfTriples = ad_utility::InputRangeTypeErased<IdTableStatic<0>>;

  // Functions to create the pairs of permutations during the index build. Each
  // of them takes the following arguments:
  // * `sortedInput`  The input, must be sorted by the first permutation in the
  //    function name.
  // * `nextSorter` A callback that is invoked for each row in each of the
  //    blocks in the input. Typically used to set up the sorting for the
  //    subsequent pair of permutations.

  // Create the SPO and SOP permutations. Additionally, count the number of
  // distinct actual (not internal) subjects in the input and write it to the
  // metadata. Also builds the patterns if specified.
  CPP_template(typename... NextSorter)(requires(sizeof...(NextSorter) <= 1))
      std::optional<PatternCreator::TripleSorter> createSPOAndSOP(
          size_t numColumns, BlocksOfTriples sortedTriples,
          NextSorter&&... nextSorter);

  // Create the OSP and OPS permutations. Additionally, count the number of
  // distinct objects and write it to the metadata.
  CPP_template(typename... NextSorter)(requires(
      sizeof...(NextSorter) <=
      1)) void createOSPAndOPS(size_t numColumns, BlocksOfTriples sortedTriples,
                               NextSorter&&... nextSorter);

  // Create the PSO and POS permutations. Additionally, count the number of
  // distinct predicates and the number of actual triples and write them to the
  // metadata. The meta-data JSON file for the index statistics will only be
  // written iff `doWriteConfiguration` is true. That parameter is set to
  // `false` when building the additional permutations for the internal triples.
  CPP_template(typename... NextSorter)(
      requires(sizeof...(NextSorter) <=
               1)) void createPSOAndPOSImpl(size_t numColumns,
                                            BlocksOfTriples sortedTriples,
                                            bool doWriteConfiguration,
                                            NextSorter&&... nextSorter);

  // Call `createPSOAndPOSImpl` with the given arguments and with
  // `doWriteConfiguration` set to `true` (see above).
  CPP_template(typename... NextSorter)(requires(
      sizeof...(NextSorter) <=
      1)) void createPSOAndPOS(size_t numColumns, BlocksOfTriples sortedTriples,
                               NextSorter&&... nextSorter);

  // Create the internal PSO and POS permutations from the sorted internal
  // triples. Return `(numInternalTriples, numInternalPredicates)`.
  template <typename InternalTriplePsoSorter>
  std::pair<size_t, size_t> createInternalPSOandPOS(
      InternalTriplePsoSorter&& internalTriplesPsoSorter);

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
  std::optional<PatternCreator::TripleSorter> createFirstPermutationPair(
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

  template <typename... Args>
  void createSecondPermutationPair(Args&&... args) {
    static_assert(std::is_same_v<SecondPermutation, SortByOSP>);
    return createOSPAndOPS(AD_FWD(args)...);
  }

  template <typename... Args>
  void createThirdPermutationPair(Args&&... args) {
    static_assert(std::is_same_v<ThirdPermutation, SortByPSO>);
    return createPSOAndPOS(AD_FWD(args)...);
  }

  // Build the OSP and OPS permutations from the output of the `PatternCreator`.
  // The permutations will have two additional columns: The subject pattern of
  // the subject (which is already created by the `PatternCreator`) and the
  // subject pattern of the object (which is created by this function). Return
  // these five columns sorted by PSO, to be used as an input for building the
  // PSO and POS permutations.
  template <typename InternalTripleSorter>
  std::unique_ptr<ExternalSorter<SortByPSO, NumColumnsIndexBuilding + 2>>
  buildOspWithPatterns(PatternCreator::TripleSorter sortersFromPatternCreator,
                       InternalTripleSorter& internalTripleSorter);

  // During the index, building add the number of internal triples and internal
  // predicates to the configuration. Note: the number of internal objects and
  // subjects will always remain zero, because we have no cheap way of computing
  // them.
  void addInternalStatisticsToConfiguration(size_t numTriplesInternal,
                                            size_t numPredicatesInternal);

  // Update `InputFileSpecification` based on `parallelParsingSpecifiedViaJson`
  // and write a summary to the log.
  static void updateInputFileSpecificationsAndLog(
      std::vector<Index::InputFileSpecification>& spec,
      std::optional<bool> parallelParsingSpecifiedViaJson);

  void storeTextScoringParamsInConfiguration(TextScoringMetric scoringMetric,
                                             float b, float k);
};

#endif  // QLEVER_SRC_INDEX_INDEXIMPL_H
