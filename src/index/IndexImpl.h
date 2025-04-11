// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2014-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_INDEX_INDEXIMPL_H
#define QLEVER_SRC_INDEX_INDEXIMPL_H

#include <array>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "backports/algorithm.h"
#include "engine/AddCombinedRowToTable.h"
#include "engine/Result.h"
#include "engine/idTable/CompressedExternalIdTable.h"
#include "global/Pattern.h"
#include "global/SpecialIds.h"
#include "index/CompressedRelation.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/DeltaTriples.h"
#include "index/DocsDB.h"
#include "index/ExternalSortFunctors.h"
#include "index/Index.h"
#include "index/IndexBuilderTypes.h"
#include "index/IndexMetaData.h"
#include "index/PatternCreator.h"
#include "index/Permutation.h"
#include "index/Postings.h"
#include "index/TextMetaData.h"
#include "index/TextScoring.h"
#include "index/Vocabulary.h"
#include "index/VocabularyMerger.h"
#include "parser/RdfParser.h"
#include "parser/TripleComponent.h"
#include "parser/WordsAndDocsFileParser.h"
#include "util/BufferedVector.h"
#include "util/CancellationHandle.h"
#include "util/File.h"
#include "util/Forward.h"
#include "util/HashMap.h"
#include "util/JoinAlgorithms/JoinAlgorithms.h"
#include "util/MemorySize/MemorySize.h"
#include "util/MmapVector.h"
#include "util/json.h"

using ad_utility::BufferedVector;
using ad_utility::MmapVector;
using ad_utility::MmapVectorView;
using std::array;
using std::shared_ptr;
using std::string;
using std::tuple;
using std::vector;

using json = nlohmann::json;

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
  using TripleVec =
      ad_utility::CompressedExternalIdTable<NumColumnsIndexBuilding>;
  // Block Id, Context Id, Word Id, Score, entity
  using TextVec = ad_utility::CompressedExternalIdTableSorter<SortText, 5>;

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
  json configurationJson_;
  Index::Vocab vocab_;
  Index::TextVocab textVocab_;
  ScoreData scoreData_;

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

  NumNormalAndInternal numSubjects_;
  NumNormalAndInternal numPredicates_;
  NumNormalAndInternal numObjects_;
  NumNormalAndInternal numTriples_;
  string indexId_;

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
  Permutation pos_{Permutation::Enum::POS, allocator_};
  Permutation pso_{Permutation::Enum::PSO, allocator_};
  Permutation sop_{Permutation::Enum::SOP, allocator_};
  Permutation spo_{Permutation::Enum::SPO, allocator_};
  Permutation ops_{Permutation::Enum::OPS, allocator_};
  Permutation osp_{Permutation::Enum::OSP, allocator_};

  // During the index building, store the IDs of the `ql:has-pattern` predicate
  // and of `ql:default-graph` as they are required to add additional triples
  // after the creation of the vocabulary is finished.
  std::optional<Id> idOfHasPatternDuringIndexBuilding_;
  std::optional<Id> idOfInternalGraphDuringIndexBuilding_;

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
  // `Permutation` object by reference (`pso_`).
  Permutation& getPermutation(Permutation::Enum p);
  const Permutation& getPermutation(Permutation::Enum p) const;

  // Creates an index from a given set of input files. Will write vocabulary and
  // on-disk index data.
  // !! The index can not directly be used after this call, but has to be setup
  // by createFromOnDiskIndex after this call.
  void createFromFiles(std::vector<Index::InputFileSpecification> files);

  // Creates an index object from an on disk index that has previously been
  // constructed. Read necessary meta data into memory and opens file handles.
  void createFromOnDiskIndex(const string& onDiskBase,
                             bool persistUpdatesOnDisk);

  // Adds a text index to a complete KB index. Reads words from the given
  // wordsfile and calculates bm25 scores with the docsfile if given.
  // Additionally adds words from literals of the existing KB. Can't be called
  // with only words or only docsfile, but with or without both. Also can't be
  // called with the pair empty and bool false
  void buildTextIndexFile(
      const std::optional<std::pair<string, string>>& wordsAndDocsFile,
      bool addWordsFromLiterals, TextScoringMetric textScoringMetric,
      std::pair<float, float> bAndKForBM25);

  // Build docsDB file from given file (one text record per line).
  void buildDocsDB(const string& docsFile) const;

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
  size_t getCardinality(Id id, Permutation::Enum permutation,
                        const LocatedTriplesSnapshot&) const;

  // ___________________________________________________________________________
  size_t getCardinality(
      const TripleComponent& comp, Permutation::Enum permutation,
      const LocatedTriplesSnapshot& locatedTriplesSnapshot) const;

  // ___________________________________________________________________________
  std::string indexToString(VocabIndex id) const;

  // ___________________________________________________________________________
  std::string_view indexToString(WordVocabIndex id) const;

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

  // Returns a set of [textRecord, term] pairs where the term is contained in
  // the textRecord. The term can be either the wordOrPrefix itself or a word
  // that has wordOrPrefix as a prefix. Returned IdTable has columns:
  // textRecord, word. Sorted by textRecord.
  IdTable getWordPostingsForTerm(
      const string& wordOrPrefix,
      const ad_utility::AllocatorWithLimit<Id>& allocator) const;

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

  IdTable readContextListHelper(
      const ad_utility::AllocatorWithLimit<Id>& allocator,
      const ContextListMetaData& contextList, bool isWordCl) const;

  IdTable readWordCl(const TextBlockMetaData& tbmd,
                     const ad_utility::AllocatorWithLimit<Id>& allocator) const;

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

  const string& getTextName() const { return textMeta_.getName(); }
  const string& getKbName() const { return pso_.getKbName(); }
  const string& getOnDiskBase() const { return onDiskBase_; }
  const string& getIndexId() const { return indexId_; }

  size_t getNofTextRecords() const { return textMeta_.getNofTextRecords(); }
  size_t getNofWordPostings() const { return textMeta_.getNofWordPostings(); }
  size_t getNofEntityPostings() const {
    return textMeta_.getNofEntityPostings();
  }
  size_t getNofNonLiteralsInTextIndex() const {
    return nofNonLiteralsInTextIndex_;
  }

  bool hasAllPermutations() const { return SPO().isLoaded(); }

  // _____________________________________________________________________________
  vector<float> getMultiplicities(
      const TripleComponent& key, Permutation::Enum permutation,
      const LocatedTriplesSnapshot& locatedTriplesSnapshot) const;

  // ___________________________________________________________________
  vector<float> getMultiplicities(Permutation::Enum permutation) const;

  // _____________________________________________________________________________
  IdTable scan(const ScanSpecificationAsTripleComponent& scanSpecification,
               const Permutation::Enum& permutation,
               Permutation::ColumnIndicesRef additionalColumns,
               const ad_utility::SharedCancellationHandle& cancellationHandle,
               const LocatedTriplesSnapshot& locatedTriplesSnapshot,
               const LimitOffsetClause& limitOffset = {}) const;

  // _____________________________________________________________________________
  IdTable scan(const ScanSpecification& scanSpecification, Permutation::Enum p,
               Permutation::ColumnIndicesRef additionalColumns,
               const ad_utility::SharedCancellationHandle& cancellationHandle,
               const LocatedTriplesSnapshot& locatedTriplesSnapshot,
               const LimitOffsetClause& limitOffset = {}) const;

  // _____________________________________________________________________________
  size_t getResultSizeOfScan(
      const ScanSpecification& scanSpecification,
      const Permutation::Enum& permutation,
      const LocatedTriplesSnapshot& locatedTriplesSnapshot) const;

 private:
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

  template <typename T>
  FirstPermutationSorterAndInternalTriplesAsPso convertPartialToGlobalIds(
      TripleVec& data, const vector<size_t>& actualLinesPerPartial,
      size_t linesPerPartial, T isQLeverInternalTriple) {
    AD_LOG_INFO << "Converting triples from local IDs to global IDs ..."
                << std::endl;
    AD_LOG_DEBUG << "Triples per partial vocabulary: " << linesPerPartial
                 << std::endl;

    // Iterate over all partial vocabularies.
    auto resultPtr =
        [&]() -> std::unique_ptr<
                  ad_utility::CompressedExternalIdTableSorterTypeErased> {
      if (loadAllPermutations()) {
        return makeSorterPtr<FirstPermutation>("first");
      } else {
        return makeSorterPtr<SortByPSO>("first");
      }
    }();
    auto internalTriplesPtr =
        makeSorterPtr<SortByPSO, NumColumnsIndexBuilding>("internalTriples");
    auto& result = *resultPtr;
    auto& internalResult = *internalTriplesPtr;
    auto triplesGenerator = data.getRows();
    // static_assert(!std::is_const_v<decltype(triplesGenerator)>);
    // static_assert(std::is_const_v<decltype(triplesGenerator)>);
    auto it = triplesGenerator.begin();
    using Buffer = IdTableStatic<NumColumnsIndexBuilding>;
    struct Buffers {
      IdTableStatic<NumColumnsIndexBuilding> triples_;
      IdTableStatic<NumColumnsIndexBuilding> internalTriples_;
    };
    using Map = ad_utility::HashMap<Id, Id>;

    ad_utility::TaskQueue<true> lookupQueue(30, 10,
                                            "looking up local to global IDs");
    // This queue will be used to push the converted triples to the sorter. It
    // is important that it has only one thread because it will not be used in a
    // thread-safe way.
    ad_utility::TaskQueue<true> writeQueue(30, 1, "Writing global Ids to file");

    // For all triple elements find their mapping from partial to global ids.
    auto transformTriple = [](Buffer::row_reference& curTriple, auto& idMap) {
      for (auto& id : curTriple) {
        // TODO<joka92> Since the mapping only maps `VocabIndex->VocabIndex`,
        // probably the mapping should also be defined as `HashMap<VocabIndex,
        // VocabIndex>` instead of `HashMap<Id, Id>`
        if (id.getDatatype() != Datatype::VocabIndex) {
          // Check that all the internal, special IDs which we have introduced
          // for performance reasons are eliminated.
          AD_CORRECTNESS_CHECK(id.getDatatype() != Datatype::Undefined);
          continue;
        }
        auto iterator = idMap.find(id);
        AD_CORRECTNESS_CHECK(iterator != idMap.end());
        id = iterator->second;
      }
    };

    // Return a lambda that pushes all the triples to the sorter. Must only be
    // called single-threaded.
    size_t numTriplesConverted = 0;
    ad_utility::ProgressBar progressBar{numTriplesConverted,
                                        "Triples converted: "};
    auto getWriteTask = [&result, &internalResult, &numTriplesConverted,
                         &progressBar](Buffers buffers) {
      return [&result, &internalResult, &numTriplesConverted, &progressBar,
              triples = std::make_shared<IdTableStatic<0>>(
                  std::move(buffers.triples_).toDynamic()),
              internalTriples = std::make_shared<IdTableStatic<0>>(
                  std::move(buffers.internalTriples_).toDynamic())] {
        result.pushBlock(*triples);
        internalResult.pushBlock(*internalTriples);

        numTriplesConverted += triples->size();
        numTriplesConverted += internalTriples->size();
        if (progressBar.update()) {
          AD_LOG_INFO << progressBar.getProgressString() << std::flush;
        }
      };
    };

    // Return a lambda that for each of the `triples` transforms its partial to
    // global IDs using the `idMap`. The map is passed as a `shared_ptr` because
    // multiple batches need access to the same map.
    auto getLookupTask = [&isQLeverInternalTriple, &writeQueue,
                          &transformTriple, &getWriteTask](
                             Buffer triples, std::shared_ptr<Map> idMap) {
      return [&isQLeverInternalTriple, &writeQueue,
              triples = std::make_shared<Buffer>(std::move(triples)),
              idMap = std::move(idMap), &getWriteTask,
              &transformTriple]() mutable {
        for (Buffer::row_reference triple : *triples) {
          transformTriple(triple, *idMap);
        }
        auto beginInternal =
            std::partition(triples->begin(), triples->end(),
                           [&isQLeverInternalTriple](const auto& row) {
                             return !isQLeverInternalTriple(row);
                           });
        IdTableStatic<NumColumnsIndexBuilding> internalTriples(
            triples->getAllocator());
        // TODO<joka921> We could leave the partitioned complete block as is,
        // and change the interface of the compressed sorters s.t. we can
        // push only a part of a block. We then would safe the copy of the
        // internal triples here, but I am not sure whether this is worth it.
        internalTriples.insertAtEnd(*triples, beginInternal - triples->begin(),
                                    triples->end() - triples->begin());
        triples->resize(beginInternal - triples->begin());

        Buffers buffers{std::move(*triples), std::move(internalTriples)};

        writeQueue.push(getWriteTask(std::move(buffers)));
      };
    };

    std::atomic<size_t> nextPartialVocabulary = 0;
    // Return the mapping from partial to global Ids for the batch with idx
    // `nextPartialVocabulary` and increase that counter by one. Return
    // `nullopt` if there are no more partial vocabularies to read.
    auto createNextVocab = [&nextPartialVocabulary, &actualLinesPerPartial,
                            this]() -> std::optional<std::pair<size_t, Map>> {
      auto idx = nextPartialVocabulary.fetch_add(1, std::memory_order_relaxed);
      if (idx >= actualLinesPerPartial.size()) {
        return std::nullopt;
      }
      std::string mmapFilename =
          absl::StrCat(onDiskBase_, PARTIAL_MMAP_IDS, idx);
      auto map = ad_utility::vocabulary_merger::IdMapFromPartialIdMapFile(
          mmapFilename);
      // Delete the temporary file in which we stored this map
      deleteTemporaryFile(mmapFilename);
      return std::pair{idx, std::move(map)};
    };

    // Set up a generator that yields all the mappings in order, but reads them
    // in parallel.
    auto mappings = ad_utility::data_structures::queueManager<
        ad_utility::data_structures::OrderedThreadSafeQueue<Map>>(
        10, 5, createNextVocab);

    // TODO<C++23> Use `views::enumerate`.
    size_t batchIdx = 0;
    for (auto& mapping : mappings) {
      auto idMap = std::make_shared<Map>(std::move(mapping));

      const size_t bufferSize = BUFFER_SIZE_PARTIAL_TO_GLOBAL_ID_MAPPINGS;
      Buffer buffer{ad_utility::makeUnlimitedAllocator<Id>()};
      buffer.reserve(bufferSize);
      auto pushBatch = [&buffer, &idMap, &lookupQueue, &getLookupTask,
                        bufferSize]() {
        lookupQueue.push(getLookupTask(std::move(buffer), idMap));
        buffer.clear();
        buffer.reserve(bufferSize);
      };
      // Update the triples that belong to this partial vocabulary.
      for ([[maybe_unused]] auto idx :
           ad_utility::integerRange(actualLinesPerPartial[batchIdx])) {
        buffer.push_back(*it);
        if (buffer.size() >= bufferSize) {
          pushBatch();
        }
        ++it;
      }
      if (!buffer.empty()) {
        pushBatch();
      }
      ++batchIdx;
    }
    lookupQueue.finish();
    writeQueue.finish();
    AD_LOG_INFO << progressBar.getFinalProgressString() << std::flush;
    return {std::move(resultPtr), std::move(internalTriplesPtr)};
  }

  // Generator that returns all words in the given context file (if not empty)
  // and then all words in all literals (if second argument is true).
  //
  // TODO: So far, this is limited to the internal vocabulary (still in the
  // testing phase, once it works, it should be easy to include the IRIs and
  // literals from the external vocabulary as well).
  cppcoro::generator<WordsFileLine> wordsInTextRecords(
      std::string contextFile, bool addWordsFromLiterals) const;

  void processEntityCaseDuringInvertedListProcessing(
      const WordsFileLine& line,
      ad_utility::HashMap<Id, Score>& entitiesInContxt, size_t& nofLiterals,
      size_t& entityNotFoundErrorMsgCount) const;

  void processWordCaseDuringInvertedListProcessing(
      const WordsFileLine& line,
      ad_utility::HashMap<WordIndex, Score>& wordsInContext,
      ScoreData& scoreData) const;

  static void logEntityNotFound(const string& word,
                                size_t& entityNotFoundErrorMsgCount);

  size_t processWordsForVocabulary(const string& contextFile,
                                   bool addWordsFromLiterals);

  void processWordsForInvertedLists(const string& contextFile,
                                    bool addWordsFromLiterals, TextVec& vec);

  // TODO<joka921> Get rid of the `numColumns` by including them into the
  // `sortedTriples` argument.
  template <typename T, typename... Callbacks>
  std::tuple<size_t, IndexMetaDataMmapDispatcher::WriteType,
             IndexMetaDataMmapDispatcher::WriteType>
  createPermutationPairImpl(size_t numColumns, const string& fileName1,
                            const string& fileName2, T&& sortedTriples,
                            std::array<size_t, 3> permutation,
                            Callbacks&&... perTripleCallbacks) {
    using MetaData = IndexMetaDataMmapDispatcher::WriteType;
    MetaData metaData1, metaData2;
    static_assert(MetaData::isMmapBased_);
    metaData1.setup(fileName1 + MMAP_FILE_SUFFIX, ad_utility::CreateTag{});
    metaData2.setup(fileName2 + MMAP_FILE_SUFFIX, ad_utility::CreateTag{});

    CompressedRelationWriter writer1{numColumns,
                                     ad_utility::File(fileName1, "w"),
                                     blocksizePermutationPerColumn_};
    CompressedRelationWriter writer2{numColumns,
                                     ad_utility::File(fileName2, "w"),
                                     blocksizePermutationPerColumn_};

    // Lift a callback that works on single elements to a callback that works on
    // blocks.
    auto liftCallback = [](auto callback) {
      return [callback](const auto& block) mutable {
        ql::ranges::for_each(block, callback);
      };
    };
    auto callback1 =
        liftCallback([&metaData1](const auto& md) { metaData1.add(md); });
    auto callback2 =
        liftCallback([&metaData2](const auto& md) { metaData2.add(md); });

    std::vector<std::function<void(const IdTableStatic<0>&)>> perBlockCallbacks{
        liftCallback(perTripleCallbacks)...};

    auto [numDistinctCol0, blockData1, blockData2] =
        CompressedRelationWriter::createPermutationPair(
            fileName1, {writer1, callback1}, {writer2, callback2},
            AD_FWD(sortedTriples), permutation, perBlockCallbacks);
    metaData1.blockData() = std::move(blockData1);
    metaData2.blockData() = std::move(blockData2);

    // There previously was a bug in the CompressedIdTableSorter that lead to
    // semantically correct blocks, but with too large block sizes for the twin
    // relation. This assertion would have caught this bug.
    AD_CORRECTNESS_CHECK(metaData1.blockData().size() ==
                         metaData2.blockData().size());

    return {numDistinctCol0, std::move(metaData1), std::move(metaData2)};
  }

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
                     Callbacks&&... perTripleCallbacks) {
    AD_LOG_INFO << "Creating permutations " << p1.readableName() << " and "
                << p2.readableName() << " ..." << std::endl;
    auto metaData = createPermutationPairImpl(
        numColumns, onDiskBase_ + ".index" + p1.fileSuffix(),
        onDiskBase_ + ".index" + p2.fileSuffix(), AD_FWD(sortedTriples),
        p1.keyOrder(), AD_FWD(perTripleCallbacks)...);

    auto& [numDistinctCol0, meta1, meta2] = metaData;
    meta1.calculateStatistics(numDistinctCol0);
    meta2.calculateStatistics(numDistinctCol0);
    AD_LOG_INFO << "Statistics for " << p1.readableName() << ": "
                << meta1.statistics() << std::endl;
    AD_LOG_INFO << "Statistics for " << p2.readableName() << ": "
                << meta2.statistics() << std::endl;

    return metaData;
  }

  void createTextIndex(const string& filename, TextVec& vec);

  void openTextFileHandle();

  void addContextToVector(TextVec& vec, TextRecordIndex context,
                          const ad_utility::HashMap<WordIndex, Score>& words,
                          const ad_utility::HashMap<Id, Score>& entities) const;

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

  // FRIEND TESTS
  friend class IndexTest_createFromTsvTest_Test;
  friend class IndexTest_createFromOnDiskIndexTest_Test;
  friend class CreatePatternsFixture_createPatterns_Test;

  bool isLiteral(const string& object) const;

 public:
  LangtagAndTriple tripleToInternalRepresentation(TurtleTriple&& triple) const;

 private:
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
  void deleteTemporaryFile(const string& path);

 public:
  // Count the number of "QLever-internal" triples (predicate ql:langtag or
  // predicate starts with @) and all other triples (that were actually part of
  // the input).
  NumNormalAndInternal numTriples() const;

  using BlocksOfTriples = cppcoro::generator<IdTableStatic<0>>;

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
  template <typename T>
  std::pair<size_t, size_t> createInternalPSOandPOS(
      T&& internalTriplesPsoSorter) {
    auto onDiskBaseBackup = onDiskBase_;
    auto configurationJsonBackup = configurationJson_;
    onDiskBase_.append(QLEVER_INTERNAL_INDEX_INFIX);
    auto internalTriplesUnique = ad_utility::uniqueBlockView(
        internalTriplesPsoSorter.template getSortedBlocks<0>());
    createPSOAndPOSImpl(NumColumnsIndexBuilding,
                        std::move(internalTriplesUnique), false);
    onDiskBase_ = std::move(onDiskBaseBackup);
    // The "normal" triples from the "internal" index builder are actually
    // internal.
    size_t numTriplesInternal =
        static_cast<NumNormalAndInternal>(configurationJson_["num-triples"])
            .normal;
    size_t numPredicatesInternal =
        static_cast<NumNormalAndInternal>(configurationJson_["num-predicates"])
            .normal;
    configurationJson_ = std::move(configurationJsonBackup);
    return {numTriplesInternal, numPredicatesInternal};
  }

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

  // Return the array {2, 1, 0, 3, 4, 5, ..., numColumns - 1};
  template <size_t numColumns>
  static constexpr auto makePermutationFirstThirdSwitched() {
    static_assert(numColumns >= 3);
    std::array<ColumnIndex, numColumns> permutation{};
    std::iota(permutation.begin(), permutation.end(), ColumnIndex{0});
    std::swap(permutation[0], permutation[2]);
    return permutation;
  }

  // In the pattern column replace UNDEF (which is created by the optional join)
  // by the special `NO_PATTERN` ID and undo the permutation of the columns that
  // was only needed for the join algorithm.
  template <typename T>
  static auto fixBlockAfterPatternJoin(T block) {
    // The permutation must be the inverse of the original permutation, which
    // just switches the third column (the object) into the first column (where
    // the join column is expected by the algorithms).
    static constexpr auto permutation =
        makePermutationFirstThirdSwitched<NumColumnsIndexBuilding + 2>();
    block.value().setColumnSubset(permutation);
    ql::ranges::for_each(
        block.value().getColumn(ADDITIONAL_COLUMN_INDEX_OBJECT_PATTERN),
        [](Id& id) {
          id = id.isUndefined() ? Id::makeFromInt(NO_PATTERN) : id;
        });
    return std::move(block.value()).template toStatic<0>();
  }

  // Return an input range of the blocks that are returned by the external
  // sorter to which `sorterPtr` points. Only the subset/permutation specified
  // by the `columnIndices` will be returned for each block.
  template <typename T1, typename T2>
  static auto lazyScanWithPermutedColumns(T1& sorterPtr, T2 columnIndices) {
    auto setSubset = [columnIndices](auto& idTable) {
      idTable.setColumnSubset(columnIndices);
    };
    return ad_utility::inPlaceTransformView(
        ad_utility::OwningView{sorterPtr->template getSortedBlocks<0>()},
        setSubset);
  }

  // Perform a lazy optional block join on the first column of `leftInput` and
  // `rightInput`. The `resultCallback` will be called for each block of
  // resulting rows. Assumes that `leftInput` and `rightInput` have 6 columns in
  // total, so the result will have 5 columns.
  template <typename T1, typename T2, typename F>
  static auto lazyOptionalJoinOnFirstColumn(T1& leftInput, T2& rightInput,
                                            F resultCallback) {
    auto projection = [](const auto& row) -> Id { return row[0]; };
    auto projectionForComparator = [](const auto& rowOrId) -> const Id& {
      using T = std::decay_t<decltype(rowOrId)>;
      if constexpr (ad_utility::SimilarTo<T, Id>) {
        return rowOrId;
      } else {
        return rowOrId[0];
      }
    };
    auto comparator = [&projectionForComparator](const auto& l, const auto& r) {
      return projectionForComparator(l).compareWithoutLocalVocab(
                 projectionForComparator(r)) < 0;
    };

    // There are 6 columns in the result (4 from the triple + graph ID, as well
    // as subject patterns of the subject and object).
    IdTable outputTable{NumColumnsIndexBuilding + 2,
                        ad_utility::makeUnlimitedAllocator<Id>()};
    // The first argument is the number of join columns.
    auto rowAdder = ad_utility::AddCombinedRowToIdTable{
        1, std::move(outputTable),
        std::make_shared<ad_utility::CancellationHandle<>>(),
        BUFFER_SIZE_JOIN_PATTERNS_WITH_OSP, resultCallback};

    ad_utility::zipperJoinForBlocksWithoutUndef(
        leftInput, rightInput, comparator, rowAdder, projection, projection,
        std::true_type{});
    rowAdder.flush();
  }

  // Build the OSP and OPS permutations from the output of the `PatternCreator`.
  // The permutations will have two additional columns: The subject pattern of
  // the subject (which is already created by the `PatternCreator`) and the
  // subject pattern of the object (which is created by this function). Return
  // these five columns sorted by PSO, to be used as an input for building the
  // PSO and POS permutations.
  template <typename T>
  std::unique_ptr<ExternalSorter<SortByPSO, NumColumnsIndexBuilding + 2>>
  buildOspWithPatterns(PatternCreator::TripleSorter sortersFromPatternCreator,
                       T& internalTripleSorter) {
    auto&& [hasPatternPredicateSortedByPSO, secondSorter] =
        sortersFromPatternCreator;
    // We need the patterns twice: once for the additional column, and once for
    // the additional permutation.
    hasPatternPredicateSortedByPSO->moveResultOnMerge() = false;
    // The column with index 1 always is `has-predicate` and is not needed here.
    // Note that the order of the columns during index building  is always
    // `SPO`, but the sorting might be different (PSO in this case).
    auto lazyPatternScan = lazyScanWithPermutedColumns(
        hasPatternPredicateSortedByPSO, std::array<ColumnIndex, 2>{0, 2});
    ad_utility::data_structures::ThreadSafeQueue<IdTable> queue{4};

    // The permutation (2, 1, 0, 3) switches the third column (the object) with
    // the first column (where the join column is expected by the algorithms).
    // This permutation is reverted as part of the `fixBlockAfterPatternJoin`
    // function.
    auto ospAsBlocksTransformed = lazyScanWithPermutedColumns(
        secondSorter,
        makePermutationFirstThirdSwitched<NumColumnsIndexBuilding + 1>());

    // Run the actual join between the OSP permutation and the `has-pattern`
    // predicate on a background thread. The result will be pushed to the
    // `queue` so that we can consume it asynchronously.
    ad_utility::JThread joinWithPatternThread{
        [&queue, &ospAsBlocksTransformed, &lazyPatternScan] {
          // Setup the callback for the join that will buffer the results and
          // push them to the queue.
          IdTable outputBufferTable{NumColumnsIndexBuilding + 2,
                                    ad_utility::makeUnlimitedAllocator<Id>()};
          auto pushToQueue = [&, bufferSize =
                                     BUFFER_SIZE_JOIN_PATTERNS_WITH_OSP.load()](
                                 IdTable& table, LocalVocab&) {
            if (table.numRows() >= bufferSize) {
              if (!outputBufferTable.empty()) {
                queue.push(std::move(outputBufferTable));
                outputBufferTable.clear();
              }
              queue.push(std::move(table));
            } else {
              outputBufferTable.insertAtEnd(table);
              if (outputBufferTable.size() >= bufferSize) {
                queue.push(std::move(outputBufferTable));
                outputBufferTable.clear();
              }
            }
            table.clear();
          };

          lazyOptionalJoinOnFirstColumn(ospAsBlocksTransformed, lazyPatternScan,
                                        pushToQueue);

          // We still might have some buffered results left, push them to the
          // queue and then finish the queue.
          if (!outputBufferTable.empty()) {
            queue.push(std::move(outputBufferTable));
            outputBufferTable.clear();
          }
          queue.finish();
        }};

    // Set up a generator that yields blocks with the following columns:
    // S P O PatternOfS PatternOfO, sorted by OPS.
    auto blockGenerator =
        [](auto& queue) -> cppcoro::generator<IdTableStatic<0>> {
      // If an exception occurs in the block that is consuming the blocks
      // yielded from this generator, we have to explicitly finish the `queue`,
      // otherwise there will be a deadlock because the threads involved in the
      // queue can never join.
      absl::Cleanup cl{[&queue]() { queue.finish(); }};
      while (auto block = queue.pop()) {
        co_yield fixBlockAfterPatternJoin(std::move(block));
      }
    }(queue);

    // Actually create the permutations.
    auto thirdSorter =
        makeSorterPtr<ThirdPermutation, NumColumnsIndexBuilding + 2>("third");
    createSecondPermutationPair(NumColumnsIndexBuilding + 2,
                                std::move(blockGenerator), *thirdSorter);
    secondSorter->clear();
    // Add the `ql:has-pattern` predicate to the sorter such that it will become
    // part of the PSO and POS permutation.
    AD_LOG_INFO << "Adding " << hasPatternPredicateSortedByPSO->size()
                << " triples to the POS and PSO permutation for "
                   "the internal `ql:has-pattern` ..."
                << std::endl;
    static_assert(NumColumnsIndexBuilding == 4,
                  "When adding additional payload columns, the following code "
                  "has to be changed");
    Id internalGraph = idOfInternalGraphDuringIndexBuilding_.value();
    // Note: We are getting the patterns sorted by PSO and then sorting them
    // again by PSO.
    // TODO<joka921> Simply get the output unsorted (should be cheaper).
    for (const auto& row : hasPatternPredicateSortedByPSO->sortedView()) {
      internalTripleSorter.push(
          std::array{row[0], row[1], row[2], internalGraph});
    }
    hasPatternPredicateSortedByPSO->clear();
    return thirdSorter;
  }

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
