// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2014-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)
#pragma once

#include <engine/ResultTable.h>
#include <global/Pattern.h>
#include <index/CompressedRelation.h>
#include <index/ConstantsIndexBuilding.h>
#include <index/DocsDB.h>
#include <index/Index.h>
#include <index/IndexBuilderTypes.h>
#include <index/IndexMetaData.h>
#include <index/PatternCreator.h>
#include <index/Permutations.h>
#include <index/StxxlSortFunctors.h>
#include <index/TextMetaData.h>
#include <index/Vocabulary.h>
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
#include <util/Timer.h>
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

using ad_utility::BufferedVector;
using ad_utility::MmapVector;
using ad_utility::MmapVectorView;
using std::array;
using std::shared_ptr;
using std::string;
using std::tuple;
using std::vector;

using json = nlohmann::json;

template <typename Comparator>
using StxxlSorter =
    ad_utility::BackgroundStxxlSorter<std::array<Id, 3>, Comparator>;

using PsoSorter = StxxlSorter<SortByPSO>;

// Several data that are passed along between different phases of the
// index builder.
struct IndexBuilderDataBase {
  // The total number of distinct words in the complete Vocabulary
  size_t nofWords;
  // Id lower and upper bound of @lang@<predicate> predicates
  Id langPredLowerBound;
  Id langPredUpperBound;
};

// All the data from IndexBuilderDataBase and a stxxl::vector of (unsorted) ID
// triples.
struct IndexBuilderDataAsStxxlVector : IndexBuilderDataBase {
  using TripleVec = stxxl::vector<array<Id, 3>>;
  // All the triples as Ids.
  std::unique_ptr<TripleVec> idTriples;
  // The number of triples for each partial vocabulary. This also depends on the
  // number of additional language filter triples.
  std::vector<size_t> actualPartialSizes;
};

// All the data from IndexBuilderDataBase and a StxxlSorter that stores all ID
// triples sorted by the PSO permutation.
struct IndexBuilderDataAsPsoSorter : IndexBuilderDataBase {
  using SorterPtr = std::unique_ptr<StxxlSorter<SortByPSO>>;
  SorterPtr psoSorter;
  IndexBuilderDataAsPsoSorter(const IndexBuilderDataBase& base,
                              SorterPtr sorter)
      : IndexBuilderDataBase{base}, psoSorter{std::move(sorter)} {}
  IndexBuilderDataAsPsoSorter() = default;
};

class IndexImpl {
 public:
  using TripleVec = stxxl::vector<array<Id, 3>>;
  // Block Id, Context Id, Word Id, Score, entity
  using TextVec = stxxl::vector<
      tuple<TextBlockIndex, TextRecordIndex, WordOrEntityIndex, Score, bool>>;
  using Posting = std::tuple<TextRecordIndex, WordIndex, Score>;

  /// Forbid copy and assignment.
  IndexImpl& operator=(const IndexImpl&) = delete;
  IndexImpl(const IndexImpl&) = delete;

  /// Allow move construction, which is mostly used in unit tests.
  IndexImpl(IndexImpl&&) noexcept = default;

  IndexImpl();

  struct IndexMetaDataMmapDispatcher {
    using WriteType = IndexMetaDataMmap;
    using ReadType = IndexMetaDataMmapView;
  };

  struct IndexMetaDataHmapDispatcher {
    using WriteType = IndexMetaDataHmap;
    using ReadType = IndexMetaDataHmap;
  };

  template <class A, class B>
  using PermutationImpl = Permutation::PermutationImpl<A, B>;

  // TODO: make those private and allow only const access
  // instantiations for the six permutations used in QLever.
  // They simplify the creation of permutations in the index class.
  Permutation::POS_T _POS{SortByPOS(), "POS", ".pos", {1, 2, 0}};
  Permutation::PSO_T _PSO{SortByPSO(), "PSO", ".pso", {1, 0, 2}};
  Permutation::SOP_T _SOP{SortBySOP(), "SOP", ".sop", {0, 2, 1}};
  Permutation::SPO_T _SPO{SortBySPO(), "SPO", ".spo", {0, 1, 2}};
  Permutation::OPS_T _OPS{SortByOPS(), "OPS", ".ops", {2, 1, 0}};
  Permutation::OSP_T _OSP{SortByOSP(), "OSP", ".osp", {2, 0, 1}};

  const auto& POS() const { return _POS; }
  auto& POS() { return _POS; }
  const auto& PSO() const { return _PSO; }
  auto& PSO() { return _PSO; }
  const auto& SPO() const { return _SPO; }
  auto& SPO() { return _SPO; }
  const auto& SOP() const { return _SOP; }
  auto& SOP() { return _SOP; }
  const auto& OPS() const { return _OPS; }
  auto& OPS() { return _OPS; }
  const auto& OSP() const { return _OSP; }
  auto& OSP() { return _OSP; }

  // Creates an index from a file. Parameter Parser must be able to split the
  // file's format into triples.
  // Will write vocabulary and on-disk index data.
  // !! The index can not directly be used after this call, but has to be setup
  // by createFromOnDiskIndex after this call.
  template <class Parser>
  void createFromFile(const string& filename);

  void addPatternsToExistingIndex();

  // Creates an index object from an on disk index that has previously been
  // constructed. Read necessary meta data into memory and opens file handles.
  void createFromOnDiskIndex(const string& onDiskBase);

  // Adds a text index to a complete KB index. First reads the given context
  // file (if file name not empty), then adds words from literals (if true).
  void addTextFromContextFile(const string& contextFile,
                              bool addWordsFromLiterals);

  // Build docsDB file from given file (one text record per line).
  void buildDocsDB(const string& docsFile);

  // Adds text index from on disk index that has previously been constructed.
  // Read necessary meta data into memory and opens file handles.
  void addTextFromOnDiskIndex();

  const auto& getVocab() const { return _vocab; };
  auto& getNonConstVocabForTesting() { return _vocab; }

  const auto& getTextVocab() const { return _textVocab; };

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
  size_t relationCardinality(const string& relationName) const;

  size_t subjectCardinality(const TripleComponent& sub) const;

  size_t objectCardinality(const TripleComponent& obj) const;

  // TODO<joka921> Once we have an overview over the folding this logic should
  // probably not be in the index class.
  std::optional<string> idToOptionalString(Id id) const {
    switch (id.getDatatype()) {
      case Datatype::Undefined:
        return std::nullopt;
      case Datatype::Double:
        return std::to_string(id.getDouble());
      case Datatype::Int:
        return std::to_string(id.getInt());
      case Datatype::VocabIndex: {
        auto result = _vocab.indexToOptionalString(id.getVocabIndex());
        if (result.has_value() && result.value().starts_with(VALUE_PREFIX)) {
          result.value() =
              ad_utility::convertIndexWordToValueLiteral(result.value());
        }
        return result;
      }
      case Datatype::LocalVocabIndex:
        // TODO:: this is why this shouldn't be here
        return std::nullopt;
      case Datatype::TextRecordIndex:
        return getTextExcerpt(id.getTextRecordIndex());
    }
    // should be unreachable because the enum is exhaustive.
    AD_FAIL();
  }

  bool getId(const string& element, Id* id) const {
    // TODO<joka921> we should parse doubles correctly in the SparqlParser and
    // then return the correct ids here or somewhere else.
    VocabIndex vocabId;
    auto success = getVocab().getId(element, &vocabId);
    *id = Id::makeFromVocabIndex(vocabId);
    return success;
  }

  std::pair<Id, Id> prefix_range(const std::string& prefix) const {
    // TODO<joka921> Do we need prefix ranges for numbers?
    auto [begin, end] = _vocab.prefix_range(prefix);
    return {Id::makeFromVocabIndex(begin), Id::makeFromVocabIndex(end)};
  }

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

  void getContextEntityScoreListsForWords(const string& words,
                                          vector<TextRecordIndex>& cids,
                                          vector<Id>& eids,
                                          vector<Score>& scores) const;

  template <size_t I>
  void getECListForWordsAndSingleSub(const string& words,
                                     const vector<array<Id, I>>& subres,
                                     size_t subResMainCol, size_t limit,
                                     vector<array<Id, 3 + I>>& res) const;

  void getECListForWordsAndTwoW1Subs(const string& words,
                                     const vector<array<Id, 1>> subres1,
                                     const vector<array<Id, 1>> subres2,
                                     size_t limit,
                                     vector<array<Id, 5>>& res) const;

  void getECListForWordsAndSubtrees(
      const string& words,
      const vector<ad_utility::HashMap<Id, vector<vector<Id>>>>& subResVecs,
      size_t limit, vector<vector<Id>>& res) const;

  void getWordPostingsForTerm(const string& term, vector<TextRecordIndex>& cids,
                              vector<Score>& scores) const;

  void getEntityPostingsForTerm(const string& term,
                                vector<TextRecordIndex>& cids, vector<Id>& eids,
                                vector<Score>& scores) const;

  string getTextExcerpt(TextRecordIndex cid) const {
    if (cid.get() >= _docsDB._size) {
      return "";
    }
    return _docsDB.getTextExcerpt(cid);
  }

  // Only for debug reasons and external encoding tests.
  // Supply an empty vector to dump all lists above a size threshold.
  void dumpAsciiLists(const vector<string>& lists, bool decodeGapsFreq) const;

  void dumpAsciiLists(const TextBlockMetaData& tbmd) const;

  float getAverageNofEntityContexts() const {
    return _textMeta.getAverageNofEntityContexts();
  };

  void setKbName(const string& name);

  void setTextName(const string& name);

  void setUsePatterns(bool usePatterns);

  void setLoadAllPermutations(bool loadAllPermutations);

  void setKeepTempFiles(bool keepTempFiles);

  uint64_t& stxxlMemoryInBytes() { return _stxxlMemoryInBytes; }
  const uint64_t& stxxlMemoryInBytes() const { return _stxxlMemoryInBytes; }

  void setOnDiskBase(const std::string& onDiskBase);

  void setSettingsFile(const std::string& filename);

  void setPrefixCompression(bool compressed);

  void setNumTriplesPerBatch(uint64_t numTriplesPerBatch) {
    _numTriplesPerBatch = numTriplesPerBatch;
  }

  const string& getTextName() const { return _textMeta.getName(); }

  const string& getKbName() const { return _PSO.metaData().getName(); }

  size_t getNofTriples() const { return _PSO.metaData().getNofTriples(); }

  size_t getNofTextRecords() const { return _textMeta.getNofTextRecords(); }
  size_t getNofWordPostings() const { return _textMeta.getNofWordPostings(); }
  size_t getNofEntityPostings() const {
    return _textMeta.getNofEntityPostings();
  }

  size_t getNofSubjects() const {
    if (hasAllPermutations()) {
      return _SPO.metaData().getNofDistinctC1();
    } else {
      AD_THROW(ad_semsearch::Exception::CHECK_FAILED,
               "Can only get # distinct subjects if all 6 permutations "
               "have been registered on sever start (and index build time) "
               "with the -a option.")
    }
  }

  size_t getNofObjects() const {
    if (hasAllPermutations()) {
      return _OSP.metaData().getNofDistinctC1();
    } else {
      AD_THROW(ad_semsearch::Exception::CHECK_FAILED,
               "Can only get # distinct subjects if all 6 permutations "
               "have been registered on sever start (and index build time) "
               "with the -a option.")
    }
  }

  size_t getNofPredicates() const { return _PSO.metaData().getNofDistinctC1(); }

  bool hasAllPermutations() const { return SPO()._isLoaded; }

  // _____________________________________________________________________________
  template <class PermutationImpl>
  vector<float> getMultiplicities(const TripleComponent& key,
                                  const PermutationImpl& p) const {
    std::optional<Id> keyId = key.toValueId(getVocab());
    vector<float> res;
    if (keyId.has_value() && p._meta.col0IdExists(keyId.value())) {
      auto metaData = p._meta.getMetaData(keyId.value());
      res.push_back(metaData.getCol1Multiplicity());
      res.push_back(metaData.getCol2Multiplicity());
    } else {
      res.push_back(1);
      res.push_back(1);
    }
    return res;
  }

  // ___________________________________________________________________
  template <class PermutationImpl>
  vector<float> getMultiplicities(const PermutationImpl& p) const {
    std::array<float, 3> m{
        static_cast<float>(getNofTriples() / getNofSubjects()),
        static_cast<float>(getNofTriples() / getNofPredicates()),
        static_cast<float>(getNofTriples() / getNofObjects())};

    return {m[p._keyOrder[0]], m[p._keyOrder[1]], m[p._keyOrder[2]]};
  }

  /**
   * @brief Perform a scan for one key i.e. retrieve all YZ from the XYZ
   * permutation for a specific key value of X
   * @tparam Permutation The permutations IndexImpl::POS()... have different
   * types
   * @param key The key (in Id space) for which to search, e.g. fixed value for
   * O in OSP permutation.
   * @param result The Id table to which we will write. Must have 2 columns.
   * @param p The Permutation to use (in particularly POS(), SOP,... members of
   * IndexImpl class).
   */
  template <class Permutation>
  void scan(Id key, IdTable* result, const Permutation& p,
            ad_utility::SharedConcurrentTimeoutTimer timer = nullptr) const {
    CompressedRelationMetaData::scan(key, result, p, std::move(timer));
  }

  /**
   * @brief Perform a scan for one key i.e. retrieve all YZ from the XYZ
   * permutation for a specific key value of X
   * @tparam Permutation The permutations IndexImpl::POS()... have different
   * types
   * @param key The key (as a raw string that is yet to be transformed to index
   * space) for which to search, e.g. fixed value for O in OSP permutation.
   * @param result The Id table to which we will write. Must have 2 columns.
   * @param p The Permutation to use (in particularly POS(), SOP,... members of
   * IndexImpl class).
   */
  template <class Permutation>
  void scan(const TripleComponent& key, IdTable* result, const Permutation& p,
            ad_utility::SharedConcurrentTimeoutTimer timer = nullptr) const {
    LOG(DEBUG) << "Performing " << p._readableName
               << " scan for full list for: " << key << "\n";
    std::optional<Id> optionalId = key.toValueId(getVocab());
    if (optionalId.has_value()) {
      LOG(TRACE) << "Successfully got key ID.\n";
      scan(optionalId.value(), result, p, std::move(timer));
    }
    LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
  }

  /**
   * @brief Perform a scan for two keys i.e. retrieve all Z from the XYZ
   * permutation for specific key values of X and Y.
   * @tparam Permutation The permutations IndexImpl::POS()... have different
   * types
   * @param col0String The first key (as a raw string that is yet to be
   * transformed to index space) for which to search, e.g. fixed value for O in
   * OSP permutation.
   * @param col1String The second key (as a raw string that is yet to be
   * transformed to index space) for which to search, e.g. fixed value for S in
   * OSP permutation.
   * @param result The Id table to which we will write. Must have 2 columns.
   * @param p The Permutation to use (in particularly POS(), SOP,... members of
   * IndexImpl class).
   */
  // _____________________________________________________________________________
  template <class PermutationInfo>
  void scan(const TripleComponent& col0String,
            const TripleComponent& col1String, IdTable* result,
            const PermutationInfo& p,
            ad_utility::SharedConcurrentTimeoutTimer timer = nullptr) const {
    std::optional<Id> col0Id = col0String.toValueId(getVocab());
    std::optional<Id> col1Id = col1String.toValueId(getVocab());
    if (!col0Id.has_value() || !col1Id.has_value()) {
      LOG(DEBUG) << "Key " << col0String << " or key " << col1String
                 << " were not found in the vocabulary \n";
      return;
    }

    LOG(DEBUG) << "Performing " << p._readableName << "  scan of relation "
               << col0String << " with fixed subject: " << col1String
               << "...\n";

    CompressedRelationMetaData::scan(col0Id.value(), col1Id.value(), result, p,
                                     timer);
  }

  // Apply the function `F` to the permutation that corresponds to the
  // `permutation` argument.
  auto applyToPermutation(Index::Permutation permutation, const auto& F) {
    using enum Index::Permutation;
    switch (permutation) {
      case POS:
        return F(_POS);
      case PSO:
        return F(_PSO);
      case SPO:
        return F(_SPO);
      case SOP:
        return F(_SOP);
      case OSP:
        return F(_OSP);
      case OPS:
        return F(_OPS);
      default:
        AD_FAIL();
    }
  }

  // TODO<joka921> reduce code duplication here
  auto applyToPermutation(Index::Permutation permutation, const auto& F) const {
    using enum Index::Permutation;
    switch (permutation) {
      case POS:
        return F(_POS);
      case PSO:
        return F(_PSO);
      case SPO:
        return F(_SPO);
      case SOP:
        return F(_SOP);
      case OSP:
        return F(_OSP);
      case OPS:
        return F(_OPS);
      default:
        AD_FAIL();
    }
  }

 private:
  string _onDiskBase;
  string _settingsFileName;
  bool _onlyAsciiTurtlePrefixes = false;
  TurtleParserIntegerOverflowBehavior _turtleParserIntegerOverflowBehavior =
      TurtleParserIntegerOverflowBehavior::Error;
  bool _turtleParserSkipIllegalLiterals = false;
  bool _keepTempFiles = false;
  uint64_t _stxxlMemoryInBytes = DEFAULT_STXXL_MEMORY_IN_BYTES;
  json _configurationJson;
  Vocabulary<CompressedString, TripleComponentComparator> _vocab;
  size_t _totalVocabularySize = 0;
  bool _vocabPrefixCompressed = true;
  Vocabulary<std::string, SimpleStringComparator> _textVocab;

  TextMetaData _textMeta;
  DocsDB _docsDB;
  vector<WordIndex> _blockBoundaries;
  off_t _currentoff_t;
  mutable ad_utility::File _textIndexFile;

  // If false, only PSO and POS permutations are loaded and expected.
  bool _loadAllPermutations = true;

  // Pattern trick data
  bool _usePatterns;
  double _fullHasPredicateMultiplicityEntities;
  double _fullHasPredicateMultiplicityPredicates;
  size_t _fullHasPredicateSize;

  size_t _parserBatchSize = PARSER_BATCH_SIZE;
  size_t _numTriplesPerBatch = NUM_TRIPLES_PER_PARTIAL_VOCAB;
  /**
   * @brief Maps pattern ids to sets of predicate ids.
   */
  CompactVectorOfStrings<Id> _patterns;
  /**
   * @brief Maps entity ids to pattern ids.
   */
  std::vector<PatternID> _hasPattern;
  /**
   * @brief Maps entity ids to sets of predicate ids
   */
  CompactVectorOfStrings<Id> _hasPredicate;

  // Create Vocabulary and directly write it to disk. Create TripleVec with all
  // the triples converted to id space. This Vec can be used for creating
  // permutations. Member _vocab will be empty after this because it is not
  // needed for index creation once the TripleVec is set up and it would be a
  // waste of RAM.
  template <class Parser>
  IndexBuilderDataAsPsoSorter createIdTriplesAndVocab(const string& ntFile);

  // ___________________________________________________________________
  template <class Parser>
  IndexBuilderDataAsStxxlVector passFileForVocabulary(const string& ntFile,
                                                      size_t linesPerPartial);

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
      ad_utility::Synchronized<TripleVec::bufwriter_type>* globalWritePtr);

  std::unique_ptr<StxxlSorter<SortByPSO>> convertPartialToGlobalIds(
      TripleVec& data, const vector<size_t>& actualLinesPerPartial,
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

  template <class MetaDataDispatcher, typename SortedTriples>
  std::optional<std::pair<typename MetaDataDispatcher::WriteType,
                          typename MetaDataDispatcher::WriteType>>
  createPermutationPairImpl(const string& fileName1, const string& fileName2,
                            SortedTriples&& sortedTriples, size_t c0, size_t c1,
                            size_t c2, auto&&... perTripleCallbacks);

  void writeSwitchedRel(CompressedRelationWriter* out, Id currentRel,
                        ad_utility::BufferedVector<array<Id, 2>>* bufPtr);

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

  template <class MetaDataDispatcher, class Comparator1, class Comparator2>
  void createPermutationPair(
      auto&& sortedTriples,
      const PermutationImpl<Comparator1, typename MetaDataDispatcher::ReadType>&
          p1,
      const PermutationImpl<Comparator2, typename MetaDataDispatcher::ReadType>&
          p2,
      auto&&... perTripleCallbacks);

  // The pairs of permutations are PSO-POS, OSP-OPS and SPO-SOP
  // the multiplicity of column 1 in partner 1 of the pair is equal to the
  // multiplity of column 2 in partner 2
  // This functions writes the multiplicities of the first column of one
  // arguments to the 2nd column multiplicities of the other
  template <class MetaData>
  void exchangeMultiplicities(MetaData* m1, MetaData* m2);

  // wrapper for createPermutation that saves a lot of code duplications
  // Writes the permutation that is specified by argument permutation
  // performs std::unique on arg vec iff arg performUnique is true (normally
  // done for first permutation that is created using vec).
  // Will sort vec.
  // returns the MetaData (MmapBased or HmapBased) for this relation.
  // Careful: only multiplicities for first column is valid after call, need to
  // call exchangeMultiplicities as done by createPermutationPair
  // the optional is std::nullopt if vec and thus the index is empty
  template <class MetaDataDispatcher, class Comparator1, class Comparator2>
  std::optional<std::pair<typename MetaDataDispatcher::WriteType,
                          typename MetaDataDispatcher::WriteType>>
  createPermutations(
      auto&& sortedTriples,
      const PermutationImpl<Comparator1, typename MetaDataDispatcher::ReadType>&
          p1,
      const PermutationImpl<Comparator2, typename MetaDataDispatcher::ReadType>&
          p2,
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

  template <typename T, typename MakeFromUint64t = std::identity>
  void readGapComprList(
      size_t nofElements, off_t from, size_t nofBytes, vector<T>& result,
      MakeFromUint64t makeFromUint64t = MakeFromUint64t{}) const;

  template <typename T, typename MakeFromUint64t = std::identity>
  void readFreqComprList(
      size_t nofElements, off_t from, size_t nofBytes, vector<T>& result,
      MakeFromUint64t makeFromUint = MakeFromUint64t{}) const;

  size_t getIndexOfBestSuitedElTerm(const vector<string>& terms) const;

  /// Calculate the block boundaries for the text index. The boundary of a
  /// block is the index in the `_textVocab` of the last word that belongs
  /// to this block.
  /// This implementation takes a reference to an `IndexImpl` and a callable,
  /// that is called once for each blockBoundary, with the `size_t`
  /// blockBoundary as a parameter. Internally uses
  /// `calculateBlockBoundariesImpl`.
  template <typename I, typename BlockBoundaryAction>
  static void calculateBlockBoundariesImpl(
      I&& index, const BlockBoundaryAction& blockBoundaryAction);

  /// Calculate the block boundaries for the text index, and store them in the
  /// _blockBoundaries member.
  void calculateBlockBoundaries();

  /// Calculate the block boundaries for the text index, and store the
  /// corresponding words in a human-readable text file at `filename`.
  /// This is for debugging the text index. Internally uses
  /// `caluclateBlockBoundariesImpl`.
  void printBlockBoundariesToFile(const string& filename) const;

  TextBlockIndex getWordBlockId(WordIndex wordIndex) const;

  TextBlockIndex getEntityBlockId(Id entityId) const;

  bool isEntityBlockId(TextBlockIndex blockIndex) const;

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

  void getRhsForSingleLhs(const IdTable& in, Id lhsId, IdTable* result) const;

  bool isLiteral(const string& object) const;

  bool shouldBeExternalized(const string& object);
  // convert value literals to internal representation
  // and add externalization characters if necessary.
  // Returns the language tag of spo[2] (the object) or ""
  // if there is none.
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
  template <class Parser>
  void readIndexBuilderSettingsFromFile();

  // Helper function for Debugging during the index build.
  // ExtVecs are not persistent, so we dump them to a mmapVector in a file with
  // given filename
  static void dumpExtVecToMmap(const TripleVec& vec, std::string filename) {
    LOG(INFO) << "Dumping ext vec to mmap" << std::endl;
    MmapVector<TripleVec::value_type> mmapVec(vec.size(), filename);
    for (size_t i = 0; i < vec.size(); ++i) {
      mmapVec[i] = vec[i];
    }
    LOG(INFO) << "Done" << std::endl;
  }

  /**
   * Delete a temporary file unless the _keepTempFiles flag is set
   * @param path
   */
  void deleteTemporaryFile(const string& path) {
    if (!_keepTempFiles) {
      ad_utility::deleteFile(path);
    }
  }

 public:
  // Count the number of "QLever-internal" triples (predicate ql:langtag or
  // predicate starts with @) and all other triples (that were actually part of
  // the input).
  std::pair<size_t, size_t> getNumTriplesActuallyAndAdded() const {
    auto [begin, end] = prefix_range("@");
    Id qleverLangtag;
    auto actualTriples = 0ul;
    auto addedTriples = 0ul;
    bool foundQleverLangtag = getId(LANGUAGE_PREDICATE, &qleverLangtag);
    AD_CHECK(foundQleverLangtag);
    // Use the PSO index to get the number of triples for each predicate and add
    // to the respective counter.
    for (const auto& [key, value] : PSO()._meta.data()) {
      auto numTriples = value.getNofElements();
      if (key == qleverLangtag || (begin <= key && key < end)) {
        addedTriples += numTriples;
      } else {
        actualTriples += numTriples;
      }
    }
    return std::pair{actualTriples, addedTriples};
  }
};
