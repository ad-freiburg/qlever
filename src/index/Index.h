// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <array>
#include <fstream>
#include <google/sparse_hash_set>
#include <memory>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <stxxl/vector>
#include <vector>
#include "../engine/ResultTable.h"
#include "../global/Pattern.h"
#include "../parser/NTriplesParser.h"
#include "../parser/TsvParser.h"
#include "../parser/TurtleParser.h"
#include "../util/BufferedVector.h"
#include "../util/File.h"
#include "../util/HashMap.h"
#include "../util/MmapVector.h"
#include "./ConstantsIndexCreation.h"
#include "./DocsDB.h"
#include "./IndexMetaData.h"
#include "./Permutations.h"
#include "./StxxlSortFunctors.h"
#include "./TextMetaData.h"
#include "./Vocabulary.h"

using ad_utility::BufferedVector;
using ad_utility::MmapVector;
using ad_utility::MmapVectorView;
using std::array;
using std::string;
using std::tuple;
using std::vector;

using json = nlohmann::json;

// a simple struct for better naming
struct VocabularyData {
  using TripleVec = stxxl::vector<array<Id, 3>>;
  // The total number of distinct words in the complete Vocabulary
  size_t nofWords;
  // Id lower and upper bound of @lang@<predicate> predicates
  Id langPredLowerBound;
  Id langPredUpperBound;
  // The number of triples in the idTriples vec that each partial vocabulary is
  // responsible for (depends on the number of additional language filter
  // triples)
  std::vector<size_t> actualPartialSizes;
  // All the triples as Ids.
  std::unique_ptr<TripleVec> idTriples;
};

class Index {
 public:
  using TripleVec = stxxl::vector<array<Id, 3>>;
  // Block Id, Context Id, Word Id, Score, entity
  using TextVec = stxxl::vector<tuple<Id, Id, Id, Score, bool>>;
  using Posting = std::tuple<Id, Id, Score>;

  // Forbid copy and assignment
  Index& operator=(const Index&) = delete;

  Index(const Index&) = delete;

  Index();

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
  // instantiations for the 6 Permutations used in QLever
  // They simplify the creation of permutations in the index class
  PermutationImpl<SortByPOS, IndexMetaDataHmap> _POS =
      Permutation::PermutationImpl<SortByPOS, IndexMetaDataHmap>(
          SortByPOS(), "POS", ".pos", {1, 2, 0});
  PermutationImpl<SortByPSO, IndexMetaDataHmap> _PSO =
      Permutation::PermutationImpl<SortByPSO, IndexMetaDataHmap>(
          SortByPSO(), "PSO", ".pso", {1, 0, 2});
  PermutationImpl<SortBySOP, IndexMetaDataMmapView> _SOP =
      Permutation::PermutationImpl<SortBySOP, IndexMetaDataMmapView>(
          SortBySOP(), "SOP", ".sop", {0, 2, 1});
  PermutationImpl<SortBySPO, IndexMetaDataMmapView> _SPO =
      Permutation::PermutationImpl<SortBySPO, IndexMetaDataMmapView>(
          SortBySPO(), "SPO", ".spo", {0, 1, 2});
  PermutationImpl<SortByOPS, IndexMetaDataMmapView> _OPS =
      Permutation::PermutationImpl<SortByOPS, IndexMetaDataMmapView>(
          SortByOPS(), "OPS", ".ops", {2, 1, 0});
  PermutationImpl<SortByOSP, IndexMetaDataMmapView> _OSP =
      Permutation::PermutationImpl<SortByOSP, IndexMetaDataMmapView>(
          SortByOSP(), "OSP", ".osp", {2, 0, 1});

  const auto& POS() const { return _POS; }
  const auto& PSO() const { return _PSO; }
  const auto& SPO() const { return _SPO; }
  const auto& SOP() const { return _SOP; }
  const auto& OPS() const { return _OPS; }
  const auto& OSP() const { return _OSP; }

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

  const auto& getVocab() const { return _vocab; };

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

  size_t subjectCardinality(const string& sub) const;

  size_t objectCardinality(const string& obj) const;

  size_t sizeEstimate(const string& sub, const string& pred,
                      const string& obj) const;

  std::optional<string> idToOptionalString(Id id) const {
    return _vocab.idToOptionalString(id);
  }

  const vector<PatternID>& getHasPattern() const;
  const CompactStringVector<Id, Id>& getHasPredicate() const;
  const CompactStringVector<size_t, Id>& getPatterns() const;
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
                                     const vector<array<Id, 1>> subres1,
                                     const vector<array<Id, 1>> subres2,
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

  string getTextExcerpt(Id cid) const {
    if (cid == ID_NO_VALUE) {
      return std::string();
    } else {
      return _docsDB.getTextExcerpt(cid);
    }
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

  void setOnDiskLiterals(bool onDiskLiterals);

  void setKeepTempFiles(bool keepTempFiles);

  void setOnDiskBase(const std::string& onDiskBase);

  void setSettingsFile(const std::string& filename);

  void setPrefixCompression(bool compressed);

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

  bool hasAllPermutations() const { return SPO()._file.isOpen(); }

  // _____________________________________________________________________________
  template <class PermutationImpl>
  vector<float> getMultiplicities(const string& key,
                                  const PermutationImpl& p) const {
    Id keyId;
    vector<float> res;
    if (_vocab.getId(key, &keyId) && p._meta.relationExists(keyId)) {
      auto rmd = p._meta.getRmd(keyId);
      auto logM1 = rmd.getCol1LogMultiplicity();
      res.push_back(static_cast<float>(pow(2, logM1)));
      auto logM2 = rmd.getCol2LogMultiplicity();
      res.push_back(static_cast<float>(pow(2, logM2)));
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
   * @tparam Permutation The permutations Index::POS()... have different types
   * @param key The key (in Id space) for which to search, e.g. fixed value for
   * O in OSP permutation.
   * @param result The Id table to which we will write. Must have 2 columns.
   * @param p The Permutation to use (in particularly POS(), SOP,... members of
   * Index class).
   */
  template <class Permutation>
  void scan(Id key, IdTable* result, const Permutation& p) const {
    if (p._meta.relationExists(key)) {
      const FullRelationMetaData& rmd = p._meta.getRmd(key)._rmdPairs;
      result->reserve(rmd.getNofElements() + 2);
      result->resize(rmd.getNofElements());
      p._file.read(result->data(), rmd.getNofElements() * 2 * sizeof(Id),
                   rmd._startFullIndex);
    }
  }

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
  template <class Permutation>
  void scan(const string& key, IdTable* result, const Permutation& p) const {
    LOG(DEBUG) << "Performing " << p._readableName
               << " scan for full list for: " << key << "\n";
    Id relId;
    if (_vocab.getId(key, &relId)) {
      LOG(TRACE) << "Successfully got key ID.\n";
      scan(relId, result, p);
    }
    LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
  }

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
  template <class PermutationInfo>
  void scan(const string& keyFirst, const string& keySecond, IdTable* result,
            const PermutationInfo& p) const {
    LOG(DEBUG) << "Performing " << p._readableName << "  scan of relation "
               << keyFirst << " with fixed subject: " << keySecond << "...\n";
    Id relId;
    Id subjId;
    if (_vocab.getId(keyFirst, &relId) && _vocab.getId(keySecond, &subjId)) {
      if (p._meta.relationExists(relId)) {
        auto rmd = p._meta.getRmd(relId);
        if (rmd.hasBlocks()) {
          pair<off_t, size_t> blockOff =
              rmd._rmdBlocks->getBlockStartAndNofBytesForLhs(subjId);
          // Functional relations have blocks point into the pair index,
          // non-functional relations have them point into lhs lists
          if (rmd.isFunctional()) {
            scanFunctionalRelation(blockOff, subjId, p._file, result);
          } else {
            pair<off_t, size_t> block2 =
                rmd._rmdBlocks->getFollowBlockForLhs(subjId);
            scanNonFunctionalRelation(blockOff, block2, subjId, p._file,
                                      rmd._rmdBlocks->_offsetAfter, result);
          }
        } else {
          // If we don't have blocks, scan the whole relation and filter /
          // restrict.
          IdTable fullRelation(2);
          fullRelation.resize(rmd.getNofElements());
          p._file.read(fullRelation.data(),
                       rmd.getNofElements() * 2 * sizeof(Id),
                       rmd._rmdPairs._startFullIndex);
          getRhsForSingleLhs(fullRelation, subjId, result);
        }
      } else {
        LOG(DEBUG) << "No such relation.\n";
      }
    } else {
      LOG(DEBUG) << "No such second order key.\n";
    }
    LOG(DEBUG) << "Scan done, got " << result->size() << " elements.\n";
  }

 private:
  string _onDiskBase;
  string _settingsFileName;
  bool _onDiskLiterals = false;
  bool _keepTempFiles = false;
  json _configurationJson;
  Vocabulary<CompressedString> _vocab;
  size_t _totalVocabularySize = 0;
  bool _vocabPrefixCompressed = true;
  Vocabulary<std::string> _textVocab;

  TextMetaData _textMeta;
  DocsDB _docsDB;
  vector<Id> _blockBoundaries;
  off_t _currentoff_t;
  mutable ad_utility::File _textIndexFile;

  // Pattern trick data
  static const uint32_t PATTERNS_FILE_VERSION;
  bool _usePatterns;
  size_t _maxNumPatterns;
  double _fullHasPredicateMultiplicityEntities;
  double _fullHasPredicateMultiplicityPredicates;
  size_t _fullHasPredicateSize;
  /**
   * @brief Maps pattern ids to sets of predicate ids.
   */
  CompactStringVector<size_t, Id> _patterns;
  /**
   * @brief Maps entity ids to pattern ids.
   */
  std::vector<PatternID> _hasPattern;
  /**
   * @brief Maps entity ids to sets of predicate ids
   */
  CompactStringVector<Id, Id> _hasPredicate;

  // Create Vocabulary and directly write it to disk. Create TripleVec with all
  // the triples converted to id space. This Vec can be used for creating
  // permutations. Member _vocab will be empty after this because it is not
  // needed for index creation once the TripleVec is set up and it would be a
  // waste of RAM.
  template <class Parser>
  VocabularyData createIdTriplesAndVocab(const string& ntFile);

  // ___________________________________________________________________
  template <class Parser>
  VocabularyData passFileForVocabulary(const string& ntFile,
                                       size_t linesPerPartial = 100000000);

  void convertPartialToGlobalIds(TripleVec& data,
                                 const vector<size_t>& actualLinesPerPartial,
                                 size_t linesPerPartial);

  // ___________________________________________________________________________
  template <class Map>
  static Id assignNextId(Map* mapPtr, const string& key);

  size_t passContextFileForVocabulary(const string& contextFile);

  void passContextFileIntoVector(const string& contextFile, TextVec& vec);

  // no need for explicit instatiation since this function is private
  template <class MetaDataDispatcher>
  std::optional<typename MetaDataDispatcher::WriteType> createPermutationImpl(
      const string& fileName, const TripleVec& vec, size_t c0, size_t c1,
      size_t c2);
  template <class MetaDataDispatcher, class Comparator1, class Comparator2>

  // _______________________________________________________________________
  // Create a pair of permutations. Only works for valid pairs (PSO-POS,
  // OSP-OPS, SPO-SOP).  First creates the permutation and then exchanges the
  // multiplicities and also writes the MetaData to disk. So we end up with
  // fully functional permutations.
  // performUnique must be set for the first pair created using vec to enforce
  // RDF standard (no duplicate triples).
  // createPatternsAfterFirst is only valid when  the pair is SPO-SOP because
  // the SPO permutation is also needed for patterns (see usage in
  // Index::createFromFile function)
  void createPermutationPair(
      VocabularyData* vec,
      const PermutationImpl<Comparator1, typename MetaDataDispatcher::ReadType>&
          p1,
      const PermutationImpl<Comparator2, typename MetaDataDispatcher::ReadType>&
          p2,
      bool performUnique = false, bool createPatternsAfterFirst = false);

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
  template <class MetaDataDispatcher, class Comparator>
  std::optional<typename MetaDataDispatcher::WriteType> createPermutation(
      TripleVec* vec,
      const PermutationImpl<Comparator, typename MetaDataDispatcher::ReadType>&
          permutation,
      bool performUnique = false);

  /**
   * @brief Creates the data required for the "pattern-trick" used for fast
   *        ql:has-relation evaluation when selection relation counts.
   * @param fileName The name of the file in which the data should be stored
   * @param args The arguments that need to be passed to the constructor of
   *             VecReaderType. VecReaderType should allow for iterating over
   *             the tuples of the spo permutation after having been constructed
   *             using args.
   */
  template <typename VecReaderType, typename... Args>
  void createPatternsImpl(const string& fileName,
                          CompactStringVector<Id, Id>& hasPredicate,
                          std::vector<PatternID>& hasPattern,
                          CompactStringVector<size_t, Id>& patterns,
                          double& fullHasPredicateMultiplicityEntities,
                          double& fullHasPredicateMultiplicityPredicates,
                          size_t& fullHasPredicateSize,
                          const size_t maxNumPatterns,
                          const Id langPredLowerBound,
                          const Id langPredUpperBound,
                          const Args&... vecReaderArgs);

  // wrap the static function using the internal member variables
  // the bool indicates wether the TripleVec has to be sorted before the pattern
  // creation
  void createPatterns(bool vecAlreadySorted, VocabularyData* idTriples);

  void createTextIndex(const string& filename, const TextVec& vec);

  ContextListMetaData writePostings(ad_utility::File& out,
                                    const vector<Posting>& postings,
                                    bool skipWordlistIfAllTheSame);

  // Add relation to permutation file. Calculate corresponding metaData
  // (Mutliplicity of second column will be invalid and has to be set by a
  // separate call to exchangeMultiplicities)
  // Args:
  //   out - permutation file to which we write the relation. Must be open.
  //   currentOffset - the offset of this relation within the permutation file
  //   relId - the Id of the 0-th column of this relation (e.g. the 'P' in PSO)
  //   data - the 1st and 2nd column of this relation (e.g. the "SO" for a fixed
  //          'P' in PSO. Must be sorted by 1. and then 2. column.
  //   distinctC1 - the number of distinct elemens in 1. column of data ("S" in
  //                PSO)
  //   functional - is this relation functional (only one triple per value for
  //                1. column)
  // Returns:
  //   The Meta Data (Permutation offsets) for this relation,
  //   Careful: only multiplicity for first column is valid in return value
  static pair<FullRelationMetaData, BlockBasedRelationMetaData> writeRel(
      ad_utility::File& out, off_t currentOffset, Id relId,
      const BufferedVector<array<Id, 2>>& data, size_t distinctC1,
      bool functional);

  static void writeFunctionalRelation(
      const BufferedVector<array<Id, 2>>& data,
      pair<FullRelationMetaData, BlockBasedRelationMetaData>& rmd);

  static void writeNonFunctionalRelation(
      ad_utility::File& out, const BufferedVector<array<Id, 2>>& data,
      pair<FullRelationMetaData, BlockBasedRelationMetaData>& rmd);

  void openTextFileHandle();

  void scanFunctionalRelation(const pair<off_t, size_t>& blockOff, Id lhsId,
                              ad_utility::File& indexFile,
                              IdTable* result) const;

  void scanNonFunctionalRelation(const pair<off_t, size_t>& blockOff,
                                 const pair<off_t, size_t>& followBlock,
                                 Id lhsId, ad_utility::File& indexFile,
                                 off_t upperBound, IdTable* result) const;

  void addContextToVector(TextVec::bufwriter_type& writer, Id context,
                          const ad_utility::HashMap<Id, Score>& words,
                          const ad_utility::HashMap<Id, Score>& entities);

  template <typename T>
  void readGapComprList(size_t nofElements, off_t from, size_t nofBytes,
                        vector<T>& result) const;

  template <typename T>
  void readFreqComprList(size_t nofElements, off_t from, size_t nofBytes,
                         vector<T>& result) const;

  size_t getIndexOfBestSuitedElTerm(const vector<string>& terms) const;

  void calculateBlockBoundaries();

  Id getWordBlockId(Id wordId) const;

  Id getEntityBlockId(Id entityId) const;

  bool isEntityBlockId(Id blockId) const;

  //! Writes a list of elements (have to be able to be cast to unit64_t)
  //! to file.
  //! Returns the number of bytes written.
  template <class Numeric>
  size_t writeList(Numeric* data, size_t nofElements,
                   ad_utility::File& file) const;

  typedef ad_utility::HashMap<Id, Id> IdCodeMap;
  typedef ad_utility::HashMap<Score, Score> ScoreCodeMap;
  typedef vector<Id> IdCodebook;
  typedef vector<Score> ScoreCodebook;

  //! Creates codebooks for lists that are supposed to be entropy encoded.
  void createCodebooks(const vector<Posting>& postings, IdCodeMap& wordCodemap,
                       IdCodebook& wordCodebook, ScoreCodeMap& scoreCodemap,
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

  bool isLiteral(const string& object);

  bool shouldBeExternalized(const string& object);
  // convert value literals to internal representation
  // and add externalization characters if necessary.
  // Returns the language tag of spo[2] (the object) or ""
  // if there is none.
  string tripleToInternalRepresentation(array<string, 3>* spo);

  /**
   * @brief Throws an exception if no patterns are loaded. Should be called from
   *        whithin any index method that returns data requiring the patterns
   *        file.
   */
  void throwExceptionIfNoPatterns() const;

  void writeConfiguration() const;
  void readConfiguration();

  // initialize the index-build-time settings for the vocabulary
  void initializeVocabularySettingsBuild();

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
};
