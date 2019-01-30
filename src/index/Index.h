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

  // Creates an index from a file. Parameter Parser must be able to split the
  // file's format into triples.
  // Will write vocabulary and on-disk index data.
  // !! The index can not directly be used after this call, but has to be setup
  // by createFromOnDiskIndex after this call.
  template <class Parser>
  void createFromFile(const string& filename, bool allPermutations);

  void addPatternsToExistingIndex();

  // Creates an index object from an on disk index
  // that has previously been constructed.
  // Read necessary meta data into memory and opens file handles.
  void createFromOnDiskIndex(const string& onDiskBase,
                             bool allPermutations = false);

  // Adds a text index to a fully initialized KB index.
  // Reads a context file and builds the index for the first time.
  void addTextFromContextFile(const string& contextFile);

  void buildDocsDB(const string& docsFile);

  // Adds text index from on disk index that has previously been constructed.
  // Read necessary meta data into memory and opens file handles.
  void addTextFromOnDiskIndex();

  // Checks if the index is ready for use, i.e. it is properly intitialized.
  bool ready() const;

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

  void scanPSO(const string& predicate, WidthTwoList* result) const;

  void scanPSO(const string& predicate, const string& subject,
               WidthOneList* result) const;

  void scanPOS(const string& predicate, WidthTwoList* result) const;

  void scanPOS(const string& predicate, const string& object,
               WidthOneList* result) const;

  void scanSOP(const string& subject, const string& object,
               WidthOneList* result) const;

  void scanSPO(const string& subject, WidthTwoList* result) const;

  void scanSOP(const string& subject, WidthTwoList* result) const;

  void scanOPS(const string& object, WidthTwoList* result) const;

  void scanOSP(const string& object, WidthTwoList* result) const;

  void scanPSO(Id predicate, WidthTwoList* result) const;
  void scanPOS(Id predicate, WidthTwoList* result) const;
  void scanSPO(Id subject, WidthTwoList* result) const;
  void scanSOP(Id subject, WidthTwoList* result) const;
  void scanOPS(Id object, WidthTwoList* result) const;
  void scanOSP(Id object, WidthTwoList* result) const;

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

  // Get multiplicities with given var (SCAN for 2 cols)
  vector<float> getPSOMultiplicities(const string& key) const;
  vector<float> getPOSMultiplicities(const string& key) const;
  vector<float> getSPOMultiplicities(const string& key) const;
  vector<float> getSOPMultiplicities(const string& key) const;
  vector<float> getOSPMultiplicities(const string& key) const;
  vector<float> getOPSMultiplicities(const string& key) const;

  // Get multiplicities for full scans (dummy)
  vector<float> getPSOMultiplicities() const;
  vector<float> getPOSMultiplicities() const;
  vector<float> getSPOMultiplicities() const;
  vector<float> getSOPMultiplicities() const;
  vector<float> getOSPMultiplicities() const;
  vector<float> getOPSMultiplicities() const;

  // --------------------------------------------------------------------------
  // TEXT RETRIEVAL
  // --------------------------------------------------------------------------
  const string& wordIdToString(Id id) const;

  size_t getSizeEstimate(const string& words) const;

  void getContextListForWords(const string& words, WidthTwoList* result) const;

  void getECListForWords(const string& words, size_t limit,
                         WidthThreeList& result) const;

  // With two or more variables.
  template <typename ResultList>
  void getECListForWords(const string& words, size_t nofVars, size_t limit,
                         ResultList& result) const;

  // With filtering. Needs many template instantiations but
  // only nofVars truly makes a difference. Others are just data types
  // of result tables.
  template <typename FilterTable, typename ResultList>
  void getFilteredECListForWords(const string& words, const FilterTable& filter,
                                 size_t filterColumn, size_t nofVars,
                                 size_t limit, ResultList& result) const;

  // Special cast with a width-one filter.
  template <typename ResultList>
  void getFilteredECListForWords(const string& words,
                                 const WidthOneList& filter, size_t nofVars,
                                 size_t limit, ResultList& result) const;

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

  const string& getKbName() const { return _psoMeta.getName(); }

  size_t getNofTriples() const { return _psoMeta.getNofTriples(); }

  size_t getNofTextRecords() const { return _textMeta.getNofTextRecords(); }
  size_t getNofWordPostings() const { return _textMeta.getNofWordPostings(); }
  size_t getNofEntityPostings() const {
    return _textMeta.getNofEntityPostings();
  }

  size_t getNofSubjects() const {
    if (hasAllPermutations()) {
      return _spoMeta.getNofDistinctC1();
    } else {
      AD_THROW(ad_semsearch::Exception::CHECK_FAILED,
               "Can only get # distinct subjects if all 6 permutations "
               "have been registered on sever start (and index build time) "
               "with the -a option.")
    }
  }

  size_t getNofObjects() const {
    if (hasAllPermutations()) {
      return _ospMeta.getNofDistinctC1();
    } else {
      AD_THROW(ad_semsearch::Exception::CHECK_FAILED,
               "Can only get # distinct subjects if all 6 permutations "
               "have been registered on sever start (and index build time) "
               "with the -a option.")
    }
  }

  size_t getNofPredicates() const { return _psoMeta.getNofDistinctC1(); }

  bool hasAllPermutations() const { return _spoFile.isOpen(); }

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

  IndexMetaDataHmap _psoMeta;
  IndexMetaDataHmap _posMeta;
  IndexMetaDataMmapView _spoMeta;
  IndexMetaDataMmapView _sopMeta;
  IndexMetaDataMmapView _ospMeta;
  IndexMetaDataMmapView _opsMeta;
  TextMetaData _textMeta;
  DocsDB _docsDB;
  vector<Id> _blockBoundaries;
  off_t _currentoff_t;
  mutable ad_utility::File _psoFile;
  mutable ad_utility::File _posFile;
  mutable ad_utility::File _spoFile;
  mutable ad_utility::File _sopFile;
  mutable ad_utility::File _ospFile;
  mutable ad_utility::File _opsFile;
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
  template <class MetaData>
  std::optional<MetaData> createPermutationImpl(const string& fileName,
                                                const TripleVec& vec, size_t c0,
                                                size_t c1, size_t c2);
  template <class MetaData, class Comparator1, class Comparator2>

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
      VocabularyData* vec, const Permutation::PermutationImpl<Comparator1>& p1,
      const Permutation::PermutationImpl<Comparator2>& p2,
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
  template <class MetaData, class Comparator>
  std::optional<MetaData> createPermutation(
      TripleVec* vec,
      const Permutation::PermutationImpl<Comparator>& permutation,
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

  void openFileHandles();

  void openTextFileHandle();

  void scanFunctionalRelation(const pair<off_t, size_t>& blockOff, Id lhsId,
                              ad_utility::File& indexFile,
                              WidthOneList* result) const;

  void scanNonFunctionalRelation(const pair<off_t, size_t>& blockOff,
                                 const pair<off_t, size_t>& followBlock,
                                 Id lhsId, ad_utility::File& indexFile,
                                 off_t upperBound, WidthOneList* result) const;

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

  void getRhsForSingleLhs(const WidthTwoList& in, Id lhsId,
                          WidthOneList* result) const;

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
};
