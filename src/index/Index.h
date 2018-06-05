// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <array>
#include <fstream>
#include <string>
#include <stxxl/vector>
#include <vector>
#include "../engine/ResultTable.h"
#include "../global/Pattern.h"
#include "../util/File.h"
#include "./DocsDB.h"
#include "./IndexMetaData.h"
#include "./StxxlSortFunctors.h"
#include "./TextMetaData.h"
#include "./ConstantsIndexCreation.h"
#include "./Vocabulary.h"

using std::array;
using std::string;
using std::tuple;
using std::vector;

class Index {
 public:
  typedef stxxl::VECTOR_GENERATOR<array<Id, 3>>::result ExtVec;
  // Block Id, Context Id, Word Id, Score, entity
  typedef stxxl::VECTOR_GENERATOR<tuple<Id, Id, Id, Score, bool>>::result
      TextVec;
  typedef std::tuple<Id, Id, Score> Posting;

  // Forbid copy and assignment
  Index& operator=(const Index&) = delete;

  Index(const Index&) = delete;

  Index();

  // Creates an index from a TSV file.
  // Will write vocabulary and on-disk index data.
  // Also ends up with fully functional in-memory metadata.
  void createFromTsvFile(const string& tsvFile, const string& onDiskBase,
                         bool allPermutations, bool onDiskLiterals = false);

  // Creates an index from a file in NTriples format.
  // Will write vocabulary and on-disk index data.
  // Also ends up with fully functional in-memory metadata.
  void createFromNTriplesFile(const string& ntFile, const string& onDiskBase,
                              bool allPermutations,
                              bool onDiskLiterals = false);

  // Creates an index object from an on disk index
  // that has previously been constructed.
  // Read necessary meta data into memory and opens file handles.
  void createFromOnDiskIndex(const string& onDiskBase,
                             bool allPermutations = false,
                             bool onDiskLiterals = false);

  // Adds a text index to a fully initialized KB index.
  // Reads a context file and builds the index for the first time.
  void addTextFromContextFile(const string& contextFile);

  void buildDocsDB(const string& docsFile);

  // Adds text index from on disk index that has previously been constructed.
  // Read necessary meta data into memory and opens file handles.
  void addTextFromOnDiskIndex();

  // Checks if the index is ready for use, i.e. it is properly intitialized.
  bool ready() const;

  const Vocabulary& getVocab() const { return _vocab; };

  const Vocabulary& getTextVocab() const { return _textVocab; };

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

  string idToString(Id id) const;

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
  const CompactStringVector<Id, Id>& getHasRelation() const;
  const CompactStringVector<size_t, Id>& getPatterns() const;

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
  bool _onDiskLiterals = false;
  Vocabulary _vocab;
  Vocabulary _textVocab;
  IndexMetaData _psoMeta;
  IndexMetaData _posMeta;
  IndexMetaData _spoMeta;
  IndexMetaData _sopMeta;
  IndexMetaData _ospMeta;
  IndexMetaData _opsMeta;
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
  bool _usePatterns;
  size_t _maxNumPatterns;
  CompactStringVector<size_t, Id> _patterns;
  std::vector<PatternID> _hasPattern;
  CompactStringVector<Id, Id> _hasRelation;

  size_t passTsvFileForVocabulary(const string& tsvFile,
                                  bool onDiskLiterals = false);

  void passTsvFileIntoIdVector(const string& tsvFile, ExtVec& data,
                               bool onDiskLiterals = false);

  // Create Vocabulary and directly write it to disk. Create ExtVec which can be
  // used for creating permutations
  // Member _vocab will be empty after this because it is not needed for index
  // creation once the ExtVec is set up and it would be a waste of RAM
  ExtVec createExtVecAndVocabFromNTriples(const string& ntFile, 
    					  const string& onDiskBase,
					  bool onDiskLiterals);

  // ___________________________________________________________________
  size_t passNTriplesFileForVocabulary(const string& ntFile,
                                       bool onDiskLiterals = false,
                                       size_t linesPerPartial = 100000000);

  void passNTriplesFileIntoIdVector(const string& ntFile, ExtVec& data,
                                    bool onDiskLiterals = false,
                                    size_t linesPerPartial = 100000000);

  size_t passContextFileForVocabulary(const string& contextFile);

  void passContextFileIntoVector(const string& contextFile, TextVec& vec);

  static void createPermutation(const string& fileName, const ExtVec& vec,
                                IndexMetaData& meta, size_t c0, size_t c1,
                                size_t c2);

  /**
   * @brief Creates the data required for the "pattern-trick" used for fast
   *        ql:has-relation evaluation when selection relation counts.
   * @param fileName The name of the file in which the data should be stored
   * @param vec The vectors of triples in spo order.
   */
  static void createPatterns(const string& fileName, const ExtVec& vec,
                             CompactStringVector<Id, Id>& hasRelation,
                             std::vector<PatternID>& hasPattern,
                             CompactStringVector<size_t, Id>& patterns,
                             size_t maxNumPatterns);

  void createTextIndex(const string& filename, const TextVec& vec);

  ContextListMetaData writePostings(ad_utility::File& out,
                                    const vector<Posting>& postings,
                                    bool skipWordlistIfAllTheSame);

  static pair<FullRelationMetaData, BlockBasedRelationMetaData> writeRel(
      ad_utility::File& out, off_t currentOffset, Id relId,
      const vector<array<Id, 2>>& data, bool functional);

  static void writeFunctionalRelation(
      const vector<array<Id, 2>>& data,
      pair<FullRelationMetaData, BlockBasedRelationMetaData>& rmd);

  static void writeNonFunctionalRelation(
      ad_utility::File& out, const vector<array<Id, 2>>& data,
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
};
