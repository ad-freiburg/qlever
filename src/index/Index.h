// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>
#include <array>
#include <fstream>
#include <vector>
#include <stxxl/vector>
#include "./Vocabulary.h"
#include "./IndexMetaData.h"
#include "./StxxlSortFunctors.h"
#include "../util/File.h"
#include "./TextMetaData.h"
#include "./DocsDB.h"


using std::string;
using std::array;
using std::vector;
using std::tuple;

class Index {
public:
  typedef stxxl::VECTOR_GENERATOR<array<Id, 3>>::result ExtVec;
  // Block Id, Context Id, Word Id, Score, entity
  typedef stxxl::VECTOR_GENERATOR<tuple<Id, Id, Id, Score, bool>>::result TextVec;
  typedef std::tuple<Id, Id, Score> Posting;


  // Forbid copy and assignment
  Index& operator=(const Index&) = delete;

  Index(const Index&) = delete;

  Index() = default;

  // Creates an index from a TSV file.
  // Will write vocabulary and on-disk index data.
  // Also ends up with fully functional in-memory metadata.
  void createFromTsvFile(const string& tsvFile, const string& onDiskBase);

  // Creates an index from a file in NTriples format.
  // Will write vocabulary and on-disk index data.
  // Also ends up with fully functional in-memory metadata.
  void createFromNTriplesFile(const string& ntFile, const string& onDiskBase);

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

  // Checks if the index is ready for use, i.e. it is properly intitialized.
  bool ready() const;

  // --------------------------------------------------------------------------
  //  -- RETRIEVAL ---
  // --------------------------------------------------------------------------
  typedef vector<array<Id, 1>> WidthOneList;
  typedef vector<array<Id, 2>> WidthTwoList;
  typedef vector<array<Id, 3>> WidthThreeList;


  // --------------------------------------------------------------------------
  // RDF RETRIEVAL
  // --------------------------------------------------------------------------
  size_t relationCardinality(const string& relationName) const;

  const string& idToString(Id id) const;

  void scanPSO(const string& predicate, WidthTwoList *result) const;

  void scanPSO(const string& predicate, const string& subject, WidthOneList *
  result) const;

  void scanPOS(const string& predicate, WidthTwoList *result) const;

  void scanPOS(const string& predicate, const string& object, WidthOneList *
  result) const;

  // --------------------------------------------------------------------------
  // TEXT RETRIEVAL
  // --------------------------------------------------------------------------
  const string& wordIdToString(Id id) const;

  void getContextListForWords(const string& words, WidthTwoList *result) const;

  void getECListForWords(const string& words, WidthThreeList *result) const;

  void getContextEntityScoreListsForWords(const string& words,
                                          vector<Id>& cids,
                                          vector<Id>& eids,
                                          vector<Score>& scores) const;

  template<size_t I>
  void getECListForWordsAndSingleSub(const string& words,
                                     const vector<array<Id, I>> subres,
                                     size_t subResMainCol,
                                     vector<array<Id, 3 + I>>& res) const;

  void getECListForWordsAndTwoW1Subs(const string& words,
                                     const vector<array<Id, 1>> subres1,
                                     const vector<array<Id, 1>> subres2,
                                     vector<array<Id, 5>>& res) const;

  void getECListForWordsAndSubtrees(
      const string& words,
      const vector<unordered_map<Id, vector<vector<Id>>>>& subResVecs,
      vector<vector<Id>>& res) const;

  void getWordPostingsForTerm(const string& term, vector<Id>& cids,
                              vector<Score>& scores) const;

  void getEntityPostingsForTerm(const string& term, vector<Id>& cids,
                                vector<Id>& eids, vector<Score>& scores) const;

  string getTextExcerpt(Id cid) const {
    return _docsDB.getTextExcerpt(cid);
  }

private:
  string _onDiskBase;
  Vocabulary _vocab;
  Vocabulary _textVocab;
  IndexMetaData _psoMeta;
  IndexMetaData _posMeta;
  TextMetaData _textMeta;
  DocsDB _docsDB;
  vector<Id> _blockBoundaries;
  off_t _currentoff_t;
  mutable ad_utility::File _psoFile;
  mutable ad_utility::File _posFile;
  mutable ad_utility::File _textIndexFile;

  size_t passTsvFileForVocabulary(const string& tsvFile);

  void passTsvFileIntoIdVector(const string& tsvFile, ExtVec& data);

  size_t passNTriplesFileForVocabulary(const string& tsvFile);

  void passNTriplesFileIntoIdVector(const string& tsvFile, ExtVec& data);

  size_t passContextFileForVocabulary(const string& contextFile);

  void passContextFileIntoVector(const string& contextFile, TextVec& vec);

  static void createPermutation(const string& fileName,
                                const ExtVec& vec,
                                IndexMetaData& meta,
                                size_t c1, size_t c2);

  void createTextIndex(const string& filename, const TextVec& vec);

  ContextListMetaData writePostings(ad_utility::File& out,
                                    const vector<Posting>& postings,
                                    bool skipWordlistIfAllTheSame);

  static RelationMetaData writeRel(ad_utility::File& out, off_t currentOffset,
                                   Id relId, const vector<array<Id, 2>>& data,
                                   bool functional);

  static RelationMetaData& writeFunctionalRelation(
      const vector<array<Id, 2>>& data, RelationMetaData& rmd);

  static RelationMetaData& writeNonFunctionalRelation(
      ad_utility::File& out,
      const vector<array<Id, 2>>& data,
      RelationMetaData& rmd);

  void openFileHandles();

  void openTextFileHandle();

  void scanFunctionalRelation(const pair<off_t, size_t>& blockOff,
                              Id lhsId, ad_utility::File& indexFile,
                              WidthOneList *result) const;

  void scanNonFunctionalRelation(const pair<off_t, size_t>& blockOff,
                                 const pair<off_t, size_t>& followBlock,
                                 Id lhsId, ad_utility::File& indexFile,
                                 off_t upperBound,
                                 WidthOneList *result) const;

  void addContextToVector(TextVec::bufwriter_type& writer, Id context,
                          const unordered_map<Id, Score>& words,
                          const unordered_map<Id, Score>& entities);

  template<typename T>
  void readGapComprList(size_t nofElements, off_t from, size_t nofBytes,
                        vector<T>& result) const;

  template<typename T>
  void readFreqComprList(size_t nofElements, off_t from, size_t nofBytes,
                         vector<T>& result) const;


  size_t getIndexOfBestSuitedElTerm(const vector<string>& terms) const;


  void calculateBlockBoundaries();

  Id getWordBlockId(Id wordId) const;

  Id getEntityBlockId(Id entityId) const;

  //! Writes a list of elements (have to be able to be cast to unit64_t)
  //! to file.
  //! Returns the number of bytes written.
  template<class Numeric>
  size_t writeList(Numeric *data, size_t nofElements,
                   ad_utility::File& file) const;

  typedef unordered_map<Id, Id> IdCodeMap;
  typedef unordered_map<Score, Score> ScoreCodeMap;
  typedef vector<Id> IdCodebook;
  typedef vector<Score> ScoreCodebook;

  //! Creates codebooks for lists that are supposed to be entropy encoded.
  void createCodebooks(const vector<Posting>& postings, IdCodeMap& wordCodemap,
                       IdCodebook& wordCodebook, ScoreCodeMap& scoreCodemap,
                       ScoreCodebook& scoreCodebook) const;

  template<class T>
  size_t writeCodebook(const vector<T>& codebook,
                       ad_utility::File& file) const;

  // FRIEND TESTS
  friend class IndexTest_createFromTsvTest_Test;

  friend class IndexTest_createFromOnDiskIndexTest_Test;
};