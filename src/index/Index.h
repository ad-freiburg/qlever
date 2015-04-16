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
#include "TextMetaData.h"


using std::string;
using std::array;
using std::vector;
using std::tuple;

class Index {
public:
  Index();

  typedef stxxl::VECTOR_GENERATOR<array<Id, 3>>::result ExtVec;
  typedef stxxl::VECTOR_GENERATOR<tuple<Id, Id, Score, bool>>::result TextVec;

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

  // Adds text index from on disk index that has previously been constructed.
  // Read necessary meta data into memory and opens file handles.
  void addTextFromOnDiskIndex();

  // Checks if the index is ready for use, i.e. it is properly intitialized.
  bool ready() const;

  typedef vector<array<Id, 1>> WidthOneList;
  typedef vector<array<Id, 2>> WidthTwoList;

  void scanPSO(const string& predicate, WidthTwoList* result) const;
  void scanPSO(const string& predicate, const string& subject, WidthOneList*
  result) const;
  void scanPOS(const string& predicate, WidthTwoList* result) const;
  void scanPOS(const string& predicate, const string& object, WidthOneList*
  result) const;

  const string& idToString(Id id) const;

  size_t relationCardinality(const string& relationName) const;

private:
  string _onDiskBase;
  Vocabulary _vocab;
  IndexMetaData _psoMeta;
  IndexMetaData _posMeta;
  TextMetaData _textMeta;
  mutable ad_utility::File _psoFile;
  mutable ad_utility::File _posFile;

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

  void createTextIndex(const string& filename, const TextVec& vec,
                       TextMetaData& meta);

  static RelationMetaData writeRel(ad_utility::File& out, off_t currentOffset,
      Id relId, const vector<array<Id, 2>>& data, bool functional);

  static RelationMetaData& writeFunctionalRelation(
      const vector<array<Id, 2>>& data, RelationMetaData& rmd);

  static RelationMetaData& writeNonFunctionalRelation(ad_utility::File& out,
      const vector<array<Id, 2>>& data, RelationMetaData& rmd);

  void openFileHandles();

  void scanFunctionalRelation(const pair<off_t, size_t>& blockOff,
      Id lhsId, ad_utility::File& indexFile, WidthOneList* result) const;

  void scanNonFunctionalRelation(const pair<off_t, size_t>& blockOff,
      const pair<off_t, size_t>& followBlock,
      Id lhsId, ad_utility::File& indexFile, off_t upperBound,
      WidthOneList* result) const;

  // FRIEND TESTS
  friend class IndexTest_createFromTsvTest_Test;
  friend class IndexTest_createFromOnDiskIndexTest_Test;

};