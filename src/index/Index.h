// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>
#include <array>
#include <fstream>
#include <vector>
#include <stxxl/vector>
#include <stdio.h>
#include <unistd.h>
#include "./Vocabulary.h"
#include "./IndexMetaData.h"
#include "./StxxlSortFunctors.h"
#include "../util/File.h"


using std::string;
using std::array;
using std::vector;

class Index {
public:
  Index();

  typedef stxxl::VECTOR_GENERATOR<array<Id, 3>>::result ExtVec;

  // Creates an index form a TSV file.
  // Will write vocabulary and on-disk index data.
  // Also ends up with fully functional in-memory metadata.
  void createFromTsvFile(const string& tsvFile, const string& onDiskBase);

  // Creates an index object from an on disk index
  // that has previously been constructed.
  // Read necessary meta data into memory and opens file handels.
  void createFromOnDiskIndex(const string& onDiskBase);

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
  mutable ad_utility::File _psoFile;
  mutable ad_utility::File _posFile;

  size_t passTsvFileForVocabulary(const string& tsvFile);

  void passTsvFileIntoIdVector(const string& tsvFile, ExtVec& data);

  static void createPermutation(const string& fileName,
      const ExtVec& vec,
      IndexMetaData& meta,
      size_t c1, size_t c2);

  static RelationMetaData writeRel(ad_utility::File& out, off_t currentOffset,
      Id relId, const vector<array<Id, 2>>& data, bool functional);


  // FRIEND TESTS
  friend class IndexTest_createFromTsvTest_Test;
  friend class IndexTest_createFromOnDiskIndexTest_Test;

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
};