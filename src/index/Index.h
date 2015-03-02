// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <string>
#include <array>
#include <fstream>
#include <stxxl/vector>
#include <stdio.h>
#include "./Vocabulary.h"
#include "./IndexMetaData.h"
#include "./StxxlSortFunctors.h"


using std::string;
using std::array;

class Index {
public:
  Index();
  typedef stxxl::VECTOR_GENERATOR<array<Id, 3>>::result ExtVec;
  void createFromTsvFile(const string& tsvFile, const string& onDiskBase);

private:
  string _onDiskBase;
  Vocabulary _vocab;
  IndexMetaData _psoMeta;
  IndexMetaData _posMeta;

  void passTsvFileForVocabulary(const string& tsvFile);
  void passTsvFileIntoIdVector(const string& tsvFile, ExtVec& data);

  static void createPermutation(const ExtVec& vec, IndexMetaData& meta,
  std::fstream& out);


  void writeMetadata(std::fstream& out);

  static RelationMetaData writeRel(std::fstream& out, off_t currentOffset,
      const ExtVec& data, size_t fromIndex, size_t toIndexInclusive);
};