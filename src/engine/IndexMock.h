// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <vector>
#include <array>
#include "../broccoli-server/Index.h"

using std::vector;
using std::array;

// Mock for basic index functionality.
// To be used in tests an very early prototypes.
// Add whatever behavior is desired.
class IndexMock {
public:

  IndexMock() {}

  explicit IndexMock(const string& basename) : _broccoliIndex() {
    _broccoliIndex.registerOntologyIndex(basename, false);
    _broccoliIndex.initInMemoryRelations();
  }

  typedef vector<array<Id, 1>> WidthOneList;
  typedef vector<array<Id, 2>> WidthTwoList;

  void scanPSO(const string& predicate, WidthTwoList* result) const;
  void scanPSO(const string& predicate, const string& subject, WidthOneList*
  result) const;
  void scanPOS(const string& predicate, WidthTwoList* result) const;
  void scanPOS(const string& predicate, const string& object, WidthOneList*
  result) const;

private:
  ad_semsearch::Index _broccoliIndex;

  void getRelationRhsBySingleLhs(ad_semsearch::Relation const& relation, Id key, WidthOneList* result) const;
};