// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <vector>
#include "Id.h"

class ResultTable {
public:

  ResultTable() { }

  virtual ~ResultTable() {
    if (fixedSizeData) {
      delete fixedSizeData;
    }
  }

  size_t _nofColumns;
  // A value >= _nofColumns indicates unsorted data
  size_t _sortedBy;

  vector<vector<Id>> _varSizeData;
  void* fixedSizeData;
};