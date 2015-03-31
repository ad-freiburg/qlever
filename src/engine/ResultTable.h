// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <vector>
#include <array>
#include "../global/Id.h"
#include "../util/Exception.h"

using std::vector;
using std::array;

class ResultTable {
public:

  enum Status {
    FINISHED = 0,
    OTHER = 1
  };

  Status _status;
  size_t _nofColumns;
  // A value >= _nofColumns indicates unsorted data
  size_t _sortedBy;

  vector<vector<Id>> _varSizeData;
  void* _fixedSizeData;

  ResultTable();
  ResultTable(const ResultTable&);
  ResultTable& operator=(const ResultTable&);
  virtual ~ResultTable();
  size_t size() const;

  void clear();
  string asDebugString() const;

};

