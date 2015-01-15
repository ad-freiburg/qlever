// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <vector>
#include <array>
#include "Id.h"

using std::vector;
using std::array;

class ResultTable {
public:

  ResultTable() : _status(Status::OTHER) {
  }

  virtual ~ResultTable() {
    if (_fixedSizeData) {
      if (_nofColumns == 1) {
        vector<array<Id, 1>>* ptr =
            reinterpret_cast<vector<array<Id, 1>>*>(_fixedSizeData);
        delete ptr;
      }
      if (_nofColumns == 2) {
        vector<array<Id, 2>>* ptr =
            reinterpret_cast<vector<array<Id, 2>>*>(_fixedSizeData);
        delete ptr;
      }
      if (_nofColumns == 3) {
        vector<array<Id, 3>>* ptr =
            reinterpret_cast<vector<array<Id, 3>>*>(_fixedSizeData);
        delete ptr;
      }
      if (_nofColumns == 4) {
        vector<array<Id, 4>>* ptr =
            reinterpret_cast<vector<array<Id, 4>>*>(_fixedSizeData);
        delete ptr;
      }
      if (_nofColumns == 5) {
        vector<array<Id, 5>>* ptr =
            reinterpret_cast<vector<array<Id, 5>>*>(_fixedSizeData);
        delete ptr;
      }
    }
  }

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
};