// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <vector>
#include <array>
#include "Id.h"
#include "../util/Exception.h"

using std::vector;
using std::array;

class ResultTable {
public:

  ResultTable() : _status(Status::OTHER) {
  }

  void clear() {
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
      _fixedSizeData = nullptr;
    } else {
      _varSizeData.clear();
    }
    _status = OTHER;
  }

  virtual ~ResultTable() {
    clear();
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

  string asString() const {
    std::ostringstream os;
    os << "First (up to) 5 rows of result with size:\n";
    if (_fixedSizeData) {
      if (_nofColumns == 1) {
        vector<array<Id, 1>>* ptr =
            reinterpret_cast<vector<array<Id, 1>>*>(_fixedSizeData);
        for (size_t i = 0; i < std::min<size_t>(5, ptr->size()); ++i) {
          os << (*ptr)[i][0] << '\n';
        }
      }
      if (_nofColumns == 2) {
        vector<array<Id, 2>>* ptr =
            reinterpret_cast<vector<array<Id, 2>>*>(_fixedSizeData);
        for (size_t i = 0; i < std::min<size_t>(5, ptr->size()); ++i) {
          os << (*ptr)[i][0] << '\t' << (*ptr)[i][1] << '\n';
        }
      }
      if (_nofColumns == 3) {
        vector<array<Id, 3>>* ptr =
            reinterpret_cast<vector<array<Id, 3>>*>(_fixedSizeData);
        for (size_t i = 0; i < std::min<size_t>(5, ptr->size()); ++i) {
        os << (*ptr)[i][0] << '\t' << (*ptr)[i][1] <<  '\t' << (*ptr)[i][2] <<
          '\n';
        }
      }
      if (_nofColumns == 4) {
        vector<array<Id, 4>>* ptr =
            reinterpret_cast<vector<array<Id, 4>>*>(_fixedSizeData);
        for (size_t i = 0; i < std::min<size_t>(5, ptr->size()); ++i) {
          os << (*ptr)[i][0] << '\t' << (*ptr)[i][1] <<  '\t' << (*ptr)[i][2]
              << '\t' << (*ptr)[i][3] <<  '\n';
        }
      }
      if (_nofColumns == 5) {
        vector<array<Id, 5>>* ptr =
            reinterpret_cast<vector<array<Id, 5>>*>(_fixedSizeData);
        for (size_t i = 0; i < std::min<size_t>(5, ptr->size()); ++i) {
          os << (*ptr)[i][0] << '\t' << (*ptr)[i][1] <<  '\t' << (*ptr)[i][2]
              << '\t' << (*ptr)[i][3] << '\t' << (*ptr)[i][4] <<  '\n';
        }
      }
    } else {
      AD_THROW(ad_semsearch::Exception::NOT_YET_IMPLEMENTED, "no asStrign for"
          " var size results, yet.")
    }
    return os.str();
  }
};

