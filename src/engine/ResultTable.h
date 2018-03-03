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
  void *_fixedSizeData;

  ResultTable();

  ResultTable(const ResultTable&);

  ResultTable(ResultTable&& other) noexcept :
      _status(ResultTable::OTHER),
      _nofColumns(0),
      _sortedBy(0),
      _varSizeData(),
      _fixedSizeData(nullptr){
    swap(*this, other);
  }

  ResultTable& operator=(ResultTable other) {
    swap(*this, other);
    return *this;
  }

  ResultTable& operator=(ResultTable&& other) noexcept {
    swap(*this, other);
    return *this;
  }

  friend void swap(ResultTable& a, ResultTable& b) noexcept {
    using std::swap;
    swap(a._status, b._status);
    swap(a._nofColumns, b._nofColumns);
    swap(a._sortedBy, b._sortedBy);
    swap(a._varSizeData, b._varSizeData);
    swap(a._fixedSizeData, b._fixedSizeData);
  }

  virtual ~ResultTable();

  size_t size() const;

  void clear();

  string asDebugString() const;

  const vector<vector<Id>> getDataAsVarSize() const {
    if (_varSizeData.size() > 0) {
      return _varSizeData;
    }

    vector<vector<Id>> res;
    if (_nofColumns == 1) {
      const auto& data = *static_cast<vector<array<Id, 1>> *>(_fixedSizeData);
      for (size_t i = 0; i < data.size(); ++i) {
        res.emplace_back(vector<Id>{{data[i][0]}});
      }
    } else if (_nofColumns == 2) {
      const auto& data = *static_cast<vector<array<Id, 2>> *>(_fixedSizeData);
      for (size_t i = 0; i < data.size(); ++i) {
        res.emplace_back(vector<Id>{{data[i][0], data[i][1]}});
      }
    } else if (_nofColumns == 3) {
      const auto& data = *static_cast<vector<array<Id, 3>> *>(_fixedSizeData);
      for (size_t i = 0; i < data.size(); ++i) {
        res.emplace_back(
            vector<Id>{{data[i][0], data[i][1], data[i][2]}});
      }
    } else if (_nofColumns == 4) {
      const auto& data = *static_cast<vector<array<Id, 4>> *>(_fixedSizeData);
      for (size_t i = 0; i < data.size(); ++i) {
        res.emplace_back(
            vector<Id>{{data[i][0], data[i][1], data[i][2], data[i][3]}});
      }
    } else if (_nofColumns == 5) {
      const auto& data = *static_cast<vector<array<Id, 5>> *>(_fixedSizeData);
      for (size_t i = 0; i < data.size(); ++i) {
        res.emplace_back(
            vector<Id>{{data[i][0], data[i][1], data[i][2],
                           data[i][3], data[i][4]}});
      }
    }
    return res;
  }

};

