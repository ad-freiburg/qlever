// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <cassert>
#include <glob.h>
#include "./ResultTable.h"

// _____________________________________________________________________________
ResultTable::ResultTable() :
    _status(ResultTable::OTHER),
    _nofColumns(0),
    _sortedBy(0),
    _varSizeData(),
    _fixedSizeData(nullptr) { }

// _____________________________________________________________________________
ResultTable::ResultTable(const ResultTable& other) :
    _status(other._status),
    _nofColumns(other._nofColumns),
    _sortedBy(other._sortedBy),
    _varSizeData(other._varSizeData) {
  if (other._nofColumns <= 5) {
    if (other._nofColumns == 1)  {
      auto newptr = new vector<array<Id, 1>>;
      _fixedSizeData = newptr;
      *newptr = *static_cast<vector<array<Id, 1>>*>(other._fixedSizeData);
    }
    if (other._nofColumns == 2)  {
      auto newptr = new vector<array<Id, 2>>;
      _fixedSizeData = newptr;
      *newptr = *static_cast<vector<array<Id, 2>>*>(other._fixedSizeData);
    }
    if (other._nofColumns == 3)  {
      auto newptr = new vector<array<Id, 3>>;
      _fixedSizeData = newptr;
      *newptr = *static_cast<vector<array<Id, 3>>*>(other._fixedSizeData);
    }
    if (other._nofColumns == 4)  {
      auto newptr = new vector<array<Id, 4>>;
      _fixedSizeData = newptr;
      *newptr = *static_cast<vector<array<Id, 4>>*>(other._fixedSizeData);
    }
    if (other._nofColumns == 5)  {
      auto newptr = new vector<array<Id, 5>>;
      _fixedSizeData = newptr;
      *newptr = *static_cast<vector<array<Id, 5>>*>(other._fixedSizeData);
    }
  } else {
    assert(!other._fixedSizeData);
  }
}

// _____________________________________________________________________________
ResultTable& ResultTable::operator=(const ResultTable& other) {
  clear();
  _varSizeData = other._varSizeData;
  _status = other._status;
  _nofColumns = other._nofColumns;
  _sortedBy = other._sortedBy;
  if (_nofColumns <= 5) {
    if (_nofColumns == 1)  {
      auto newptr = new vector<array<Id, 1>>;
      _fixedSizeData = newptr;
      *newptr = *static_cast<vector<array<Id, 1>>*>(other._fixedSizeData);
    }
    if (_nofColumns == 2)  {
      auto newptr = new vector<array<Id, 2>>;
      _fixedSizeData = newptr;
      *newptr = *static_cast<vector<array<Id, 2>>*>(other._fixedSizeData);
    }
    if (_nofColumns == 3)  {
      auto newptr = new vector<array<Id, 3>>;
      _fixedSizeData = newptr;
      *newptr = *static_cast<vector<array<Id, 3>>*>(other._fixedSizeData);
    }
    if (_nofColumns == 4)  {
      auto newptr = new vector<array<Id, 4>>;
      _fixedSizeData = newptr;
      *newptr = *static_cast<vector<array<Id, 4>>*>(other._fixedSizeData);
    }
    if (_nofColumns == 5)  {
      auto newptr = new vector<array<Id, 5>>;
      _fixedSizeData = newptr;
      *newptr = *static_cast<vector<array<Id, 5>>*>(other._fixedSizeData);
    }
  } else {
    assert(!other._fixedSizeData);
  }
  return *this;
}

// _____________________________________________________________________________
void ResultTable::clear() {
  if (_fixedSizeData) {
    if (_nofColumns == 1) {
      vector<array<Id, 1>>* ptr =
          static_cast<vector<array<Id, 1>>*>(_fixedSizeData);
      delete ptr;
    }
    if (_nofColumns == 2) {
      vector<array<Id, 2>>* ptr =
          static_cast<vector<array<Id, 2>>*>(_fixedSizeData);
      delete ptr;
    }
    if (_nofColumns == 3) {
      vector<array<Id, 3>>* ptr =
          static_cast<vector<array<Id, 3>>*>(_fixedSizeData);
      delete ptr;
    }
    if (_nofColumns == 4) {
      vector<array<Id, 4>>* ptr =
          static_cast<vector<array<Id, 4>>*>(_fixedSizeData);
      delete ptr;
    }
    if (_nofColumns == 5) {
      vector<array<Id, 5>>* ptr =
          static_cast<vector<array<Id, 5>>*>(_fixedSizeData);
      delete ptr;
    }

  }
  _fixedSizeData = nullptr;
  _varSizeData.clear();
  _status = OTHER;
}

// _____________________________________________________________________________
ResultTable::~ResultTable() {
  clear();
}

// _____________________________________________________________________________
string ResultTable::asDebugString() const {
  std::ostringstream os;
  os << "First (up to) 5 rows of result with size:\n";
  if (_fixedSizeData) {
    if (_nofColumns == 1) {
      vector<array<Id, 1>>* ptr =
          static_cast<vector<array<Id, 1>>*>(_fixedSizeData);
      for (size_t i = 0; i < std::min<size_t>(5, ptr->size()); ++i) {
        os << (*ptr)[i][0] << '\n';
      }
    }
    if (_nofColumns == 2) {
      vector<array<Id, 2>>* ptr =
          static_cast<vector<array<Id, 2>>*>(_fixedSizeData);
      for (size_t i = 0; i < std::min<size_t>(5, ptr->size()); ++i) {
        os << (*ptr)[i][0] << '\t' << (*ptr)[i][1] << '\n';
      }
    }
    if (_nofColumns == 3) {
      vector<array<Id, 3>>* ptr =
          static_cast<vector<array<Id, 3>>*>(_fixedSizeData);
      for (size_t i = 0; i < std::min<size_t>(5, ptr->size()); ++i) {
        os << (*ptr)[i][0] << '\t' << (*ptr)[i][1] << '\t' << (*ptr)[i][2] <<
            '\n';
      }
    }
    if (_nofColumns == 4) {
      vector<array<Id, 4>>* ptr =
          static_cast<vector<array<Id, 4>>*>(_fixedSizeData);
      for (size_t i = 0; i < std::min<size_t>(5, ptr->size()); ++i) {
        os << (*ptr)[i][0] << '\t' << (*ptr)[i][1] << '\t' << (*ptr)[i][2]
            << '\t' << (*ptr)[i][3] << '\n';
      }
    }
    if (_nofColumns == 5) {
      vector<array<Id, 5>>* ptr =
          static_cast<vector<array<Id, 5>>*>(_fixedSizeData);
      for (size_t i = 0; i < std::min<size_t>(5, ptr->size()); ++i) {
        os << (*ptr)[i][0] << '\t' << (*ptr)[i][1] << '\t' << (*ptr)[i][2]
            << '\t' << (*ptr)[i][3] << '\t' << (*ptr)[i][4] << '\n';
      }
    }
  } else {
    for (size_t i = 0; i < std::min<size_t>(5, _varSizeData.size()); ++i) {
      for (size_t j = 0; j < _varSizeData[i].size(); ++j) {
        os << _varSizeData[i][j] << '\t';
      }
      os << '\n';
    }
  }
  return os.str();
}

// _____________________________________________________________________________
size_t ResultTable::size() const {
  size_t rv = 0;
  if (_fixedSizeData) {
    if (_nofColumns == 1) {
      rv = static_cast<vector<array<Id, 1>>*>(_fixedSizeData)->size();
    }
    if (_nofColumns == 2) {
      rv =static_cast<vector<array<Id, 2>>*>(_fixedSizeData)->size();
    }
    if (_nofColumns == 3) {
      rv =static_cast<vector<array<Id, 3>>*>(_fixedSizeData)->size();
    }
    if (_nofColumns == 4) {
      rv =static_cast<vector<array<Id, 4>>*>(_fixedSizeData)->size();
    }
    if (_nofColumns == 5) {
      rv =static_cast<vector<array<Id, 5>>*>(_fixedSizeData)->size();
    }
  } else {
    rv =_varSizeData.size();
  }
  return rv;
}
