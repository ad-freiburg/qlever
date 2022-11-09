// Copyright 2015 -2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchhold@gmail.com>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/ResultTable.h"

#include <cassert>

#include "engine/LocalVocab.h"

// _____________________________________________________________________________
ResultTable::ResultTable(ad_utility::AllocatorWithLimit<Id> allocator)
    : _sortedBy(),
      _idTable(std::move(allocator)),
      _resultTypes(),
      // TODO: Why initialize with a pointer to an empty local vocabulary
      // instead of with nullptr?
      _localVocab(std::make_shared<LocalVocab>()) {}

// _____________________________________________________________________________
void ResultTable::clear() {
  _localVocab = nullptr;
  _idTable.clear();
}

// _____________________________________________________________________________
ResultTable::~ResultTable() { clear(); }

// _____________________________________________________________________________
string ResultTable::asDebugString() const {
  std::ostringstream os;
  os << "First (up to) 5 rows of result with size:\n";
  for (size_t i = 0; i < std::min<size_t>(5, _idTable.size()); ++i) {
    for (size_t j = 0; j < _idTable.cols(); ++j) {
      os << _idTable(i, j) << '\t';
    }
    os << '\n';
  }
  return std::move(os).str();
}

// _____________________________________________________________________________
size_t ResultTable::size() const { return _idTable.size(); }
