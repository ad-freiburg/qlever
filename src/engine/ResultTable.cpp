// Copyright 2015 -2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchhold@gmail.com>

#include "engine/ResultTable.h"

#include <cassert>

#include "engine/LocalVocab.h"

// _____________________________________________________________________________
string ResultTable::asDebugString() const {
  std::ostringstream os;
  os << "First (up to) 5 rows of result with size:\n";
  for (size_t i = 0; i < std::min<size_t>(5, _idTable.size()); ++i) {
    for (size_t j = 0; j < _idTable.numColumns(); ++j) {
      os << _idTable(i, j) << '\t';
    }
    os << '\n';
  }
  return std::move(os).str();
}
