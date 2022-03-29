// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "DocsDB.h"

#include <algorithm>

#include "../global/Constants.h"

// _____________________________________________________________________________
void DocsDB::init(const string& fileName) {
  _dbFile.open(fileName.c_str(), "r");
  if (_dbFile.empty()) {
    _size = 0;
  } else {
    off_t posLastOfft = _dbFile.getLastOffset(&_startOfOffsets);
    _size = (posLastOfft - _startOfOffsets) / sizeof(off_t);
  }
}

// _____________________________________________________________________________
string DocsDB::getTextExcerpt(UnknownIndex cid) const {
  off_t ft[2];
  off_t& from = ft[0];
  off_t& to = ft[1];
  off_t at = _startOfOffsets + cid * sizeof(off_t);
  at += _dbFile.read(ft, sizeof(ft), at);
  while (to == from) {
    at += _dbFile.read(&to, sizeof(off_t), at);
  }
  assert(to > from);
  size_t nofBytes = static_cast<size_t>(to - from);
  string line(nofBytes, '\0');
  _dbFile.read(line.data(), nofBytes, from);
  return line;
}
