// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <algorithm>
#include "DocsDB.h"
#include "../global/Constants.h"

// _____________________________________________________________________________
void DocsDB::init(const string& fileName) {
  _dbFile.open(fileName.c_str(), "r");
  AD_CHECK(_dbFile.isOpen());
  off_t offsetsFrom;
  off_t offsetsTo = _dbFile.getLastOffset(&offsetsFrom);
  _offsets.resize(static_cast<size_t>(offsetsTo - offsetsFrom) /
                      (sizeof(off_t) + sizeof(Id)));
  _dbFile.read(_offsets.data(), static_cast<size_t>(offsetsTo - offsetsFrom),
               offsetsFrom);
}

// _____________________________________________________________________________
string DocsDB::getTextExcerpt(Id cid) const {
  auto it = std::lower_bound(_offsets.begin(), _offsets.end(), cid,
                             [](decltype(_offsets[0])& pair, Id key) {
                               return pair.first < key;
                             });
  char* buf = new char[BUFFER_SIZE_DOCSFILE_LINE];
  _dbFile.seek(it->first, 0);
  string line;
  _dbFile.readLine(&line, buf, BUFFER_SIZE_DOCSFILE_LINE);
  delete[] buf;
  return line;
}
