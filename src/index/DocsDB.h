// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_INDEX_DOCSDB_H
#define QLEVER_SRC_INDEX_DOCSDB_H

#include <string>
#include <utility>
#include <vector>

#include "../global/Id.h"
#include "../util/File.h"

using std::pair;
using std::string;
using std::vector;

class DocsDB {
 public:
  void init(const string& fileName);
  string getTextExcerpt(TextRecordIndex cid) const;

  mutable ad_utility::File _dbFile;
  off_t _startOfOffsets;
  size_t _size = 0;
};

#endif  // QLEVER_SRC_INDEX_DOCSDB_H
