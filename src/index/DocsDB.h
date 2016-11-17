// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <vector>
#include <string>
#include <utility>
#include "../global/Id.h"
#include "../util/File.h"

using std::vector;
using std::pair;
using std::string;

class DocsDB {
public:
  void init(const string& fileName);
  string getTextExcerpt(Id cid) const;

  mutable ad_utility::File _dbFile;
  off_t _startOfOffsets;
  size_t _size;
};

