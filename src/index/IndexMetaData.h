// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <array>
#include <vector>

#include "../engine/Id.h"


using std::array;
using std::vector;

class RelationMetaData {

};

class IndexMetaData {
public:
  IndexMetaData();
  void add(const RelationMetaData& rmd);
  off_t getSizeOfData() const;

private:
  vector<RelationMetaData> _data;
};