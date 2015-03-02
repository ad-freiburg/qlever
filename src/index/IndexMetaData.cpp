// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <stdio.h>
#include "./IndexMetaData.h"

// _____________________________________________________________________________
IndexMetaData::IndexMetaData() {

}

// _____________________________________________________________________________
void IndexMetaData::add(const RelationMetaData& rmd) {
  _data.push_back(rmd);
}

// _____________________________________________________________________________
off_t IndexMetaData::getSizeOfData() const {
  return 0;
}
