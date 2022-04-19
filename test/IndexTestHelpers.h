//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_INDEXTESTHELPERS_H
#define QLEVER_INDEXTESTHELPERS_H

#include "../src/index/Index.h"

inline Index makeIndexWithTestSettings() {
  Index index;
  index.setNumTriplesPerBatch(2);
  index.stxxlMemoryInBytes() = 1024ul * 1024ul * 50;
  return index;
}

#endif  // QLEVER_INDEXTESTHELPERS_H
