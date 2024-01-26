// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <array>
#include <vector>

#include "index/Index.h"

class FTSAlgorithms {
 public:
  // Filters all IdTable entries out where the WordIndex does not lay inside the
  // idRange.
  static IdTable filterByRange(const IdRange<WordVocabIndex>& idRange,
                               const IdTable& idPreFilter);
};
