// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_INDEX_FTSALGORITHMS_H
#define QLEVER_SRC_INDEX_FTSALGORITHMS_H

#include <array>
#include <vector>

#include "index/Index.h"

namespace qlever {

class FTSAlgorithms {
 public:
  // Filters all IdTable entries out where the WordIndex does not lay inside the
  // idRange.
  static IdTable filterByRange(const IdRange<WordVocabIndex>& idRange,
                               const IdTable& idPreFilter);
};

}  // namespace qlever

#endif  // QLEVER_SRC_INDEX_FTSALGORITHMS_H
