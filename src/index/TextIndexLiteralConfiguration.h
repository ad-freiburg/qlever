// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_SRC_INDEX_TEXTINDEXLITERALCONFIGURATION_H
#define QLEVER_SRC_INDEX_TEXTINDEXLITERALCONFIGURATION_H

#include "index/TextIndexLiteralFilter.h"

struct TextIndexLiteralConfiguration {
  std::string regex_;
  TextIndexLiteralFilter::FilterType whitelist_;
  bool addAllLiterals_;
};

#endif  // QLEVER_SRC_INDEX_TEXTINDEXLITERALCONFIGURATION_H
