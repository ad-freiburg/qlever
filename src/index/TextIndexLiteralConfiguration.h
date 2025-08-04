// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Felix Meisen (fesemeisen@outlook.de)

#ifndef QLEVER_SRC_INDEX_TEXTINDEXLITERALCONFIGURATION_H
#define QLEVER_SRC_INDEX_TEXTINDEXLITERALCONFIGURATION_H

struct TextIndexLiteralConfiguration {
  std::string regex_;
  bool whitelist_;
  bool addAllLiterals_;
};

#endif  // QLEVER_SRC_INDEX_TEXTINDEXLITERALCONFIGURATION_H
