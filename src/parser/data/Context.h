// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>

#include "../../engine/ResultTable.h"
#include "../../index/Index.h"

enum ContextRole { SUBJECT, PREDICATE, OBJECT };

struct Context {
  const size_t _row;
  const ResultTable& _res;
  const ad_utility::HashMap<std::string, size_t>& _variableColumns;
  const Index& _qecIndex;
};
