// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>

#include "qlever/engine/ResultTable.h"
#include "qlever/engine/VariableToColumnMap.h"
#include "qlever/parser/data/Variable.h"
#include "qlever/util/HashMap.h"

// Forward declarations to avoid cyclic dependencies
class Index;
enum ContextRole : int { SUBJECT, PREDICATE, OBJECT };

struct Context {
  const size_t _row;
  const ResultTable& _res;
  const VariableToColumnMap& _variableColumns;
  const Index& _qecIndex;
};
