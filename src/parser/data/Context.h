// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>

#include "engine/ResultTable.h"
#include "parser/data/Variable.h"
#include "util/HashMap.h"

// Forward declarations to avoid cyclic dependencies
class Index;
enum ContextRole : int { SUBJECT, PREDICATE, OBJECT };

struct Context {
  const size_t _row;
  const ResultTable& _res;
  // TODO<joka921> Should be `VariableToColumnMap` once this is in a separate
  // header.
  const ad_utility::HashMap<Variable, size_t>& _variableColumns;
  const Index& _qecIndex;
};
