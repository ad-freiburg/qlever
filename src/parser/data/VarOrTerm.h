// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Robin Textor-Falconi (textorr@informatik.uni-freiburg.de)

#pragma once

#include <string>
#include <variant>

#include "engine/ResultTable.h"
#include "index/Index.h"
#include "parser/data/GraphTerm.h"
#include "parser/data/Variable.h"
#include "util/VisitMixin.h"

using VarOrTermBase = std::variant<Variable, GraphTerm>;

// TODO: This class should have some documentation.
using VarOrTerm = GraphTerm;
