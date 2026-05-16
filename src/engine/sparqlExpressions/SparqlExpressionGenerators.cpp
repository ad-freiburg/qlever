// Copyright 2026, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Under QLEVER_CHEAPER_COMPILATION, `idOrLiteralOrIriToId` is declared (not
// defined inline) in SparqlExpressionGenerators.h. Including the impl header
// here provides the single out-of-line definition, so the std::visit vtable is
// only instantiated once rather than in every including TU.
#include "engine/sparqlExpressions/SparqlExpressionGeneratorsImpl.h"
