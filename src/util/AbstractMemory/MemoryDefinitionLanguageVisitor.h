// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023, schlegea@informatik.uni-freiburg.de)
#pragma once

#include <antlr4-runtime.h>

#include "util/AbstractMemory/generated/MemoryDefinitionLanguageParser.h"
#include "util/json.h"

/*
This visitor will translate the memory definition language to the amount of
bytes defined.
*/
class ToSizeTMemoryDefinitionLanguageVisitor final {
 public:
  using Parser = MemoryDefinitionLanguageParser;

  size_t visitMemoryDefinitionString(
      Parser::MemoryDefinitionStringContext* context) const;

  size_t visitPureByteDefinition(
      Parser::PureByteDefinitionContext* context) const;

  size_t visitMemoryUnitDefinition(
      Parser::MemoryUnitDefinitionContext* context) const;
};
