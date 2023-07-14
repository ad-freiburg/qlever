// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023, schlegea@informatik.uni-freiburg.de)
#pragma once

#include <antlr4-runtime.h>

#include "util/AbstractMemory/Memory.h"
#include "util/AbstractMemory/generated/MemoryDefinitionLanguageParser.h"
#include "util/json.h"

/*
This visitor will translate the memory definition language to an instance of
´Memory`, holding the memory size defined.
*/
class ToMemoryInstanceMemoryDefinitionLanguageVisitor final {
 public:
  using Parser = MemoryDefinitionLanguageParser;

  ad_utility::Memory visitMemoryDefinitionString(
      Parser::MemoryDefinitionStringContext* context) const;

  ad_utility::Memory visitPureByteDefinition(
      Parser::PureByteDefinitionContext* context) const;

  ad_utility::Memory visitMemoryUnitDefinition(
      Parser::MemoryUnitDefinitionContext* context) const;
};
