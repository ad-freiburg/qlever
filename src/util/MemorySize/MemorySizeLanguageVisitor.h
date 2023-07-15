// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023, schlegea@informatik.uni-freiburg.de)
#pragma once

#include <antlr4-runtime.h>

#include "util/MemorySize/MemorySize.h"
#include "util/MemorySize/generated/MemorySizeLanguageParser.h"
#include "util/json.h"

/*
This visitor will translate the memory size language to an instance of
`MemorySize`.
*/
class ToMemorySizeInstanceMemorySizeLanguageVisitor final {
 public:
  using Parser = MemorySizeLanguageParser;

  ad_utility::MemorySize visitMemorySizeString(
      Parser::MemorySizeStringContext* context) const;

 private:
  ad_utility::MemorySize visitPureByteSize(
      Parser::PureByteSizeContext* context) const;

  ad_utility::MemorySize visitMemoryUnitSize(
      Parser::MemoryUnitSizeContext* context) const;
};
