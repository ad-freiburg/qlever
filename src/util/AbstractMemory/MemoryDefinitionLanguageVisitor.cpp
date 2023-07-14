// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023, schlegea@informatik.uni-freiburg.de)

#include <absl/container/flat_hash_map.h>

#include <string>

#include "util/AbstractMemory/Memory.h"
#include "util/AbstractMemory/MemoryDefinitionLanguageVisitor.h"
#include "util/Exception.h"
#include "util/StringUtils.h"

// ____________________________________________________________________________
ad_utility::Memory
ToMemoryInstanceMemoryDefinitionLanguageVisitor::visitMemoryDefinitionString(
    Parser::MemoryDefinitionStringContext* context) const {
  if (context->pureByteDefinition()) {
    return visitPureByteDefinition(context->pureByteDefinition());
  } else {
    // Must be a `memoryUnitDefinition`.
    AD_CONTRACT_CHECK(context->memoryUnitDefinition());
    return visitMemoryUnitDefinition(context->memoryUnitDefinition());
  }
}

// ____________________________________________________________________________
ad_utility::Memory
ToMemoryInstanceMemoryDefinitionLanguageVisitor::visitPureByteDefinition(
    Parser::PureByteDefinitionContext* context) const {
  return ad_utility::Memory::bytes(
      std::stoul(context->UNSIGNED_INTEGER()->getText()));
}

// ____________________________________________________________________________
ad_utility::Memory
ToMemoryInstanceMemoryDefinitionLanguageVisitor::visitMemoryUnitDefinition(
    Parser::MemoryUnitDefinitionContext* context) const {
  /*
  Create an instance of `Memory`.

  @param memoryUnit Which memory unit to use for the creation. Must be one of:
  - 'k' (KB)
  - 'm' (MB)
  - 'g' (GB)
  - 't' (TB)
  - 'p' (PB)
  @param numUnits Amount of kilobytes, megabytes, etc..
  */
  auto createMemoryInstance = [](char memoryUnit, auto numUnits) {
    ad_utility::Memory toReturn;

    switch (memoryUnit) {
      case 'k':
        toReturn = ad_utility::Memory::kilobytes(numUnits);
        break;
      case 'm':
        toReturn = ad_utility::Memory::megabytes(numUnits);
        break;
      case 'g':
        toReturn = ad_utility::Memory::gigabytes(numUnits);
        break;
      case 't':
        toReturn = ad_utility::Memory::terabytes(numUnits);
        break;
      case 'p':
        toReturn = ad_utility::Memory::petabytes(numUnits);
        break;
      default:
        // Whatever this is, it is false.
        AD_CORRECTNESS_CHECK(false);
    }

    return toReturn;
  };

  // Which memory unit are we looking at?
  const char memoryUnitType =
      ad_utility::getLowercase(context->MEMORY_UNIT()->getText()).front();

  // We have to interpret the amount of units.
  if (context->UNSIGNED_INTEGER()) {
    return createMemoryInstance(
        memoryUnitType, std::stoul(context->UNSIGNED_INTEGER()->getText()));
  } else {
    AD_CORRECTNESS_CHECK(context->FLOAT());
    return createMemoryInstance(memoryUnitType,
                                std::stod(context->FLOAT()->getText()));
  }
}
