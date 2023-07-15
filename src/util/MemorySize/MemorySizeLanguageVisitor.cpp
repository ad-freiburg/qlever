// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023, schlegea@informatik.uni-freiburg.de)

#include <absl/container/flat_hash_map.h>

#include <string>

#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"
#include "util/MemorySize/MemorySizeLanguageVisitor.h"
#include "util/StringUtils.h"

// ____________________________________________________________________________
ad_utility::MemorySize
ToMemorySizeInstanceMemorySizeLanguageVisitor::visitMemorySizeString(
    Parser::MemorySizeStringContext* context) const {
  if (context->pureByteSize()) {
    return visitPureByteSize(context->pureByteSize());
  } else {
    // Must be a `memoryUnitSize`.
    AD_CONTRACT_CHECK(context->memoryUnitSize());
    return visitMemoryUnitSize(context->memoryUnitSize());
  }
}

// ____________________________________________________________________________
ad_utility::MemorySize
ToMemorySizeInstanceMemorySizeLanguageVisitor::visitPureByteSize(
    Parser::PureByteSizeContext* context) const {
  return ad_utility::MemorySize::bytes(
      std::stoul(context->UNSIGNED_INTEGER()->getText()));
}

// ____________________________________________________________________________
ad_utility::MemorySize
ToMemorySizeInstanceMemorySizeLanguageVisitor::visitMemoryUnitSize(
    Parser::MemoryUnitSizeContext* context) const {
  /*
  Create an instance of `MemorySize`.

  @param memoryUnit Which memory unit to use for the creation. Must be one of:
  - 'k' (kB)
  - 'm' (MB)
  - 'g' (GB)
  - 't' (TB)
  @param numUnits Amount of kilobytes, megabytes, etc..
  */
  auto createMemoryInstance = [](char memoryUnit, auto numUnits) {
    ad_utility::MemorySize toReturn;

    switch (memoryUnit) {
      case 'k':
        toReturn = ad_utility::MemorySize::kilobytes(numUnits);
        break;
      case 'm':
        toReturn = ad_utility::MemorySize::megabytes(numUnits);
        break;
      case 'g':
        toReturn = ad_utility::MemorySize::gigabytes(numUnits);
        break;
      case 't':
        toReturn = ad_utility::MemorySize::terabytes(numUnits);
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
