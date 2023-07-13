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
size_t ToSizeTMemoryDefinitionLanguageVisitor::visitMemoryDefinitionString(
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
size_t ToSizeTMemoryDefinitionLanguageVisitor::visitPureByteDefinition(
    Parser::PureByteDefinitionContext* context) const {
  // We don't have to convert anything, just return the number of bytes given.
  return std::stoul(context->UNSIGNED_INTEGER()->getText());
}

// ____________________________________________________________________________
size_t ToSizeTMemoryDefinitionLanguageVisitor::visitMemoryUnitDefinition(
    Parser::MemoryUnitDefinitionContext* context) const {
  // Convert the memory unit name to the number of bytes per such unit.
  size_t bytesPerUnit{0};
  switch (ad_utility::getLowercase(context->MEMORY_UNIT()->getText()).front()) {
    case 'k':
      bytesPerUnit = ad_utility::detail::numberOfBytesPerKB;
      break;
    case 'm':
      bytesPerUnit = ad_utility::detail::numberOfBytesPerMB;
      break;
    case 'g':
      bytesPerUnit = ad_utility::detail::numberOfBytesPerGB;
      break;
    case 't':
      bytesPerUnit = ad_utility::detail::numberOfBytesPerTB;
      break;
    case 'p':
      bytesPerUnit = ad_utility::detail::numberOfBytesPerPB;
      break;
    default:
      // Whatever this is, it is false.
      AD_CORRECTNESS_CHECK(false);
  }

  // We have to interpret the amount of units.
  if (context->UNSIGNED_INTEGER()) {
    return ad_utility::detail::convertMemoryUnitsToBytes(
        std::stoul(context->UNSIGNED_INTEGER()->getText()), bytesPerUnit);
  } else {
    AD_CORRECTNESS_CHECK(context->FLOAT());
    return ad_utility::detail::convertMemoryUnitsToBytes(
        std::stod(context->FLOAT()->getText()), bytesPerUnit);
  }
}
