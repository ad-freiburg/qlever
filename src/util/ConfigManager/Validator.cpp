// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (September of 2023,
// schlegea@informatik.uni-freiburg.de)

#include "util/ConfigManager/Validator.h"

#include <functional>

#include "util/Exception.h"

namespace ad_utility {
// ____________________________________________________________________________
const std::string& ErrorMessage::getMessage() const { return message_; }

// ____________________________________________________________________________
void ConfigOptionValidatorManager::checkValidator() const {
  // Do we have a wrapper saved?
  AD_CORRECTNESS_CHECK(wrappedValidatorFunction_);

  // All the behavior is defined via a wrapper, so we only have to invoke it.
  std::invoke(wrappedValidatorFunction_);
}

// ____________________________________________________________________________
std::string_view ConfigOptionValidatorManager::getDescription() const {
  return descriptor_;
}

// ____________________________________________________________________________
size_t ConfigOptionValidatorManager::getInitializationId() const {
  return initializationId_;
}
}  // namespace ad_utility
