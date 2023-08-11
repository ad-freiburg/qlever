// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <absl/strings/str_cat.h>

#include <stdexcept>

#include "util/ConfigManager/ConfigOption.h"
#include "util/Exception.h"

namespace ad_utility {

// A proxy/reference to a `ConfigOption`. Also saves the type of value,
// that the configuration option holds.
template <typename T>
requires ad_utility::isTypeContainedIn<T, ConfigOption::AvailableTypes>
class ConfigOptionProxy {
  ConfigOption* option_;

 public:
  using Type = T;

  // Custom constructor.
  ConfigOptionProxy(ConfigOption& opt) : option_(&opt) {
    // Make sure, that the given `ConfigOption` holds values of the right type.
    if (!opt.holdsType<T>()) {
      throw std::runtime_error(absl::StrCat(
          "Error while creating 'ConfigOptionProxy' for 'ConfigOption' '",
          opt.getIdentifier(),
          "': Mismatch between the template type of proxy and the type of "
          "values, the configuration option can hold."));
    }
  }

  // Get access to the configuration option, this is a proxy for.
  ConfigOption& getConfigOption() const {
    AD_CORRECTNESS_CHECK(option_ != nullptr);
    return *option_;
  }
};
}  // namespace ad_utility
