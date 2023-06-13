// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel February of 2023, schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>

#include "util/Algorithm.h"
#include "util/ConfigManager/ConfigOption.h"
#include "util/ConfigManager/ConfigToString.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/Forward.h"

namespace ad_utility {

// ___________________________________________________________________________
std::string getDefaultValueConfigOptions(const ConfigManager& config) {
  // Nothing to do here, if we have no configuration options.
  if (config.getConfigurationOptions().size() == 0) {
    return "";
  }

  /*
  Because we want to create a list, we don't know how many entries there will be
  and need a string stream.
  */
  std::ostringstream stream;

  // Prints the default value of a configuration option and the accompanying
  // text.
  auto defaultConfigurationOptionToString = [](const ConfigOption& option) {
    return absl::StrCat("Configuration option '", option.getIdentifier(),
                        "' was not set at runtime, using default value '",
                        option.getDefaultValueAsString(), "'.");
  };

  forEachExcludingTheLastOne(
      config.getConfigurationOptions(),
      [&stream,
       &defaultConfigurationOptionToString](const ConfigOption& option) {
        if (option.hasDefaultValue() && !option.wasSetAtRuntime()) {
          stream << defaultConfigurationOptionToString(option) << "\n";
        }
      },
      [&stream,
       &defaultConfigurationOptionToString](const ConfigOption& option) {
        if (option.hasDefaultValue() && !option.wasSetAtRuntime()) {
          stream << defaultConfigurationOptionToString(option);
        }
      });

  return stream.str();
}
}  // namespace ad_utility
