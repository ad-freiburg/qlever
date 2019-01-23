// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)

#ifndef QLEVER_RUNTIMESETTINGS_H

#include <chrono>
#include "../global/Constants.h"

// Contains settings for the QLever engine that we possibly want to specify
// at runtime without recompiling.
struct RuntimeSettings {
  std::chrono::seconds _timeoutQueryComputation =
      DEFAULT_QUERY_COMPUTATIION_TIMEOUT;
  std::chrono::seconds _timeoutStringTranslation =
      DEFAULT_STRING_TRANSLATION_TIMEOUT;
};
#define QLEVER_RUNTIMESETTINGS_H

#endif  // QLEVER_RUNTIMESETTINGS_H
