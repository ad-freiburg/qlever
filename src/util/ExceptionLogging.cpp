// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tomas Damek <tomas.damek@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/ExceptionLogging.h"

#include <absl/strings/str_cat.h>

#include "util/Log.h"

namespace ad_utility::exceptionLogging {

// ___________________________________________________________________________
void logErrorWithHighlighting(
    std::string& errorMsg, const std::optional<ExceptionMetadata>& metadata) {
  AD_LOG_ERROR << errorMsg << std::endl;
  if (!metadata.has_value()) {
    return;
  }
  // The `coloredError()` message might fail because of the different
  // Unicode handling of QLever and ANTLR. Make sure to detect this case so
  // that we can fix it if it happens.
  try {
    AD_LOG_ERROR << metadata->coloredError() << std::endl;
  } catch (const std::exception& e) {
    errorMsg.append(absl::StrCat(
        " Highlighting an error for the command line log failed: ", e.what()));
    AD_LOG_ERROR << "Failed to highlight error in operation. " << e.what()
                 << std::endl;
    AD_LOG_ERROR << metadata->query_ << std::endl;
  }
}

}  // namespace ad_utility::exceptionLogging
