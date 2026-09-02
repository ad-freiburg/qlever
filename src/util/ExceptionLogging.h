// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Tomas Damek <tomas.damek@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_EXCEPTIONLOGGING_H
#define QLEVER_SRC_UTIL_EXCEPTIONLOGGING_H

#include <optional>
#include <string>

#include "util/ParseException.h"

namespace exceptionLogging {

// Log `errorMsg`. If `metadata` is present, additionally try to log the
// query with the offending clause highlighted (see
// `ExceptionMetadata::coloredError()`). Highlighting can fail because of
// differing Unicode handling between QLever and ANTLR; in that case log the
// raw query instead and append the failure reason to `errorMsg`.
void logErrorWithHighlighting(std::string& errorMsg,
                              const std::optional<ExceptionMetadata>& metadata);

}  // namespace exceptionLogging

#endif  // QLEVER_SRC_UTIL_EXCEPTIONLOGGING_H
