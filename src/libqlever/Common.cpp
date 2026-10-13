// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "libqlever/Common.h"

#include <util/Log.h>

#include "libqlever/NullStream.h"

namespace qlever {

void disableLogging() {
  static ad_utility::NullStream nullStream;
  ad_utility::setGlobalLoggingStream(&nullStream.stream());
}

}  // namespace qlever
