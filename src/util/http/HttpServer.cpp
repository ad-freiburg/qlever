// Copyright 2021-2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include "util/http/HttpServer.h"

#include "global/RuntimeParameters.h"

ad_utility::MemorySize getRequestBodyLimit() {
  return RuntimeParameters().get<"request-body-limit">();
}
