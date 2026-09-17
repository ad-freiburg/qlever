// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/metrics/Resource.h"

#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>
#include <opentelemetry/sdk/common/env_variables.h>
#include <opentelemetry/semconv/service_attributes.h>

#include <string>
#include <string_view>

#include "CompilationInfo.h"

namespace resource_sdk = opentelemetry::sdk::resource;
namespace otel_common = opentelemetry::sdk::common;
namespace semconv = opentelemetry::semconv;

namespace ad_utility::metrics {
namespace {

constexpr std::string_view DEFAULT_SERVICE_NAME = "qlever";

// Names of the environment variables from which the OTEL SDK itself detects
// resource attributes (see `OTELResourceDetector`).
constexpr const char* SERVICE_NAME_ENV_VAR = "OTEL_SERVICE_NAME";
constexpr const char* RESOURCE_ATTRIBUTES_ENV_VAR = "OTEL_RESOURCE_ATTRIBUTES";

}  // namespace

// _____________________________________________________________________________
bool hasServiceNameFromEnv() {
  std::string value;
  if (otel_common::GetStringEnvironmentVariable(SERVICE_NAME_ENV_VAR, value) &&
      !value.empty()) {
    return true;
  }
  if (!otel_common::GetStringEnvironmentVariable(RESOURCE_ATTRIBUTES_ENV_VAR,
                                                 value)) {
    return false;
  }
  // `OTEL_RESOURCE_ATTRIBUTES` is a comma-separated list of `key=value` pairs.
  for (std::string_view entry : absl::StrSplit(value, ',')) {
    std::string_view key = entry.substr(0, entry.find('='));
    if (absl::StripAsciiWhitespace(key) == semconv::service::kServiceName) {
      return true;
    }
  }
  return false;
}

// _____________________________________________________________________________
const resource_sdk::Resource& sharedResource() {
  static const resource_sdk::Resource resource =
      resource_sdk::Resource::Create(detail::qleverResourceAttributes());
  return resource;
}

// _____________________________________________________________________________
resource_sdk::ResourceAttributes detail::qleverResourceAttributes() {
  // The same build information that the `qlever.build_info` metric exposes to
  // Prometheus (see `ServerMetrics.cpp`). On the OTLP side this information
  // belongs on the resource, so it does not have to be repeated for every
  // metric and span.
  resource_sdk::ResourceAttributes attributes{
      {semconv::service::kServiceVersion,
       *qlever::version::projectVersionWithoutLinking.rlock()},
      {"qlever.git_hash", *qlever::version::gitShortHashWithoutLinking.rlock()},
      {"qlever.compiler", *qlever::version::compilerWithoutLinking.rlock()},
      {"qlever.compiler_version",
       *qlever::version::compilerVersionWithoutLinking.rlock()},
      {"qlever.cxx_standard",
       *qlever::version::cxxStandardWithoutLinking.rlock()},
      {"qlever.compile_time",
       *qlever::version::timeOfCompilationUnixWithoutLinking.rlock()}};
  if (!hasServiceNameFromEnv()) {
    attributes.SetAttribute(semconv::service::kServiceName,
                            DEFAULT_SERVICE_NAME);
  }
  return attributes;
}

}  // namespace ad_utility::metrics
