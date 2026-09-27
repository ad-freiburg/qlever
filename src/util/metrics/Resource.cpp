// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/metrics/Resource.h"

#include <opentelemetry/sdk/resource/resource_detector.h>
#include <opentelemetry/semconv/service_attributes.h>

#include "CompilationInfo.h"

namespace resource_sdk = opentelemetry::sdk::resource;
namespace semconv = opentelemetry::semconv;

namespace ad_utility::metrics {

// _____________________________________________________________________________
const resource_sdk::Resource& sharedResource() {
  static const resource_sdk::Resource resource = detail::sharedResourceImpl();
  return resource;
}

// _____________________________________________________________________________
resource_sdk::ResourceAttributes detail::qleverResourceAttributes() {
  // The same build information that the `qlever.build_info` metric exposes to
  // Prometheus (see `ServerMetrics.cpp`). On the OTLP side this information
  // belongs on the resource, so it does not have to be repeated for every
  // metric and span.
  return resource_sdk::ResourceAttributes{
      {"qlever.compiler", *qlever::version::compilerWithoutLinking.rlock()},
      {"qlever.compiler_version",
       *qlever::version::compilerVersionWithoutLinking.rlock()},
      {semconv::service::kServiceVersion,
       *qlever::version::projectVersionWithoutLinking.rlock()},
      {"qlever.git_hash", *qlever::version::gitShortHashWithoutLinking.rlock()},
      {"qlever.compile_time",
       *qlever::version::timeOfCompilationUnixWithoutLinking.rlock()},
      {"qlever.cxx_standard",
       *qlever::version::cxxStandardWithoutLinking.rlock()}};
}

// _____________________________________________________________________________
resource_sdk::Resource detail::sharedResourceImpl() {
  return resource_sdk::Resource::GetDefault()
      // `service.name` is a required attribute. Provide a default, which is
      // overwritten by a user-provided value (see `OTELResourceDetector`).
      .Merge(resource_sdk::Resource(
          {{semconv::service::kServiceName, DEFAULT_SERVICE_NAME}}))
      .Merge(resource_sdk::OTELResourceDetector().Detect())
      .Merge(resource_sdk::Resource(qleverResourceAttributes()));
}

}  // namespace ad_utility::metrics
