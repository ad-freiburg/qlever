// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_METRICS_RESOURCE_H
#define QLEVER_SRC_UTIL_METRICS_RESOURCE_H

#include <opentelemetry/sdk/resource/resource.h>

namespace ad_utility::metrics {

constexpr std::string_view DEFAULT_SERVICE_NAME = "qlever";

// The identity shared by all OTEL providers so that a backend
// can tell that metrics and spans come from the same QLever instance.
//
// NOTE: Call only after `qlever::version::copyVersionInfo`.
const opentelemetry::sdk::resource::Resource& sharedResource();

namespace detail {
// The resource attributes that QLever itself contributes. Exposed for testing.
opentelemetry::sdk::resource::ResourceAttributes qleverResourceAttributes();
// Exposed for testing.
opentelemetry::sdk::resource::Resource sharedResourceImpl();
}  // namespace detail

}  // namespace ad_utility::metrics

#endif  // QLEVER_SRC_UTIL_METRICS_RESOURCE_H
