// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <opentelemetry/semconv/service_attributes.h>

#include <cstdlib>
#include <optional>
#include <string>

#include "util/Algorithm.h"
#include "util/GTestHelpers.h"
#include "util/metrics/Metrics.h"
#include "util/metrics/Resource.h"

namespace {
namespace semconv = opentelemetry::semconv;
namespace resource_sdk = opentelemetry::sdk::resource;

// RAII helper that sets an environment variable and unsets it again on
// destruction.
class ScopedEnvironmentVariable {
  std::string name_;

 public:
  ScopedEnvironmentVariable(std::string name, const std::string& value)
      : name_{std::move(name)} {
    setenv(name_.c_str(), value.c_str(), /*overwrite=*/1);
  }
  ~ScopedEnvironmentVariable() { unsetenv(name_.c_str()); }
};
}  // namespace

// _____________________________________________________________________________
TEST(Metrics, initialize) {
  EXPECT_EQ(ad_utility::metrics::initialize(false), nullptr);
  EXPECT_NE(ad_utility::metrics::initialize(true), nullptr);
}

namespace {
auto QLeverResourceAttributesExist() {
  std::vector<std::string> attrs = {
      "qlever.git_hash",         "qlever.compiler",
      "qlever.compiler_version", "qlever.cxx_standard",
      "qlever.compile_time",     semconv::service::kServiceVersion};
  return testing::AllOfArray(ad_utility::transform(attrs, [](auto& attr) {
    return testing::Contains(testing::Pair(
        attr,
        testing::VariantWith<std::string>(testing::Not(testing::IsEmpty()))));
  }));
}
};  // namespace

// _____________________________________________________________________________
TEST(Metrics, resourceAttributesContainBuildInformation) {
  // Check that all attributes exist. They don't contain the actual values as
  // `copyVersionInfo` hasn't been called.
  EXPECT_THAT(ad_utility::metrics::detail::qleverResourceAttributes(),
              QLeverResourceAttributesExist());
}

// _____________________________________________________________________________
TEST(Metrics, sharedResourceImpl) {
  auto expectSharedResourceImpl =
      [](std::optional<std::pair<std::string, std::string>> envVariable,
         const std::string& expectedServiceName =
             ad_utility::metrics::DEFAULT_SERVICE_NAME,
         ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
        auto trace = generateLocationTrace(l);
        std::optional<ScopedEnvironmentVariable> variable;
        if (envVariable.has_value()) {
          variable.emplace(envVariable.value().first,
                           envVariable.value().second);
        }
        auto attributes =
            ad_utility::metrics::detail::sharedResourceImpl().GetAttributes();
        EXPECT_THAT(attributes, QLeverResourceAttributesExist());
        // The service name is read by the OTEL sdk from the environment
        // variables. Only if no service name is set via `qlever` is injected.
        EXPECT_THAT(
            attributes,
            testing::Contains(testing::Pair(
                semconv::service::kServiceName,
                testing::VariantWith<std::string>(expectedServiceName))));
      };
  expectSharedResourceImpl(std::nullopt);
  expectSharedResourceImpl({{"OTEL_SERVICE_NAME", "my-qlever"}}, "my-qlever");
  expectSharedResourceImpl({{"OTEL_SERVICE_NAME", ""}});
  expectSharedResourceImpl(
      {{"OTEL_RESOURCE_ATTRIBUTES", "service.name=mo-qlever"}}, "mo-qlever");
  // The SDK doesn't trim whitespaces so this is not detected as `service.name`.
  expectSharedResourceImpl(
      {{"OTEL_RESOURCE_ATTRIBUTES",
        "deployment.environment=prod, service.name=ma-qlever"}});
  expectSharedResourceImpl({{"OTEL_RESOURCE_ATTRIBUTES",
                             "service.namespace=qlever,my.service.name=x"}});
}
