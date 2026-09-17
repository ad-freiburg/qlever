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
#include <string_view>

#include "util/GTestHelpers.h"
#include "util/metrics/Metrics.h"
#include "util/metrics/Resource.h"

namespace {
namespace semconv = opentelemetry::semconv;
namespace resource_sdk = opentelemetry::sdk::resource;

std::optional<std::string> getAttribute(
    const resource_sdk::ResourceAttributes& attributes, std::string_view key) {
  auto it = attributes.find(std::string{key});
  if (it == attributes.end()) {
    return std::nullopt;
  }
  return opentelemetry::nostd::get<std::string>(it->second);
}

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

// _____________________________________________________________________________
TEST(Metrics, resourceAttributesContainBuildInformation) {
  // Check that all attributes exist. They don't contain the actual values as
  // `copyVersionInfo` hasn't been called.
  auto attributes = ad_utility::metrics::detail::qleverResourceAttributes();
  for (std::string_view key :
       {"qlever.git_hash", "qlever.compiler", "qlever.compiler_version",
        "qlever.cxx_standard", "qlever.compile_time",
        semconv::service::kServiceVersion}) {
    EXPECT_THAT(getAttribute(attributes, key),
                testing::Optional(testing::Not(testing::IsEmpty())));
  }
}

// _____________________________________________________________________________
TEST(Metrics, hasServiceNameFromEnv) {
  using ad_utility::metrics::hasServiceNameFromEnv;
  using ad_utility::metrics::detail::qleverResourceAttributes;
  auto expectServiceName =
      [](std::optional<std::pair<std::string, std::string>> envVariable,
         bool expectedHasServiceName,
         ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
        auto trace = generateLocationTrace(l);
        std::optional<ScopedEnvironmentVariable> variable;
        if (envVariable.has_value()) {
          variable.emplace(envVariable.value().first,
                           envVariable.value().second);
        }
        EXPECT_THAT(hasServiceNameFromEnv(),
                    testing::Eq(expectedHasServiceName));
        // The service name is read by the OTEL sdk from the environment
        // variables. Only if no service name is set via `qlever` is injected.
        testing::Matcher<std::optional<std::string>> serviceNameMatcher =
            testing::Eq(std::nullopt);
        if (!expectedHasServiceName) {
          serviceNameMatcher = testing::Optional(std::string{"qlever"});
        }
        EXPECT_THAT(getAttribute(qleverResourceAttributes(),
                                 semconv::service::kServiceName),
                    serviceNameMatcher);
      };
  expectServiceName(std::nullopt, false);
  expectServiceName({{"OTEL_SERVICE_NAME", "my-qlever"}}, true);
  expectServiceName({{"OTEL_SERVICE_NAME", ""}}, false);
  expectServiceName({{"OTEL_RESOURCE_ATTRIBUTES", "service.name=mo-qlever"}},
                    true);
  expectServiceName({{"OTEL_RESOURCE_ATTRIBUTES",
                      "deployment.environment=prod, service.name=ma-qlever"}},
                    true);
  expectServiceName({{"OTEL_RESOURCE_ATTRIBUTES",
                      "service.namespace=qlever,my.service.name=x"}},
                    false);
}
