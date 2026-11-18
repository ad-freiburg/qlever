// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>
#include <gmock/gmock.h>
#include <unicode/uchar.h>

#include "engine/tensor/TensorIndex.h"
#include <absl/cleanup/cleanup.h>

#include "../util/GTestHelpers.h"
#include "../util/RuntimeParametersTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "global/RuntimeParameters.h"
#include "rdfTypes/TensorData.h"
namespace {
using namespace ad_utility;
using namespace std::literals;
TEST(TensorPerformance, BigTensorInnerProduct) {
  // Use a single thread for this test. At the end of the test, the runtime
  // parameter is restored first, and then the global (BLAS) thread settings
  // are reinitialized from it (cleanups run in reverse order of declaration).
  absl::Cleanup reinitialize{
      [] { TensorIndexImpl::initializeGlobalRuntimeParameters(); }};
  auto restoreNumThreads = setRuntimeParameterForTest<
      &RuntimeParameters::tensorIndexMaxNumThreads_>(size_t{1});
  TensorIndexImpl::initializeGlobalRuntimeParameters();
  constexpr size_t N = 512;
  std::vector<std::string> vectors;
  for (size_t i = 0; i < N; i++) {
    std::vector<float> data(N);
    for (size_t j = 0; j < data.size(); j++) {
      data[j] = i == j ? 1 : 0;
    }
    std::string long_str = R"({"data":[)";
    for (size_t j = 0; j < data.size(); j++) {
      long_str += std::to_string(data[j]);
      if (j != data.size() - 1) {
        long_str += ",";
      }
    }
    long_str += absl::StrCat(R"(],"shape":[)", N, R"(],"type":"float32"})");
    vectors.push_back(long_str);
  }
  std::vector<TensorData> tensors;
  tensors.reserve(N);
  for (const auto& str : vectors) {
    tensors.push_back(TensorData::parseFromString(str));
  }
  for (size_t i = 0; i < N; i++) {
    for (size_t j = 0; j < N; j++) {
      const auto& vecA = tensors.at(i);
      const auto& vecB = tensors.at(j);
      auto sim = TensorData::cosineSimilarity(vecA, vecB);
      if (i != j) {
        EXPECT_FLOAT_EQ(sim, 0.0f);
      } else {
        EXPECT_FLOAT_EQ(sim, 1.0f);
      }
    }
  }
}
// Parse and print many large tensors (exercises the fast JSON path).
TEST(TensorPerformance, ParseAndPrintManyTensors) {
  constexpr size_t numTensors = 2000;
  constexpr size_t dim = 768;
  for (size_t i = 0; i < numTensors; ++i) {
    std::vector<float> data(dim);
    for (size_t j = 0; j < dim; ++j) {
      data[j] = static_cast<float>(i * dim + j) / 997.0f - 3.5f;
    }
    TensorData tensor{std::move(data)};
    auto parsed = TensorData::parseFromString(tensor.toJsonString());
    ASSERT_EQ(parsed, tensor);
    ASSERT_EQ(TensorData::deserialize(tensor.serialize()), tensor);
  }
}
}  // namespace
