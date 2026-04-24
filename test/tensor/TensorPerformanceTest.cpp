// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>
#include <gmock/gmock.h>
#include <unicode/uchar.h>

#include "../util/GTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "engine/TensorIndex.h"
#include "global/RuntimeParameters.h"
#include "rdfTypes/TensorData.h"
namespace {
using namespace ad_utility;
using namespace std::literals;
TEST(TensorPerformance, BigTensorInnerProduct) {
  setRuntimeParameter<&RuntimeParameters::tensorIndexMaxNumThreads_>(1);
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
  for (size_t i = 0; i < N; i++) {
    for (size_t j = 0; j < N; j++) {
      auto vecA = TensorData::parseFromString(vectors.at(i));
      auto vecB = TensorData::parseFromString(vectors.at(j));
      auto sim = TensorData::cosineSimilarity(vecA, vecB);
      if (i != j) {
        EXPECT_FLOAT_EQ(sim, 0.0f);
      } else {
        EXPECT_FLOAT_EQ(sim, 1.0f);
      }
    }
  }
}
}  // namespace
