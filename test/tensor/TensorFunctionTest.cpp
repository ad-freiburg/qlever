// Copyright 2026, Technical University of Graz, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>
#include <gmock/gmock.h>
#include <unicode/uchar.h>

#include "../util/GTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "rdfTypes/TensorData.h"
namespace {
using namespace ad_utility;
using namespace std::literals;

auto generateIdentityTensors(size_t numTensors, size_t dimension, float value = 1.0f) {
  std::vector<TensorData> tensors;
  for (size_t i = 0; i < numTensors; ++i) {
    std::vector<float> data(dimension, 0.0f);
    data[i % dimension] = value;  // create an identity-like tensor
    tensors.emplace_back(data, std::vector<size_t>{dimension},
                         TensorData::DType::FLOAT);
  }
  return tensors;
}

// _____________________________________________________________________________
TEST(TensorFunction, CosineSimilarity) {
  auto tensors = generateIdentityTensors(3, 3);
  EXPECT_FLOAT_EQ(TensorData::cosineSimilarity(tensors[0], tensors[0]), 1.0f);
  EXPECT_FLOAT_EQ(TensorData::cosineSimilarity(tensors[0], tensors[1]), 0.0f);
  EXPECT_FLOAT_EQ(TensorData::cosineSimilarity(tensors[1], tensors[2]), 0.0f);
}
TEST(TensorFunction, DotProduct) {
  auto tensors = generateIdentityTensors(3, 3);
  EXPECT_FLOAT_EQ(TensorData::dot(tensors[0], tensors[0]), 1.0f);
  EXPECT_FLOAT_EQ(TensorData::dot(tensors[0], tensors[1]), 0.0f);
  EXPECT_FLOAT_EQ(TensorData::dot(tensors[1], tensors[2]), 0.0f);
}
TEST(TensorFunction, Norm) {
  auto tensors = generateIdentityTensors(3, 3, 2.0f);
  EXPECT_FLOAT_EQ(TensorData::norm(tensors[0]), 2.0f);
  EXPECT_FLOAT_EQ(TensorData::norm(tensors[1]), 2.0f);
  EXPECT_FLOAT_EQ(TensorData::norm(tensors[2]), 2.0f);
}
}  // anonymous namespace
