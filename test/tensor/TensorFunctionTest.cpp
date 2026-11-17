// Copyright 2026, Graz Technical University, University of Padova
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
TEST(TensorFunction, EuclideanDistance) {
  auto tensors = generateIdentityTensors(3, 3, 3.0f);
  EXPECT_FLOAT_EQ(TensorData::euclideanDistance(tensors[0], tensors[0]), 0.0f);
  EXPECT_FLOAT_EQ(TensorData::euclideanDistance(tensors[0], tensors[1]),
                  std::sqrt(18.0f));
}
TEST(TensorFunction, CosineSimilarityZeroVector) {
  auto tensors = generateIdentityTensors(1, 3);
  auto zero = TensorData::zeros_like(tensors[0]);
  EXPECT_EQ(zero.shape(), tensors[0].shape());
  EXPECT_EQ(TensorData::cosineSimilarity(tensors[0], zero), 0.0f);
}
TEST(TensorFunction, Arithmetic) {
  auto tensors = generateIdentityTensors(2, 2, 2.0f);
  auto sum = tensors[0] + tensors[1];
  EXPECT_THAT(sum.tensorData(), ::testing::ElementsAre(2.0f, 2.0f));
  auto diff = tensors[0] - tensors[1];
  EXPECT_THAT(diff.tensorData(), ::testing::ElementsAre(2.0f, -2.0f));
  auto prod = tensors[0] * tensors[1];
  EXPECT_THAT(prod.tensorData(), ::testing::ElementsAre(0.0f, 0.0f));
  auto scaled = TensorData::scale(sum, 0.5f);
  EXPECT_THAT(scaled.tensorData(), ::testing::ElementsAre(1.0f, 1.0f));
  sum.addInPlace(tensors[0]).scaleInPlace(2.0f);
  EXPECT_THAT(sum.tensorData(), ::testing::ElementsAre(8.0f, 4.0f));
  EXPECT_THROW(TensorData::add(tensors[0], TensorData({1.0f, 2.0f, 3.0f})),
               std::runtime_error);
  EXPECT_THROW(TensorData::dot(tensors[0], TensorData({1.0f, 2.0f, 3.0f})),
               std::runtime_error);
}
}  // anonymous namespace
