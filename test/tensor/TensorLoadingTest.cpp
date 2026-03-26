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
// _____________________________________________________________________________
TEST(TensorParse, TensorDump) {
  auto tensor = TensorData({1.0f, 2.0f, 3.0f}, {3}, TensorData::DType::FLOAT);
  auto [tensorString, datatypeIri] = tensor.toString();
  EXPECT_THAT(tensorString,
              R"({"data":[1.0,2.0,3.0],"shape":[3],"type":"float32"})");
  EXPECT_THAT(datatypeIri, "https://w3id.org/rdf-tensor/datatypes#DataTensor");
}
// _____________________________________________________________________________
TEST(TensorParse, TensorConstruction) {
  auto tensor_from_string = TensorData::parseFromString(
      R"({"data":[1.0,2.0,3.0],"shape":[3],"type":"float32"})");
  auto tensor = TensorData({1.0f, 2.0f, 3.0f}, {3}, TensorData::DType::FLOAT);
  EXPECT_EQ(tensor_from_string[0], tensor[0]);
  EXPECT_EQ(tensor_from_string.size(), tensor.size());
  EXPECT_EQ(tensor_from_string.shape(), tensor.shape());
  EXPECT_EQ(tensor_from_string.dtype(), tensor.dtype());

  // also test that parsing an invalid string throws an error
  EXPECT_THROW(TensorData::parseFromString("invalid string"),
               std::runtime_error);
  std::string_view wrongType =
      R"({"data":[1.0,2.0,3.0],"shape":[3],"type":"invalid_type"})";
  EXPECT_THROW(TensorData::parseFromString(wrongType),
               std::runtime_error);  // invalid type
  std::string_view missingType = R"({"data":[1.0,2.0,3.0],"shape":[3]})";
  EXPECT_THROW(TensorData::parseFromString(missingType),
               std::runtime_error);  // missing type
  std::string_view missingShape = R"({"data":[1.0,2.0,3.0],"type":"float32"})";
  EXPECT_THROW(TensorData::parseFromString(missingShape),
               std::runtime_error);  // missing shape
}
TEST(TensorParse, BigTensorConstruction) {
  std::vector<float> data(1000);
  for (size_t i = 0; i < data.size(); i++) {
    data[i] = static_cast<float>(i);
  }
  std::string long_str = R"({"data":[)";
  for (size_t i = 0; i < data.size(); i++) {
    long_str += std::to_string(data[i]);
    if (i != data.size() - 1) {
      long_str += ",";
    }
  }
  long_str += R"(],"shape":[1000],"type":"float32"})";
  auto tensor_from_string = TensorData::parseFromString(long_str);
}
}  // namespace
