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
  EXPECT_THROW(TensorData::parseFromString(
                   R"({"data":[1.0,2.0,3.0],"shape":[3],"type":0})"),
               std::runtime_error);  // invalid type
  EXPECT_THROW(
      TensorData::parseFromString(R"({"data":[1.0,2.0,3.0],"shape":[3]})"),
      std::runtime_error);  // missing type
  EXPECT_THROW(
      TensorData::parseFromString(R"({"data":[1.0,2.0,3.0],"type":"float32"})"),
      std::runtime_error);  // missing shape
}
}  // namespace
