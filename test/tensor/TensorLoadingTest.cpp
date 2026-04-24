// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>
#include <gmock/gmock.h>
#include <unicode/uchar.h>

#include "../util/GTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "TensorTestHelpers.h"
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
TEST(TensorParse, TensorLoadIRI) {
  // Invalid literal
  auto nonData = "\"Example non-data literal\"@en";
  auto invalidDatatype =
      R"("{\"data\":[1.0,2.0,3.0],\"shape\":[3],\"type":\"invalid_type\"}")"
      "^^<https://w3id.org/rdf-tensor/datatypes#DataTensor>";
  auto invalidIriDatatype =
      R"("{\"data\":[1.0,2.0,3.0],\"shape\":[3],\"type":\"invalid_type\"}")"
      "^^<https://w3id.org/rdf-tensor/wrong-data>";
  auto invalidData = R"("{\"data\":[1.0,2.0,3.0],\"shape":[3]}\"")"
                     "^^<https://w3id.org/rdf-tensor/datatypes#DataTensor>";
  std::vector<std::string> invalidLiterals{nonData, invalidDatatype,
                                           invalidData, invalidIriDatatype};
  for (auto literal : invalidLiterals) {
    auto tensor = TensorData::fromLiteral(literal);
    EXPECT_EQ(tensor, std::nullopt);
  }
  auto tensor = TensorData({1.0f, 2.0f, 3.0f}, {3}, TensorData::DType::FLOAT);
  auto validData =  // Valid Vector Basis
      R"("{\"data\":[1.0,2.0,3.0],\"shape\":[3],\"type":\"float32\"}")"
      "^^<https://w3id.org/rdf-tensor/datatypes#DataTensor>";
  auto parsedTensor = TensorData::fromLiteral(validData);
  EXPECT_TENSOR_DATA(parsedTensor, tensor);
}
// _____________________________________________________________________________
TEST(TensorParse, TensorConstruction) {
  auto tensor_from_string = TensorData::parseFromString(
      R"({"data":[1.0,2.0,3.0],"shape":[3],"type":"float32"})");
  auto tensor = TensorData({1.0f, 2.0f, 3.0f}, {3}, TensorData::DType::FLOAT);
  EXPECT_TENSOR_DATA(tensor, tensor_from_string);

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
struct SerializeDeserializeParam {
  std::size_t numElements;
  TensorData::DType dtype;
};

class TensorSerialization
    : public ::testing::TestWithParam<SerializeDeserializeParam> {};
TEST_P(TensorSerialization, SerializeDeserialize) {
  auto param = GetParam();
  std::vector<float> data(param.numElements);
  for (size_t i = 0; i < data.size(); i++) {
    data[i] = static_cast<float>(i);
  }
  auto tensor = TensorData(std::move(data), {param.numElements}, param.dtype);
  auto buffer = tensor.serialize();
  auto deserialized_tensor = TensorData::deserialize(buffer);
  EXPECT_TENSOR_DATA(tensor, deserialized_tensor);
}
INSTANTIATE_TEST_SUITE_P(
    TensorParseSerialization, TensorSerialization,
    ::testing::Values(SerializeDeserializeParam{0, TensorData::DType::FLOAT},
                      SerializeDeserializeParam{512, TensorData::DType::FLOAT},
                      SerializeDeserializeParam{128, TensorData::DType::DOUBLE})

);
}  // namespace
