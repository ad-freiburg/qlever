
#include <gmock/gmock.h>
#include <unicode/uchar.h>

#include "../util/GTestHelpers.h"
#include "TensorTestHelpers.h"
#include "util/TensorData.h"
namespace {
using namespace ad_utility;
using namespace tensorTestHelpers;
// _____________________________________________________________________________
TEST(TensorQuery, TensorDump) {
  auto tensor = TensorData({1.0f, 2.0f, 3.0f}, {3}, TensorData::DType::FLOAT);
  auto [tensorString, datatypeIri] = tensor.toString();
  EXPECT_THAT(tensorString, R"({"data":[1.0,2.0,3.0],"shape":[3],"type":0})");
  EXPECT_THAT(datatypeIri, "tensor:DataTensor");
}
// _____________________________________________________________________________
TEST(TensorQuery, TensorConstruction) {
  auto tensor_from_string = TensorData::parseFromString(
      R"({"data":[1.0,2.0,3.0],"shape":[3],"type":0})");
  auto tensor = TensorData({1.0f, 2.0f, 3.0f}, {3}, TensorData::DType::FLOAT);
  EXPECT_EQ(tensor_from_string[0], tensor[0]);
  EXPECT_EQ(tensor_from_string.size(), tensor.size());
  EXPECT_EQ(tensor_from_string.shape(), tensor.shape());
  EXPECT_EQ(tensor_from_string.dtype(), tensor.dtype());
}
// _____________________________________________________________________________
TEST_F(TensorQueryTest, TensorQueryConstruction) {
  auto [qet, qec, parsed] = qlv().parseAndPlanQuery(R"(
      SELECT ?s (cosineSimilarity(?v, ("{\"data\":[1.0,2.0,3.0],\"shape\":[3],\"type\":0}")) AS ?sim) WHERE { 
      ?s <p1> ?v.
      }
      ORDER BY DESC(?similarity)      
      )");
  auto res = qet->getResult();
}
}  // namespace
