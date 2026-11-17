// Copyright 2026, Graz Technical University, University of Padova
// Institute for Visual Computing, Department of Information Engineering
// Authors: Benedikt Kantz <benedikt.kantz@tugraz.at>

// Unit tests for `ad_utility::TensorData`.

#include <gmock/gmock.h>

#include <bit>
#include <cmath>
#include <cstring>
#include <sstream>
#include <limits>
#include <random>

#include "../util/GTestHelpers.h"
#include "rdfTypes/TensorData.h"

namespace {
using namespace ad_utility;
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::HasSubstr;
using ::testing::NanSensitiveFloatEq;
using ::testing::Optional;
using ::testing::Pointwise;
using DType = TensorData::DType;

constexpr std::string_view tensorType =
    "https://w3id.org/rdf-tensor/datatypes#DataTensor";
constexpr std::string_view numericTensorType =
    "https://w3id.org/rdf-tensor/datatypes#NumericDataTensor";

// Matcher for exact equality up to NaN-sensitive float comparison.
auto tensorEq(const TensorData& expected) {
  using namespace ::testing;
  return AllOf(Property(&TensorData::dtype, Eq(expected.dtype())),
               Property(&TensorData::shape, Eq(expected.shape())),
               Property(&TensorData::tensorData,
                        Pointwise(NanSensitiveFloatEq(), expected.tensorData())));
}

TensorData vec(std::vector<float> v) { return TensorData{std::move(v)}; }

std::vector<float> randomVector(size_t n, std::mt19937& gen) {
  std::uniform_real_distribution<float> dist{-10.0f, 10.0f};
  std::vector<float> v(n);
  for (auto& x : v) {
    x = dist(gen);
  }
  return v;
}

// Reference implementations in double precision.
double refDot(const std::vector<float>& a, const std::vector<float>& b) {
  double s = 0;
  for (size_t i = 0; i < a.size(); ++i) s += double(a[i]) * double(b[i]);
  return s;
}
double refNorm(const std::vector<float>& a) { return std::sqrt(refDot(a, a)); }
double refEuclid(const std::vector<float>& a, const std::vector<float>& b) {
  double s = 0;
  for (size_t i = 0; i < a.size(); ++i) {
    double d = double(a[i]) - double(b[i]);
    s += d * d;
  }
  return std::sqrt(s);
}

// _____________________________________________________________________________
TEST(TensorData, Construction) {
  TensorData empty;
  EXPECT_EQ(empty.size(), 0u);
  EXPECT_TRUE(empty.empty());
  EXPECT_THAT(empty.shape(), ElementsAre(0u));
  EXPECT_EQ(empty.dtype(), DType::FLOAT);

  auto t = vec({1, 2, 3});
  EXPECT_THAT(t.shape(), ElementsAre(3u));
  EXPECT_EQ(t.numDimensions(), 1u);
  EXPECT_EQ(t.dtype(), DType::FLOAT);
  EXPECT_FLOAT_EQ(t[1], 2.0f);
  EXPECT_EQ(t.span().size(), 3u);

  TensorData m{{1, 2, 3, 4, 5, 6}, {2, 3}, DType::DOUBLE};
  EXPECT_THAT(m.shape(), ElementsAre(2u, 3u));
  EXPECT_EQ(m.numDimensions(), 2u);
  EXPECT_EQ(m.dtype(), DType::DOUBLE);

  EXPECT_EQ(t, vec({1, 2, 3}));
  EXPECT_NE(t, vec({1, 2, 4}));
  EXPECT_NE(t, (TensorData{{1, 2, 3}, {3}, DType::DOUBLE}));
  EXPECT_NE(t, (TensorData{{1, 2, 3}, {1, 3}, DType::FLOAT}));

  auto data = std::move(t).releaseData();
  EXPECT_THAT(data, ElementsAre(1.0f, 2.0f, 3.0f));
}

// _____________________________________________________________________________
TEST(TensorData, DtypeNames) {
  for (auto d : {DType::FLOAT, DType::DOUBLE, DType::BOOL, DType::INT}) {
    EXPECT_THAT(TensorData::dtypeFromString(TensorData::dtypeToString(d)),
                Optional(d));
  }
  EXPECT_EQ(TensorData::dtypeToString(DType::FLOAT), "float32");
  EXPECT_EQ(TensorData::dtypeToString(DType::DOUBLE), "float64");
  EXPECT_EQ(TensorData::dtypeToString(DType::BOOL), "bool");
  EXPECT_EQ(TensorData::dtypeToString(DType::INT), "int64");
  EXPECT_EQ(TensorData::dtypeFromString("float"), std::nullopt);
  EXPECT_EQ(TensorData::dtypeFromString(""), std::nullopt);
}

// _____________________________________________________________________________
TEST(TensorData, DtypePromotion) {
  using enum TensorData::DType;
  EXPECT_EQ(TensorData::promoteDtypes(FLOAT, FLOAT), FLOAT);
  EXPECT_EQ(TensorData::promoteDtypes(FLOAT, DOUBLE), DOUBLE);
  EXPECT_EQ(TensorData::promoteDtypes(INT, FLOAT), FLOAT);
  EXPECT_EQ(TensorData::promoteDtypes(INT, INT), INT);
  EXPECT_EQ(TensorData::promoteDtypes(BOOL, BOOL), INT);
  EXPECT_EQ(TensorData::promoteDtypes(BOOL, INT), INT);
  EXPECT_EQ(TensorData::promoteDtypes(INT, INT, true), FLOAT);
  EXPECT_EQ(TensorData::promoteDtypes(DOUBLE, INT, true), DOUBLE);
}

// _____________________________________________________________________________
TEST(TensorData, ToString) {
  auto [json, type] = vec({1, 2.5f, -3}).toString();
  EXPECT_EQ(json, R"({"data":[1.0,2.5,-3.0],"shape":[3],"type":"float32"})");
  EXPECT_EQ(type, tensorType);

  // Shortest round-trip representation of floats.
  EXPECT_EQ(vec({0.1f, 1e-7f, 3e20f}).toJsonString(),
            R"({"data":[0.1,1e-07,3e+20],"shape":[3],"type":"float32"})");

  // Non-finite values.
  float inf = std::numeric_limits<float>::infinity();
  float nan = std::numeric_limits<float>::quiet_NaN();
  EXPECT_EQ(vec({nan, inf, -inf}).toJsonString(),
            R"({"data":[NaN,Infinity,-Infinity],"shape":[3],"type":"float32"})");

  // Dtype specific printing.
  EXPECT_EQ((TensorData{{1, 0, 1, 1}, {2, 2}, DType::BOOL}).toJsonString(),
            R"({"data":[true,false,true,true],"shape":[2,2],"type":"bool"})");
  EXPECT_EQ((TensorData{{1, -2, 3.5f}, {3}, DType::INT}).toJsonString(),
            R"({"data":[1,-2,3.5],"shape":[3],"type":"int64"})");
  EXPECT_EQ((TensorData{{1, 0.5f}, {1, 2}, DType::DOUBLE}).toJsonString(),
            R"({"data":[1.0,0.5],"shape":[1,2],"type":"float64"})");

  EXPECT_EQ(TensorData{}.toJsonString(),
            R"({"data":[],"shape":[0],"type":"float32"})");
}

// _____________________________________________________________________________
TEST(TensorData, ToLiteral) {
  auto lit = vec({1, 2}).toLiteral();
  ASSERT_TRUE(lit.isLiteral());
  EXPECT_TRUE(lit.hasDatatype());
  EXPECT_EQ(lit.getDatatype(), tensorType);
  EXPECT_EQ(lit.getContent(),
            R"({"data":[1.0,2.0],"shape":[2],"type":"float32"})");
  // The literal can be parsed again.
  EXPECT_THAT(TensorData::fromLiteral(lit.toStringRepresentation()),
              Optional(vec({1, 2})));
}

// _____________________________________________________________________________
TEST(TensorData, ParseAndPrintRoundTrip) {
  float inf = std::numeric_limits<float>::infinity();
  float nan = std::numeric_limits<float>::quiet_NaN();
  std::vector<TensorData> tensors{
      vec({1, 2, 3}),
      vec({0.1f, -1e-30f, 1e30f, 123456.789f, 3.14159265f}),
      vec({nan, inf, -inf, -0.0f}),
      TensorData{},
      TensorData{{1, 2, 3, 4, 5, 6}, {2, 3}, DType::FLOAT},
      TensorData{{1, 2, 3, 4, 5, 6, 7, 8}, {2, 2, 2}, DType::DOUBLE},
      TensorData{{1, 0, 0, 1}, {4}, DType::BOOL},
      TensorData{{1, -2, 1 << 20, 0}, {2, 2}, DType::INT},
      TensorData{{0.5f, 1.5f}, {2}, DType::INT},
      TensorData{{}, {0, 3}, DType::FLOAT},
      TensorData{{42}, {1, 1, 1}, DType::FLOAT},
  };
  std::mt19937 gen{42};
  tensors.push_back(vec(randomVector(1000, gen)));
  for (const auto& t : tensors) {
    auto json = t.toJsonString();
    EXPECT_THAT(TensorData::parseFromString(json), tensorEq(t)) << json;
    auto opt = TensorData::tryParseFromString(json);
    ASSERT_TRUE(opt.has_value());
    EXPECT_THAT(opt.value(), tensorEq(t));
    auto lit = t.toLiteral();
    auto fromLit = TensorData::fromLiteral(lit.toStringRepresentation());
    ASSERT_TRUE(fromLit.has_value()) << json;
    EXPECT_THAT(fromLit.value(), tensorEq(t));
  }
}

// _____________________________________________________________________________
TEST(TensorData, ParseVariants) {
  auto expected = vec({1, 2, 3});
  // Whitespace, different key order, integer and exponent notation.
  for (std::string_view s : {
           R"({"data":[1,2,3],"shape":[3],"type":"float32"})",
           R"(  { "type" : "float32" , "shape" : [ 3 ] , "data" : [ 1.0 , 2e0 , 0.3E1 ] }  )",
           "{\n\t\"data\": [1, 2, 3],\r\n \"shape\": [3], \"type\": "
           "\"float32\"\n}",
           // Trailing content after the object is ignored.
           R"({"data":[1,2,3],"shape":[3],"type":"float32"} trailing)",
           // Extra keys or escape sequences in keys trigger the slow path.
           R"({"data":[1,2,3],"shape":[3],"type":"float32","extra":{"a":[1]}})",
           R"({"data":[1,2,3],"shape":[3],"type":"float32"})",
       }) {
    EXPECT_THAT(TensorData::parseFromString(s), tensorEq(expected)) << s;
  }

  // Nested data is flattened (row-major).
  EXPECT_THAT(TensorData::parseFromString(
                  R"({"data":[[1,2,3],[4,5,6]],"shape":[2,3],"type":"float32"})"),
              tensorEq(TensorData{{1, 2, 3, 4, 5, 6}, {2, 3}, DType::FLOAT}));
  // Nested data in the slow path.
  EXPECT_THAT(
      TensorData::parseFromString(
          R"({"x":1,"data":[[1,2],[3,4]],"shape":[2,2],"type":"float64"})"),
      tensorEq(TensorData{{1, 2, 3, 4}, {2, 2}, DType::DOUBLE}));

  // Booleans.
  EXPECT_THAT(TensorData::parseFromString(
                  R"({"data":[true,false],"shape":[2],"type":"bool"})"),
              tensorEq(TensorData{{1, 0}, {2}, DType::BOOL}));
  EXPECT_THAT(TensorData::parseFromString(
                  R"({"data":[true,false],"shape":[2],"type":"bool","x":0})"),
              tensorEq(TensorData{{1, 0}, {2}, DType::BOOL}));

  // Out of range for float: rounded to infinity or zero.
  auto big = TensorData::parseFromString(
      R"({"data":[1e300,-1e300,1e-300],"shape":[3],"type":"float64"})");
  EXPECT_EQ(big[0], std::numeric_limits<float>::infinity());
  EXPECT_EQ(big[1], -std::numeric_limits<float>::infinity());
  EXPECT_EQ(big[2], 0.0f);

  // Empty tensor.
  EXPECT_THAT(
      TensorData::parseFromString(R"({"data":[],"shape":[0],"type":"float32"})"),
      tensorEq(TensorData{}));
}

// _____________________________________________________________________________
TEST(TensorData, ParseErrors) {
  auto expectError = [](std::string_view s, std::string_view messagePart,
                        ad_utility::source_location l =
                            ad_utility::source_location::current()) {
    auto trace = generateLocationTrace(l);
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(TensorData::parseFromString(s),
                                          HasSubstr(messagePart),
                                          std::runtime_error);
    EXPECT_EQ(TensorData::tryParseFromString(s), std::nullopt);
  };
  expectError("invalid string", "Failed to parse tensor data");
  expectError("", "Failed to parse tensor data");
  expectError(R"({"data":[1,2,3],"shape":[3],"type":"float32")",
              "Failed to parse tensor data");
  expectError(R"([1,2,3])", "not a JSON object");
  expectError(R"({"data":[1.0,2.0,3.0],"shape":[3]})", "Missing required");
  expectError(R"({"data":[1.0,2.0,3.0],"type":"float32"})", "Missing required");
  expectError(R"({"shape":[3],"type":"float32"})", "Missing required");
  expectError(R"({"data":[1,2,3],"shape":[3],"type":"invalid_type"})",
              "Unknown dtype");
  expectError(R"({"data":[1,2,3],"shape":[3],"type":3})", "not a string");
  expectError(R"({"data":"abc","shape":[3],"type":"float32"})",
              "not an array");
  expectError(R"({"data":[1,2,3],"shape":3,"type":"float32"})",
              "not an array");
  expectError(R"({"data":[1,"a",3],"shape":[3],"type":"float32"})",
              "non-numeric");
  expectError(R"({"data":[1,null,3],"shape":[3],"type":"float32"})",
              "non-numeric");
  expectError(R"({"data":[1,2,3],"shape":[-3],"type":"float32"})",
              "non-negative integer");
  expectError(R"({"data":[1,2,3],"shape":[1.5],"type":"float32"})",
              "non-negative integer");
  expectError(R"({"data":[1,2,3],"shape":[],"type":"float32"})",
              "Shape cannot be empty");
  expectError(R"({"data":[1,2,3],"shape":[4],"type":"float32"})",
              "does not match the shape");
  expectError(R"({"data":[1,2,3],"shape":[2,2],"type":"float32"})",
              "does not match the shape");
  expectError(R"({"data":[[1,2],[3]],"shape":[2,2],"type":"float32"})",
              "does not match the shape");
  // Overflow of the shape product.
  expectError(
      R"({"data":[1],"shape":[4294967296,4294967296,4294967296],"type":"float32"})",
      "does not match the shape");
  // Very long inputs are truncated in the error message.
  std::string longInput = R"({"data":[)";
  for (size_t i = 0; i < 10000; ++i) longInput += "1,";
  longInput += R"(1],"shape":[3],"type":"float32"})";
  try {
    TensorData::parseFromString(longInput);
    FAIL() << "Expected an exception";
  } catch (const std::runtime_error& e) {
    EXPECT_LT(std::string_view{e.what()}.size(), 2000u);
  }
}

// _____________________________________________________________________________
TEST(TensorData, FromLiteral) {
  auto lit = [](std::string_view content, std::string_view type) {
    return absl::StrCat("\"", content, "\"^^<", type, ">");
  };
  constexpr std::string_view valid =
      R"({"data":[1.0,2.0,3.0],"shape":[3],"type":"float32"})";
  auto expected = vec({1, 2, 3});
  EXPECT_THAT(TensorData::fromLiteral(lit(valid, tensorType)),
              Optional(expected));
  EXPECT_THAT(TensorData::fromLiteral(lit(valid, numericTensorType)),
              Optional(expected));
  // Escaped content is also accepted.
  EXPECT_THAT(
      TensorData::fromLiteral(lit(
          R"({\"data\":[1.0,2.0,3.0],\"shape\":[3],\"type\":\"float32\"})",
          tensorType)),
      Optional(expected));

  // Wrong datatype IRI, no datatype, language tag, not a literal.
  EXPECT_EQ(TensorData::fromLiteral(
                lit(valid, "https://w3id.org/rdf-tensor/wrong-data")),
            std::nullopt);
  EXPECT_EQ(TensorData::fromLiteral(
                lit(valid, "http://www.w3.org/2001/XMLSchema#string")),
            std::nullopt);
  EXPECT_EQ(TensorData::fromLiteral(absl::StrCat("\"", valid, "\"")),
            std::nullopt);
  EXPECT_EQ(TensorData::fromLiteral(absl::StrCat("\"", valid, "\"@en")),
            std::nullopt);
  EXPECT_EQ(TensorData::fromLiteral("<http://example.org/iri>"), std::nullopt);
  EXPECT_EQ(TensorData::fromLiteral(""), std::nullopt);
  EXPECT_EQ(TensorData::fromLiteral("\""), std::nullopt);
  EXPECT_EQ(TensorData::fromLiteral("\"abc"), std::nullopt);
  // Malformed content.
  EXPECT_EQ(TensorData::fromLiteral(lit("not json", tensorType)), std::nullopt);
  EXPECT_EQ(TensorData::fromLiteral(lit(
                R"({"data":[1,2,3],"shape":[2],"type":"float32"})", tensorType)),
            std::nullopt);
  EXPECT_EQ(TensorData::fromLiteral(lit(
                R"({"data":[1,2,3],"shape":[3],"type":"foo"})", tensorType)),
            std::nullopt);
}

// _____________________________________________________________________________
TEST(TensorData, ParseFromPair) {
  std::string valid = R"({"data":[1,2],"shape":[2],"type":"float32"})";
  using P = std::optional<std::pair<std::string, const char*>>;
  EXPECT_EQ(TensorData::parseFromPair(std::nullopt), std::nullopt);
  EXPECT_THAT(TensorData::parseFromPair(P{{valid, nullptr}}),
              Optional(vec({1, 2})));
  EXPECT_THAT(TensorData::parseFromPair(P{{valid, tensorType.data()}}),
              Optional(vec({1, 2})));
  EXPECT_THAT(TensorData::parseFromPair(P{{valid, numericTensorType.data()}}),
              Optional(vec({1, 2})));
  EXPECT_EQ(TensorData::parseFromPair(P{{valid, "http://other#type"}}),
            std::nullopt);
  EXPECT_THROW(TensorData::parseFromPair(P{{"garbage", tensorType.data()}}),
               std::runtime_error);
}

// _____________________________________________________________________________
TEST(TensorData, SerializeDeserialize) {
  float nan = std::numeric_limits<float>::quiet_NaN();
  std::mt19937 gen{7};
  std::vector<TensorData> tensors{
      TensorData{},
      vec({1, 2, 3}),
      vec({nan, -0.0f, std::numeric_limits<float>::infinity()}),
      TensorData{{1, 2, 3, 4, 5, 6}, {2, 3}, DType::DOUBLE},
      TensorData{{1, 0}, {2}, DType::BOOL},
      TensorData{{1, 2, 3, 4}, {2, 1, 2}, DType::INT},
      TensorData{{}, {3, 0}, DType::FLOAT},
      TensorData{{5}, {}, DType::FLOAT},
      vec(randomVector(4096, gen)),
  };
  for (const auto& t : tensors) {
    auto buffer = t.serialize();
    EXPECT_EQ(buffer.size(), t.serializedSize());
    EXPECT_EQ(buffer.size(), 1 + 8 + 8 * t.shape().size() + 4 * t.size());
    EXPECT_EQ(buffer[0], static_cast<uint8_t>(t.dtype()));
    EXPECT_THAT(TensorData::deserialize(buffer), tensorEq(t));
    EXPECT_THAT(TensorData::deserialize(ql::span<const uint8_t>{buffer}),
                tensorEq(t));
    std::vector<uint8_t> buffer2(t.serializedSize());
    t.serializeInto(buffer2);
    EXPECT_EQ(buffer, buffer2);
  }
}

// _____________________________________________________________________________
TEST(TensorData, SerializationFormatIsStable) {
  // The on-disk format of existing tensor vocabularies must stay byte
  // identical: [u8 dtype][u64 ndim][u64 shape...][f32 data...] in host byte
  // order (little endian on all supported platforms), dtype codes FLOAT=0,
  // DOUBLE=1, BOOL=2, INT=3. The buffers below are written by hand.
  static_assert(std::endian::native == std::endian::little);
  // float64 tensor of shape [2, 1] with data [1.5, -2.0].
  const std::vector<uint8_t> bytes{
      0x01,                                            // dtype DOUBLE
      0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // ndim = 2
      0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // shape[0] = 2
      0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // shape[1] = 1
      0x00, 0x00, 0xC0, 0x3F,                          // 1.5f
      0x00, 0x00, 0x00, 0xC0,                          // -2.0f
  };
  TensorData expected{{1.5f, -2.0f}, {2, 1}, DType::DOUBLE};
  EXPECT_THAT(TensorData::deserialize(bytes), tensorEq(expected));
  EXPECT_EQ(expected.serialize(), bytes);

  // float32 tensor of shape [3] with data [1, 0.5, 0] (the most common case).
  const std::vector<uint8_t> floatBytes{
      0x00,                                            // dtype FLOAT
      0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // ndim = 1
      0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,  // shape[0] = 3
      0x00, 0x00, 0x80, 0x3F,                          // 1.0f
      0x00, 0x00, 0x00, 0x3F,                          // 0.5f
      0x00, 0x00, 0x00, 0x00,                          // 0.0f
  };
  EXPECT_THAT(TensorData::deserialize(floatBytes), tensorEq(vec({1, 0.5f, 0})));
  EXPECT_EQ(vec({1, 0.5f, 0}).serialize(), floatBytes);

  // Empty default tensor (shape [0]) and the BOOL / INT dtype codes.
  const std::vector<uint8_t> emptyBytes{
      0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  };
  EXPECT_THAT(TensorData::deserialize(emptyBytes), tensorEq(TensorData{}));
  EXPECT_EQ(TensorData{}.serialize(), emptyBytes);
  EXPECT_EQ((TensorData{{1}, {1}, DType::BOOL}).serialize()[0], 2u);
  EXPECT_EQ((TensorData{{1}, {1}, DType::INT}).serialize()[0], 3u);
}

// _____________________________________________________________________________
TEST(TensorData, DeserializeErrors) {
  auto valid = vec({1, 2, 3}).serialize();
  auto expectThrow = [](const std::vector<uint8_t>& buffer,
                        std::string_view message,
                        ad_utility::source_location l =
                            ad_utility::source_location::current()) {
    auto trace = generateLocationTrace(l);
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(TensorData::deserialize(buffer),
                                          HasSubstr(message),
                                          std::runtime_error);
  };
  expectThrow({}, "too small");
  expectThrow({0, 1, 0}, "too small");
  // Invalid dtype.
  auto b = valid;
  b[0] = 17;
  expectThrow(b, "Invalid dtype");
  // Too many dimensions for the buffer (also guards against overflow).
  b = valid;
  uint64_t hugeDims = std::numeric_limits<uint64_t>::max();
  std::memcpy(b.data() + 1, &hugeDims, 8);
  expectThrow(b, "too small to deserialize TensorData shape");
  // Data that is not a multiple of 4 bytes.
  b = valid;
  b.push_back(0);
  expectThrow(b, "multiple of float size");
  // Shape that doesn't match the data.
  b = valid;
  b.resize(b.size() - 4);
  expectThrow(b, "does not match its shape");
}

// _____________________________________________________________________________
TEST(TensorData, ElementwiseArithmetic) {
  auto a = vec({1, 2, 3});
  auto b = vec({4, 5, 6});
  EXPECT_EQ(TensorData::add(a, b), vec({5, 7, 9}));
  EXPECT_EQ(TensorData::subtract(a, b), vec({-3, -3, -3}));
  EXPECT_EQ(TensorData::multiply(a, b), vec({4, 10, 18}));
  EXPECT_THAT(TensorData::divide(a, b),
              tensorEq(vec({1.0f / 4, 2.0f / 5, 3.0f / 6})));
  EXPECT_EQ(TensorData::scale(a, 2.0f), vec({2, 4, 6}));
  EXPECT_EQ(a + b, vec({5, 7, 9}));
  EXPECT_EQ(a - b, vec({-3, -3, -3}));
  EXPECT_EQ(a * b, vec({4, 10, 18}));
  EXPECT_THAT(a / b, tensorEq(vec({1.0f / 4, 2.0f / 5, 3.0f / 6})));
  // The inputs are unchanged.
  EXPECT_EQ(a, vec({1, 2, 3}));
  EXPECT_EQ(b, vec({4, 5, 6}));

  // Rvalue operators.
  EXPECT_EQ(vec({1, 2, 3}) + b, vec({5, 7, 9}));
  EXPECT_EQ(vec({1, 2, 3}) - b, vec({-3, -3, -3}));
  EXPECT_EQ(vec({1, 2, 3}) * b, vec({4, 10, 18}));
  EXPECT_THAT(vec({1, 2, 3}) / b,
              tensorEq(vec({1.0f / 4, 2.0f / 5, 3.0f / 6})));

  // Operators on `std::optional`.
  std::optional<TensorData> optA = a;
  std::optional<TensorData> none;
  EXPECT_THAT(optA + b, Optional(vec({5, 7, 9})));
  EXPECT_THAT(optA - b, Optional(vec({-3, -3, -3})));
  EXPECT_THAT(optA * b, Optional(vec({4, 10, 18})));
  EXPECT_TRUE((optA / b).has_value());
  EXPECT_EQ(none + b, std::nullopt);
  EXPECT_EQ(none - b, std::nullopt);
  EXPECT_EQ(none * b, std::nullopt);
  EXPECT_EQ(none / b, std::nullopt);

  // Multi-dimensional tensors keep their shape.
  TensorData m{{1, 2, 3, 4}, {2, 2}, DType::FLOAT};
  EXPECT_EQ(m + m, (TensorData{{2, 4, 6, 8}, {2, 2}, DType::FLOAT}));

  // Division by zero follows IEEE semantics.
  auto d = TensorData::divide(vec({1, -1, 0}), vec({0, 0, 0}));
  EXPECT_EQ(d[0], std::numeric_limits<float>::infinity());
  EXPECT_EQ(d[1], -std::numeric_limits<float>::infinity());
  EXPECT_TRUE(std::isnan(d[2]));

  // NaN propagates.
  float nan = std::numeric_limits<float>::quiet_NaN();
  EXPECT_TRUE(std::isnan(TensorData::add(vec({nan}), vec({1}))[0]));

  // Empty tensors.
  EXPECT_EQ(TensorData::add(TensorData{}, TensorData{}), TensorData{});
  EXPECT_EQ(TensorData::scale(TensorData{}, 3.0f), TensorData{});
}

// _____________________________________________________________________________
TEST(TensorData, ArithmeticDtypes) {
  TensorData i{{1, 2}, {2}, DType::INT};
  TensorData bo{{1, 0}, {2}, DType::BOOL};
  TensorData f{{1, 2}, {2}, DType::FLOAT};
  TensorData d{{1, 2}, {2}, DType::DOUBLE};
  EXPECT_EQ(TensorData::add(i, i).dtype(), DType::INT);
  EXPECT_EQ(TensorData::add(bo, bo).dtype(), DType::INT);
  EXPECT_EQ(TensorData::add(bo, bo), (TensorData{{2, 0}, {2}, DType::INT}));
  EXPECT_EQ(TensorData::multiply(i, f).dtype(), DType::FLOAT);
  EXPECT_EQ(TensorData::subtract(f, d).dtype(), DType::DOUBLE);
  EXPECT_EQ(TensorData::divide(i, i).dtype(), DType::FLOAT);
  EXPECT_EQ(TensorData::divide(d, i).dtype(), DType::DOUBLE);
  EXPECT_EQ(TensorData::scale(i, 0.5f).dtype(), DType::FLOAT);
  EXPECT_EQ(TensorData::scale(d, 0.5f).dtype(), DType::DOUBLE);
}

// _____________________________________________________________________________
TEST(TensorData, ShapeMismatch) {
  auto a = vec({1, 2, 3});
  auto b = vec({1, 2});
  TensorData m{{1, 2, 3}, {1, 3}, DType::FLOAT};
  auto expectThrow = [](auto f) {
    AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(f(), HasSubstr("different shapes"),
                                          std::runtime_error);
  };
  for (const auto& other : {b, m}) {
    expectThrow([&] { return TensorData::add(a, other); });
    expectThrow([&] { return TensorData::subtract(a, other); });
    expectThrow([&] { return TensorData::multiply(a, other); });
    expectThrow([&] { return TensorData::divide(a, other); });
    expectThrow([&] { return a + other; });
    expectThrow([&] { return TensorData::dot(a, other); });
    expectThrow([&] { return TensorData::cosineSimilarity(a, other); });
    expectThrow([&] { return TensorData::euclideanDistance(a, other); });
    expectThrow([&] {
      auto c = a;
      return c.addInPlace(other);
    });
    expectThrow([&] {
      auto c = a;
      return c.subtractInPlace(other);
    });
    expectThrow([&] {
      auto c = a;
      return c.multiplyInPlace(other);
    });
    expectThrow([&] {
      auto c = a;
      return c.divideInPlace(other);
    });
    expectThrow([&] { return binaryTensorMap(a, other, std::plus<>{}); });
  }
}

// _____________________________________________________________________________
TEST(TensorData, InPlace) {
  auto a = vec({1, 2, 3});
  auto b = vec({4, 5, 6});
  const float* storage = a.span().data();
  a.addInPlace(b);
  EXPECT_EQ(a, vec({5, 7, 9}));
  a.subtractInPlace(b);
  EXPECT_EQ(a, vec({1, 2, 3}));
  a.multiplyInPlace(b);
  EXPECT_EQ(a, vec({4, 10, 18}));
  a.divideInPlace(b);
  EXPECT_EQ(a, vec({1, 2, 3}));
  a.scaleInPlace(3.0f);
  EXPECT_EQ(a, vec({3, 6, 9}));
  a += b;
  EXPECT_EQ(a, vec({7, 11, 15}));
  a -= b;
  EXPECT_EQ(a, vec({3, 6, 9}));
  a *= b;
  EXPECT_EQ(a, vec({12, 30, 54}));
  a /= b;
  EXPECT_EQ(a, vec({3, 6, 9}));
  // No reallocation happened.
  EXPECT_EQ(a.span().data(), storage);

  // Aliasing: `a += a`.
  a += a;
  EXPECT_EQ(a, vec({6, 12, 18}));
  a *= a;
  EXPECT_EQ(a, vec({36, 144, 324}));

  // Chaining.
  auto c = vec({1, 1});
  c.addInPlace(vec({1, 1})).scaleInPlace(0.5f);
  EXPECT_EQ(c, vec({1, 1}));

  // Dtype update.
  TensorData i{{2, 4}, {2}, DType::INT};
  i.scaleInPlace(0.5f);
  EXPECT_EQ(i, (TensorData{{1, 2}, {2}, DType::FLOAT}));

  // `unaryTensorMap` on rvalues reuses the storage.
  auto u = vec({1, 4, 9});
  const float* uStorage = u.span().data();
  auto r = unaryTensorMap(std::move(u), [](float x) { return std::sqrt(x); });
  EXPECT_EQ(r, vec({1, 2, 3}));
  EXPECT_EQ(r.span().data(), uStorage);
  const auto constTensor = vec({1, 2});
  EXPECT_EQ(unaryTensorMap(constTensor, [](float x) { return -x; }),
            vec({-1, -2}));
  EXPECT_EQ(constTensor, vec({1, 2}));
}

// _____________________________________________________________________________
TEST(TensorData, ZerosLike) {
  TensorData m{{1, 2, 3, 4, 5, 6}, {2, 3}, DType::DOUBLE};
  EXPECT_EQ(TensorData::zeros_like(m),
            (TensorData{{0, 0, 0, 0, 0, 0}, {2, 3}, DType::DOUBLE}));
  EXPECT_EQ(TensorData::zeros_like(TensorData{}), TensorData{});
}

// _____________________________________________________________________________
TEST(TensorData, Reductions) {
  auto a = vec({1, 2, 3});
  auto b = vec({4, -5, 6});
  EXPECT_FLOAT_EQ(TensorData::dot(a, b), 12.0f);
  EXPECT_FLOAT_EQ(TensorData::norm(a), std::sqrt(14.0f));
  EXPECT_FLOAT_EQ(TensorData::euclideanDistance(a, b),
                  std::sqrt(9.0f + 49.0f + 9.0f));
  EXPECT_FLOAT_EQ(TensorData::cosineSimilarity(a, b),
                  12.0f / (std::sqrt(14.0f) * std::sqrt(77.0f)));
  EXPECT_FLOAT_EQ(TensorData::cosineSimilarity(a, a), 1.0f);
  EXPECT_FLOAT_EQ(TensorData::cosineSimilarity(a, TensorData::scale(a, -2)),
                  -1.0f);
  EXPECT_FLOAT_EQ(TensorData::euclideanDistance(a, a), 0.0f);

  // Zero vectors: the cosine similarity is defined as 0.
  auto zero = vec({0, 0, 0});
  EXPECT_EQ(TensorData::cosineSimilarity(a, zero), 0.0f);
  EXPECT_EQ(TensorData::cosineSimilarity(zero, zero), 0.0f);
  EXPECT_EQ(TensorData::norm(zero), 0.0f);
  EXPECT_EQ(TensorData::dot(a, zero), 0.0f);

  // Empty tensors.
  TensorData empty;
  EXPECT_EQ(TensorData::dot(empty, empty), 0.0f);
  EXPECT_EQ(TensorData::norm(empty), 0.0f);
  EXPECT_EQ(TensorData::euclideanDistance(empty, empty), 0.0f);
  EXPECT_EQ(TensorData::cosineSimilarity(empty, empty), 0.0f);

  // NaN propagates.
  float nan = std::numeric_limits<float>::quiet_NaN();
  auto withNan = vec({1, nan, 3});
  EXPECT_TRUE(std::isnan(TensorData::dot(a, withNan)));
  EXPECT_TRUE(std::isnan(TensorData::norm(withNan)));
  EXPECT_TRUE(std::isnan(TensorData::cosineSimilarity(a, withNan)));
  EXPECT_TRUE(std::isnan(TensorData::euclideanDistance(a, withNan)));

  // Large values: the product of the norms would overflow in float.
  auto huge = vec({1e19f, 1e19f});
  EXPECT_FLOAT_EQ(TensorData::cosineSimilarity(huge, huge), 1.0f);

  // Multi-dimensional tensors.
  TensorData m{{1, 0, 0, 1}, {2, 2}, DType::FLOAT};
  EXPECT_FLOAT_EQ(TensorData::dot(m, m), 2.0f);
}

// _____________________________________________________________________________
// Compare the `TensorData` functions against double precision reference
// implementations on random data of various sizes (including sizes that are
// not multiples of the vectorization width and sizes above the internal
// threshold of 2^16 elements for parallelization).
class TensorDataRandom : public ::testing::TestWithParam<size_t> {};

TEST_P(TensorDataRandom, AgainstReference) {
  size_t n = GetParam();
  std::mt19937 gen{static_cast<uint32_t>(n)};
  auto a = randomVector(n, gen);
  auto b = randomVector(n, gen);
  // Tolerance that accounts for float accumulation (with BLAS).
  auto near = [n](double expected) {
    double tol = 1e-5 * std::max(1.0, std::abs(expected)) +
                 1e-6 * std::sqrt(static_cast<double>(n)) * 100;
    return ::testing::DoubleNear(expected, tol);
  };
  TensorData ta{a};
  TensorData tb{b};
  EXPECT_THAT(double(TensorData::dot(ta, tb)), near(refDot(a, b)));
  EXPECT_THAT(double(TensorData::norm(ta)), near(refNorm(a)));
  EXPECT_THAT(double(TensorData::euclideanDistance(ta, tb)),
              near(refEuclid(a, b)));
  double refCos = n == 0 ? 0.0 : refDot(a, b) / (refNorm(a) * refNorm(b));
  EXPECT_THAT(double(TensorData::cosineSimilarity(ta, tb)),
              ::testing::DoubleNear(refCos, 1e-5));
  if (n > 0) {
    EXPECT_THAT(double(TensorData::cosineSimilarity(ta, ta)),
                ::testing::DoubleNear(1.0, 1e-5));
  }

  // Elementwise operations, out of place and in place.
  auto check = [&](auto outOfPlace, auto inPlace, auto op) {
    TensorData result = outOfPlace(ta, tb);
    TensorData c = ta;
    inPlace(c, tb);
    ASSERT_EQ(result.size(), n);
    ASSERT_EQ(c.size(), n);
    for (size_t i = 0; i < n; ++i) {
      ASSERT_EQ(result[i], op(a[i], b[i])) << i;
      ASSERT_EQ(c[i], op(a[i], b[i])) << i;
    }
  };
  check(&TensorData::add, [](TensorData& x, const TensorData& y) { x += y; },
        std::plus<float>{});
  check(&TensorData::subtract,
        [](TensorData& x, const TensorData& y) { x -= y; },
        std::minus<float>{});
  check(&TensorData::multiply,
        [](TensorData& x, const TensorData& y) { x *= y; },
        std::multiplies<float>{});
  check(&TensorData::divide,
        [](TensorData& x, const TensorData& y) { x /= y; },
        std::divides<float>{});
  auto scaled = TensorData::scale(ta, 0.25f);
  auto c = ta;
  c.scaleInPlace(0.25f);
  for (size_t i = 0; i < n; ++i) {
    ASSERT_EQ(scaled[i], a[i] * 0.25f);
    ASSERT_EQ(c[i], a[i] * 0.25f);
  }
  // Aliasing (`t += t`) on large inputs.
  auto d = ta;
  d += d;
  for (size_t i = 0; i < n; ++i) {
    ASSERT_EQ(d[i], a[i] + a[i]);
  }
}

INSTANTIATE_TEST_SUITE_P(Sizes, TensorDataRandom,
                         ::testing::Values(0, 1, 2, 3, 7, 15, 16, 17, 31, 100,
                                           768, 1023, 1024, 1025, 4097, 65535,
                                           65536, 3 * 65536 + 5));

// _____________________________________________________________________________
TEST(TensorData, PrintTo) {
  std::stringstream s;
  PrintTo(vec({1, 2}), &s);
  EXPECT_EQ(s.str(), R"({"data":[1.0,2.0],"shape":[2],"type":"float32"})");
}

}  // namespace
