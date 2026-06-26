// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Sebastian Walter <swalter@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "global/Constants.h"
#include "rdfTypes/EmbeddingVector.h"

using ad_utility::parseFloatVectorArrayBody;
using ad_utility::parseFp32VectorLiteral;
using ::testing::ElementsAre;
using ::testing::Optional;

namespace {
// Wrap a vector body in the full serialized `fp32Vector` literal form, i.e.
// `"<body>"^^<...fp32Vector>`. `EMBEDDING_FP32_LITERAL_SUFFIX` already starts
// with the closing quote (`"^^<...>`), so only the opening quote is prepended.
std::string fp32Literal(std::string_view body) {
  return absl::StrCat("\"", body, EMBEDDING_FP32_LITERAL_SUFFIX);
}
}  // namespace

// _____________________________________________________________________________
TEST(EmbeddingVector, ParseBodyValid) {
  EXPECT_THAT(parseFloatVectorArrayBody("[1, 2, 3]"),
              Optional(ElementsAre(1.0f, 2.0f, 3.0f)));
  // Negative numbers and decimals (exactly representable in `float`).
  EXPECT_THAT(parseFloatVectorArrayBody("[0.5, -1.25, 3]"),
              Optional(ElementsAre(0.5f, -1.25f, 3.0f)));
  // A single element is a valid (one-dimensional) vector.
  EXPECT_THAT(parseFloatVectorArrayBody("[42]"), Optional(ElementsAre(42.0f)));
  // Scientific notation.
  EXPECT_THAT(parseFloatVectorArrayBody("[1e3, -2.5e-1]"),
              Optional(ElementsAre(1000.0f, -0.25f)));
}

// _____________________________________________________________________________
TEST(EmbeddingVector, ParseBodyWhitespaceTolerant) {
  EXPECT_THAT(parseFloatVectorArrayBody("  [ 1 ,\t2 ,\n 3 ]  "),
              Optional(ElementsAre(1.0f, 2.0f, 3.0f)));
}

// _____________________________________________________________________________
TEST(EmbeddingVector, ParseBodyEmptyArrayRejected) {
  // An embedding has dimension >= 1, so the empty array is not valid.
  EXPECT_EQ(parseFloatVectorArrayBody("[]"), std::nullopt);
  EXPECT_EQ(parseFloatVectorArrayBody("[  ]"), std::nullopt);
}

// _____________________________________________________________________________
TEST(EmbeddingVector, ParseBodyMalformedRejected) {
  EXPECT_EQ(parseFloatVectorArrayBody(""), std::nullopt);
  // Missing the enclosing brackets.
  EXPECT_EQ(parseFloatVectorArrayBody("1, 2, 3"), std::nullopt);
  // Not a JSON array.
  EXPECT_EQ(parseFloatVectorArrayBody("{}"), std::nullopt);
  EXPECT_EQ(parseFloatVectorArrayBody("42"), std::nullopt);
  // Trailing / doubled commas.
  EXPECT_EQ(parseFloatVectorArrayBody("[1, 2,]"), std::nullopt);
  EXPECT_EQ(parseFloatVectorArrayBody("[1,, 2]"), std::nullopt);
  // Trailing garbage after the array.
  EXPECT_EQ(parseFloatVectorArrayBody("[1, 2] extra"), std::nullopt);
}

// _____________________________________________________________________________
TEST(EmbeddingVector, ParseBodyNonNumericRejected) {
  EXPECT_EQ(parseFloatVectorArrayBody("[\"a\", \"b\"]"), std::nullopt);
  EXPECT_EQ(parseFloatVectorArrayBody("[true]"), std::nullopt);
  EXPECT_EQ(parseFloatVectorArrayBody("[null]"), std::nullopt);
  // Nested arrays / objects are not numbers.
  EXPECT_EQ(parseFloatVectorArrayBody("[[1], [2]]"), std::nullopt);
}

// _____________________________________________________________________________
TEST(EmbeddingVector, ParseBodyNonFiniteRejected) {
  // `NaN`/`Infinity` are not valid JSON and are rejected at parse time.
  EXPECT_EQ(parseFloatVectorArrayBody("[NaN]"), std::nullopt);
  EXPECT_EQ(parseFloatVectorArrayBody("[Infinity]"), std::nullopt);
  // A finite JSON number that overflows the `float` range becomes `inf` and is
  // rejected by the finiteness check.
  EXPECT_EQ(parseFloatVectorArrayBody("[1e40]"), std::nullopt);
}

// _____________________________________________________________________________
TEST(EmbeddingVector, ParseLiteralValid) {
  EXPECT_THAT(parseFp32VectorLiteral(fp32Literal("[1, 2, 3]")),
              Optional(ElementsAre(1.0f, 2.0f, 3.0f)));
  EXPECT_THAT(parseFp32VectorLiteral(fp32Literal("[-0.5]")),
              Optional(ElementsAre(-0.5f)));
}

// _____________________________________________________________________________
TEST(EmbeddingVector, ParseLiteralWrongShapeRejected) {
  // Missing the opening quote.
  EXPECT_EQ(parseFp32VectorLiteral(
                absl::StrCat("[1, 2, 3]", EMBEDDING_FP32_LITERAL_SUFFIX)),
            std::nullopt);
  // Wrong datatype suffix.
  EXPECT_EQ(parseFp32VectorLiteral(
                "\"[1, 2, 3]\"^^<http://www.w3.org/2001/XMLSchema#string>"),
            std::nullopt);
  // No datatype suffix at all (plain string literal).
  EXPECT_EQ(parseFp32VectorLiteral("\"[1, 2, 3]\""), std::nullopt);
  // Correct suffix but malformed body.
  EXPECT_EQ(parseFp32VectorLiteral(fp32Literal("[1, 2,]")), std::nullopt);
  EXPECT_EQ(parseFp32VectorLiteral(fp32Literal("[]")), std::nullopt);
}
