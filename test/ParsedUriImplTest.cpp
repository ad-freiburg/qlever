//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <type_traits>

#include "util/GTestHelpers.h"
#include "util/ParsedUri.h"

using qlever::util::BoostParsedUri;
using qlever::util::ParsedUri;

template <typename T>
class ParsedUriImplTest : public ::testing::Test {};
// The type alias avoids passing a template with commas directly to the macro,
// which the C preprocessor would misinterpret as multiple arguments.
using ParsedUriImplementations = ::testing::Types<ParsedUri, BoostParsedUri>;
TYPED_TEST_SUITE(ParsedUriImplTest, ParsedUriImplementations);

// _____________________________________________________________________________
TYPED_TEST(ParsedUriImplTest, ConstructFromValidUri) {
  EXPECT_NO_THROW(TypeParam("https://example.com/path?q=1#frag"));
  EXPECT_NO_THROW(TypeParam("urn:isbn:0451450523"));
  EXPECT_NO_THROW(TypeParam("relative/path"));
  EXPECT_NO_THROW(TypeParam(""));
}

// _____________________________________________________________________________
TYPED_TEST(ParsedUriImplTest, EqualityOperator) {
  TypeParam a("https://example.com/path");
  TypeParam b("https://example.com/path");
  EXPECT_EQ(a, b);
  EXPECT_EQ(a, a);

  EXPECT_NE(a, TypeParam("http://example.com/path"));
  EXPECT_NE(a, TypeParam("https://other.com/path"));
  EXPECT_NE(a, TypeParam("https://example.com/other"));
  EXPECT_NE(a, TypeParam("https://example.com/path?q"));
  EXPECT_NE(a, TypeParam("https://example.com/path#f"));
}

// _____________________________________________________________________________
TYPED_TEST(ParsedUriImplTest, InequalityOperator) {
  EXPECT_TRUE(TypeParam("https://example.com/a") !=
              TypeParam("https://example.com/b"));
  EXPECT_FALSE(TypeParam("https://example.com/path") !=
               TypeParam("https://example.com/path"));
}

// _____________________________________________________________________________
TYPED_TEST(ParsedUriImplTest, ToIriString) {
  EXPECT_EQ(TypeParam("https://example.com/path").toIriString(),
            "<https://example.com/path>");
  EXPECT_EQ(TypeParam("urn:isbn:0451450523").toIriString(),
            "<urn:isbn:0451450523>");
  EXPECT_EQ(TypeParam("http://a/b/c/d;p?q").toIriString(),
            "<http://a/b/c/d;p?q>");
}

// _____________________________________________________________________________
TYPED_TEST(ParsedUriImplTest, ResolveUriRfc3986) {
  // RFC 3986 Section 5.4 normal resolution examples — same for both
  // implementations since no percent-encoded characters are involved.
  TypeParam base("http://a/b/c/d;p?q");
  EXPECT_EQ(base.resolveUri("g"), TypeParam("http://a/b/c/g"));
  EXPECT_EQ(base.resolveUri("g/"), TypeParam("http://a/b/c/g/"));
  EXPECT_EQ(base.resolveUri("/g"), TypeParam("http://a/g"));
  EXPECT_EQ(base.resolveUri("../g"), TypeParam("http://a/b/g"));
  EXPECT_EQ(base.resolveUri("../../g"), TypeParam("http://a/g"));
  EXPECT_EQ(base.resolveUri("http://other.com/path"),
            TypeParam("http://other.com/path"));
}

// _____________________________________________________________________________
TYPED_TEST(ParsedUriImplTest, ResolveRelativeBase) {
  if constexpr (std::is_same_v<TypeParam, ParsedUri>) {
    AD_EXPECT_THROW_WITH_MESSAGE(
        TypeParam("relative/base").resolveUri("path"),
        ::testing::HasSubstr("Is the base URI absolute?"));
  } else {
    // Boost.URL silently returns the base unchanged when it cannot resolve
    // against a relative base (no scheme available).
    EXPECT_EQ(TypeParam("relative/base").resolveUri("path").toIriString(),
              "<relative/base>");
  }
}

// _____________________________________________________________________________
TYPED_TEST(ParsedUriImplTest, PercentEncodingDuringResolution) {
  TypeParam base("http://example.com/abc/def/");

  // %2E%2E is a percent-encoded "..". ParsedUri treats it literally (no
  // decoding of encoded dot segments), while BoostParsedUri decodes it and
  // resolves the resulting dot segment.
  {
    auto result = base.resolveUri("%2E%2E/xyz");
    if constexpr (std::is_same_v<TypeParam, ParsedUri>) {
      EXPECT_EQ(result.toIriString(),
                "<http://example.com/abc/def/%2E%2E/xyz>");
    } else {
      EXPECT_EQ(result.toIriString(), "<http://example.com/abc/xyz>");
    }
  }

  // Lowercase hex digits in percent-encoding: ParsedUri preserves the original
  // case, while BoostParsedUri normalizes them to uppercase (%c3 -> %C3).
  {
    auto result = base.resolveUri("/%c3%A4");
    if constexpr (std::is_same_v<TypeParam, ParsedUri>) {
      EXPECT_EQ(result.toIriString(), "<http://example.com/%c3%A4>");
    } else {
      EXPECT_EQ(result.toIriString(), "<http://example.com/%C3%A4>");
    }
  }
}
