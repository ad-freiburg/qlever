//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "util/GTestHelpers.h"
#include "util/ParsedUri.h"

using qlever::util::ParsedUri;

// _____________________________________________________________________________
TEST(ParsedUri, ConstructFromValidUri) {
  EXPECT_NO_THROW(ParsedUri("https://example.com/path?q=1#frag"));
  EXPECT_NO_THROW(ParsedUri("urn:isbn:0451450523"));
  EXPECT_NO_THROW(ParsedUri("relative/path"));
  EXPECT_NO_THROW(ParsedUri(""));
}

// _____________________________________________________________________________
TEST(ParsedUri, EqualityOperator) {
  ParsedUri a("https://example.com/path");
  ParsedUri b("https://example.com/path");
  EXPECT_EQ(a, b);
  EXPECT_EQ(a, a);

  EXPECT_NE(a, ParsedUri("http://example.com/path"));
  EXPECT_NE(a, ParsedUri("https://other.com/path"));
  EXPECT_NE(a, ParsedUri("https://example.com/other"));
  EXPECT_NE(a, ParsedUri("https://example.com/path?q"));
  EXPECT_NE(a, ParsedUri("https://example.com/path#f"));
}

// _____________________________________________________________________________
TEST(ParsedUri, InequalityOperator) {
  EXPECT_TRUE(ParsedUri("https://example.com/a") !=
              ParsedUri("https://example.com/b"));
  EXPECT_FALSE(ParsedUri("https://example.com/path") !=
               ParsedUri("https://example.com/path"));
}

// _____________________________________________________________________________
TEST(ParsedUri, ToIriString) {
  EXPECT_EQ(ParsedUri("https://example.com/path").toIriString(),
            "<https://example.com/path>");
  EXPECT_EQ(ParsedUri("urn:isbn:0451450523").toIriString(),
            "<urn:isbn:0451450523>");
  EXPECT_EQ(ParsedUri("http://a/b/c/d;p?q").toIriString(),
            "<http://a/b/c/d;p?q>");
}

// _____________________________________________________________________________
TEST(ParsedUri, ResolveUriRfc3986) {
  // RFC 3986 Section 5.4 normal resolution examples.
  ParsedUri base("http://a/b/c/d;p?q");
  EXPECT_EQ(base.resolveUri("g"), ParsedUri("http://a/b/c/g"));
  EXPECT_EQ(base.resolveUri("g/"), ParsedUri("http://a/b/c/g/"));
  EXPECT_EQ(base.resolveUri("/g"), ParsedUri("http://a/g"));
  EXPECT_EQ(base.resolveUri("../g"), ParsedUri("http://a/b/g"));
  EXPECT_EQ(base.resolveUri("../../g"), ParsedUri("http://a/g"));
  EXPECT_EQ(base.resolveUri("http://other.com/path"),
            ParsedUri("http://other.com/path"));
}

// _____________________________________________________________________________
TEST(ParsedUri, ResolveRelativeBase) {
  AD_EXPECT_THROW_WITH_MESSAGE(
      ParsedUri("relative/base").resolveUri("path"),
      ::testing::HasSubstr("Is the base URI absolute?"));
}

// _____________________________________________________________________________
TEST(ParsedUri, PercentEncodingDuringResolution) {
  ParsedUri base("http://example.com/abc/def/");

  // %2E%2E is a percent-encoded "..". `ParsedUri` treats it literally (no
  // decoding of encoded dot segments).
  EXPECT_EQ(base.resolveUri("%2E%2E/xyz").toIriString(),
            "<http://example.com/abc/def/%2E%2E/xyz>");

  // Lowercase hex digits in percent-encoding are preserved as-is.
  EXPECT_EQ(base.resolveUri("/%c3%A4").toIriString(),
            "<http://example.com/%c3%A4>");
}
