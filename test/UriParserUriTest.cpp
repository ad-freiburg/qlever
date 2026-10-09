//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "util/GTestHelpers.h"
#include "util/UriParserUri.h"

namespace {

using qlever::util::UriParserUri;

// Convert a UriParserUri back to its string representation.
std::string uriToString(const UriParserUri& uri) {
  int charsRequired;
  uriToStringCharsRequiredA(&uri.get(), &charsRequired);
  std::string result(charsRequired + 1, '\0');
  uriToStringA(result.data(), &uri.get(), charsRequired + 1, nullptr);
  result.resize(charsRequired);
  return result;
}

auto toStringView = &UriParserUri::toStringView;

}  // namespace

// _____________________________________________________________________________
TEST(UriParserUri, ConstructFromValidUri) {
  EXPECT_NO_THROW(UriParserUri("https://example.com/path?q=1#frag"));
  EXPECT_NO_THROW(UriParserUri("http://user:pass@example.com:8080/path"));
  EXPECT_NO_THROW(UriParserUri("urn:isbn:0451450523"));
  EXPECT_NO_THROW(UriParserUri("relative/path"));
  EXPECT_NO_THROW(UriParserUri(""));
}

// _____________________________________________________________________________
TEST(UriParserUri, ConstructFromInvalidUri) {
  AD_EXPECT_THROW_WITH_MESSAGE(
      UriParserUri("http://[invalid"),
      ::testing::HasSubstr("Failed to parse URI: http://[invalid"));
}

// _____________________________________________________________________________
TEST(UriParserUri, GetReturnsUnderlying) {
  UriParserUri uri("https://example.com:8080/path");
  const UriUriA& raw = uri.get();
  std::string_view scheme(raw.scheme.first,
                          raw.scheme.afterLast - raw.scheme.first);
  EXPECT_EQ(scheme, "https");
  std::string_view host(raw.hostText.first,
                        raw.hostText.afterLast - raw.hostText.first);
  EXPECT_EQ(host, "example.com");
  std::string_view port(raw.portText.first,
                        raw.portText.afterLast - raw.portText.first);
  EXPECT_EQ(port, "8080");
}

// _____________________________________________________________________________
TEST(UriParserUri, ParsedUriOwnsStringData) {
  {
    UriParserUri uri = [] {
      std::string uriStr = "https://example.com/path";
      return UriParserUri{uriStr};
    }();
    // The address sanitizer should catch if this comparison accesses invalid
    // memory.
    EXPECT_EQ(toStringView(uri.get().pathHead->text), "path");
  }
  {
    std::string_view uriStr = "https://example.com/path";
    UriParserUri uri{uriStr};
    // The pointers should differ.
    EXPECT_NE(uri.get().scheme.first, uriStr.data());
    // Sanity check.
    EXPECT_EQ(toStringView(uri.get().scheme), "https");
  }
}

// _____________________________________________________________________________
TEST(UriParserUri, EqualityOperator) {
  UriParserUri a("https://example.com/path");
  UriParserUri b("https://example.com/path");
  EXPECT_EQ(a, b);
  EXPECT_EQ(a, a);

  EXPECT_NE(a, UriParserUri("http://example.com/path"));
  EXPECT_NE(a, UriParserUri("https://other.com/path"));
  EXPECT_NE(a, UriParserUri("https://example.com/other"));
  EXPECT_NE(a, UriParserUri("https://example.com/path?q"));
  EXPECT_NE(a, UriParserUri("https://example.com/path#f"));
}

// _____________________________________________________________________________
TEST(UriParserUri, EqualityOperatorWithUserInfoAndPort) {
  EXPECT_EQ(UriParserUri("http://user@example.com/"),
            UriParserUri("http://user@example.com/"));
  EXPECT_NE(UriParserUri("http://user@example.com/"),
            UriParserUri("http://other@example.com/"));

  EXPECT_EQ(UriParserUri("http://example.com:80/"),
            UriParserUri("http://example.com:80/"));
  EXPECT_NE(UriParserUri("http://example.com:80/"),
            UriParserUri("http://example.com:8080/"));
}

// _____________________________________________________________________________
TEST(UriParserUri, EqualityOperatorAbsolutePath) {
  // URIs with and without authority differ in how absolutePath is set.
  EXPECT_NE(UriParserUri("/absolute/path"), UriParserUri("relative/path"));
  EXPECT_EQ(UriParserUri("/absolute/path"), UriParserUri("/absolute/path"));
  EXPECT_NE(UriParserUri("/path"), UriParserUri("path"));
}

// _____________________________________________________________________________
TEST(UriParserUri, EqualityOperatorMultiSegmentPath) {
  EXPECT_EQ(UriParserUri("https://example.com/a/b/c"),
            UriParserUri("https://example.com/a/b/c"));
  EXPECT_NE(UriParserUri("https://example.com/a/b"),
            UriParserUri("https://example.com/a"));
  EXPECT_NE(UriParserUri("https://example.com/a"),
            UriParserUri("https://example.com/a/b"));
}

// _____________________________________________________________________________
TEST(UriParserUri, CopyConstructor) {
  UriParserUri original("https://example.com/path?q=1#frag");
  UriParserUri copy(original);

  EXPECT_EQ(original, copy);
  EXPECT_EQ(uriToString(copy), "https://example.com/path?q=1#frag");
}

// _____________________________________________________________________________
TEST(UriParserUri, CopyConstructorFromMovedFrom) {
  UriParserUri original("https://example.com/path");
  UriParserUri moved(std::move(original));
  UriParserUri copy(original);
  // The destructors of all three should run without double frees.
}

// _____________________________________________________________________________
TEST(UriParserUri, CopyConstructorIsIndependent) {
  // Destroying the original must not affect the copy.
  std::unique_ptr<UriParserUri> original =
      std::make_unique<UriParserUri>("https://example.com/path");
  UriParserUri copy(*original);
  original.reset();

  EXPECT_EQ(uriToString(copy), "https://example.com/path");
}

// _____________________________________________________________________________
TEST(UriParserUri, CopyAssignment) {
  UriParserUri a("https://example.com/path");
  UriParserUri b("https://other.com");

  b = a;
  EXPECT_EQ(a, b);
  EXPECT_EQ(uriToString(b), "https://example.com/path");
}

// _____________________________________________________________________________
TEST(UriParserUri, CopyAssignmentToMovedFrom) {
  UriParserUri source("https://example.com/path");
  UriParserUri dest("https://other.com");
  UriParserUri moved(std::move(dest));
  // Assigning to it must not attempt to free uninitialized memory.
  dest = source;
  EXPECT_EQ(uriToString(dest), "https://example.com/path");
}

// _____________________________________________________________________________
TEST(UriParserUri, CopyAssignmentFromMovedFrom) {
  UriParserUri dest("https://example.com/path");
  UriParserUri source("https://other.com");
  UriParserUri moved(std::move(source));
  // After the assignment, `dest` should also be invalid and its destructor must
  // not double-free.
  dest = source;
}

// _____________________________________________________________________________
TEST(UriParserUri, CopyAssignmentSelf) {
  UriParserUri a("https://example.com/path");
  UriParserUri& ref = a;
  ref = a;
  EXPECT_EQ(uriToString(a), "https://example.com/path");
}

// _____________________________________________________________________________
TEST(UriParserUri, MoveConstructor) {
  UriParserUri expected("https://example.com/path");
  UriParserUri original("https://example.com/path");
  UriParserUri moved(std::move(original));

  EXPECT_EQ(moved, expected);
  EXPECT_EQ(uriToString(moved), "https://example.com/path");
  // The destructors should run without double frees here.
}

// _____________________________________________________________________________
TEST(UriParserUri, MoveAssignment) {
  UriParserUri expected("https://example.com/path");
  UriParserUri a("https://example.com/path");
  UriParserUri b("https://other.com");

  b = std::move(a);
  EXPECT_EQ(b, expected);
  EXPECT_EQ(uriToString(b), "https://example.com/path");
  // The destructors should run without double frees here.
}

// _____________________________________________________________________________
TEST(UriParserUri, MoveAssignmentToInvalid) {
  UriParserUri source("https://example.com/path");
  UriParserUri dest("https://other.com");
  UriParserUri moved(std::move(dest));
  // Move-assigning to it must not attempt to free uninitialized memory.
  dest = std::move(source);
  EXPECT_EQ(uriToString(dest), "https://example.com/path");
}

// _____________________________________________________________________________
TEST(UriParserUri, MoveAssignmentSelf) {
  UriParserUri a("https://example.com/path");
  // The implementation guards against self-move with `if (this != &other)`.
  UriParserUri* ptr = &a;
  *ptr = std::move(a);
  EXPECT_EQ(uriToString(a), "https://example.com/path");
}

// _____________________________________________________________________________
TEST(UriParserUri, ResolveUri) {
  // RFC 3986 Section 5.4 resolution examples.
  UriParserUri base{"http://a/b/c/d;p?q"};

  EXPECT_EQ(base.resolveUri("g"), UriParserUri("http://a/b/c/g"));
  EXPECT_EQ(base.resolveUri("./g"), UriParserUri("http://a/b/c/g"));
  EXPECT_EQ(base.resolveUri("g/"), UriParserUri("http://a/b/c/g/"));
  EXPECT_EQ(base.resolveUri("/g"), UriParserUri("http://a/g"));
  EXPECT_EQ(base.resolveUri("../g"), UriParserUri("http://a/b/g"));
  EXPECT_EQ(base.resolveUri("../../g"), UriParserUri("http://a/g"));
  EXPECT_EQ(base.resolveUri("http://other.com/path"),
            UriParserUri("http://other.com/path"));
}

// _____________________________________________________________________________
TEST(UriParserUri, ResolveUriInvalidReference) {
  UriParserUri base("http://example.com/path");
  std::string_view invalidUrl = "http://[invalid";
  AD_EXPECT_THROW_WITH_MESSAGE(
      base.resolveUri(invalidUrl),
      ::testing::AllOf(::testing::HasSubstr("URL is invalid"),
                       ::testing::HasSubstr(invalidUrl)));
}

// _____________________________________________________________________________
TEST(UriParserUri, ResolveUriRelativeBase) {
  // With URI_RESOLVE_STRICTLY, resolving against a relative base must fail.
  UriParserUri base("relative/path");
  AD_EXPECT_THROW_WITH_MESSAGE(
      base.resolveUri("something"),
      ::testing::HasSubstr("Could not resolve relative URI."));
}
