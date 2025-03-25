//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <string>

#include "util/ParseException.h"
#include "util/http/HttpParser/AcceptHeaderQleverVisitor.h"

using namespace antlr4;
using std::string;
using namespace ad_utility;

namespace ad_utility {
// Comparison operator to make the tests more readable.
bool operator==(const MediaTypeWithQuality& a, const MediaType& b) {
  return std::holds_alternative<MediaType>(a._mediaType) &&
         std::get<MediaType>(a._mediaType) == b;
}

bool operator==(const MediaTypeWithQuality& a, const std::string& b) {
  return std::holds_alternative<MediaTypeWithQuality::TypeWithWildcard>(
             a._mediaType) &&
         std::get<MediaTypeWithQuality::TypeWithWildcard>(a._mediaType)._type ==
             b;
}

bool isTotalWildcard(const MediaTypeWithQuality& a) {
  return std::holds_alternative<MediaTypeWithQuality::Wildcard>(a._mediaType);
}

}  // namespace ad_utility

TEST(AcceptHeaderParser, singleType) {
  auto c = parseAcceptHeader("application/json");
  ASSERT_EQ(c.size(), 1u);
  ASSERT_EQ(c[0], MediaType::json);
}

TEST(AcceptHeaderParser, multipleTypes) {
  auto c = parseAcceptHeader("application/json,text/plain   ,  text/csv");
  ASSERT_EQ(c.size(), 3u);
  ASSERT_EQ(c[0], MediaType::json);
  ASSERT_EQ(c[1], MediaType::textPlain);
  ASSERT_EQ(c[2], MediaType::csv);
}

TEST(AcceptHeaderParser, ignoreUnknown) {
  auto c =
      parseAcceptHeader("application/json,unknown/strangeType   ,  text/csv");
  ASSERT_EQ(c.size(), 2u);
  ASSERT_EQ(c[0], MediaType::json);
  ASSERT_EQ(c[1], MediaType::csv);
}

TEST(AcceptHeaderParser, MultipleTypesCaseInsensitive) {
  auto c = parseAcceptHeader("appLicaTion/jSOn,teXt/Plain   ,  Text/Csv");
  ASSERT_EQ(c.size(), 3u);
  ASSERT_EQ(c[0], MediaType::json);
  ASSERT_EQ(c[1], MediaType::textPlain);
  ASSERT_EQ(c[2], MediaType::csv);
}

TEST(AcceptHeaderParser, AllTypesUnknownThrow) {
  auto p = std::string{"appLicaTion/unknown, unknown/Html   ,  strange/Css"};
  ASSERT_THROW(parseAcceptHeader(p), AcceptHeaderQleverVisitor::Exception);
}

TEST(AcceptHeaderParser, QualityValues) {
  auto p = std::string{
      "application/json;q=0.35, text/Plain, application/octet-stream;q=0.123"};
  auto c = parseAcceptHeader(p);
  ASSERT_EQ(c.size(), 3u);
  ASSERT_FLOAT_EQ(c[0]._qualityValue, 1.0f);
  ASSERT_EQ(c[0], MediaType::textPlain);
  ASSERT_FLOAT_EQ(c[1]._qualityValue, 0.35f);
  ASSERT_EQ(c[1], MediaType::json);
  ASSERT_FLOAT_EQ(c[2]._qualityValue, 0.123f);
  ASSERT_EQ(c[2], MediaType::octetStream);

  p = "application/json;q=0.3542, text/Html";
  ASSERT_THROW(parseAcceptHeader(p), AcceptHeaderQleverVisitor::ParseException);

  p = "application/json;q=1.3, text/Html";
  ASSERT_THROW(parseAcceptHeader(p), AcceptHeaderQleverVisitor::ParseException);
}

TEST(AcceptHeaderParser, CharsetParametersNotSupported) {
  // Currently the `charset=UTF-8` is simply ignored.
  auto p = std::string{"application/json;charset=UTF-8, text/Plain"};
  auto c = parseAcceptHeader(p);
  ASSERT_EQ(c.size(), 2u);
  ASSERT_EQ(c[0], MediaType::json);
  ASSERT_EQ(c[1], MediaType::textPlain);
}

TEST(AcceptHeaderParser, WildcardSubtype) {
  auto p = std::string{"text/*, application/json"};
  auto c = parseAcceptHeader(p);
  ASSERT_EQ(c.size(), 2u);
  ASSERT_FLOAT_EQ(c[0]._qualityValue, 1.0f);
  ASSERT_EQ(c[0], MediaType::json);
  ASSERT_FLOAT_EQ(c[1]._qualityValue, 1.0f);
  ASSERT_EQ(c[1], "text");

  p = std::string{"text/*, application/json;q=0.9"};
  c = parseAcceptHeader(p);
  ASSERT_EQ(c.size(), 2u);
  ASSERT_FLOAT_EQ(c[0]._qualityValue, 1.0f);
  ASSERT_EQ(c[0], "text");
  ASSERT_FLOAT_EQ(c[1]._qualityValue, 0.9f);
  ASSERT_EQ(c[1], MediaType::json);
}

TEST(AcceptHeaderParser, TotalWildCard) {
  auto p = std::string{"text/*, */*, application/json"};
  auto c = parseAcceptHeader(p);
  ASSERT_EQ(c.size(), 3u);
  ASSERT_FLOAT_EQ(c[0]._qualityValue, 1.0f);
  ASSERT_EQ(c[0], MediaType::json);
  ASSERT_FLOAT_EQ(c[1]._qualityValue, 1.0f);
  ASSERT_EQ(c[1], "text");
  ASSERT_FLOAT_EQ(c[2]._qualityValue, 1.0f);
  ASSERT_TRUE(isTotalWildcard(c[2]));
}

TEST(AcceptHeaderParser, IllegalInput) {
  ASSERT_THROW(parseAcceptHeader("application/json text/html"), ParseException);
  ASSERT_THROW(parseAcceptHeader("application/json; text/html"),
               ParseException);
  ASSERT_THROW(parseAcceptHeader("application/json,q=1.0, text/html"),
               ParseException);
  ASSERT_THROW(parseAcceptHeader("application"), ParseException);
  ASSERT_THROW(parseAcceptHeader("application/"), ParseException);
}

TEST(AcceptHeaderParser, FindMediaTypeFromAcceptHeader) {
  std::string p = "text/html,application/sparql-results+json";
  auto result = getMediaTypeFromAcceptHeader(p);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value(), MediaType::sparqlJson);

  p = "application/json, image/jpeg";
  ASSERT_FALSE(getMediaTypeFromAcceptHeader(p).has_value());

  // The wildcard matches all the supported media types, sparql-json has
  // higher priority;
  p = "*/*, text/html";
  result = getMediaTypeFromAcceptHeader(p);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value(), MediaType::sparqlJson);

  // The wildcard matches csv and tsv, but not the json variants
  p = "text/*, text/html";
  result = getMediaTypeFromAcceptHeader(p);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value(), MediaType::tsv);

  // The wildcard matches csv/tsv, qlever-json has higher precedence;
  p = "text/*, application/qlever-results+json";
  result = getMediaTypeFromAcceptHeader(p);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value(), MediaType::qleverJson);

  // The wildcard matches tsv/csv, which has the higher quality value;
  p = "text/*, application/json; q=0.3";
  result = getMediaTypeFromAcceptHeader(p);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value(), MediaType::tsv);
}
