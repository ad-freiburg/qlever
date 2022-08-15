//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>
#include <parser/ParseException.h>

#include <string>

#include "../src/util/HttpServer/HttpParser/AcceptHeaderQleverVisitor.h"
#include "../src/util/HttpServer/HttpParser/generated/AcceptHeaderLexer.h"

using namespace antlr4;
using std::string;
using namespace ad_utility;

const std::vector<MediaType>& supportedMediaTypes() {
  static auto vector = [] {
    std::vector<MediaType> result;
    for (const auto& [mediaType, impl] : detail::getAllMediaTypes()) {
      result.push_back(mediaType);
    }
    return result;
  }();
  return vector;
}

auto parse = [](std::string_view input) {
  return parseAcceptHeader(input, supportedMediaTypes());
};

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
  auto c = parse("application/json");
  ASSERT_EQ(c.size(), 1u);
  ASSERT_EQ(c[0], MediaType::json);
}

TEST(AcceptHeaderParser, multipleTypes) {
  auto c = parse("application/json,text/html   ,  text/css");
  ASSERT_EQ(c.size(), 3u);
  ASSERT_EQ(c[0], ad_utility::MediaType::json);
  ASSERT_EQ(c[1], ad_utility::MediaType::html);
  ASSERT_EQ(c[2], ad_utility::MediaType::css);
}

TEST(AcceptHeaderParser, ignoreUnknown) {
  auto c = parse("application/json,unknown/strangeType   ,  text/css");
  ASSERT_EQ(c.size(), 2u);
  ASSERT_EQ(c[0], ad_utility::MediaType::json);
  ASSERT_EQ(c[1], ad_utility::MediaType::css);
}

TEST(AcceptHeaderParser, MultipleTypesCaseInsensitive) {
  auto c = parse("appLicaTion/jSOn,teXt/Html   ,  Text/Css");
  ASSERT_EQ(c.size(), 3u);
  ASSERT_EQ(c[0], ad_utility::MediaType::json);
  ASSERT_EQ(c[1], ad_utility::MediaType::html);
  ASSERT_EQ(c[2], ad_utility::MediaType::css);
}

TEST(AcceptHeaderParser, AllTypesUnknownThrow) {
  auto p = std::string{"appLicaTion/unknown, unknown/Html   ,  strange/Css"};
  ASSERT_THROW(parse(p), AcceptHeaderQleverVisitor::Exception);
}

TEST(AcceptHeaderParser, QualityValues) {
  auto p = std::string{"application/json;q=0.35, text/Html, image/png;q=0.123"};
  auto c = parse(p);
  ASSERT_EQ(c.size(), 3u);
  ASSERT_FLOAT_EQ(c[0]._qualityValue, 1.0f);
  ASSERT_EQ(c[0], MediaType::html);
  ASSERT_FLOAT_EQ(c[1]._qualityValue, 0.35f);
  ASSERT_EQ(c[1], MediaType::json);
  ASSERT_FLOAT_EQ(c[2]._qualityValue, 0.123f);
  ASSERT_EQ(c[2], MediaType::png);

  p = "application/json;q=0.3542, text/Html";
  ASSERT_THROW(parse(p), AcceptHeaderQleverVisitor::ParseException);

  p = "application/json;q=1.3, text/Html";
  ASSERT_THROW(parse(p), AcceptHeaderQleverVisitor::ParseException);
}

TEST(AcceptHeaderParser, CharsetParametersNotSupported) {
  auto p = std::string{"application/json;charset=UTF-8, text/Html"};
  ASSERT_THROW(parse(p), AcceptHeaderQleverVisitor::NotSupportedException);
}

TEST(AcceptHeaderParser, WildcardSubtype) {
  auto p = std::string{"text/*, application/json"};
  auto c = parse(p);
  ASSERT_EQ(c.size(), 2u);
  ASSERT_FLOAT_EQ(c[0]._qualityValue, 1.0f);
  ASSERT_EQ(c[0], MediaType::json);
  ASSERT_FLOAT_EQ(c[1]._qualityValue, 1.0f);
  ASSERT_EQ(c[1], "text");

  p = std::string{"text/*, application/json;q=0.9"};
  c = parse(p);
  ASSERT_EQ(c.size(), 2u);
  ASSERT_FLOAT_EQ(c[0]._qualityValue, 1.0f);
  ASSERT_EQ(c[0], "text");
  ASSERT_FLOAT_EQ(c[1]._qualityValue, 0.9f);
  ASSERT_EQ(c[1], MediaType::json);
}

TEST(AcceptHeaderParser, TotalWildCard) {
  auto p = std::string{"text/*, */*, application/json"};
  auto c = parse(p);
  ASSERT_EQ(c.size(), 3u);
  ASSERT_FLOAT_EQ(c[0]._qualityValue, 1.0f);
  ASSERT_EQ(c[0], MediaType::json);
  ASSERT_FLOAT_EQ(c[1]._qualityValue, 1.0f);
  ASSERT_EQ(c[1], "text");
  ASSERT_FLOAT_EQ(c[2]._qualityValue, 1.0f);
  ASSERT_TRUE(isTotalWildcard(c[2]));
}

TEST(AcceptHeaderParser, IllegalInput) {
  ASSERT_THROW(parse("application/json text/html"), ParseException);
  ASSERT_THROW(parse("application/json; text/html"), ParseException);
  ASSERT_THROW(parse("application/json,q=1.0, text/html"), ParseException);
  ASSERT_THROW(parse("application"), ParseException);
  ASSERT_THROW(parse("application/"), ParseException);
}

TEST(AcceptHeaderParser, FindMediaTypeFromAcceptHeader) {
  std::vector<MediaType> supportedTypes;
  supportedTypes.push_back(MediaType::json);
  supportedTypes.push_back(MediaType::png);

  std::string p = "text/html,application/json";
  auto result = getMediaTypeFromAcceptHeader(p, supportedTypes);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value(), MediaType::json);

  p = "text/html, image/jpeg";
  ASSERT_FALSE(getMediaTypeFromAcceptHeader(p, supportedTypes).has_value());

  // The wildcard matches json or png, json has higher priority;
  p = "*/*, text/html";
  result = getMediaTypeFromAcceptHeader(p, supportedTypes);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value(), MediaType::json);

  // The wildcard matches png, but not json;
  p = "image/*, text/html";
  result = getMediaTypeFromAcceptHeader(p, supportedTypes);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value(), MediaType::png);

  // The wildcard matches png, json has higher precedence;
  p = "image/*, application/json";
  result = getMediaTypeFromAcceptHeader(p, supportedTypes);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value(), MediaType::json);

  // The wildcard matches png, which has the higher quality value;
  p = "image/*, application/json; q=0.3";
  result = getMediaTypeFromAcceptHeader(p, supportedTypes);
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result.value(), MediaType::png);
}
