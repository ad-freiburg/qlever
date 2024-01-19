// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "parser/IriType.h"
#include "parser/LiteralOrIriType.h"
#include "parser/LiteralType.h"
#include "parser/NormalizedString.h"

TEST(IriTypeTest, IriTypeCreation) {
  IriType iri(fromStringUnsafe("http://www.wikidata.org/entity/Q3138"));

  EXPECT_THAT("http://www.wikidata.org/entity/Q3138",
              asStringView(iri.getIri()));
}

TEST(LiteralTypeTest, LiteralTypeTest) {
  LiteralType literal(fromStringUnsafe("Hello World"));

  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringView(literal.getContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralTypeTest, LiteralTypeTestWithDatatype) {
  LiteralType literal(fromStringUnsafe("Hello World"),
                      fromStringUnsafe("xsd:string"),
                      LiteralDescriptor::DATATYPE);

  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_TRUE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringView(literal.getContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THAT("xsd:string", asStringView(literal.getDatatype()));
}

TEST(LiteralTypeTest, LiteralTypeTestWithLanguagetag) {
  LiteralType literal(fromStringUnsafe("Hallo Welt"), fromStringUnsafe("de"),
                      LiteralDescriptor::LANGUAGE_TAG);

  EXPECT_TRUE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hallo Welt", asStringView(literal.getContent()));
  EXPECT_THAT("de", asStringView(literal.getLanguageTag()));
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIriType, LiteralOrIriTypeWithIri) {
  LiteralOrIriType iri(
      IriType(fromStringUnsafe("http://www.wikidata.org/entity/Q3138")));

  EXPECT_TRUE(iri.isIri());
  EXPECT_THAT("http://www.wikidata.org/entity/Q3138",
              asStringView(iri.getIriString()));
  EXPECT_FALSE(iri.isLiteral());
  EXPECT_THROW(iri.hasLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(iri.hasDatatype(), ad_utility::Exception);
  EXPECT_THROW(iri.getLiteralContent(), ad_utility::Exception);
  EXPECT_THROW(iri.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(iri.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIriType, LiteralOrIriTypeWithLiteral) {
  LiteralOrIriType literal(LiteralType(fromStringUnsafe("Hello World")));

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriString(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringView(literal.getLiteralContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIriType, LiteralOrIriTypeWithLiteralAndDatatype) {
  LiteralOrIriType literal(LiteralType(fromStringUnsafe("Hello World"),
                                       fromStringUnsafe("xsd:string"),
                                       LiteralDescriptor::DATATYPE));

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriString(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_TRUE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringView(literal.getLiteralContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THAT("xsd:string", asStringView(literal.getDatatype()));
}

TEST(LiteralOrIriType, LiteralOrIriTypeWithLiteralAndLanguageTag) {
  LiteralOrIriType literal(LiteralType(fromStringUnsafe("Hej världen"),
                                       fromStringUnsafe("se"),
                                       LiteralDescriptor::LANGUAGE_TAG));

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriString(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_TRUE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hej världen", asStringView(literal.getLiteralContent()));
  EXPECT_THAT("se", asStringView(literal.getLanguageTag()));
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIriTypeFromString, CreateIriTypeFromString) {
  std::string s = "<http://example.org/book/book1>";
  LiteralOrIriType iri = LiteralOrIriType::fromStringToLiteralOrIri(s);
  EXPECT_TRUE(iri.isIri());
  EXPECT_FALSE(iri.isLiteral());
  EXPECT_THAT("http://example.org/book/book1",
              asStringView(iri.getIriString()));
}

TEST(LiteralOrIriTypeFromString, CreateIriTypeFromStringInvalidCharacter) {
  std::string s = "<http://example.org/book/book^1>";
  EXPECT_THROW(LiteralOrIriType::fromStringToLiteralOrIri(s),
               ad_utility::Exception);
}

TEST(LiteralOrIriTypeFromString, CreateLiteralTypeFromString) {
  std::string s = "\"Hej världen\"";
  LiteralOrIriType literal = LiteralOrIriType::fromStringToLiteralOrIri(s);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hej världen", asStringView(literal.getLiteralContent()));
}

TEST(LiteralOrIriTypeFromString, CreateLiteralTypeWithLanguageTagFromString) {
  std::string s = "\"Hej världen\"@se";
  LiteralOrIriType literal = LiteralOrIriType::fromStringToLiteralOrIri(s);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_TRUE(literal.hasLanguageTag());
  EXPECT_THAT("Hej världen", asStringView(literal.getLiteralContent()));
  EXPECT_THAT("se", asStringView(literal.getLanguageTag()));
}

TEST(LiteralOrIriTypeFromString, CreateLiteralTypeWithDatatypeFromString) {
  std::string s = "\"ABCD\"^^test:type";
  LiteralOrIriType literal = LiteralOrIriType::fromStringToLiteralOrIri(s);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_THAT("ABCD", asStringView(literal.getLiteralContent()));
  EXPECT_THAT("test:type", asStringView(literal.getDatatype()));
}

TEST(LiteralOrIriTypeFromString,
     CreateLiteralTypeWithDatatypeFromStringWithThreeDoubleQuotes) {
  std::string s = R"("""ABCD"""^^test:type)";
  LiteralOrIriType literal = LiteralOrIriType::fromStringToLiteralOrIri(s);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_THAT("ABCD", asStringView(literal.getLiteralContent()));
  EXPECT_THAT("test:type", asStringView(literal.getDatatype()));
}

TEST(LiteralOrIriTypeFromString,
     CreateLiteralTypeWithDatatypeFromStringWithThreeSingleQuotes) {
  std::string s = R"('''ABCD'''^^test:type)";
  LiteralOrIriType literal = LiteralOrIriType::fromStringToLiteralOrIri(s);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_THAT("ABCD", asStringView(literal.getLiteralContent()));
  EXPECT_THAT("test:type", asStringView(literal.getDatatype()));
}

TEST(LiteralOrIriTypeFromString, CreateLiteralTypeFromStringInvalidQuotation) {
  std::string s = "\"Hej världen";
  EXPECT_THROW(LiteralOrIriType::fromStringToLiteralOrIri(s),
               ad_utility::Exception);
  s = "Hej världen";
  EXPECT_THROW(LiteralOrIriType::fromStringToLiteralOrIri(s),
               ad_utility::Exception);
  s = "\"\"\"Hej världen\"";
  EXPECT_THROW(LiteralOrIriType::fromStringToLiteralOrIri(s),
               ad_utility::Exception);
  s = "\"Hej världen'";
  EXPECT_THROW(LiteralOrIriType::fromStringToLiteralOrIri(s),
               ad_utility::Exception);
}
