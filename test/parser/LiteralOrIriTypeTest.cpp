// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "parser/Iri.h"
#include "parser/Literal.h"
#include "parser/LiteralOrIri.h"
#include "parser/NormalizedString.h"

TEST(IriTypeTest, IriTypeCreation) {
  Iri iri(fromStringUnsafe("http://www.wikidata.org/entity/Q3138"));

  EXPECT_THAT("http://www.wikidata.org/entity/Q3138",
              asStringView(iri.getContent()));
}

TEST(LiteralTypeTest, LiteralTypeTest) {
  Literal literal(fromStringUnsafe("Hello World"));

  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringView(literal.getContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralTypeTest, LiteralTypeTestWithDatatype) {
  Literal literal(fromStringUnsafe("Hello World"),
                      fromStringUnsafe("xsd:string"),
                      LiteralDescriptor::DATATYPE);

  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_TRUE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringView(literal.getContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THAT("xsd:string", asStringView(literal.getDatatype()));
}

TEST(LiteralTypeTest, LiteralTypeTestWithLanguagetag) {
  Literal literal(fromStringUnsafe("Hallo Welt"), fromStringUnsafe("de"),
                      LiteralDescriptor::LANGUAGE_TAG);

  EXPECT_TRUE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hallo Welt", asStringView(literal.getContent()));
  EXPECT_THAT("de", asStringView(literal.getLanguageTag()));
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIriType, LiteralOrIriTypeWithIri) {
  LiteralOrIri iri(
      Iri(fromStringUnsafe("http://www.wikidata.org/entity/Q3138")));

  EXPECT_TRUE(iri.isIri());
  EXPECT_THAT("http://www.wikidata.org/entity/Q3138",
              asStringView(iri.getIriContent()));
  EXPECT_FALSE(iri.isLiteral());
  EXPECT_THROW(iri.hasLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(iri.hasDatatype(), ad_utility::Exception);
  EXPECT_THROW(iri.getLiteralContent(), ad_utility::Exception);
  EXPECT_THROW(iri.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(iri.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIriType, LiteralOrIriTypeWithLiteral) {
  LiteralOrIri literal(Literal(fromStringUnsafe("Hello World")));

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriContent(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringView(literal.getLiteralContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIriType, LiteralOrIriTypeWithLiteralAndDatatype) {
  LiteralOrIri literal(Literal(fromStringUnsafe("Hello World"),
                                       fromStringUnsafe("xsd:string"),
                                       LiteralDescriptor::DATATYPE));

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriContent(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_TRUE(literal.hasDatatype());
  EXPECT_THAT("Hello World", asStringView(literal.getLiteralContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THAT("xsd:string", asStringView(literal.getDatatype()));
}

TEST(LiteralOrIriType, LiteralOrIriTypeWithLiteralAndLanguageTag) {
  LiteralOrIri literal(Literal(fromStringUnsafe("Hej världen"),
                                       fromStringUnsafe("se"),
                                       LiteralDescriptor::LANGUAGE_TAG));

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriContent(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_TRUE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hej världen", asStringView(literal.getLiteralContent()));
  EXPECT_THAT("se", asStringView(literal.getLanguageTag()));
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

TEST(LiteralOrIriTypeFromString, CreateIriTypeFromString) {
  std::string s = "<http://example.org/book/book1>";
  LiteralOrIri iri = LiteralOrIri::fromRdfToLiteralOrIri(s);
  EXPECT_TRUE(iri.isIri());
  EXPECT_FALSE(iri.isLiteral());
  EXPECT_THAT("http://example.org/book/book1",
              asStringView(iri.getIriContent()));
}

TEST(LiteralOrIriTypeFromString, CreateIriTypeFromStringInvalidCharacter) {
  std::string s = "<http://example.org/book/book^1>";
  EXPECT_THROW(LiteralOrIri::fromRdfToLiteralOrIri(s),
               ad_utility::Exception);
}

TEST(LiteralOrIriTypeFromString, CreateLiteralTypeFromString) {
  std::string s = "\"Hej världen\"";
  LiteralOrIri literal = LiteralOrIri::fromRdfToLiteralOrIri(s);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hej världen", asStringView(literal.getLiteralContent()));
}

TEST(LiteralOrIriTypeFromString, CreateLiteralTypeFromStringWithSingleQuote) {
  std::string s = "\'Hej världen\'";
  LiteralOrIri literal = LiteralOrIri::fromRdfToLiteralOrIri(s);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THAT("Hej världen", asStringView(literal.getLiteralContent()));
}

TEST(LiteralOrIriTypeFromString, CreateLiteralTypeWithLanguageTagFromString) {
  std::string s = "\"Hej världen\"@se";
  LiteralOrIri literal = LiteralOrIri::fromRdfToLiteralOrIri(s);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_TRUE(literal.hasLanguageTag());
  EXPECT_THAT("Hej världen", asStringView(literal.getLiteralContent()));
  EXPECT_THAT("se", asStringView(literal.getLanguageTag()));
}

TEST(LiteralOrIriTypeFromString, CreateLiteralTypeWithDatatypeFromString) {
  std::string s = "\"ABCD\"^^test:type";
  LiteralOrIri literal = LiteralOrIri::fromRdfToLiteralOrIri(s);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_THAT("ABCD", asStringView(literal.getLiteralContent()));
  EXPECT_THAT("test:type", asStringView(literal.getDatatype()));
}

TEST(LiteralOrIriTypeFromString,
     CreateLiteralTypeWithDatatypeFromStringWithThreeDoubleQuotes) {
  std::string s = R"("""ABCD"""^^test:type)";
  LiteralOrIri literal = LiteralOrIri::fromRdfToLiteralOrIri(s);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_THAT("ABCD", asStringView(literal.getLiteralContent()));
  EXPECT_THAT("test:type", asStringView(literal.getDatatype()));
}

TEST(LiteralOrIriTypeFromString,
     CreateLiteralTypeWithDatatypeFromStringWithThreeSingleQuotes) {
  std::string s = R"('''ABCD'''^^test:type)";
  LiteralOrIri literal = LiteralOrIri::fromRdfToLiteralOrIri(s);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_THAT("ABCD", asStringView(literal.getLiteralContent()));
  EXPECT_THAT("test:type", asStringView(literal.getDatatype()));
}

TEST(LiteralOrIriTypeFromString, CreateLiteralTypeFromStringInvalidQuotation) {
  std::string s = "\"Hej världen";
  EXPECT_THROW(LiteralOrIri::fromRdfToLiteralOrIri(s),
               ad_utility::Exception);
  s = "Hej världen";
  EXPECT_THROW(LiteralOrIri::fromRdfToLiteralOrIri(s),
               ad_utility::Exception);
  s = "\"\"\"Hej världen\"";
  EXPECT_THROW(LiteralOrIri::fromRdfToLiteralOrIri(s),
               ad_utility::Exception);
  s = "\"Hej världen'";
  EXPECT_THROW(LiteralOrIri::fromRdfToLiteralOrIri(s),
               ad_utility::Exception);
}
