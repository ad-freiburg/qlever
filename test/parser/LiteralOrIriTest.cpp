// Copyright 2023 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Benedikt Maria Beckermann <benedikt.beckermann@dagstuhl.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "index/IndexImpl.h"
#include "parser/LiteralOrIri.h"
#include "parser/NormalizedString.h"
#include "rdfTypes/Iri.h"
#include "rdfTypes/Literal.h"
#include "util/HashSet.h"

namespace {

using namespace ad_utility::triple_component;
using Iri = ad_utility::triple_component::Iri;
using Literal = ad_utility::triple_component::Literal;
constexpr std::string_view myDatatype =
    "http://www.w3.org/2001/XMLSchema#myDatatype";
std::string myDatatypeWithBrackets = absl::StrCat("<", myDatatype, ">");

// _____________________________________________________________________________
TEST(IriTest, IriCreation) {
  Iri iri = Iri::fromIriref("<http://www.wikidata.org/entity/Q3138>");

  EXPECT_EQ("http://www.wikidata.org/entity/Q3138",
            asStringViewUnsafe(iri.getContent()));
}

// _____________________________________________________________________________
TEST(IriTest, getBaseIri) {
  // Helper lambda that calls `Iri::getBaseIri` and returns the result as a
  // string (including the angle brackets).
  auto getBaseIri = [](std::string_view iriSv, bool domainOnly) {
    return Iri::fromIriref(iriSv)
        .getBaseIri(domainOnly)
        .toStringRepresentation();
  };
  // IRI with path.
  EXPECT_EQ(getBaseIri("<http://purl.uniprot.org/uniprot/>", false),
            "<http://purl.uniprot.org/uniprot/>");
  EXPECT_EQ(getBaseIri("<http://purl.uniprot.org/uniprot>", false),
            "<http://purl.uniprot.org/uniprot/>");
  EXPECT_EQ(getBaseIri("<http://purl.uniprot.org/uniprot/>", true),
            "<http://purl.uniprot.org/>");
  EXPECT_EQ(getBaseIri("<http://purl.uniprot.org/uniprot>", true),
            "<http://purl.uniprot.org/>");
  // IRI with domain only.
  EXPECT_EQ(getBaseIri("<http://purl.uniprot.org/>", false),
            "<http://purl.uniprot.org/>");
  EXPECT_EQ(getBaseIri("<http://purl.uniprot.org>", false),
            "<http://purl.uniprot.org/>");
  EXPECT_EQ(getBaseIri("<http://purl.uniprot.org/>", true),
            "<http://purl.uniprot.org/>");
  EXPECT_EQ(getBaseIri("<http://purl.uniprot.org>", true),
            "<http://purl.uniprot.org/>");
  // IRI without scheme.
  EXPECT_EQ(getBaseIri("<blabla>", false), "<blabla/>");
  EXPECT_EQ(getBaseIri("<blabla>", true), "<blabla/>");
}

// _____________________________________________________________________________
TEST(IriTest, emptyIri) {
  EXPECT_TRUE(Iri{}.empty());
  EXPECT_FALSE(Iri::fromIriref("<http://www.wikidata.org>").empty());
}

// _____________________________________________________________________________
TEST(IriTest, fromIrirefConsiderBase) {
  // Helper lambda that calls `Iri::fromIrirefConsiderBase` with the two base
  // IRIs and returns the results as a string (including the angle brackets).
  Iri baseForRelativeIris;
  Iri baseForAbsoluteIris;
  auto fromIrirefConsiderBase = [&baseForRelativeIris, &baseForAbsoluteIris](
                                    std::string_view iriStringWithBrackets) {
    return Iri::fromIrirefConsiderBase(iriStringWithBrackets,
                                       baseForRelativeIris, baseForAbsoluteIris)
        .toStringRepresentation();
  };

  // Check that it works for "real" base IRIs.
  baseForRelativeIris = Iri::fromIriref("<http://.../uniprot/>");
  baseForAbsoluteIris = Iri::fromIriref("<http://.../>");
  EXPECT_EQ(fromIrirefConsiderBase("<http://purl.uniprot.org/uniprot/>"),
            "<http://purl.uniprot.org/uniprot/>");
  EXPECT_EQ(fromIrirefConsiderBase("<UPI001AF4585D>"),
            "<http://.../uniprot/UPI001AF4585D>");
  EXPECT_EQ(fromIrirefConsiderBase("</prosite/PS51927>"),
            "<http://.../prosite/PS51927>");

  // Check that with the default base, all IRIs remain unchanged.
  baseForRelativeIris = Iri{};
  baseForAbsoluteIris = Iri{};
  EXPECT_EQ(fromIrirefConsiderBase("<http://purl.uniprot.org/uniprot/>"),
            "<http://purl.uniprot.org/uniprot/>");
  EXPECT_EQ(fromIrirefConsiderBase("</a>"), "</a>");
  EXPECT_EQ(fromIrirefConsiderBase("<a>"), "<a>");
  EXPECT_EQ(fromIrirefConsiderBase("<>"), "<>");
}

// _____________________________________________________________________________
TEST(LiteralTest, LiteralTest) {
  Literal literal = Literal::literalWithoutQuotes("Hello World");

  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_EQ("Hello World", asStringViewUnsafe(literal.getContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(LiteralTest, XsdStringDatatypeIsNormalizedAway) {
  Literal literal1 = Literal::literalWithoutQuotes(
      "Hello World",
      Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"));
  Literal literal2 = Literal::fromStringRepresentation(
      "\"Hello World\"^^<http://www.w3.org/2001/XMLSchema#string>");

  for (const Literal& literal : {literal1, literal2}) {
    EXPECT_FALSE(literal.hasLanguageTag());
    EXPECT_FALSE(literal.hasDatatype());
    EXPECT_EQ("Hello World", asStringViewUnsafe(literal.getContent()));
    EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
    EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
  }
}

// _____________________________________________________________________________
TEST(LiteralTest, LiteralTestWithDatatype) {
  Literal literal = Literal::literalWithoutQuotes(
      "Hello World", Iri::fromIriref(myDatatypeWithBrackets));

  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_TRUE(literal.hasDatatype());
  EXPECT_EQ("Hello World", asStringViewUnsafe(literal.getContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_EQ(myDatatype, asStringViewUnsafe(literal.getDatatype()));
}

// _____________________________________________________________________________
TEST(LiteralTest, LiteralTestWithLanguagetag) {
  Literal literal = Literal::literalWithoutQuotes("Hallo Welt", "@de");

  EXPECT_TRUE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_EQ("Hallo Welt", asStringViewUnsafe(literal.getContent()));
  EXPECT_EQ("de", asStringViewUnsafe(literal.getLanguageTag()));
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(LiteralOrIri, LiteralOrIriWithIri) {
  LiteralOrIri iri =
      LiteralOrIri::iriref("<http://www.wikidata.org/entity/Q3138>");

  EXPECT_TRUE(iri.isIri());
  EXPECT_EQ("http://www.wikidata.org/entity/Q3138",
            asStringViewUnsafe(iri.getIriContent()));
  EXPECT_FALSE(iri.isLiteral());
  EXPECT_THROW(iri.hasLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(iri.hasDatatype(), ad_utility::Exception);
  EXPECT_THROW(iri.getLiteralContent(), ad_utility::Exception);
  EXPECT_THROW(iri.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(iri.getDatatype(), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(LiteralOrIri, LiteralOrIriWithPrefixedIri) {
  LiteralOrIri iri = LiteralOrIri::prefixedIri(
      Iri::fromIriref("<http://www.wikidata.org/entity/>"), "Q3138");

  EXPECT_TRUE(iri.isIri());
  EXPECT_EQ("http://www.wikidata.org/entity/Q3138",
            asStringViewUnsafe(iri.getIriContent()));
  EXPECT_FALSE(iri.isLiteral());
  EXPECT_THROW(iri.hasLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(iri.hasDatatype(), ad_utility::Exception);
  EXPECT_THROW(iri.getLiteralContent(), ad_utility::Exception);
  EXPECT_THROW(iri.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(iri.getDatatype(), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(LiteralOrIri, LiteralOrIriWithLiteral) {
  LiteralOrIri literal = LiteralOrIri::literalWithoutQuotes("Hello World");

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriContent(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_EQ("Hello World", asStringViewUnsafe(literal.getLiteralContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(LiteralOrIri, LiteralOrIriWithLiteralWithQuotes) {
  LiteralOrIri literal = LiteralOrIri::literalWithQuotes("\"Hello World\"");

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriContent(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_EQ("Hello World", asStringViewUnsafe(literal.getLiteralContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(LiteralOrIri, LiteralOrIriWithLiteralAndDatatype) {
  LiteralOrIri literal = LiteralOrIri::literalWithoutQuotes(
      "Hello World", Iri::fromIriref(myDatatypeWithBrackets));

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriContent(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_TRUE(literal.hasDatatype());
  EXPECT_EQ("Hello World", asStringViewUnsafe(literal.getLiteralContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_EQ(myDatatype, asStringViewUnsafe(literal.getDatatype()));
}

// _____________________________________________________________________________
TEST(LiteralOrIri, LiteralOrIriWithLiteralWithQuotesAndDatatype) {
  LiteralOrIri literal = LiteralOrIri::literalWithQuotes(
      "\"Hello World\"", Iri::fromIriref(myDatatypeWithBrackets));

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriContent(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_TRUE(literal.hasDatatype());
  EXPECT_EQ("Hello World", asStringViewUnsafe(literal.getLiteralContent()));
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);
  EXPECT_EQ(myDatatype, asStringViewUnsafe(literal.getDatatype()));
}

// _____________________________________________________________________________
TEST(LiteralOrIri, LiteralOrIriWithLiteralAndLanguageTag) {
  LiteralOrIri literal =
      LiteralOrIri::literalWithoutQuotes("Hej världen", "@se");

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriContent(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_TRUE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_EQ("Hej världen", asStringViewUnsafe(literal.getLiteralContent()));
  EXPECT_EQ("se", asStringViewUnsafe(literal.getLanguageTag()));
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(LiteralOrIri, LiteralOrIriWithLiteralWithQuotesAndLanguageTag) {
  LiteralOrIri literal =
      LiteralOrIri::literalWithQuotes("'''Hej världen'''", "@se");

  EXPECT_FALSE(literal.isIri());
  EXPECT_THROW(literal.getIriContent(), ad_utility::Exception);
  EXPECT_TRUE(literal.isLiteral());
  EXPECT_TRUE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_EQ("Hej världen", asStringViewUnsafe(literal.getLiteralContent()));
  EXPECT_EQ("se", asStringViewUnsafe(literal.getLanguageTag()));
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(LiteralOrIri, GetContent) {
  LiteralOrIri iri = LiteralOrIri::iriref("<https://example.org/books/book1>");
  LiteralOrIri literalWithLanguageTag =
      LiteralOrIri::literalWithoutQuotes("Hello World", "@de");
  LiteralOrIri literalWithDatatype = LiteralOrIri::literalWithoutQuotes(
      "ABC", Iri::fromIriref("<https://example.org>"));

  EXPECT_EQ("https://example.org/books/book1",
            asStringViewUnsafe(iri.getContent()));
  EXPECT_EQ("Hello World",
            asStringViewUnsafe(literalWithLanguageTag.getContent()));
  EXPECT_EQ("ABC", asStringViewUnsafe(literalWithDatatype.getContent()));
}

// _____________________________________________________________________________
TEST(LiteralOrIri, EnsureLiteralsAreEncoded) {
  LiteralOrIri literal1 =
      LiteralOrIri::literalWithQuotes(R"("This is to be \"\\ encoded")");
  EXPECT_EQ(R"(This is to be "\ encoded)",
            asStringViewUnsafe(literal1.getContent()));

  LiteralOrIri literal2 =
      LiteralOrIri::literalWithoutQuotes(R"(This is to be \"\\ encoded)");
  EXPECT_EQ(R"(This is to be "\ encoded)",
            asStringViewUnsafe(literal2.getContent()));
}

// _____________________________________________________________________________
TEST(LiteralOrIri, Printing) {
  LiteralOrIri literal1 = LiteralOrIri::literalWithoutQuotes("hallo");
  std::stringstream str;
  PrintTo(literal1, &str);
  EXPECT_EQ(str.str(), "\"hallo\"");
}

// _____________________________________________________________________________
TEST(LiteralOrIri, Hashing) {
  auto lit = LiteralOrIri::literalWithoutQuotes("bimbamm");
  auto iri = LiteralOrIri::iriref("<bimbamm>");
  ad_utility::HashSet<LiteralOrIri> set{lit, iri};
  EXPECT_THAT(set, ::testing::UnorderedElementsAre(lit, iri));
}

// _____________________________________________________________________________
TEST(LiteralTest, isPlain) {
  LiteralOrIri literal = LiteralOrIri::literalWithoutQuotes("Hello World!");
  EXPECT_TRUE(literal.getLiteral().isPlain());
  literal = LiteralOrIri::literalWithoutQuotes(
      "Hello World!", Iri::fromIriref(myDatatypeWithBrackets));
  EXPECT_FALSE(literal.getLiteral().isPlain());
  literal = LiteralOrIri::literalWithoutQuotes("Hello World!", "@en");
  EXPECT_FALSE(literal.getLiteral().isPlain());
}

// _____________________________________________________________________________
TEST(LiteralTest, isPlainWithXsdString) {
  auto literal = LiteralOrIri::literalWithoutQuotes(
      "Hello World!",
      Iri::fromIriref("<http://www.w3.org/2001/XMLSchema#string>"));
  EXPECT_TRUE(literal.getLiteral().isPlain());
}

// _____________________________________________________________________________
TEST(LiteralTest, SetSubstr) {
  LiteralOrIri literal = LiteralOrIri::literalWithoutQuotes(
      "Hello World!", Iri::fromIriref(myDatatypeWithBrackets));
  literal.getLiteral().setSubstr(0, 5);
  EXPECT_EQ("Hello", asStringViewUnsafe(literal.getContent()));
  EXPECT_EQ(myDatatype, asStringViewUnsafe(literal.getDatatype()));

  literal = LiteralOrIri::literalWithoutQuotes(
      "Hello World!", Iri::fromIriref(myDatatypeWithBrackets));
  literal.getLiteral().setSubstr(6, 5);
  EXPECT_EQ("World", asStringViewUnsafe(literal.getContent()));
  EXPECT_EQ(myDatatype, asStringViewUnsafe(literal.getDatatype()));

  // Substring works at the byte level (not the UTF-8 character level).
  literal = LiteralOrIri::literalWithoutQuotes("Äpfel");
  literal.getLiteral().setSubstr(0, 2);
  EXPECT_EQ("Ä", asStringViewUnsafe(literal.getContent()));

  // Test with invalid values.
  literal = LiteralOrIri::literalWithoutQuotes(
      "Hello World!", Iri::fromIriref(myDatatypeWithBrackets));
  EXPECT_THROW(literal.getLiteral().setSubstr(12, 1), ad_utility::Exception);
  EXPECT_THROW(literal.getLiteral().setSubstr(6, 7), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(LiteralOrIriTest, GetIri) {
  LiteralOrIri iri = LiteralOrIri::iriref("<https://example.org/books/book1>");
  EXPECT_EQ("https://example.org/books/book1",
            asStringViewUnsafe(iri.getIriContent()));

  LiteralOrIri literal = LiteralOrIri::literalWithoutQuotes("Hello World!");
  EXPECT_THROW(literal.getIri(), ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(LiteralTest, removeDatatypeOrLanguageTag) {
  LiteralOrIri literal = LiteralOrIri::literalWithoutQuotes(
      "Hello World!", Iri::fromIriref(myDatatypeWithBrackets));
  literal.getLiteral().removeDatatypeOrLanguageTag();
  EXPECT_EQ("Hello World!", asStringViewUnsafe(literal.getContent()));
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_THROW(literal.getDatatype(), ad_utility::Exception);

  literal = LiteralOrIri::literalWithoutQuotes("Hello World!", "@en");
  literal.getLiteral().removeDatatypeOrLanguageTag();
  EXPECT_EQ("Hello World!", asStringViewUnsafe(literal.getContent()));
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_THROW(literal.getLanguageTag(), ad_utility::Exception);

  literal = LiteralOrIri::literalWithoutQuotes("Hello World!");
  literal.getLiteral().removeDatatypeOrLanguageTag();
  EXPECT_EQ("Hello World!", asStringViewUnsafe(literal.getContent()));
}

// _____________________________________________________________________________
TEST(LiteralTest, replaceContent) {
  LiteralOrIri literal = LiteralOrIri::literalWithoutQuotes(
      "Hello!", Iri::fromIriref(myDatatypeWithBrackets));
  literal.getLiteral().replaceContent("Thüss!");
  EXPECT_EQ("Thüss!", asStringViewUnsafe(literal.getContent()));
  EXPECT_EQ(myDatatype, asStringViewUnsafe(literal.getDatatype()));
  literal.getLiteral().replaceContent("Hi!");
  EXPECT_EQ("Hi!", asStringViewUnsafe(literal.getContent()));
  EXPECT_EQ(myDatatype, asStringViewUnsafe(literal.getDatatype()));
  literal.getLiteral().replaceContent("Hello World!");
  EXPECT_EQ("Hello World!", asStringViewUnsafe(literal.getContent()));
  EXPECT_EQ(myDatatype, asStringViewUnsafe(literal.getDatatype()));
  literal = LiteralOrIri::literalWithoutQuotes("Hello!");
  literal.getLiteral().replaceContent("Hi!");
  EXPECT_EQ("Hi!", asStringViewUnsafe(literal.getContent()));
  literal.getLiteral().replaceContent("Hello World!");
  EXPECT_EQ("Hello World!", asStringViewUnsafe(literal.getContent()));
}

// _____________________________________________________________________________
TEST(LiteralTest, concat) {
  LiteralOrIri literal = LiteralOrIri::literalWithoutQuotes("Hello ");
  LiteralOrIri literalOther = LiteralOrIri::literalWithoutQuotes("World!");
  literal.getLiteral().concat(literalOther.getLiteral());
  EXPECT_EQ("Hello World!", asStringViewUnsafe(literal.getContent()));
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  literal = LiteralOrIri::literalWithoutQuotes(
      "Hello ", Iri::fromIriref(myDatatypeWithBrackets));
  literalOther = LiteralOrIri::literalWithoutQuotes(
      "World!", Iri::fromIriref(myDatatypeWithBrackets));
  literal.getLiteral().concat(literalOther.getLiteral());
  EXPECT_EQ("Hello World!", asStringViewUnsafe(literal.getContent()));
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_TRUE(literal.hasDatatype());
  EXPECT_EQ(myDatatype, asStringViewUnsafe(literal.getDatatype()));
  literal = LiteralOrIri::literalWithoutQuotes("Hello World!", "@en");
  literalOther = LiteralOrIri::literalWithoutQuotes("Bye!", "@en");
  literal.getLiteral().concat(literalOther.getLiteral());
  EXPECT_EQ("Hello World!Bye!", asStringViewUnsafe(literal.getContent()));
  EXPECT_TRUE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  EXPECT_EQ("en", asStringViewUnsafe(literal.getLanguageTag()));
  literal = LiteralOrIri::literalWithoutQuotes(
      "Hello ", Iri::fromIriref(myDatatypeWithBrackets));
  literalOther = LiteralOrIri::literalWithoutQuotes("World!");
  literal.getLiteral().concat(literalOther.getLiteral());
  EXPECT_EQ("Hello World!", asStringViewUnsafe(literal.getContent()));
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  literal = LiteralOrIri::literalWithoutQuotes("Hello World!", "@en");
  literalOther = LiteralOrIri::literalWithoutQuotes("Bye!");
  literal.getLiteral().concat(literalOther.getLiteral());
  EXPECT_EQ("Hello World!Bye!", asStringViewUnsafe(literal.getContent()));
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
  literal = LiteralOrIri::literalWithoutQuotes("Hello World!", "@en");
  literalOther = LiteralOrIri::literalWithoutQuotes("Thüss!", "@de");
  literal.getLiteral().concat(literalOther.getLiteral());
  EXPECT_EQ("Hello World!Thüss!", asStringViewUnsafe(literal.getContent()));
  EXPECT_FALSE(literal.hasLanguageTag());
  EXPECT_FALSE(literal.hasDatatype());
}

// _____________________________________________________________________________
TEST(LiteralTest, spaceshipOperatorLangtagLiteral) {
  LiteralOrIri l1 = LiteralOrIri::fromStringRepresentation(
      "\"Comparative evaluation of the protective effect of sodium "
      "valproate, phenazepam and ionol in stress-induced liver damage in "
      "rats\"@nl");
  LiteralOrIri l2 = LiteralOrIri::fromStringRepresentation(
      "\"Comparative evaluation of the protective effect of sodium "
      "valproate, phenazepam and ionol in stress-induced liver damage in "
      "rats\"@en");
  using namespace ad_utility::testing;
  // Ensure that the global singleton comparator (which is used for the <=>
  // comparison) is available. Creating a QEC sets this comparator.
  getQec(TestIndexConfig{});
  ASSERT_NO_THROW(IndexImpl::staticGlobalSingletonComparator());
  EXPECT_THAT(l1, testing::Not(testing::Eq(l2)));
  EXPECT_THAT(ql::compareThreeWay(l1, l2),
              testing::Not(testing::Eq(ql::strong_ordering::equal)));
}

// _____________________________________________________________________________
TEST(LiteralOrIri, toStringRepresentation) {
  {
    auto iri = LiteralOrIri::iriref("<bladibladibludiblu>");
    std::string expected = "<bladibladibludiblu>";

    auto res = iri.toStringRepresentation();
    EXPECT_EQ(res, expected);
    // The previous call did not move;
    EXPECT_EQ(iri.toStringRepresentation(), expected);

    res = std::move(iri).toStringRepresentation();
    EXPECT_EQ(res, expected);
    EXPECT_TRUE(iri.toStringRepresentation().empty());
  }
  // Similar tests, but for a literal:
  {
    auto lit = LiteralOrIri::literalWithoutQuotes("bladibladibludiblu");
    std::string expected = "\"bladibladibludiblu\"";

    auto res = lit.toStringRepresentation();
    EXPECT_EQ(res, expected);
    // The previous call did not move;
    EXPECT_EQ(lit.toStringRepresentation(), expected);

    res = std::move(lit).toStringRepresentation();
    EXPECT_EQ(res, expected);
    EXPECT_TRUE(lit.toStringRepresentation().empty());
  }
}

}  // namespace
