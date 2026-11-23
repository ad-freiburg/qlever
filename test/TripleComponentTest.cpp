// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>
//

#include <absl/hash/hash.h>
#include <gmock/gmock.h>

#include "./util/GTestHelpers.h"
#include "./util/IdTestHelpers.h"
#include "./util/LocalVocabTestHelpers.h"
#include "./util/ParsedQueryTestHelpers.h"
#include "./util/TripleComponentTestHelpers.h"
#include "global/Id.h"
#include "index/TripleComponentConversions.h"
#include "index/vocabulary/EncodedIriManager.h"
#include "parser/TripleComponent.h"
#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/Literal.h"
#include "util/IndexTestHelpers.h"
#include "util/LocalVocabEntryTestHelpers.h"

using namespace std::literals;
using namespace ad_utility::testing;
namespace {
auto I = IntId;
auto D = DoubleId;
auto lit = tripleComponentLiteral;
}  // namespace

TEST(TripleComponent, setAndGetString) {
  const char* s = "someString\"%%\\";
  auto testString = [](auto input) {
    TripleComponent t{input};
    ASSERT_TRUE(t.isString());
    ASSERT_FALSE(t.isDouble());
    ASSERT_FALSE(t.isInt());
    ASSERT_FALSE(t.isVariable());
    ASSERT_EQ(t, input);
    ASSERT_EQ(t.getString(), input);
  };
  testString(s);
  testString(std::string_view{s});
  testString(std::string{s});
}

TEST(TripleComponent, setAndGetDouble) {
  double value = 83.12;
  TripleComponent object{value};
  ASSERT_FALSE(object.isString());
  ASSERT_FALSE(object.isVariable());
  ASSERT_FALSE(object.isInt());
  ASSERT_TRUE(object.isDouble());
  ASSERT_EQ(object, value);
  ASSERT_EQ(object.getDouble(), value);
}

TEST(TripleComponent, setAndGetInt) {
  int value = -42;
  TripleComponent object{value};
  ASSERT_FALSE(object.isString());
  ASSERT_FALSE(object.isDouble());
  ASSERT_FALSE(object.isVariable());
  ASSERT_TRUE(object.isInt());
  ASSERT_EQ(object, value);
  ASSERT_EQ(object.getInt(), value);
}

TEST(TripleComponent, setAndGetVariable) {
  TripleComponent tc{Variable{"?x"}};
  ASSERT_TRUE(tc.isVariable());
  ASSERT_FALSE(tc.isString());
  ASSERT_FALSE(tc.isDouble());
  ASSERT_FALSE(tc.isInt());
  ASSERT_EQ(tc, Variable{"?x"});
  ASSERT_EQ(tc.getVariable(), Variable{"?x"});
}

TEST(TripleComponent, setAndGetId) {
  Id id = Id::makeFromVocabIndex(VocabIndex::make(1));
  TripleComponent tc{id};
  ASSERT_TRUE(tc.isId());
  ASSERT_FALSE(tc.isVariable());
  ASSERT_FALSE(tc.isString());
  ASSERT_FALSE(tc.isDouble());
  ASSERT_FALSE(tc.isInt());
  ASSERT_FALSE(tc.isBool());
  ASSERT_FALSE(tc.isIri());
  ASSERT_FALSE(tc.isLiteral());
  ASSERT_FALSE(tc.isUndef());
  ASSERT_EQ(tc, id);
  ASSERT_EQ(tc.getId(), id);
  const TripleComponent tcConst = std::move(tc);
  ASSERT_EQ(tcConst.getId(), id);
}

// Test that a `TripleComponent` constructed from a `LiteralOrIri` holds the
// contained `Iri` or `Literal`.
TEST(TripleComponent, constructFromLiteralOrIri) {
  using ad_utility::triple_component::LiteralOrIri;
  TripleComponent iri{LiteralOrIri::fromStringRepresentation("<x>")};
  ASSERT_TRUE(iri.isIri());
  EXPECT_EQ(iri.getIri().toStringRepresentation(), "<x>");

  TripleComponent literal{LiteralOrIri::fromStringRepresentation("\"abc\"@en")};
  ASSERT_TRUE(literal.isLiteral());
  EXPECT_EQ(literal.getLiteral().toStringRepresentation(), "\"abc\"@en");
}

// Test that `getNumber` returns the value of an integer or a double (also as an
// `Id`), and `std::nullopt` for every other value.
TEST(TripleComponent, getNumber) {
  EXPECT_EQ(TripleComponent{-42}.getNumber(), -42.0);
  EXPECT_EQ(TripleComponent{0.5}.getNumber(), 0.5);
  EXPECT_EQ(TripleComponent{I(-42)}.getNumber(), -42.0);
  EXPECT_EQ(TripleComponent{D(0.5)}.getNumber(), 0.5);
  EXPECT_EQ(TripleComponent{lit("\"500\"")}.getNumber(), std::nullopt);
  EXPECT_EQ(TripleComponent{Id::makeFromBool(true)}.getNumber(), std::nullopt);
}

TEST(TripleComponent, assignmentOperator) {
  TripleComponent object;
  object = -12.435;
  ASSERT_TRUE(object.isDouble());
  ASSERT_EQ(object, -12.435);
  object = 483;
  ASSERT_TRUE(object.isInt());
  ASSERT_EQ(object, 483);

  {
    auto literal = lit("\"a\"", "@en");
    object = literal;
    EXPECT_EQ(object, literal);
    EXPECT_TRUE(object.isLiteral());
    EXPECT_EQ(object.getLiteral(), literal);
    EXPECT_EQ(std::as_const(object).getLiteral(), literal);
  }

  auto testString = [&object](auto input) {
    object = input;
    ASSERT_TRUE(object.isString());
    ASSERT_EQ(input, object);
  };
  testString("<someIri>");
  testString("aPlainString"s);

  object = Variable{"?alpha"};
  ASSERT_TRUE(object.isVariable());
  ASSERT_EQ(object, Variable{"?alpha"});
}

TEST(TripleComponent, toRdfLiteral) {
  TripleComponent literal{lit("\"aTypedLiteral\"", "^^<someType>")};
  EXPECT_EQ(toRdfLiteral(literal), "\"aTypedLiteral\"^^<someType>");
  std::vector<std::string> strings{"plainString", "<IRI>"};
  for (const auto& s : strings) {
    ASSERT_EQ(s, toRdfLiteral(TripleComponent{s}));
  }

  TripleComponent object{42};
  ASSERT_EQ(toRdfLiteral(object),
            R"("42"^^<http://www.w3.org/2001/XMLSchema#int>)");

  object = -43.3;
  ASSERT_EQ(toRdfLiteral(object),
            R"("-43.3"^^<http://www.w3.org/2001/XMLSchema#decimal>)");
  object = DateYearOrDuration{123456, DateYearOrDuration::Type::Year};
  ASSERT_EQ(toRdfLiteral(object),
            R"("123456"^^<http://www.w3.org/2001/XMLSchema#gYear>)");

  // Test encoded IRI - covers the "else" branch in toRdfLiteral
  // Create an EncodedIriManager with a test prefix
  EncodedIriManager encodedIriManager{
      std::vector<std::string>{"http://example.org/"}};
  std::string encodableIri = "<http://example.org/123>";
  auto encodedIdOpt = encodedIriManager.encode(encodableIri);

  // Create a TripleComponent with the encoded ID
  TripleComponent encodedIriComponent{encodedIdOpt.value()};
  std::string result = toRdfLiteral(encodedIriComponent);

  // This function is just used for cache keys etc, so it is not an issue that
  // the result is not human readable
  EXPECT_THAT(result, ::testing::HasSubstr("encodedId: "));
}

// _____________________________________________________________________________
TEST(TripleComponent, toRdfLiteralView) {
  // For all `TripleComponent`s that store their string representation, the view
  // is equal to the string that `toRdfLiteral` returns.
  auto expectSameAsToRdfLiteral = [](const TripleComponent& tripleComponent) {
    auto view = toRdfLiteralView(tripleComponent);
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(view.value(), toRdfLiteral(tripleComponent));
  };
  expectSameAsToRdfLiteral(lit("\"aTypedLiteral\"", "^^<someType>"));
  expectSameAsToRdfLiteral(TripleComponent{"plainString"});
  expectSameAsToRdfLiteral(iri("<someIri>"));
  expectSameAsToRdfLiteral(TripleComponent{Variable{"?alpha"}});

  // The view points into the `TripleComponent`, it is not a copy.
  TripleComponent iriComponent = iri("<someIri>");
  EXPECT_EQ(toRdfLiteralView(iriComponent).value().data(),
            iriComponent.getIri().toStringRepresentation().data());

  // `TripleComponent`s that store a directly encoded value have no string
  // representation that could be borrowed, so only `toRdfLiteral` works for
  // them.
  std::vector<TripleComponent> encodedValues{
      TripleComponent{42}, TripleComponent{-43.3}, TripleComponent{true},
      TripleComponent{
          DateYearOrDuration{123456, DateYearOrDuration::Type::Year}}};
  for (const auto& tripleComponent : encodedValues) {
    EXPECT_EQ(toRdfLiteralView(tripleComponent), std::nullopt);
    // `toRdfLiteral` still builds a new string for them.
    EXPECT_FALSE(toRdfLiteral(tripleComponent).empty());
  }
}

TEST(TripleComponent, toValueIdIfNotString) {
  TripleComponent tc{42};
  ASSERT_EQ(toValueIdIfNotString(tc, encodedIriManager()).value(), I(42));
  tc = 131.4;
  ASSERT_EQ(toValueIdIfNotString(tc, encodedIriManager()).value(), D(131.4));

  tc = TripleComponent{GeoPoint(47.9, 7.8)};
  ASSERT_EQ(toValueIdIfNotString(tc, encodedIriManager()).value().getDatatype(),
            Datatype::GeoPoint);
  ASSERT_FLOAT_EQ(toValueIdIfNotString(tc, encodedIriManager())
                      .value()
                      .getGeoPoint()
                      .getLat(),
                  47.9);
  ASSERT_FLOAT_EQ(toValueIdIfNotString(tc, encodedIriManager())
                      .value()
                      .getGeoPoint()
                      .getLng(),
                  7.8);

  DateYearOrDuration date{123456, DateYearOrDuration::Type::Year};
  tc = date;
  ASSERT_EQ(toValueIdIfNotString(tc, encodedIriManager()).value(),
            Id::makeFromDate(date));
  tc = "<x>";
  ASSERT_FALSE(toValueIdIfNotString(tc, encodedIriManager()).has_value());
  tc = lit("\"a\"");
  ASSERT_FALSE(toValueIdIfNotString(tc, encodedIriManager()).has_value());

  tc = Variable{"?x"};
  // Note: we cannot simply write `ASSERT_THROW(toValueIdIfNotString(tc),
  // Exception)` because `toValueIdIfNotString` is marked `nodiscard` and we
  // would get a compiler warning.
  auto f = [&]() {
    [[maybe_unused]] auto x = toValueIdIfNotString(tc, encodedIriManager());
  };
  ASSERT_THROW(f(), ad_utility::Exception);
}

TEST(TripleComponent, toValueId) {
  auto qec = getQec("<x> <y> <z>. <x> <y> \"alpha\".");
  const auto& index = qec->getIndex();

  TripleComponent tc = iri("<x>");
  auto getId = makeGetId(qec->getIndex());
  Id id = getId("<x>");
  ASSERT_EQ(toValueId(tc, index).value(), id);

  tc = lit("\"alpha\"");
  id = getId("\"alpha\"");
  EXPECT_EQ(toValueId(tc, index).value(), id);

  tc = iri("<notexisting>");
  ASSERT_FALSE(toValueId(tc, index).has_value());
  tc = 42;

  ASSERT_EQ(toValueIdIfNotString(tc, encodedIriManager()).value(), I(42));

  tc = iri(HAS_PATTERN_PREDICATE);
  ASSERT_EQ(toValueId(tc, index).value(),
            getId(std::string{HAS_PATTERN_PREDICATE}));

  auto lv = ad_utility::testing::makeLocalVocab();
  auto expectLocalVocab = [&lv, &index](TripleComponent tc, size_t pos) {
    auto id = toValueId(std::move(tc), index, lv);
    ASSERT_TRUE(id.getDatatype() == Datatype::LocalVocabIndex);
    auto lve = lv.getWord(id.getLocalVocabIndex());
    // Check that the constructed LVEs have the correct position in vocab set
    EXPECT_TRUE(lve.positionInVocabKnown_);
    testing::Matcher<LocalVocabEntry::IdProxy> boundMatcher =
        testing::Eq(LocalVocabEntry::IdProxy::make(
            Id::makeFromVocabIndex(VocabIndex::make(pos)).getBits()));
    EXPECT_THAT(lve.lowerBoundInVocab_, boundMatcher);
    EXPECT_THAT(lve.upperBoundInVocab_, boundMatcher);
  };
  expectLocalVocab(iri("<notexisting>"), 5);
  expectLocalVocab(lit("\"a\""), 0);
  expectLocalVocab(lit("\"b\""), 1);
}

TEST(TripleComponent, toValueIdOrBounds) {
  // The vocabulary is "alpha" ql:default-graph ql:has-pattern ql:has-predicate
  // ql:internal-graph ql:langtag <x> <y> <z>
  auto qec = getQec("<x> <y> <z>. <x> <y> \"alpha\".");
  const auto& index = qec->getIndex();
  auto getId = makeGetId(index);

  auto expectIsInVocab = [&index, &getId](TripleComponent tc) {
    AD_CORRECTNESS_CHECK(tc.isLiteral() || tc.isIri());
    auto idOrBounds = toValueIdOrBounds(tc, index);
    auto expectedId =
        getId(tc.isLiteral() ? tc.getLiteral().toStringRepresentation()
                             : tc.getIri().toStringRepresentation());
    EXPECT_THAT(idOrBounds, testing::VariantWith<Id>(testing::Eq(expectedId)));
  };
  using BoundsT = std::pair<VocabIndex, VocabIndex>;
  auto bounds = [](size_t lower, size_t upper) {
    return BoundsT{VocabIndex::make(lower), VocabIndex::make(upper)};
  };
  auto makePos = [](VocabIndex vi) {
    return LocalVocabEntry::IdProxy::make(Id::makeFromVocabIndex(vi).getBits());
  };
  auto expectBounds = [&index, &makePos](TripleComponent tc, BoundsT bounds) {
    AD_CORRECTNESS_CHECK(tc.isLiteral() || tc.isIri());
    // Check that toValueIdOrBounds returns the expected bounds
    auto idOrBounds = toValueIdOrBounds(tc, index);
    EXPECT_THAT(idOrBounds, testing::VariantWith<BoundsT>(testing::Eq(bounds)));
    // Check that the bounds are the same as from LocalVocabEntry
    auto lve =
        tc.isLiteral()
            ? LocalVocabEntry(tc.getLiteral(), index.getLocalVocabContext())
            : localVocabEntry(tc.getIri(), index.getLocalVocabContext());
    LocalVocabEntry::PositionInVocab positionFromBounds{makePos(bounds.first),
                                                        makePos(bounds.second)};
    EXPECT_EQ(lve.positionInVocab(), positionFromBounds);
  };

  expectIsInVocab(iri("<x>"));
  expectIsInVocab(lit("\"alpha\""));
  expectBounds(lit("\"a\""), bounds(0, 0));
  expectBounds(lit("\"b\""), bounds(1, 1));
  expectBounds(iri("<a>"), bounds(1, 1));
  expectBounds(iri("<k>"), bounds(5, 5));
  expectBounds(iri("<xx>"), bounds(6, 6));
  expectBounds(iri("<yy>"), bounds(7, 7));
}

TEST(TripleComponent, settingVariablesAsStringsIsIllegal) {
  ASSERT_THROW(TripleComponent("?x"sv), ad_utility::Exception);
  ASSERT_THROW(TripleComponent("?x"s), ad_utility::Exception);
  TripleComponent tc{42};
  auto assignString = [&]() { tc = "?y"s; };
  auto assignStringView = [&]() { tc = "?y"sv; };

  ASSERT_THROW(assignString(), ad_utility::Exception);
  ASSERT_THROW(assignStringView(), ad_utility::Exception);
}

TEST(TripleComponent, settingLiteralsAsStringsIsIllegal) {
  ASSERT_THROW(TripleComponent("\"x\""sv), ad_utility::Exception);
  ASSERT_THROW(TripleComponent("\'x\'"s), ad_utility::Exception);
  TripleComponent tc{42};
  auto assignString = [&]() { tc = "'y'"s; };
  auto assignStringView = [&]() { tc = "\"y\""sv; };

  ASSERT_THROW(assignString(), ad_utility::Exception);
  ASSERT_THROW(assignStringView(), ad_utility::Exception);
}

TEST(TripleComponent, invalidDatatypeForLiteral) {
  // No datatype.
  ASSERT_NO_THROW(lit("\"alpha\""));
  // A datatype.
  ASSERT_NO_THROW(lit("\"alpha\"", "^^<someType>"));
  // A language tag.
  ASSERT_NO_THROW(lit("\"alpha\"", "@fr-ca"));

  // Something that is invalid because it is none of the above
  ASSERT_ANY_THROW(lit("\"alpha\"", "fr-ca"));
}

// _____________________________________________________________________________1
TEST(TripleComponent, toString) {
  using namespace ::testing;
  auto match = [](const auto& matcher) {
    auto makeTcAndToString = [](auto&& val) {
      return TripleComponent{AD_FWD(val)}.toString();
    };
    return ResultOf(makeTcAndToString, matcher);
  };

  EXPECT_THAT(GeoPoint(13, 14), match(StartsWith("G:POINT(14.")));
  EXPECT_THAT("hello", match("hello"));
  EXPECT_THAT(12, match("12"));
  EXPECT_THAT(12.3, match("12.3"));
  using Tc = TripleComponent;
  EXPECT_THAT(Tc::UNDEF{}, match("UNDEF"));
  EXPECT_THAT(Variable("?x"), match("?x"));
  EXPECT_THAT(Tc::Literal::literalWithoutQuotes("hallo"), match("\"hallo\""));
  EXPECT_THAT(Tc::Iri::fromIrirefWithoutBrackets("blim"), match("<blim>"));
  EXPECT_THAT(DateYearOrDuration(Date(2000, 1, 1)), match("DATE: 2000-01-01"));
  EXPECT_THAT(true, match("true"));
  EXPECT_THAT(false, match("false"));
  EXPECT_THAT(Id::makeFromInt(42), match("I:42"));
}

// Test that `toIdOrLocalVocabEntry` returns the encoded `Id` of an encodable
// IRI and a `LocalVocabEntry` for everything else.
TEST(TripleComponent, toIdOrLocalVocabEntry) {
  using ad_utility::triple_component::Iri;
  using ad_utility::triple_component::Literal;
  using ad_utility::triple_component::LiteralOrIri;
  TestIndexConfig config{"<subject> <predicate> <http://example.org/42>"};
  config.encodedPrefixesWithoutAngleBrackets = {"http://example.org/"};
  auto* qec = getQec(std::move(config));
  const auto& context = qec->getLocalVocabContext();

  // An encodable IRI yields its encoded `Id`.
  auto encodable = Iri::fromIriref("<http://example.org/69>");
  auto expectedId = qec->getIndex()
                        .encodedIriManager()
                        .encode(encodable.toStringRepresentation())
                        .value();
  auto result = toIdOrLocalVocabEntry(LiteralOrIri{encodable}, context);
  ASSERT_TRUE(std::holds_alternative<Id>(result));
  // Compare the bits, as `Id::operator==` also matches a `LocalVocabIndex`.
  EXPECT_EQ(std::get<Id>(result).getBits(), expectedId.getBits());

  // Everything else becomes a `LocalVocabEntry`, including IRIs from the
  // vocabulary and literals that look like an encodable IRI.
  auto expectEntry = [&context](LiteralOrIri word) {
    auto result = toIdOrLocalVocabEntry(word, context);
    ASSERT_TRUE(std::holds_alternative<LocalVocabEntry>(result));
    const auto& entry = std::get<LocalVocabEntry>(result);
    EXPECT_EQ(entry, word);
  };
  expectEntry(LiteralOrIri{Iri::fromIriref("<subject>")});
  expectEntry(LiteralOrIri{Iri::fromIriref("<http://example.org/5a>")});
  expectEntry(
      LiteralOrIri{Literal::literalWithoutQuotes("<http://example.org/69>")});
}

// Test the construction, access, copy, move, equality, hashing and string
// output of a `TripleComponent` that holds a nested triple term.
TEST(TripleComponent, recursiveTripleTerm) {
  auto makeTerm = [](int64_t value) -> TripleComponent {
    return TripleComponent::TripleTerm{
        iri("<s>"), iri("<p>"),
        TripleComponent::TripleTerm{iri("<inner>"), iri("<q>"), value}};
  };
  // The components of the nested term are accessible.
  auto term = makeTerm(42);
  ASSERT_TRUE(term.isTripleTerm());
  const auto& outer = term.getTripleTerm();
  EXPECT_EQ(outer.subject(), iri("<s>"));
  EXPECT_EQ(outer.predicate(), iri("<p>"));
  ASSERT_TRUE(outer.object().isTripleTerm());
  EXPECT_EQ(outer.object().getTripleTerm().object(), int64_t{42});

  // Copies and moves keep the contents, equality and hashing compare the
  // contents and not the allocation.
  auto copy = term;
  auto moved = std::move(term);
  term = false;
  EXPECT_EQ(copy, moved);
  auto independentlyAllocated = makeTerm(42);
  EXPECT_EQ(copy, independentlyAllocated);
  EXPECT_NE(copy, makeTerm(43));
  EXPECT_EQ(absl::Hash<TripleComponent>{}(copy),
            absl::Hash<TripleComponent>{}(independentlyAllocated));

  // Overwriting one holder leaves the other intact, the string output is the
  // Turtle syntax, and a triple term is not a number.
  copy = true;
  EXPECT_EQ(moved, independentlyAllocated);
  EXPECT_EQ(moved.toString(), "<<( <s> <p> <<( <inner> <q> 42 )>> )>>");
  EXPECT_FALSE(moved.getNumber().has_value());

  // Assignment of a `TripleComponent` that holds a triple term.
  TripleComponent assigned;
  assigned = moved;
  EXPECT_EQ(assigned, moved);
  assigned = makeTerm(43);
  EXPECT_EQ(assigned, makeTerm(43));
}

// Test that moved-from `ParsedTripleTerm`s compare equal to each other and
// unequal to valid terms, and that they can be assigned again.
TEST(TripleComponent, tripleTermEqualityAfterMove) {
  using Term = TripleComponent::TripleTerm;
  Term first{iri("<s>"), iri("<p>"), iri("<o>")};
  Term second{iri("<s>"), iri("<p>"), iri("<o>")};
  // Two moved-from terms are equal, a moved-from and a valid term are not.
  auto valid = std::move(first);
  auto otherValid = std::move(second);
  EXPECT_EQ(first, second);
  EXPECT_NE(first, valid);
  EXPECT_NE(valid, first);
  EXPECT_NE(second, otherValid);
  EXPECT_EQ(valid, otherValid);

  // A moved-from term can be assigned again, and moving from it again works.
  first = valid;
  EXPECT_EQ(first, valid);
  EXPECT_NE(first, second);
  second = std::move(first);
  EXPECT_EQ(second, valid);
  EXPECT_NE(first, second);
}

// Test that every conversion of a triple term to an `Id` or to RDF output
// throws.
TEST(TripleComponent, tripleTermConversionsAreUnsupported) {
  TripleComponent term =
      TripleComponent::TripleTerm{iri("<s>"), iri("<p>"), lit("object")};
  AD_EXPECT_THROW_WITH_MESSAGE(
      (void)toValueIdIfNotString(term, encodedIriManager()),
      ::testing::HasSubstr(
          "Converting parsed triple terms to IDs is not supported"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      (void)toRdfLiteral(term),
      ::testing::HasSubstr(
          "Converting parsed triple terms to RDF output is not yet supported"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      (void)toRdfLiteralView(term),
      ::testing::HasSubstr(
          "Converting parsed triple terms to RDF output is not yet supported"));
  auto qec = getQec("<s> <p> <o> .");
  const auto& index = qec->getIndex();
  LocalVocab localVocab{qec->getAllocator()};
  AD_EXPECT_THROW_WITH_MESSAGE(
      (void)toValueId(term, index),
      ::testing::HasSubstr(
          "Converting parsed triple terms to IDs is not supported"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      (void)toValueIdOrBounds(term, index),
      ::testing::HasSubstr(
          "Converting parsed triple terms to IDs is not supported"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      (void)toValueId(std::move(term), index, localVocab),
      ::testing::HasSubstr(
          "Converting parsed triple terms to IDs is not supported"));
}
