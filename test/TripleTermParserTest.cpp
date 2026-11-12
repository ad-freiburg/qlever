// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/hash/hash.h>
#include <absl/strings/str_join.h>
#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "index/TripleTermIndex.h"
#include "parser/RdfParser.h"
#include "parser/Tokenizer.h"
#include "parser/TokenizerCtre.h"

namespace {
using TripleTerm = TripleComponent::TripleTerm;
using Iri = TripleComponent::Iri;

// The full-IRI form of `ex:<local>`.
std::string ex(std::string_view local) {
  return absl::StrCat("<http://example.org/", local, ">");
}

// The string forms of the subject, predicate, and object of `term`.
std::vector<std::string> componentStrings(const TripleTerm& term) {
  std::vector<std::string> result;
  for (const auto& iri : term.components()) {
    result.push_back(iri.toStringRepresentation());
  }
  return result;
}
}  // namespace

template <typename TokenizerType>
class TripleTermParserTest : public ::testing::Test {
 protected:
  EncodedIriManager manager_;
  RdfStringParser<TurtleParser<TokenizerType>> parser_{&manager_};
};
using Tokenizers = ::testing::Types<Tokenizer, TokenizerCtre>;
TYPED_TEST_SUITE(TripleTermParserTest, Tokenizers);

TYPED_TEST(TripleTermParserTest, OrdinaryParsingAfterExplicitTerm) {
  auto& parser = this->parser_;
  parser.setInputStream("<<(<http://a> <http://p> <http://b>)>>");
  ASSERT_TRUE(parser.parseTripleTermPrototype().has_value());
  parser.setInputStream("<http://report> <http://mentions> <http://other> .");
  auto triples = parser.parseAndReturnAllTriples();
  ASSERT_EQ(triples.size(), 1);
  EXPECT_EQ(triples[0].subject_.getIri().toStringRepresentation(),
            "<http://report>");
  EXPECT_EQ(triples[0].predicate_.getIri().toStringRepresentation(),
            "<http://mentions>");
  EXPECT_EQ(triples[0].object_.getIri().toStringRepresentation(),
            "<http://other>");
}

TYPED_TEST(TripleTermParserTest, StructuredComponentsWithoutAssertion) {
  auto& parser = this->parser_;
  parser.setInputStream("@prefix ex: <http://example.org/> .");
  ASSERT_TRUE(parser.parseDirectiveManually());
  const auto previous = parser.getLastParseResult();
  parser.setInputStream("<<(ex:Alice # comment\n ex:knows ex:Bob)>> trailing");
  auto term = parser.parseTripleTermPrototype();
  ASSERT_TRUE(term.has_value());
  const std::vector<std::string> expected{ex("Alice"), ex("knows"), ex("Bob")};
  EXPECT_EQ(componentStrings(*term), expected);
  EXPECT_TRUE(parser.getTriples().empty());
  EXPECT_EQ(parser.getUnparsedRemainder(), " trailing");
  EXPECT_EQ(parser.getLastParseResult(), previous);
}

TYPED_TEST(TripleTermParserTest, RejectsUnsupportedAndMalformedTerms) {
  auto& parser = this->parser_;
  for (const std::string input :
       {"<<(<http://a> <http://p>)>>", "<<(<http://a> <http://p> <http://b>)>",
        "<<(<http://a> <http://p> <http://b> <http://extra>)>>",
        "<<(<http://a> <http://p> 42)>>", "<<(<http://a> <http://p> _:blank)>>",
        "<<(<http://a> <http://p> <<(<http://x> <http://y> <http://z>)>>)>>"}) {
    SCOPED_TRACE(input);
    parser.setInputStream(input);
    const auto previous = parser.getLastParseResult();
    EXPECT_THROW(parser.parseTripleTermPrototype(), ParseException);
    EXPECT_TRUE(parser.getTriples().empty());
    EXPECT_EQ(parser.getLastParseResult(), previous);
  }
  parser.setInputStream("<http://ordinary>");
  EXPECT_FALSE(parser.parseTripleTermPrototype().has_value());
  EXPECT_EQ(parser.getUnparsedRemainder(), "<http://ordinary>");
}

TYPED_TEST(TripleTermParserTest, ResolvedPrefixesShareAnIndexEntry) {
  auto& parser = this->parser_;
  parser.setInputStream("@prefix ex: <http://example.org/> .");
  ASSERT_TRUE(parser.parseDirectiveManually());
  // Explicit mock of the FINAL vocabulary lookup. Production integration must
  // resolve partial-vocabulary IDs before interning; parser-local IDs won't do.
  std::map<std::string, VocabIndex> vocabulary{
      {ex("Alice"), VocabIndex::make(1)},
      {ex("knows"), VocabIndex::make(2)},
      {ex("Bob"), VocabIndex::make(3)}};
  qlever::tripleTermPrototype::TripleTermIndexBuilder builder;
  for (const std::string input :
       {"<<(ex:Alice ex:knows ex:Bob)>>",
        "<<(<http://example.org/Alice> <http://example.org/knows> "
        "<http://example.org/Bob>)>>"}) {
    parser.setInputStream(input);
    auto term = parser.parseTripleTermPrototype();
    ASSERT_TRUE(term.has_value());
    qlever::tripleTermPrototype::TripleTermIndexBuilder::Triple resolved;
    for (size_t i = 0; i < 3; ++i) {
      resolved[i] = Id::makeFromVocabIndex(
          vocabulary.at(term->components()[i].toStringRepresentation()));
    }
    EXPECT_EQ(builder.intern(resolved).get(), 0);
  }
  EXPECT_EQ(builder.size(), 1);
  auto knows = Id::makeFromVocabIndex(VocabIndex::make(2));
  auto bob = Id::makeFromVocabIndex(VocabIndex::make(3));
  EXPECT_EQ(builder.scan(knows, bob).size(), 1);
  EXPECT_TRUE(parser.getTriples().empty());
}

// The example from the design note: two reports mention the same term, a third
// one a different term. Only the outer triples are emitted.
TYPED_TEST(TripleTermParserTest, TurtleObjectsAreTripleTerms) {
  auto& parser = this->parser_;
  parser.setInputStream(
      "@prefix ex: <http://example.org/> .\n"
      "ex:reportA ex:reifies <<( ex:alice ex:p ex:bob )>> .\n"
      "ex:reportB ex:reifies <<(ex:alice ex:p ex:bob)>> , ex:other ;\n"
      "           ex:about <<( <http://example.org/alice> ex:p ex:charlie )>> "
      ".\n");
  auto triples = parser.parseAndReturnAllTriples();
  ASSERT_EQ(triples.size(), 4);
  const auto& bobA = triples[0].object_;
  const auto& bobB = triples[1].object_;
  const auto& charlie = triples[3].object_;
  ASSERT_TRUE(bobA.isTripleTerm());
  ASSERT_TRUE(bobB.isTripleTerm());
  ASSERT_TRUE(charlie.isTripleTerm());
  EXPECT_TRUE(triples[2].object_.isIri());

  // Structural identity, independent of spacing and of prefixed vs. full IRIs.
  EXPECT_EQ(bobA, bobB);
  EXPECT_EQ(absl::HashOf(bobA), absl::HashOf(bobB));
  EXPECT_NE(bobA, charlie);
  const std::vector<std::string> expectedBob{ex("alice"), ex("p"), ex("bob")};
  EXPECT_EQ(componentStrings(bobA.getTripleTerm()), expectedBob);
  EXPECT_EQ(bobA.toString(), absl::StrCat("<<( ", ex("alice"), " ", ex("p"),
                                          " ", ex("bob"), " )>>"));

  // Mentioning a term does not assert it.
  for (const auto& triple : triples) {
    EXPECT_NE(triple.subject_.toString(), ex("alice"));
  }
}

// RDF 1.2 only allows triple terms in object position.
TYPED_TEST(TripleTermParserTest, TurtleRejectsTripleTermsOutsideObjects) {
  for (const std::string input :
       {"<<( <http://a> <http://p> <http://b> )>> <http://q> <http://c> .",
        "<http://s> <<( <http://a> <http://p> <http://b> )>> <http://c> ."}) {
    SCOPED_TRACE(input);
    RdfStringParser<TurtleParser<TypeParam>> parser{&this->manager_};
    parser.setInputStream(input);
    EXPECT_THROW(parser.parseAndReturnAllTriples(), ParseException);
  }
}

// The parser encodes ordinary IRIs into `Id`s if a prefix is configured, but
// keeps the components of a triple term as IRIs; the index builder will encode
// them in one place.
TYPED_TEST(TripleTermParserTest, TermComponentsAreNotEncodedByTheParser) {
  // The encoder only encodes IRIs of the form `<prefix><number>`.
  EncodedIriManager manager{std::vector<std::string>{"http://example.org/"}};
  const std::vector<std::string> components{ex("1"), ex("2"), ex("3")};
  for (const auto& iri : components) {
    ASSERT_TRUE(manager.encode(iri).has_value()) << iri;
  }
  RdfStringParser<TurtleParser<TypeParam>> parser{&manager};
  parser.setInputStream(absl::StrCat(ex("10"), " ", ex("11"), " <<( ",
                                     absl::StrJoin(components, " "), " )>> ."));
  auto triples = parser.parseAndReturnAllTriples();
  ASSERT_EQ(triples.size(), 1);
  // The outer subject and predicate are encoded as usual ...
  EXPECT_TRUE(triples[0].subject_.isId());
  EXPECT_TRUE(triples[0].predicate_.isId());
  // ... but the components of the term stay IRIs.
  ASSERT_TRUE(triples[0].object_.isTripleTerm());
  EXPECT_EQ(componentStrings(triples[0].object_.getTripleTerm()), components);
}

TYPED_TEST(TripleTermParserTest, NQuadsObjectsAreTripleTerms) {
  RdfStringParser<NQuadParser<TypeParam>> parser{&this->manager_};
  parser.setInputStream(
      "<http://a> <http://r> <<( <http://s> <http://p> <http://o> )>> .\n"
      "<http://b> <http://r> <<(<http://s> <http://p> <http://o>)>> <http://g> "
      ".\n");
  auto triples = parser.parseAndReturnAllTriples();
  ASSERT_EQ(triples.size(), 2);
  ASSERT_TRUE(triples[0].object_.isTripleTerm());
  EXPECT_EQ(triples[0].object_, triples[1].object_);
  EXPECT_EQ(triples[1].graphIri_.toString(), "<http://g>");
}

// A copy shares the immutable components; equality is by value.
TEST(TripleTerm, ValueSemantics) {
  auto iri = [](std::string_view local) { return Iri::fromIriref(ex(local)); };
  TripleTerm term{iri("s"), iri("p"), iri("o")};
  TripleTerm copy = term;
  EXPECT_EQ(copy, term);
  EXPECT_EQ(&copy.components(), &term.components());
  TripleTerm sameValue{iri("s"), iri("p"), iri("o")};
  EXPECT_EQ(sameValue, term);
  EXPECT_EQ(absl::HashOf(sameValue), absl::HashOf(term));
  EXPECT_NE(term, (TripleTerm{iri("o"), iri("p"), iri("s")}));
  EXPECT_EQ(term.subject(), iri("s"));
  EXPECT_EQ(term.predicate(), iri("p"));
  EXPECT_EQ(term.object(), iri("o"));
}
