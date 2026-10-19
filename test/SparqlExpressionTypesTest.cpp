//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/SparqlExpressionTypes.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "util/AllocatorTestHelpers.h"
#include "util/GTestHelpers.h"
#include "util/IndexTestHelpers.h"

using namespace sparqlExpression;

TEST(SparqlExpressionTypes, expressionResult) {
  ExpressionResult a = Id::makeFromDouble(42.3);
  ExpressionResult b = Id::makeFromDouble(1.0);
  ASSERT_NO_THROW(b = copyExpressionResult(a));
  ASSERT_EQ(a, b);

  a = VectorWithMemoryLimit<Id>(ad_utility::testing::makeAllocator());
  a.emplace<Id>(Id::makeFromDouble(42.0));
  ASSERT_NO_THROW(b = copyExpressionResult(a));
  ASSERT_EQ(a, b);

  auto c = VectorWithMemoryLimit<Id>(ad_utility::testing::makeAllocator());
  c.emplace_back(Id::makeFromDouble(42.0));
  ASSERT_EQ(c.size(), 1);
  copyExpressionResult(static_cast<ExpressionResult>(std::move(c)));
  ASSERT_TRUE(c.empty());
}

TEST(SparqlExpressionTypes, printIdOrString) {
  using namespace ad_utility::triple_component;
  std::stringstream str;

  IdOrLocalVocabEntry idOrString{Id::makeUndefined()};
  PrintTo(idOrString, &str);
  ASSERT_EQ(str.str(), "U:0");
  auto* qec = ad_utility::testing::getQec();
  idOrString = LocalVocabEntry::literalWithoutQuotes(
      "bimm", qec->getLocalVocabContext());
  // Clear the stringstream.
  str.str({});
  PrintTo(idOrString, &str);
  ASSERT_EQ(str.str(), "\"bimm\"");
}

// _____________________________________________________________________________
TEST(SparqlExpressionTypes, makeIdOrLocalVocabEntry) {
  using namespace ad_utility::testing;
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
  auto result = makeIdOrLocalVocabEntry(LiteralOrIri{encodable}, context);
  ASSERT_TRUE(std::holds_alternative<Id>(result));
  // Compare the bits, as `Id::operator==` also matches a `LocalVocabIndex`.
  EXPECT_EQ(std::get<Id>(result).getBits(), expectedId.getBits());

  // Everything else becomes a `LocalVocabEntry`, including IRIs from the
  // vocabulary and literals that look like an encodable IRI.
  auto expectEntry = [&context](LiteralOrIri word) {
    auto result = makeIdOrLocalVocabEntry(word, context);
    ASSERT_TRUE(std::holds_alternative<LocalVocabEntry>(result));
    const auto& entry = std::get<LocalVocabEntry>(result);
    EXPECT_EQ(entry, word);
    EXPECT_FALSE(isEncodableIri(entry));
  };
  expectEntry(LiteralOrIri{Iri::fromIriref("<subject>")});
  expectEntry(LiteralOrIri{Iri::fromIriref("<http://example.org/5a>")});
  expectEntry(
      LiteralOrIri{Literal::literalWithoutQuotes("<http://example.org/69>")});

  EXPECT_TRUE(isEncodableIri(LocalVocabEntry{encodable, context}));
}
