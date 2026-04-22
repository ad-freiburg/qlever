// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@students.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "engine/ConstructTripleInstantiator.h"
#include "engine/ConstructTypes.h"
#include "global/Constants.h"

namespace {
using namespace qlever::constructExport;
using ::testing::ElementsAre;
using ::testing::Optional;
using ::testing::UnorderedElementsAre;

//______________________________________________________________________________

EvaluatedTerm makeTerm(std::string str, const char* type = nullptr) {
  return std::make_shared<const EvaluatedTermData>(std::move(str), type);
}

EvaluatedTriple makeTriple(EvaluatedTerm s, EvaluatedTerm p, EvaluatedTerm o) {
  return EvaluatedTriple{std::move(s), std::move(p), std::move(o)};
}

// Matches an `EvaluatedTerm` (shared_ptr<const EvaluatedTermData>) by
// dereferencing it and checking both fields. `type` uses pointer equality,
// matching the compile-time constants (e.g. XSD_INT_TYPE) or nullptr.
static constexpr auto matchesEvaluatedTerm = [](const auto& str,
                                                const char* type) {
  return ::testing::Pointee(::testing::AllOf(
      AD_FIELD(EvaluatedTermData, rdfTermString_, std::string(str)),
      AD_FIELD(EvaluatedTermData, rdfTermDataType_, ::testing::Eq(type))));
};

// Matches an `EvaluatedTriple` by applying `matchesEvaluatedTerm` with
// `nullptr` type to each of subject, predicate, and object.
static constexpr auto matchesEvaluatedTriple = [](const auto& s, const auto& p,
                                                  const auto& o) {
  return ::testing::AllOf(
      AD_FIELD(EvaluatedTriple, subject_, matchesEvaluatedTerm(s, nullptr)),
      AD_FIELD(EvaluatedTriple, predicate_, matchesEvaluatedTerm(p, nullptr)),
      AD_FIELD(EvaluatedTriple, object_, matchesEvaluatedTerm(o, nullptr)));
};

//______________________________________________________________________________

TEST(InstantiateTerm, PrecomputedConstantIsReturnedAsIs) {
  auto term = makeTerm("<http://example.org/subject>");
  PreprocessedTerm preprocessed = PrecomputedConstant{term};
  auto batchResult = BatchEvaluationResult{{}, 1};

  auto result = instantiateTerm(preprocessed, batchResult, 0, 0);

  EXPECT_THAT(result, Optional(matchesEvaluatedTerm(
                          "<http://example.org/subject>", nullptr)));
}

TEST(InstantiateTerm, PrecomputedConstantIgnoresRowIndices) {
  auto term = makeTerm("<http://example.org/subject>");
  PreprocessedTerm preprocessed = PrecomputedConstant{term};
  auto batchResult = BatchEvaluationResult{{}, 5};

  auto result0 = instantiateTerm(preprocessed, batchResult, 0, 0);
  auto result3 = instantiateTerm(preprocessed, batchResult, 3, 100);

  EXPECT_THAT(result0, Optional(matchesEvaluatedTerm(
                           "<http://example.org/subject>", nullptr)));
  EXPECT_THAT(result3, Optional(matchesEvaluatedTerm(
                           "<http://example.org/subject>", nullptr)));
}

TEST(InstantiateTerm, PrecomputedVariableBound) {
  auto term = makeTerm("<http://example.org/value>");
  EvaluatedVariableValues vals = {std::nullopt, term, std::nullopt};
  auto batchResult = BatchEvaluationResult{{{2, std::move(vals)}}, 3};

  PreprocessedTerm preprocessed = PrecomputedVariable{2};
  auto result = instantiateTerm(preprocessed, batchResult, 1, 99);

  EXPECT_THAT(result, Optional(matchesEvaluatedTerm(
                          "<http://example.org/value>", nullptr)));
}

TEST(InstantiateTerm, PrecomputedVariableUnbound) {
  EvaluatedVariableValues vals = {std::nullopt, std::nullopt};
  auto batchResult = BatchEvaluationResult{{{0, std::move(vals)}}, 2};

  PreprocessedTerm preprocessed = PrecomputedVariable{0};
  auto result = instantiateTerm(preprocessed, batchResult, 1, 1);

  EXPECT_THAT(result, ::testing::Eq(std::nullopt));
}

TEST(InstantiateTerm, PrecomputedBlankNodeUsesRowIdxTotal) {
  auto batchResult = BatchEvaluationResult{{}, 1};
  PreprocessedTerm preprocessed = PrecomputedBlankNode{"_:g", "_label"};

  auto result = instantiateTerm(preprocessed, batchResult, 0, 42);

  EXPECT_THAT(result, Optional(matchesEvaluatedTerm("_:g42_label", nullptr)));
}

TEST(InstantiateTerm, PrecomputedBlankNodeIgnoresBatchRowIdx) {
  // rowIdxTotal drives blank node ID, not rowIdxInBatch.
  auto batchResult = BatchEvaluationResult{{}, 5};
  PreprocessedTerm preprocessed = PrecomputedBlankNode{"_:u", "_x"};

  auto r0 = instantiateTerm(preprocessed, batchResult, 0, 7);
  auto r1 = instantiateTerm(preprocessed, batchResult, 4, 7);

  EXPECT_THAT(r0, Optional(matchesEvaluatedTerm("_:u7_x", nullptr)));
  EXPECT_THAT(r1, Optional(matchesEvaluatedTerm("_:u7_x", nullptr)));
}

//______________________________________________________________________________

TEST(InstantiateBatch, EmptyTemplate) {
  PreprocessedConstructTemplate tmpl;
  auto batchResult = BatchEvaluationResult{{}, 3};

  auto result = instantiateBatch(tmpl, batchResult, 0);

  EXPECT_THAT(result, ::testing::IsEmpty());
}

TEST(InstantiateBatch, EmptyBatch) {
  auto s = makeTerm("<http://s>");
  auto p = makeTerm("<http://p>");
  auto o = makeTerm("<http://o>");
  PreprocessedConstructTemplate tmpl;
  tmpl.preprocessedTriples_ = {
      {PrecomputedConstant{s}, PrecomputedConstant{p}, PrecomputedConstant{o}}};
  auto batchResult = BatchEvaluationResult{{}, 0};

  auto result = instantiateBatch(tmpl, batchResult, 0);

  EXPECT_THAT(result, ::testing::IsEmpty());
}

TEST(InstantiateBatch, ConstantTripleReplicatedAcrossRows) {
  auto s = makeTerm("<http://s>");
  auto p = makeTerm("<http://p>");
  auto o = makeTerm("<http://o>");
  PreprocessedConstructTemplate tmpl;
  tmpl.preprocessedTriples_ = {
      {PrecomputedConstant{s}, PrecomputedConstant{p}, PrecomputedConstant{o}}};
  auto batchResult = BatchEvaluationResult{{}, 3};

  auto result = instantiateBatch(tmpl, batchResult, 0);

  EXPECT_THAT(
      result,
      ElementsAre(
          matchesEvaluatedTriple("<http://s>", "<http://p>", "<http://o>"),
          matchesEvaluatedTriple("<http://s>", "<http://p>", "<http://o>"),
          matchesEvaluatedTriple("<http://s>", "<http://p>", "<http://o>")));
}

TEST(InstantiateBatch, UnboundVariableDropsTriple) {
  // column 0: [bound, unbound, bound]
  EvaluatedVariableValues vals = {makeTerm("<http://a>"), std::nullopt,
                                  makeTerm("<http://c>")};
  auto batchResult = BatchEvaluationResult{{{0, vals}}, 3};
  auto p = makeTerm("<http://p>");
  auto o = makeTerm("<http://o>");
  PreprocessedConstructTemplate tmpl;
  tmpl.preprocessedTriples_ = {
      {PrecomputedVariable{0}, PrecomputedConstant{p}, PrecomputedConstant{o}}};

  auto result = instantiateBatch(tmpl, batchResult, 0);

  EXPECT_THAT(
      result,
      ElementsAre(
          matchesEvaluatedTriple("<http://a>", "<http://p>", "<http://o>"),
          matchesEvaluatedTriple("<http://c>", "<http://p>", "<http://o>")));
}

TEST(InstantiateBatch, BlankNodeIdIncludesBatchOffset) {
  auto p = makeTerm("<http://p>");
  auto o = makeTerm("<http://o>");
  PreprocessedConstructTemplate tmpl;
  tmpl.preprocessedTriples_ = {{PrecomputedBlankNode{"_:g", ""},
                                PrecomputedConstant{p},
                                PrecomputedConstant{o}}};
  auto batchResult = BatchEvaluationResult{{}, 2};

  auto result = instantiateBatch(tmpl, batchResult, /*batchOffset=*/5);

  EXPECT_THAT(
      result,
      ElementsAre(matchesEvaluatedTriple("_:g5", "<http://p>", "<http://o>"),
                  matchesEvaluatedTriple("_:g6", "<http://p>", "<http://o>")));
}

TEST(InstantiateBatch, MultipleTriples) {
  auto s = makeTerm("<http://s>");
  auto p1 = makeTerm("<http://p1>");
  auto o1 = makeTerm("<http://o1>");
  auto p2 = makeTerm("<http://p2>");
  auto o2 = makeTerm("<http://o2>");
  PreprocessedConstructTemplate tmpl;
  tmpl.preprocessedTriples_ = {{PrecomputedConstant{s}, PrecomputedConstant{p1},
                                PrecomputedConstant{o1}},
                               {PrecomputedConstant{s}, PrecomputedConstant{p2},
                                PrecomputedConstant{o2}}};
  auto batchResult = BatchEvaluationResult{{}, 2};

  auto result = instantiateBatch(tmpl, batchResult, 0);

  // Row-major: for each row, all triples in template order.
  EXPECT_THAT(
      result,
      ElementsAre(
          matchesEvaluatedTriple("<http://s>", "<http://p1>", "<http://o1>"),
          matchesEvaluatedTriple("<http://s>", "<http://p2>", "<http://o2>"),
          matchesEvaluatedTriple("<http://s>", "<http://p1>", "<http://o1>"),
          matchesEvaluatedTriple("<http://s>", "<http://p2>", "<http://o2>")));
}

//______________________________________________________________________________

static constexpr auto testTermFormatting =
    [](const std::string& rdfTermString, const char* rdfTermDataType,
       const std::string& expectedStringWhenIncludeDataType,
       const std::string& expectedStringWhenNotIncludeDataType) {
      auto term = EvaluatedTermData{rdfTermString, rdfTermDataType};
      EXPECT_EQ(expectedStringWhenIncludeDataType, formatTerm(term, true));
      EXPECT_EQ(expectedStringWhenNotIncludeDataType, formatTerm(term, false));
    };

TEST(FormatTerm, dataTypeNull) {
  // By the definition of `EvaluatedTermData`, if `rdfTermDataType_`==`nullptr`,
  // then `rdfTermString_` is expected to already hold the value of the rdf term
  // string include the complete serialized form (including the appended
  // datatype string via ^^).

  // Setting `includeDataType` = true instructs `formatTerm` to append the
  // datataype string (which is contained in `rdfTermDataType`) to the string
  // representing the rdf term (`rdfTermString`). But if
  // `rdfTermDataType` == nullptr, then we expect `rdfTermString` to already
  // be appended with its dataType. Thus, the value of `includeDataType` is
  // ignored.
  testTermFormatting("1^^xsd:integer", nullptr, "1^^xsd:integer",
                     "1^^xsd:integer");
}

TEST(FormatTerm, XSD_INT_TYPE) {
  testTermFormatting("1", XSD_INT_TYPE,
                     "\"1\"^^<http://www.w3.org/2001/XMLSchema#int>", "1");
}

TEST(FormatTerm, XSD_DECIMAL_TYPE) {
  testTermFormatting("1", XSD_DECIMAL_TYPE,
                     "\"1\"^^<http://www.w3.org/2001/XMLSchema#decimal>", "1");
}

TEST(FormatTerm, XSD_BOOLEAN_TYPE) {
  testTermFormatting("1", XSD_BOOLEAN_TYPE,
                     "\"1\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
                     "\"1\"^^<http://www.w3.org/2001/XMLSchema#boolean>");
  testTermFormatting("0", XSD_BOOLEAN_TYPE,
                     "\"0\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
                     "\"0\"^^<http://www.w3.org/2001/XMLSchema#boolean>");
  testTermFormatting("true", XSD_BOOLEAN_TYPE,
                     "\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
                     "true");
  testTermFormatting("false", XSD_BOOLEAN_TYPE,
                     "\"false\"^^<http://www.w3.org/2001/XMLSchema#boolean>",
                     "false");
}

TEST(FormatTerm, XSD_DOUBLE_TYPE) {
  testTermFormatting("1.0", XSD_DOUBLE_TYPE,
                     "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>",
                     "\"1.0\"^^<http://www.w3.org/2001/XMLSchema#double>");
  testTermFormatting("INF", XSD_DOUBLE_TYPE,
                     "\"INF\"^^<http://www.w3.org/2001/XMLSchema#double>",
                     "\"INF\"^^<http://www.w3.org/2001/XMLSchema#double>");
  testTermFormatting("false", XSD_DOUBLE_TYPE,
                     "\"false\"^^<http://www.w3.org/2001/XMLSchema#double>",
                     "\"false\"^^<http://www.w3.org/2001/XMLSchema#double>");
}

//______________________________________________________________________________

TEST(FormatTriple, TurtleIriObject) {
  auto triple = makeTriple(makeTerm("<http://s>"), makeTerm("<http://p>"),
                           makeTerm("<http://o>"));
  EXPECT_EQ("<http://s> <http://p> <http://o> .\n",
            formatTriple(triple, ad_utility::MediaType::turtle));
}

TEST(FormatTriple, TurtleLiteralObject) {
  auto triple = makeTriple(makeTerm("<http://s>"), makeTerm("<http://p>"),
                           makeTerm("\"hello\""));
  EXPECT_EQ("<http://s> <http://p> \"hello\" .\n",
            formatTriple(triple, ad_utility::MediaType::turtle));
}

TEST(FormatTriple, CsvSimpleTerms) {
  auto triple = makeTriple(makeTerm("<http://s>"), makeTerm("<http://p>"),
                           makeTerm("<http://o>"));
  EXPECT_EQ("<http://s>,<http://p>,<http://o>\n",
            formatTriple(triple, ad_utility::MediaType::csv));
}

TEST(FormatTriple, CSVUsesCSVEscaping) {
  // check whether literals that contain special characters of CSV are escaped
  // correctly (i.e. whether `RdfEscaping::escapeForCsv` is used for the term
  // strings.)
  auto triple = makeTriple(makeTerm("<http://s>"), makeTerm("<http://p>"),
                           makeTerm("val,uee"));
  EXPECT_EQ("<http://s>,<http://p>,\"val,uee\"\n",
            formatTriple(triple, ad_utility::MediaType::csv));
}

TEST(FormatTriple, CsvCommaInSubject) {
  auto triple = makeTriple(makeTerm("sub,ject"), makeTerm("<http://p>"),
                           makeTerm("<http://o>"));
  EXPECT_EQ("\"sub,ject\",<http://p>,<http://o>\n",
            formatTriple(triple, ad_utility::MediaType::csv));
}

TEST(FormatTriple, CsvCommaInPredicate) {
  auto triple = makeTriple(makeTerm("<http://s>"), makeTerm("pred,icate"),
                           makeTerm("<http://o>"));
  EXPECT_EQ("<http://s>,\"pred,icate\",<http://o>\n",
            formatTriple(triple, ad_utility::MediaType::csv));
}

TEST(FormatTriple, TsvSimpleTerms) {
  auto triple = makeTriple(makeTerm("<http://s>"), makeTerm("<http://p>"),
                           makeTerm("<http://o>"));
  EXPECT_EQ("<http://s>\t<http://p>\t<http://o>\n",
            formatTriple(triple, ad_utility::MediaType::tsv));
}

TEST(FormatTriple, TsvObjectWithTab) {
  // check whether literals that contain special characters of TSV are escaped
  // correctly (i.e. whether `RdfEscaping::escapeForTsv` is used for the term
  // strings.)
  auto triple = makeTriple(makeTerm("<http://s>"), makeTerm("<http://p>"),
                           makeTerm("val\tueu"));
  EXPECT_EQ("<http://s>\t<http://p>\tval ueu\n",
            formatTriple(triple, ad_utility::MediaType::tsv));
}

TEST(FormatTriple, TsvTabInSubject) {
  auto triple = makeTriple(makeTerm("sub\tject"), makeTerm("<http://p>"),
                           makeTerm("<http://o>"));
  EXPECT_EQ("sub ject\t<http://p>\t<http://o>\n",
            formatTriple(triple, ad_utility::MediaType::tsv));
}

TEST(FormatTriple, TsvTabInPredicate) {
  auto triple = makeTriple(makeTerm("<http://s>"), makeTerm("pred\ticate"),
                           makeTerm("<http://o>"));
  EXPECT_EQ("<http://s>\tpred icate\t<http://o>\n",
            formatTriple(triple, ad_utility::MediaType::tsv));
}

TEST(FormatTriple, UnsupportedMediaType) {
  auto triple = makeTriple(makeTerm("<http://s>"), makeTerm("<http://p>"),
                           makeTerm("<http://o>"));
  EXPECT_ANY_THROW(formatTriple(triple, ad_utility::MediaType::ntriples));
}

//______________________________________________________________________________

TEST(CreateStringTriple, ReturnsFormattedTerms) {
  auto triple = makeTriple(makeTerm("<http://s>"), makeTerm("<http://p>"),
                           makeTerm("<http://o>"));
  auto result = createStringTriple(triple);
  EXPECT_EQ(result.subject_, "<http://s>");
  EXPECT_EQ(result.predicate_, "<http://p>");
  EXPECT_EQ(result.object_, "<http://o>");
}

}  // namespace
