// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "../QueryPlannerTestHelpers.h"
#include "../printers/PayloadVariablePrinters.h"
#include "../util/TripleComponentTestHelpers.h"
#include "engine/TensorSearch.h"
#include "engine/TensorSearchConfig.h"
#include "parser/MagicServiceQuery.h"
#include "parser/PayloadVariables.h"

namespace h = queryPlannerTestHelpers;
namespace {
using Var = Variable;
constexpr auto iri = ad_utility::testing::iri;
using queryPlannerTestHelpers::NamedTag;
}  // namespace
using ::testing::HasSubstr;

// _____________________________________________________________________________
TEST(QueryTensorSearchPlanner, TensorSearchService) {
  auto scan = h::IndexScanFromStrings;
  using V = Variable;
  PayloadVariables emptyPayload{};

  // Simple base cases
  h::expect(
      "PREFIX tensorSearch: <https://qlever.cs.uni-freiburg.de/tensorSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE tensorSearch: {"
      "_:config tensorSearch:numNN 1 ; "
      "tensorSearch:left ?y ;"
      "tensorSearch:right ?b . "
      "{ ?a <p> ?b } }}",
      h::tensorSearch(1, -1, -1, TENSOR_SEARCH_DEFAULT_ALGORITHM,
                      TENSOR_SEARCH_DEFAULT_DISTANCE, V{"?y"}, V{"?b"},
                      std::nullopt, emptyPayload, scan("?x", "<p>", "?y"),
                      scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX tensorSearch: <https://qlever.cs.uni-freiburg.de/tensorSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE tensorSearch: {"
      "_:config tensorSearch:algorithm tensorSearch:naive ;"
      "tensorSearch:numNN 1 ; "
      "tensorSearch:left ?y ;"
      "tensorSearch:right ?b ."
      "{ ?a <p> ?b } }}",
      h::tensorSearch(1, -1, -1, TensorSearchAlgorithm::NAIVE,
                      TENSOR_SEARCH_DEFAULT_DISTANCE, V{"?y"}, V{"?b"},
                      std::nullopt, emptyPayload, scan("?x", "<p>", "?y"),
                      scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX tensorSearch: <https://qlever.cs.uni-freiburg.de/tensorSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE tensorSearch: {"
      "_:config tensorSearch:algorithm tensorSearch:annoy ;"
      "tensorSearch:numNN 1 ; "
      "tensorSearch:left ?y ;"
      "tensorSearch:right ?b ."
      "{ ?a <p> ?b } }}",
      h::tensorSearch(1, -1, -1, TensorSearchAlgorithm::ANNOY,
                      TENSOR_SEARCH_DEFAULT_DISTANCE, V{"?y"}, V{"?b"},
                      std::nullopt, emptyPayload, scan("?x", "<p>", "?y"),
                      scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX tensorSearch: <https://qlever.cs.uni-freiburg.de/tensorSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE tensorSearch: {"
      "_:config tensorSearch:algorithm tensorSearch:annoy ;"
      "tensorSearch:numNN 100 ; "
      "tensorSearch:left ?y ;"
      "tensorSearch:right ?b . "
      "{ ?a <p> ?b } }}",
      h::tensorSearch(100, -1, -1, TensorSearchAlgorithm::ANNOY,
                      TENSOR_SEARCH_DEFAULT_DISTANCE, V{"?y"}, V{"?b"},
                      std::nullopt, emptyPayload, scan("?x", "<p>", "?y"),
                      scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX tensorSearch: <https://qlever.cs.uni-freiburg.de/tensorSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE tensorSearch: {"
      "_:config tensorSearch:algorithm tensorSearch:annoy ;"
      "tensorSearch:searchK 20 ; "
      "tensorSearch:left ?y ;"
      "tensorSearch:right ?b ."
      "{ ?a <p> ?b } }}",
      h::tensorSearch(100, 20, -1, TensorSearchAlgorithm::ANNOY,
                      TENSOR_SEARCH_DEFAULT_DISTANCE, V{"?y"}, V{"?b"},
                      std::nullopt, emptyPayload, scan("?x", "<p>", "?y"),
                      scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX tensorSearch: <https://qlever.cs.uni-freiburg.de/tensorSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE tensorSearch: {"
      "_:config tensorSearch:algorithm tensorSearch:annoy ;"
      "tensorSearch:nTrees 20 ; "
      "tensorSearch:left ?y ;"
      "tensorSearch:right ?b ."
      "{ ?a <p> ?b } }}",
      h::tensorSearch(100, -1, 20, TensorSearchAlgorithm::ANNOY,
                      TENSOR_SEARCH_DEFAULT_DISTANCE, V{"?y"}, V{"?b"},
                      std::nullopt, emptyPayload, scan("?x", "<p>", "?y"),
                      scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX tensorSearch: <https://qlever.cs.uni-freiburg.de/tensorSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE tensorSearch: {"
      "_:config tensorSearch:distance tensorSearch:cosine ;"
      "tensorSearch:numNN 1 ; "
      "tensorSearch:left ?y ;"
      "tensorSearch:right ?b ."
      "{ ?a <p> ?b } }}",
      h::tensorSearch(1, -1, -1, TENSOR_SEARCH_DEFAULT_ALGORITHM,
                      TensorDistanceAlgorithm::COSINE_SIMILARITY, V{"?y"},
                      V{"?b"}, std::nullopt, emptyPayload,
                      scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX tensorSearch: <https://qlever.cs.uni-freiburg.de/tensorSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE tensorSearch: {"
      "_:config tensorSearch:distance tensorSearch:angular ;"
      "tensorSearch:numNN 1 ; "
      "tensorSearch:left ?y ;"
      "tensorSearch:right ?b . "
      "{ ?a <p> ?b } }}",
      h::tensorSearch(1, -1, -1, TENSOR_SEARCH_DEFAULT_ALGORITHM,
                      TensorDistanceAlgorithm::ANGULAR_DISTANCE, V{"?y"},
                      V{"?b"}, std::nullopt, emptyPayload,
                      scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX tensorSearch: <https://qlever.cs.uni-freiburg.de/tensorSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE tensorSearch: {"
      "_:config tensorSearch:distance tensorSearch:dot ;"
      "tensorSearch:numNN 1 ; "
      "tensorSearch:left ?y ;"
      "tensorSearch:right ?b . "
      "{ ?a <p> ?b } }}",
      h::tensorSearch(1, -1, -1, TENSOR_SEARCH_DEFAULT_ALGORITHM,
                      TensorDistanceAlgorithm::DOT_PRODUCT, V{"?y"}, V{"?b"},
                      std::nullopt, emptyPayload, scan("?x", "<p>", "?y"),
                      scan("?a", "<p>", "?b")));
}

// _____________________________________________________________________________
TEST(QueryTensorSearchPlanner, TensorSearchServicePayloadVars) {
  // Test the <payload> option which allows selecting columns from the graph
  // pattern inside the service.

  auto scan = h::IndexScanFromStrings;
  using V = Variable;
  using PV = PayloadVariables;

  h::expect(
      "PREFIX tensorSearch:<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE tensorSearch: {"
      "_:config tensorSearch:distance tensorSearch:dot ;"
      "tensorSearch:numNN 1 ; "
      "tensorSearch:right ?b ;"
      "tensorSearch:bindDistance ?dist ."
      "_:config tensorSearch:left ?y ."
      "_:config tensorSearch:payload ?a ."
      "{ ?a <p> ?b } }}",
      h::tensorSearch(1, -1, -1, TENSOR_SEARCH_DEFAULT_ALGORITHM,
                      TensorDistanceAlgorithm::DOT_PRODUCT, V{"?y"}, V{"?b"},
                      V{"?dist"}, PV{std::vector<V>{V{"?a"}}},
                      scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b")));
  h::expect(
      "PREFIX tensorSearch:<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE tensorSearch: {"
      "_:config tensorSearch:distance tensorSearch:dot ;"
      "tensorSearch:numNN 1 ; "
      "tensorSearch:right ?b ;"
      "tensorSearch:bindDistance ?dist ."
      "_:config tensorSearch:left ?y ."
      "_:config tensorSearch:payload ?a , ?a2 ."
      "{ ?a <p> ?a2 . ?a2 <p> ?b } }}",
      h::tensorSearch(
          1, -1, -1, TENSOR_SEARCH_DEFAULT_ALGORITHM,
          TensorDistanceAlgorithm::DOT_PRODUCT, V{"?y"}, V{"?b"}, V{"?dist"},
          PV{std::vector<V>{V{"?a"}, V{"?a2"}}}, scan("?x", "<p>", "?y"),
          h::Join(scan("?a", "<p>", "?a2"), scan("?a2", "<p>", "?b"))));

  // Right variable and duplicates are possible (silently deduplicated during
  // query result computation)
  h::expect(
      "PREFIX tensorSearch:<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE tensorSearch: {"
      "_:config tensorSearch:distance tensorSearch:dot ;"
      "tensorSearch:numNN 1 ; "
      "tensorSearch:right ?b ;"
      "tensorSearch:bindDistance ?dist ."
      "_:config tensorSearch:left ?y ."
      "_:config tensorSearch:payload ?a, ?a, ?b, ?a2 ."
      "{ ?a <p> ?a2 . ?a2 <p> ?b } }}",
      h::tensorSearch(
          1, -1, -1, TENSOR_SEARCH_DEFAULT_ALGORITHM,
          TensorDistanceAlgorithm::DOT_PRODUCT, V{"?y"}, V{"?b"}, V{"?dist"},
          PV{std::vector<V>{V{"?a"}, V{"?a"}, V{"?b"}, V{"?a2"}}},
          scan("?x", "<p>", "?y"),
          h::Join(scan("?a", "<p>", "?a2"), scan("?a2", "<p>", "?b"))));

  // Selecting all payload variables using "all"
  h::expect(
      "PREFIX tensorSearch:<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE tensorSearch: {"
      "_:config tensorSearch:distance tensorSearch:dot ;"
      "tensorSearch:numNN 1 ; "
      "tensorSearch:right ?b ;"
      "tensorSearch:bindDistance ?dist ."
      "_:config tensorSearch:left ?y ."
      "_:config tensorSearch:payload <all> ."
      "{ ?a <p> ?a2 . ?a2 <p> ?b } }}",
      h::tensorSearch(
          1, -1, -1, TENSOR_SEARCH_DEFAULT_ALGORITHM,
          TensorDistanceAlgorithm::DOT_PRODUCT, V{"?y"}, V{"?b"}, V{"?dist"},
          PayloadVariables::all(), scan("?x", "<p>", "?y"),
          h::Join(scan("?a", "<p>", "?a2"), scan("?a2", "<p>", "?b"))));
  h::expect(
      "PREFIX tensorSearch:<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE tensorSearch: {"
      "_:config tensorSearch:distance tensorSearch:dot ;"
      "tensorSearch:numNN 1 ; "
      "tensorSearch:right ?b ;"
      "tensorSearch:bindDistance ?dist ."
      "_:config tensorSearch:left ?y ."
      "_:config tensorSearch:payload tensorSearch:all ."
      "{ ?a <p> ?a2 . ?a2 <p> ?b } }}",
      h::tensorSearch(
          1, -1, -1, TENSOR_SEARCH_DEFAULT_ALGORITHM,
          TensorDistanceAlgorithm::DOT_PRODUCT, V{"?y"}, V{"?b"}, V{"?dist"},
          PayloadVariables::all(), scan("?x", "<p>", "?y"),
          h::Join(scan("?a", "<p>", "?a2"), scan("?a2", "<p>", "?b"))));

  // All and explicitly named ones just select all
  h::expect(
      "PREFIX tensorSearch:<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y."
      "SERVICE tensorSearch: {"
      "_:config tensorSearch:distance tensorSearch:dot ;"
      "tensorSearch:numNN 1 ; "
      "tensorSearch:right ?b ;"
      "tensorSearch:bindDistance ?dist ."
      "_:config tensorSearch:left ?y ."
      "_:config tensorSearch:payload <all> ."
      "_:config tensorSearch:payload ?a ."
      "{ ?a <p> ?a2 . ?a2 <p> ?b } }}",
      h::tensorSearch(
          1, -1, -1, TENSOR_SEARCH_DEFAULT_ALGORITHM,
          TensorDistanceAlgorithm::DOT_PRODUCT, V{"?y"}, V{"?b"}, V{"?dist"},
          PayloadVariables::all(), scan("?x", "<p>", "?y"),
          h::Join(scan("?a", "<p>", "?a2"), scan("?a2", "<p>", "?b"))));
}
// _____________________________________________________________________________
TEST(QueryTensorSearchPlanner, TensorSearchMultipleServiceSharedLeft) {
  auto scan = h::IndexScanFromStrings;
  using V = Variable;
  using PV = PayloadVariables;

  // Two SERVICE tensorSearch blocks that both declare the same left variable
  h::expect(
      "PREFIX tensorSearch: <https://qlever.cs.uni-freiburg.de/tensorSearch/>"
      "SELECT * WHERE {"
      "?x <p> ?y ."
      "SERVICE tensorSearch: {"
      "  _:config tensorSearch:algorithm tensorSearch:annoy ;"
      "    tensorSearch:left ?y ;"
      "    tensorSearch:right ?b ;"
      "    tensorSearch:numNN 5 ;"
      "    tensorSearch:bindDistance ?db ."
      "  { ?ab <p1> ?b }"
      "}"
      "SERVICE tensorSearch: {"
      "  _:config tensorSearch:algorithm tensorSearch:annoy ;"
      "    tensorSearch:left ?y ;"
      "    tensorSearch:right ?c ;"
      "    tensorSearch:numNN 3 ;"
      "    tensorSearch:payload ?ac ;"
      "    tensorSearch:bindDistance ?dc ."
      "  { ?ac <p2> ?c }"
      " }"
      "}",
      // Both orders of assembling the two tensor-search children are allowed
      ::testing::AnyOf(
          h::tensorSearch(
              3, -1, -1, TENSOR_SEARCH_DEFAULT_ALGORITHM,
              TENSOR_SEARCH_DEFAULT_DISTANCE, V{"?y"}, V{"?c"}, V{"?dc"},
              PV{std::vector<V>{V{"?ac"}}},
              h::tensorSearch(5, -1, -1, TENSOR_SEARCH_DEFAULT_ALGORITHM,
                              TENSOR_SEARCH_DEFAULT_DISTANCE, V{"?y"}, V{"?b"},
                              V{"?db"}, PV{}, scan("?x", "<p>", "?y"),
                              scan("?ab", "<p1>", "?b")),
              scan("?ac", "<p2>", "?c")),
          h::tensorSearch(
              5, -1, -1, TENSOR_SEARCH_DEFAULT_ALGORITHM,
              TENSOR_SEARCH_DEFAULT_DISTANCE, V{"?y"}, V{"?b"}, V{"?db"}, PV{},
              h::tensorSearch(3, -1, -1, TENSOR_SEARCH_DEFAULT_ALGORITHM,
                              TENSOR_SEARCH_DEFAULT_DISTANCE, V{"?y"}, V{"?c"},
                              V{"?dc"}, PV{std::vector<V>{V{"?ac"}}},
                              scan("?x", "<p>", "?y"),
                              scan("?ac", "<p2>", "?c")),
              scan("?ab", "<p1>", "?b"))));
}
//
// _____________________________________________________________________________
TEST(QueryTensorSearchPlanner, TensorSearchMissingConfig) {
  // Tests with incomplete config
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX tensorSearch: "
                "<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE tensorSearch: {"
                "_:config tensorSearch:right ?b ;"
                "tensorSearch:numNN 5 . "
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("Missing parameter `<left>`"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX tensorSearch: "
                "<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE tensorSearch: {"
                "_:config tensorSearch:right ?b ;"
                "tensorSearch:numNN 5 . "
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("Missing parameter `<left>`"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX tensorSearch: "
                "<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE tensorSearch: {"
                "_:config tensorSearch:left ?y ;"
                "tensorSearch:numNN 5 . "
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("Missing parameter `<right>`"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX tensorSearch: "
                "<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE tensorSearch: {"
                "_:config tensorSearch:left ?y ;"
                "tensorSearch:numNN 5 . "
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("Missing parameter `<right>`"));
}

// _____________________________________________________________________________
TEST(QueryTensorSearchPlanner, TensorSearchInvalidOperationsInService) {
  // Test that unallowed operations inside the SERVICE statement throw
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX tensorSearch: "
                "<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y."
                "SERVICE tensorSearch: {"
                "_:config tensorSearch:left ?y ;"
                "tensorSearch:right ?b ;"
                "tensorSearch:numNN 1 . "
                "{ ?a <p> ?b }"
                "SERVICE <http://example.com/> { ?a <something> <else> }"
                " }}",
                ::testing::_),
      ::testing::ContainsRegex("Unsupported element in a magic service query "
                               "of type `tensor search`"));
}

// _____________________________________________________________________________
TEST(QueryTensorSearchPlanner, TensorSearchServiceMultipleGraphPatterns) {
  // Test that the SERVICE statement may only contain at most one graph
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX tensorSearch: "
                "<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y."
                "SERVICE tensorSearch: {"
                "_:config tensorSearch:left ?y ;"
                "tensorSearch:right ?b ;"
                "tensorSearch:numNN 1 . "
                "{ ?a <p> ?b }"
                "{ ?a <p2> ?c } }}",
                ::testing::_),
      ::testing::ContainsRegex("A magic SERVICE query must not contain more "
                               "than one nested group graph pattern"));
}

// _____________________________________________________________________________
TEST(QueryPlanner, TensorSearchIncorrectConfigValues) {
  // Tests with mistakes in the config
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX tensorSearch: "
                "<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE tensorSearch: {"
                "_:config tensorSearch:right ?b ;"
                "tensorSearch:left ?y ;"
                "tensorSearch:numNN \"5\" . "
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("`<numNN>` expects an integer"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX tensorSearch: "
                "<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE tensorSearch: {"
                "_:config tensorSearch:right ?b ;"
                "tensorSearch:left ?y ;"
                "tensorSearch:searchK \"1\" ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("`<searchK>` expects an integer"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX tensorSearch: "
                "<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE tensorSearch: {"
                "_:config tensorSearch:right ?b ;"
                "tensorSearch:left ?y ;"
                "tensorSearch:nTrees \"1\" ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("`<nTrees>` expects an integer"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX tensorSearch: "
                "<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE tensorSearch: {"
                "_:config tensorSearch:right ?b ;"
                "tensorSearch:left ?y ;"
                "tensorSearch:numNN 5 ;"
                "tensorSearch:algorithm \"1\" ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("parameter `<algorithm>` needs an IRI"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX tensorSearch: "
                "<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE tensorSearch: {"
                "_:config tensorSearch:right ?b ;"
                "tensorSearch:left ?y ;"
                "tensorSearch:numNN 5 ;"
                "tensorSearch:algorithm <http://example.com/some-nonsense> ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("`<algorithm>` does not refer to a supported "
                               "tensor search algorithm"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX tensorSearch: "
                "<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE tensorSearch: {"
                "_:config tensorSearch:right ?b ;"
                "tensorSearch:left ?y ;"
                "tensorSearch:numNN 5 ;"
                "<http://example.com/some-nonsense> 123 ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("Unsupported argument"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX tensorSearch: "
                "<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE tensorSearch: {"
                "_:config tensorSearch:right ?b ;"
                "tensorSearch:left ?y ;"
                "tensorSearch:numNN 5 ;"
                "tensorSearch:bindDistance 123 ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("`<bindDistance>` has to be a variable"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX tensorSearch: "
                "<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE tensorSearch: {"
                "_:config tensorSearch:right ?b ;"
                "tensorSearch:left ?y ;"
                "tensorSearch:numNN 5 ;"
                "tensorSearch:payload 123 ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex(
          "`<payload>` parameter must be either a variable "
          "to be selected or `<all>`"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX tensorSearch: "
                "<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE tensorSearch: {"
                "_:config tensorSearch:right ?b ;"
                "tensorSearch:left ?y ;"
                "tensorSearch:numNN 5 ;"
                "tensorSearch:payload <http://some.iri.that.is.not.all> ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex(
          "`<payload>` parameter must be either a variable "
          "to be selected or `<all>`"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX tensorSearch: "
                "<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE tensorSearch: {"
                "_:config tensorSearch:right ?b ;"
                "tensorSearch:left ?y ;"
                "tensorSearch:numNN 5 ;"
                "tensorSearch:bindDistance ?dist_a ;"
                "tensorSearch:bindDistance ?dist_b ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("`<bindDistance>` has already been set"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX tensorSearch: "
                "<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE tensorSearch: {"
                "_:config tensorSearch:right 123 ;"
                "tensorSearch:left ?y ;"
                "tensorSearch:numKnn 5 ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("`<right>` has to be a variable"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("PREFIX tensorSearch: "
                "<https://qlever.cs.uni-freiburg.de/tensorSearch/>"
                "SELECT * WHERE {"
                "?x <p> ?y ."
                "SERVICE tensorSearch: {"
                "_:config tensorSearch:right ?b ;"
                "tensorSearch:left \"abc\" ;"
                "tensorSearch:numKnn 5 ."
                " { ?a <p> ?b . }"
                "}}",
                ::testing::_),
      ::testing::ContainsRegex("`<left>` has to be a variable"));
}

// _____________________________________________________________________________
TEST(QueryTensorSearchPlanner, TensorSearchLegacyPredicateSupport) {
  auto scan = h::IndexScanFromStrings;
  using V = Variable;

  // Test that the nearest neighbors special predicate is still accepted but
  // produces a warning
  h::expect(
      "SELECT ?x ?y WHERE {"
      "?x <p> ?y."
      "?a <p> ?b."
      "?y <tensor-nearest-neighbors:500> ?b }",
      h::QetWithWarnings(
          {"special predicate <tensor-nearest-neighbors:...> is deprecated"},
          h::tensorSearch(500, -1, -1, TENSOR_SEARCH_DEFAULT_ALGORITHM,
                          TENSOR_SEARCH_DEFAULT_DISTANCE, V{"?y"}, V{"?b"},
                          std::nullopt, PayloadVariables::all(),
                          scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b"))));
  h::expect(
      "SELECT ?x ?y WHERE {"
      "?x <p> ?y."
      "?a <p> ?b."
      "?y <tensor-nearest-neighbors:20> ?b }",
      h::QetWithWarnings(
          {"special predicate <tensor-nearest-neighbors:...> is deprecated"},
          h::tensorSearch(20, -1, -1, TENSOR_SEARCH_DEFAULT_ALGORITHM,
                          TENSOR_SEARCH_DEFAULT_DISTANCE, V{"?y"}, V{"?b"},
                          std::nullopt, PayloadVariables::all(),
                          scan("?x", "<p>", "?y"), scan("?a", "<p>", "?b"))));

  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?a <p> ?b."
                "?y <tensor-nearest-neighbors:1:-200> ?b. "
                "}",
                ::testing::_),
      ::testing::ContainsRegex("unknown triple"));

  AD_EXPECT_THROW_WITH_MESSAGE(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?a <p> ?b."
                "?y <tensor-nearest-neighbors:0:-1> ?b .}",
                ::testing::_),
      ::testing::ContainsRegex("unknown triple"));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?a <p> ?b."
                "?y <tensor-nearest-neighbors:500> ?b ."
                "?y <a> ?b}",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?y <p> ?b."
                "?y <tensor-nearest-neighbors:1> ?b }",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?y <tensor-nearest-neighbors:2> <a> }",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "<a> <tensor-nearest-neighbors:2> ?y }",
                ::testing::_));

  EXPECT_ANY_THROW(
      h::expect("SELECT ?x ?y WHERE {"
                "?x <p> ?y."
                "?a <p> ?b."
                "?y <tensor-nearest-neighbors:> ?b }",
                ::testing::_));
}
