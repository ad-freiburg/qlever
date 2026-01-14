// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../../util/TripleComponentTestHelpers.h"
#include "parser/data/GraphTerm.h"

namespace {

constexpr auto iri = ad_utility::testing::iri;
constexpr auto lit = ad_utility::testing::tripleComponentLiteral;

// _____________________________________________________________________________
TEST(GraphTerm, toSparql) {
  using V = Variable;
  using GT = GraphTerm;
  EXPECT_EQ(GT(V("?x")).toSparql(), "?x");
  EXPECT_EQ(GT(Iri("<x>")).toSparql(), "<x>");
  EXPECT_EQ(GT(Literal("\"x\"")).toSparql(), "\"x\"");
  EXPECT_EQ(GT(Literal("\"x\"@en")).toSparql(), "\"x\"@en");
  EXPECT_EQ(GT(BlankNode(true, "blubb")).toSparql(), "_:g_blubb");
}

// _____________________________________________________________________________
TEST(GraphTerm, toTripleComponent) {
  using V = Variable;
  using GT = GraphTerm;
  EXPECT_EQ(GT(V("?x")).toTripleComponent(), V("?x"));
  EXPECT_EQ(GT(Iri("<x>")).toTripleComponent(), iri("<x>"));
  EXPECT_EQ(GT(Literal("\"x\"")).toTripleComponent(), lit("x"));
  EXPECT_EQ(GT(Literal("\"x\"@en")).toTripleComponent(), lit("x", "@en"));
  EXPECT_EQ(GT(BlankNode(true, "blubb")).toTripleComponent(),
            V("?_QLever_internal_variable_bn_g_blubb"));
}
}  // namespace
