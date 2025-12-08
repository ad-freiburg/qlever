//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Generated with Claude Code

#include <gtest/gtest.h>

#include "parser/ExternalValuesQuery.h"
#include "parser/SparqlTriple.h"
#include "util/Exception.h"

using parsedQuery::ExternalValuesQuery;
using parsedQuery::ExternalValuesException;

// Test extracting identifier from valid IRI
TEST(ExternalValuesQuery, extractIdentifierValid) {
  std::string iri = "<https://qlever.cs.uni-freiburg.de/external-values-myid>";
  std::string identifier = ExternalValuesQuery::extractIdentifier(iri);
  EXPECT_EQ(identifier, "myid");
}

// Test extracting identifier with complex identifier
TEST(ExternalValuesQuery, extractIdentifierComplex) {
  std::string iri =
      "<https://qlever.cs.uni-freiburg.de/external-values-test-123-abc>";
  std::string identifier = ExternalValuesQuery::extractIdentifier(iri);
  EXPECT_EQ(identifier, "test-123-abc");
}

// Test that empty identifier throws exception
TEST(ExternalValuesQuery, extractIdentifierEmpty) {
  std::string iri = "<https://qlever.cs.uni-freiburg.de/external-values->";
  EXPECT_THROW(ExternalValuesQuery::extractIdentifier(iri),
               ExternalValuesException);
}

// Test that wrong prefix throws exception
TEST(ExternalValuesQuery, extractIdentifierWrongPrefix) {
  std::string iri = "<https://example.com/external-values-myid>";
  EXPECT_THROW(ExternalValuesQuery::extractIdentifier(iri),
               ExternalValuesException);
}

// Test that missing closing bracket throws exception
TEST(ExternalValuesQuery, extractIdentifierMissingBracket) {
  std::string iri = "<https://qlever.cs.uni-freiburg.de/external-values-myid";
  EXPECT_THROW(ExternalValuesQuery::extractIdentifier(iri),
               ExternalValuesException);
}

// Test addParameter with variables
TEST(ExternalValuesQuery, addParameterVariables) {
  ExternalValuesQuery query;
  query.identifier_ = "test";

  // Create a triple with <variables> predicate and a variable as object
  TripleComponent subject = TripleComponent::UNDEF{};
  TripleComponent predicate = TripleComponent::Iri::fromIriref("<variables>");
  TripleComponent object = Variable{"?x"};

  SparqlTriple triple{subject, predicate, object};
  query.addParameter(triple);

  ASSERT_EQ(query.variables_.size(), 1u);
  EXPECT_EQ(query.variables_[0], Variable{"?x"});
}

// Test addParameter with multiple variables
TEST(ExternalValuesQuery, addParameterMultipleVariables) {
  ExternalValuesQuery query;
  query.identifier_ = "test";

  TripleComponent subject = TripleComponent::UNDEF{};
  TripleComponent predicate = TripleComponent::Iri::fromIriref("<variables>");

  SparqlTriple triple1{subject, predicate, Variable{"?x"}};
  SparqlTriple triple2{subject, predicate, Variable{"?y"}};
  SparqlTriple triple3{subject, predicate, Variable{"?z"}};

  query.addParameter(triple1);
  query.addParameter(triple2);
  query.addParameter(triple3);

  ASSERT_EQ(query.variables_.size(), 3u);
  EXPECT_EQ(query.variables_[0], Variable{"?x"});
  EXPECT_EQ(query.variables_[1], Variable{"?y"});
  EXPECT_EQ(query.variables_[2], Variable{"?z"});
}

// Test addParameter with non-variable object throws exception
TEST(ExternalValuesQuery, addParameterNonVariable) {
  ExternalValuesQuery query;
  query.identifier_ = "test";

  TripleComponent subject = TripleComponent::UNDEF{};
  TripleComponent predicate = TripleComponent::Iri::fromIriref("<variables>");
  TripleComponent object =
      TripleComponent::Iri::fromIriref("<http://example.com>");

  SparqlTriple triple{subject, predicate, object};

  EXPECT_THROW(query.addParameter(triple), ExternalValuesException);
}

// Test addParameter with unknown predicate throws exception
TEST(ExternalValuesQuery, addParameterUnknownPredicate) {
  ExternalValuesQuery query;
  query.identifier_ = "test";

  TripleComponent subject = TripleComponent::UNDEF{};
  TripleComponent predicate = TripleComponent::Iri::fromIriref("<unknown>");
  TripleComponent object = Variable{"?x"};

  SparqlTriple triple{subject, predicate, object};

  EXPECT_THROW(query.addParameter(triple), ExternalValuesException);
}
