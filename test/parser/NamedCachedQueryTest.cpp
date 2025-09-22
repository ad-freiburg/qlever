//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "parser/NamedCachedQuery.h"
#include "parser/SparqlTriple.h"
#include "parser/TripleComponent.h"

using namespace parsedQuery;

namespace {

// Helper function to create a simple SparqlTriple for testing
SparqlTriple createTestTriple() {
  using namespace ad_utility::sparql_types;
  TripleComponent subject{Variable{"?s"}};
  VarOrPath predicate = Variable{"?p"};
  TripleComponent object{Variable{"?o"}};
  return SparqlTriple{std::move(subject), std::move(predicate),
                      std::move(object)};
}

}  // namespace

// Test fixture for NamedCachedQuery tests
class NamedCachedQueryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    testIdentifier_ = "test_query_name";
    query_ = std::make_unique<NamedCachedQuery>(testIdentifier_);
  }

  std::string testIdentifier_;
  std::unique_ptr<NamedCachedQuery> query_;
};

// Test constructor
TEST_F(NamedCachedQueryTest, Construction) {
  // Test construction with identifier
  NamedCachedQuery query("my_query");

  // Verify construction succeeded by calling validateAndGetIdentifier
  // (this should work since no parameters were added)
  EXPECT_EQ(query.validateAndGetIdentifier(), "my_query");
}

TEST_F(NamedCachedQueryTest, ConstructionWithEmptyIdentifier) {
  // Test construction with empty identifier
  NamedCachedQuery query("");

  // Verify that empty identifier is preserved
  EXPECT_EQ(query.validateAndGetIdentifier(), "");
}

// Test validateAndGetIdentifier with no child graph pattern
TEST_F(NamedCachedQueryTest, ValidateAndGetIdentifierWithoutChildPattern) {
  // When no child graph pattern is set, validateAndGetIdentifier should succeed
  const std::string& result = query_->validateAndGetIdentifier();
  EXPECT_EQ(result, testIdentifier_);
}

// Test validateAndGetIdentifier with child graph pattern (should throw)
TEST_F(NamedCachedQueryTest, ValidateAndGetIdentifierWithChildPattern) {
  // Set a child graph pattern to make validation fail
  query_->childGraphPattern_ = GraphPattern{};

  // validateAndGetIdentifier should throw when child pattern exists
  EXPECT_THROW(query_->validateAndGetIdentifier(), std::runtime_error);

  // Verify the specific error message
  try {
    query_->validateAndGetIdentifier();
    FAIL() << "Expected std::runtime_error to be thrown";
  } catch (const std::runtime_error& e) {
    EXPECT_THAT(e.what(),
                ::testing::HasSubstr(
                    "The body of a named cache query request must be empty"));
  }
}

// Test addParameter (should always throw)
TEST_F(NamedCachedQueryTest, AddParameterThrows) {
  SparqlTriple testTriple = createTestTriple();

  // addParameter should always throw since body must be empty
  EXPECT_THROW(query_->addParameter(testTriple), std::runtime_error);

  // Verify the specific error message
  try {
    query_->addParameter(testTriple);
    FAIL() << "Expected std::runtime_error to be thrown";
  } catch (const std::runtime_error& e) {
    EXPECT_THAT(e.what(),
                ::testing::HasSubstr(
                    "The body of a named cache query request must be empty"));
  }
}

// Test addParameter with different triple configurations
TEST_F(NamedCachedQueryTest, AddParameterWithDifferentTriples) {
  // Test with different types of triples - all should throw
  using namespace ad_utility::sparql_types;

  // Triple with IRIs
  {
    TripleComponent subject{
        TripleComponent::Iri::fromIriref("<http://example.org/s>")};
    VarOrPath predicate = PropertyPath::fromIri(
        TripleComponent::Iri::fromIriref("<http://example.org/p>"));
    TripleComponent object{
        TripleComponent::Iri::fromIriref("<http://example.org/o>")};
    SparqlTriple iriTriple{std::move(subject), std::move(predicate),
                           std::move(object)};

    EXPECT_THROW(query_->addParameter(iriTriple), std::runtime_error);
  }

  // Triple with literals
  {
    TripleComponent subject{Variable{"?s"}};
    VarOrPath predicate = PropertyPath::fromIri(
        TripleComponent::Iri::fromIriref("<http://example.org/p>"));
    TripleComponent object{
        TripleComponent::Literal::fromStringRepresentation("\"literal\"")};
    SparqlTriple literalTriple{std::move(subject), std::move(predicate),
                               std::move(object)};

    EXPECT_THROW(query_->addParameter(literalTriple), std::runtime_error);
  }
}

// Test throwBecauseNotEmpty static method indirectly
TEST_F(NamedCachedQueryTest, ThrowBecauseNotEmptyBehavior) {
  // The throwBecauseNotEmpty method is private/static, but we can test its
  // behavior through the public methods that call it

  SparqlTriple testTriple = createTestTriple();

  // Verify that addParameter throws with the expected message from
  // throwBecauseNotEmpty
  EXPECT_THROW(query_->addParameter(testTriple), std::runtime_error);

  // Set child pattern and verify validateAndGetIdentifier throws with same
  // message
  query_->childGraphPattern_ = GraphPattern{};
  EXPECT_THROW(query_->validateAndGetIdentifier(), std::runtime_error);

  // Both should throw with the same message (coming from throwBecauseNotEmpty)
  std::string addParameterMessage, validateMessage;

  try {
    query_->addParameter(testTriple);
  } catch (const std::runtime_error& e) {
    addParameterMessage = e.what();
  }

  try {
    query_->validateAndGetIdentifier();
  } catch (const std::runtime_error& e) {
    validateMessage = e.what();
  }

  EXPECT_EQ(addParameterMessage, validateMessage);
  EXPECT_THAT(addParameterMessage,
              ::testing::HasSubstr(
                  "The body of a named cache query request must be empty"));
}

// Test inheritance from MagicServiceQuery
TEST_F(NamedCachedQueryTest, InheritanceFromMagicServiceQuery) {
  // Test that NamedCachedQuery properly inherits from MagicServiceQuery
  MagicServiceQuery* basePtr = query_.get();
  ASSERT_NE(basePtr, nullptr);

  // Test that virtual function calls work through base pointer
  SparqlTriple testTriple = createTestTriple();
  EXPECT_THROW(basePtr->addParameter(testTriple), std::runtime_error);
}

// Test edge cases and error conditions
TEST_F(NamedCachedQueryTest, EdgeCases) {
  // Test with very long identifier
  std::string longIdentifier(1000, 'a');
  NamedCachedQuery longQuery(longIdentifier);
  EXPECT_EQ(longQuery.validateAndGetIdentifier(), longIdentifier);

  // Test with special characters in identifier
  std::string specialIdentifier = "query_with-special.chars@123";
  NamedCachedQuery specialQuery(specialIdentifier);
  EXPECT_EQ(specialQuery.validateAndGetIdentifier(), specialIdentifier);

  // Test that identifier is properly moved in constructor
  std::string originalIdentifier = "movable_identifier";
  std::string copyOfIdentifier = originalIdentifier;
  NamedCachedQuery moveQuery(std::move(copyOfIdentifier));
  EXPECT_EQ(moveQuery.validateAndGetIdentifier(), originalIdentifier);
  // copyOfIdentifier should now be empty or in valid but unspecified state
}

// Test const correctness
TEST_F(NamedCachedQueryTest, ConstCorrectness) {
  // Test that validateAndGetIdentifier can be called on const object
  const NamedCachedQuery& constQuery = *query_;
  const std::string& result = constQuery.validateAndGetIdentifier();
  EXPECT_EQ(result, testIdentifier_);

  // Test that the returned reference is to the actual identifier
  EXPECT_EQ(&result, &constQuery.validateAndGetIdentifier());
}

// Test sequence of operations
TEST_F(NamedCachedQueryTest, SequenceOfOperations) {
  // Multiple calls to validateAndGetIdentifier should work
  EXPECT_EQ(query_->validateAndGetIdentifier(), testIdentifier_);
  EXPECT_EQ(query_->validateAndGetIdentifier(), testIdentifier_);
  EXPECT_EQ(query_->validateAndGetIdentifier(), testIdentifier_);

  // After setting child pattern, all subsequent calls should fail
  query_->childGraphPattern_ = GraphPattern{};
  EXPECT_THROW(query_->validateAndGetIdentifier(), std::runtime_error);
  EXPECT_THROW(query_->validateAndGetIdentifier(), std::runtime_error);

  // addParameter should always fail regardless of state
  SparqlTriple testTriple = createTestTriple();
  EXPECT_THROW(query_->addParameter(testTriple), std::runtime_error);
  EXPECT_THROW(query_->addParameter(testTriple), std::runtime_error);
}
