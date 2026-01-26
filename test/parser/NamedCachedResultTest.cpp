//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../util/GTestHelpers.h"
#include "parser/GraphPatternOperation.h"
#include "parser/NamedCachedResult.h"
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

// Test fixture for NamedCachedResult tests
class NamedCachedResultTest : public ::testing::Test {
 protected:
  void SetUp() override {
    testIdentifier_ = "test_query_name";
    testIdentifierAsIri_ = TripleComponent::Iri::fromIrirefWithoutBrackets(
        absl::StrCat(CACHED_RESULT_WITH_NAME_PREFIX, testIdentifier_));
    query_ = std::make_unique<NamedCachedResult>(testIdentifierAsIri_);
  }

  TripleComponent::Iri testIdentifierAsIri_;
  std::string testIdentifier_;
  std::unique_ptr<NamedCachedResult> query_;
};

// Test constructor
TEST_F(NamedCachedResultTest, Construction) {
  AD_EXPECT_THROW_WITH_MESSAGE(
      NamedCachedResult(
          TripleComponent::Iri::fromIriref("<someIRIThatIsNotACacheRequest>")),
      ::testing::HasSubstr("must start with"));
  // Test construction with identifier
  NamedCachedResult query(TripleComponent::Iri::fromIrirefWithoutBrackets(
      absl::StrCat(CACHED_RESULT_WITH_NAME_PREFIX, "my_query")));
  EXPECT_EQ(query.identifier(), "my_query");
}

// Test addParameter (should always throw)
TEST_F(NamedCachedResultTest, AddParameterThrows) {
  SparqlTriple testTriple = createTestTriple();

  // addParameter should always throw since body must be empty
  AD_EXPECT_THROW_WITH_MESSAGE(
      query_->addParameter(testTriple),
      ::testing::HasSubstr(
          "The body of a named cache query request must be empty"));
}

// Test inheritance from MagicServiceQuery
TEST_F(NamedCachedResultTest, InheritanceFromMagicServiceQuery) {
  // Test that NamedCachedResult properly inherits from MagicServiceQuery
  MagicServiceQuery* basePtr = query_.get();
  ASSERT_NE(basePtr, nullptr);

  // Test that virtual function calls work through base pointer
  SparqlTriple testTriple = createTestTriple();
  EXPECT_THROW(basePtr->addParameter(testTriple), std::runtime_error);
}

// Test const correctness
TEST_F(NamedCachedResultTest, ConstCorrectness) {
  // Test that validateAndGetIdentifier can be called on a const object
  const NamedCachedResult& constQuery = *query_;
  const std::string& result = constQuery.identifier();
  EXPECT_EQ(result, testIdentifier_);

  // Test that the returned reference is to the actual identifier
  EXPECT_EQ(&result, &constQuery.identifier());
}

// Test sequence of operations
TEST_F(NamedCachedResultTest, SequenceOfOperations) {
  // Multiple calls to validateAndGetIdentifier should work
  EXPECT_EQ(query_->identifier(), testIdentifier_);
  EXPECT_EQ(query_->identifier(), testIdentifier_);
  EXPECT_EQ(query_->identifier(), testIdentifier_);

  // addGraph should always fail regardless of state.
  auto errorMatcher = ::testing::HasSubstr("must be empty");
  AD_EXPECT_THROW_WITH_MESSAGE(
      query_->addGraph(GraphPatternOperation(parsedQuery::BasicGraphPattern())),
      errorMatcher);
  // addParameter should always fail regardless of state
  SparqlTriple testTriple = createTestTriple();
  AD_EXPECT_THROW_WITH_MESSAGE(query_->addParameter(testTriple), errorMatcher);
}
