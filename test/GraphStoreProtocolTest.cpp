// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "./util/GTestHelpers.h"
#include "./util/HttpRequestHelpers.h"
#include "engine/GraphStoreProtocol.h"

TEST(GraphStoreProtocolTest, extractTargetGraph) {
  EXPECT_THAT(GraphStoreProtocol::extractTargetGraph({{"default", {""}}}),
              DEFAULT{});
  EXPECT_THAT(GraphStoreProtocol::extractTargetGraph({{"graph", {"<foo>"}}}),
              TripleComponent::Iri::fromIriref("<foo>"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::extractTargetGraph({}),
      testing::HasSubstr("No graph IRI specified in the request."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::extractTargetGraph({{"unrelated", {"a", "b"}}}),
      testing::HasSubstr("No graph IRI specified in the request."));
  AD_EXPECT_THROW_WITH_MESSAGE(
      GraphStoreProtocol::extractTargetGraph(
          {{"default", {""}}, {"graph", {"<foo>"}}}),
      testing::HasSubstr("Only one of `default` and `graph` may be used for "
                         "graph identification."));
}

TEST(GraphStoreProtocolTest, transformGraphStoreProtocol) {
  ad_utility::httpUtils::HttpRequest auto f =
      ad_utility::testing::MakeGetRequest("/?default");
  ad_utility::url_parser::ParsedRequest ff =
      ad_utility::url_parser::ParsedRequest{};
  GraphStoreProtocol::transformGraphStoreProtocol(f, ff);
  EXPECT_THAT(GraphStoreProtocol::transformGraphStoreProtocol(
                  f, ad_utility::url_parser::ParsedRequest{}),
              testing::_);
}
