// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "engine/LoadURL.h"
#include "util/GTestHelpers.h"
#include "util/HttpClientTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"
#include "util/RuntimeParametersTestHelpers.h"

namespace {

auto pqLoadURL = [](std::string url, bool silent = false) {
  return parsedQuery::LoadURL{ad_utility::httpUtils::Url{url}, silent};
};

// Fixture that sets up a test index and a factory for producing mocks for the
// `getResultFunction` needed by the `Service` operation.
class LoadURLTest : public ::testing::Test {
 protected:
  // Query execution context (with small test index) and allocator for testing,
  // see `IndexTestHelpers.h`. Note that `getQec` returns a pointer to a static
  // `QueryExecutionContext`, so no need to ever delete `testQec`.
  QueryExecutionContext* testQec = ad_utility::testing::getQec();
  ad_utility::AllocatorWithLimit<Id> testAllocator =
      ad_utility::testing::makeAllocator();

  // Factory for generating mocks of the `sendHttpOrHttpsRequest` that returns a
  // predefined response for testing.
  static auto constexpr getResultFunctionFactory =
      [](std::string predefinedResult,
         boost::beast::http::status status = boost::beast::http::status::ok,
         std::string contentType = "text/turtle",
         std::exception_ptr mockException = nullptr,
         ad_utility::source_location loc =
             ad_utility::source_location::current()) -> SendRequestType {
    httpClientTestHelpers::RequestMatchers matchers{
        .method_ = testing::Eq(boost::beast::http::verb::get),
        .postData_ = testing::Eq(""),
        .contentType_ = testing::Eq(""),
        .accept_ = testing::Eq(""),
    };
    return httpClientTestHelpers::getResultFunctionFactory(
        predefinedResult, contentType, status, matchers, mockException, loc);
  };
};

TEST_F(LoadURLTest, basicMethods) {
  // Create an instance of the operation.
  LoadURL loadURL{testQec, pqLoadURL("https://mundhahs.dev")};

  // Test the basic methods.
  EXPECT_THAT(loadURL.getDescriptor(),
              testing::Eq("LOAD URL https://mundhahs.dev:443/"));
  EXPECT_THAT(loadURL.getCacheKey(), testing::StartsWith("LOAD URL"));
  EXPECT_THAT(loadURL.getResultWidth(), testing::Eq(3));
  EXPECT_THAT(loadURL.getMultiplicity(0), testing::Eq(1));
  EXPECT_THAT(loadURL.getMultiplicity(1), testing::Eq(1));
  EXPECT_THAT(loadURL.getMultiplicity(2), testing::Eq(1));
  EXPECT_THAT(loadURL.getExternallyVisibleVariableColumns(),
              testing::UnorderedElementsAreArray(VariableToColumnMap{
                  {Variable("?s"), makeAlwaysDefinedColumn(0)},
                  {Variable("?p"), makeAlwaysDefinedColumn(1)},
                  {Variable("?o"), makeAlwaysDefinedColumn(2)}}));
  EXPECT_THAT(loadURL.getSizeEstimate(), testing::Eq(100'000));
  EXPECT_THAT(loadURL.getCostEstimate(), testing::Eq(1'000'000));
  EXPECT_THAT(loadURL.knownEmptyResult(), testing::IsFalse());
  EXPECT_THAT(loadURL.getChildren(), testing::IsEmpty());
}

TEST_F(LoadURLTest, computeResult) {
  auto expectThrowOnlyIfNotSilent =
      [this](parsedQuery::LoadURL pq, SendRequestType sendFunc,
             const testing::Matcher<string>& expectedError,
             ad_utility::source_location loc =
                 ad_utility::source_location::current()) {
        auto g = generateLocationTrace(loc);
        LoadURL load{testQec, pq, sendFunc};

        AD_EXPECT_THROW_WITH_MESSAGE(load.computeResultOnlyForTesting(),
                                     expectedError);
        pq.silent_ = true;
        LoadURL silentLoad{testQec, pq, sendFunc};
        EXPECT_NO_THROW(silentLoad.computeResultOnlyForTesting());
      };
  auto expectThrowAlways = [this](parsedQuery::LoadURL pq,
                                  SendRequestType sendFunc,
                                  const testing::Matcher<string>& expectedError,
                                  ad_utility::source_location loc =
                                      ad_utility::source_location::current()) {
    auto g = generateLocationTrace(loc);
    LoadURL load{testQec, pq, sendFunc};

    AD_EXPECT_THROW_WITH_MESSAGE(load.computeResultOnlyForTesting(),
                                 expectedError);
    pq.silent_ = true;
    LoadURL silentLoad{testQec, pq, sendFunc};
    AD_EXPECT_THROW_WITH_MESSAGE(silentLoad.computeResultOnlyForTesting(),
                                 expectedError);
  };
  auto expectLoad =
      [this](std::string responseBody, std::string contentType,
             std::vector<std::array<TripleComponent, 3>> expectedIdTable,
             ad_utility::source_location loc =
                 ad_utility::source_location::current()) {
        auto g = generateLocationTrace(loc);

        LoadURL loadURL{
            testQec, pqLoadURL("https://mundhahs.dev"),
            getResultFunctionFactory(
                responseBody, boost::beast::http::status::ok, contentType)};
        auto res = loadURL.computeResultOnlyForTesting();

        auto& idTable = res.idTable();
        auto& lv = res.localVocab();

        std::vector<std::vector<IntOrId>> idVector;
        for (const auto& row : expectedIdTable) {
          auto& idVecRow = idVector.emplace_back();
          for (auto& field : row) {
            auto idOpt = field.toValueId(testQec->getIndex().getVocab());
            if (!idOpt) {
              ASSERT_THAT(field.isLiteral() || field.isIri(),
                          testing::IsTrue());
              using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
              auto lveOpt = lv.getIndexOrNullopt(
                  field.isLiteral() ? LiteralOrIri{field.getLiteral()}
                                    : LiteralOrIri{field.getIri()});
              ASSERT_THAT(lveOpt, testing::Not(testing::Eq(std::nullopt)));
              idOpt = Id::makeFromLocalVocabIndex(lveOpt.value());
            }
            idVecRow.emplace_back(idOpt.value());
          }
        }

        IdTable expectedId = makeIdTableFromVector(idVector);
        EXPECT_EQ(idTable, makeIdTableFromVector(idVector));
        EXPECT_THAT(idTable, testing::Eq(std::ref(expectedId)));
      };
  expectThrowOnlyIfNotSilent(
      pqLoadURL("https://mundhahs.dev"),
      getResultFunctionFactory("<x> <b> <c>",
                               boost::beast::http::status::not_found),
      testing::HasSubstr("RDF dataset responded with HTTP status code: 404"));
  expectThrowOnlyIfNotSilent(
      pqLoadURL("https://mundhahs.dev"),
      getResultFunctionFactory("<x> <b> <c>", boost::beast::http::status::ok,
                               "foo/bar"),
      testing::HasSubstr(
          "Unsupported `Content-Type` of response: \"foo/bar\""));
  expectThrowOnlyIfNotSilent(
      pqLoadURL("https://mundhahs.dev"),
      getResultFunctionFactory("<x> <b> <c>", boost::beast::http::status::ok,
                               "text/plain"),
      testing::HasSubstr(
          "Unsupported `Content-Type` of response: \"text/plain\""));
  expectThrowOnlyIfNotSilent(
      pqLoadURL("https://mundhahs.dev"),
      getResultFunctionFactory("<x> <b> <c>", boost::beast::http::status::ok,
                               ""),
      testing::HasSubstr("QLever requires the `Content-Type` header to be "
                         "set for the HTTP response."));
  expectThrowOnlyIfNotSilent(
      pqLoadURL("https://mundhahs.dev"),
      getResultFunctionFactory("this is not turtle",
                               boost::beast::http::status::ok, "text/turtle"),
      testing::HasSubstr("Parse error at byte position 0"));
  expectThrowAlways(
      pqLoadURL("https://mundhahs.dev"),
      getResultFunctionFactory(
          "<x> <y> <z>", boost::beast::http::status::ok, "text/turtle",
          std::make_exception_ptr(ad_utility::CancellationException(
              ad_utility::CancellationState::TIMEOUT))),
      testing::HasSubstr("Operation timed out."));
  expectThrowAlways(
      pqLoadURL("https://mundhahs.dev"),
      getResultFunctionFactory(
          "<x> <y> <z>", boost::beast::http::status::ok, "text/turtle",
          std::make_exception_ptr(
              ad_utility::detail::AllocationExceedsLimitException(10_GB,
                                                                  5_GB))),
      testing::HasSubstr("Tried to allocate"));

  auto Iri = ad_utility::triple_component::Iri::fromIriref;
  auto Literal =
      ad_utility::triple_component::Literal::fromStringRepresentation;
  expectLoad("<x> <b> <c>", "text/turtle",
             {{Iri("<x>"), Iri("<b>"), Iri("<c>")}});
  expectLoad("<x> <b> <c> ; <d> <y>", "text/turtle",
             {{Iri("<x>"), Iri("<b>"), Iri("<c>")},
              {Iri("<x>"), Iri("<d>"), Iri("<y>")}});
  expectLoad("<x> <b> <c> , <y>", "text/turtle",
             {{Iri("<x>"), Iri("<b>"), Iri("<c>")},
              {Iri("<x>"), Iri("<b>"), Iri("<y>")}});
  expectLoad("<x> <b> <c> , \"foo\"@en", "text/turtle",
             {{Iri("<x>"), Iri("<b>"), Iri("<c>")},
              {Iri("<x>"), Iri("<b>"), Literal("\"foo\"@en")}});
  expectLoad(
      "@prefix foo: <http://mundhahs.dev/rdf/> . foo:bar <is-a> <x>",
      "text/turtle",
      {{Iri("<http://mundhahs.dev/rdf/bar>"), Iri("<is-a>"), Iri("<x>")}});
}

TEST_F(LoadURLTest, getCacheKey) {
  {
    auto cleanup = setRuntimeParameterForTest<"cache-load-results">(true);

    LoadURL load1{testQec, pqLoadURL("https://mundhahs.dev")};
    LoadURL load2{testQec, pqLoadURL("https://mundhahs.dev")};
    LoadURL load3{testQec, pqLoadURL("https://mundhahs.dev", true)};
    EXPECT_THAT(load1.getCacheKey(), testing::Eq(load2.getCacheKey()));
    EXPECT_THAT(load1.getCacheKey(),
                testing::Not(testing::Eq(load3.getCacheKey())));
    EXPECT_THAT(load1.getCacheKey(),
                testing::Eq("LOAD URL https://mundhahs.dev:443/"));
    EXPECT_THAT(load3.getCacheKey(),
                testing::Eq("LOAD URL https://mundhahs.dev:443/ SILENT"));
  }
  {
    auto cleanup = setRuntimeParameterForTest<"cache-load-results">(false);

    LoadURL load1{testQec, pqLoadURL("https://mundhahs.dev")};
    LoadURL load2{testQec, pqLoadURL("https://mundhahs.dev")};
    LoadURL load3{testQec, pqLoadURL("https://mundhahs.dev", true)};
    EXPECT_THAT(load1.getCacheKey(),
                testing::Not(testing::Eq(load2.getCacheKey())));
    EXPECT_THAT(load1.getCacheKey(),
                testing::Not(testing::Eq(load3.getCacheKey())));
  }
}

TEST_F(LoadURLTest, clone) {
  LoadURL loadUrl{testQec, pqLoadURL("https://mundhahs.dev")};
  // When the results are not cached, cloning should create a decoupled object.
  // The cache breaker will be different.
  {
    auto cleanup = setRuntimeParameterForTest<"cache-load-results">(false);
    auto clone = loadUrl.clone();
    ASSERT_THAT(clone, testing::Not(testing::Eq(nullptr)));
    EXPECT_THAT(clone->getDescriptor(), testing::Eq(loadUrl.getDescriptor()));
    EXPECT_THAT(clone->getCacheKey(),
                testing::Not(testing::Eq(loadUrl.getCacheKey())));
  }
  // When the results are cached, we get decoupled object that is the same.
  {
    auto cleanup = setRuntimeParameterForTest<"cache-load-results">(true);
    auto clone = loadUrl.clone();
    ASSERT_THAT(clone, testing::Not(testing::Eq(nullptr)));
    EXPECT_THAT(clone->getDescriptor(), testing::Eq(loadUrl.getDescriptor()));
    EXPECT_THAT(*clone, IsDeepCopy(loadUrl));
  }
}

}  // namespace
