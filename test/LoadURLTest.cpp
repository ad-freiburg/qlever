// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "engine/LoadURL.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"
#include "util/RuntimeParametersTestHelpers.h"

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

  // Factory for generating mocks of the `sendHttpOrHttpsRequest` function that
  // is used by default by a `Service` operation (see the constructor in
  // `Service.h`). Each mock does the following:
  //
  // 1. It tests that the request method is POST, the content-type header is
  //    `application/sparql-query`, and the accept header is
  //    `text/tab-separated-values` (our `Service` always does this).
  //
  // 2. It tests that the host and port are as expected.
  //
  // 3. It tests that the post data is as expected.
  //
  // 4. It returns the specified JSON.
  //
  // NOTE: In a previous version of this test, we set up an actual test server.
  // The code can be found in the history of this PR.
  static auto constexpr getResultFunctionFactory =
      [](std::string predefinedResult,
         boost::beast::http::status status = boost::beast::http::status::ok,
         std::string contentType = "text/turtle",
         std::exception_ptr mockException = nullptr,
         ad_utility::source_location loc =
             ad_utility::source_location::current())
      -> LoadURL::GetResultFunction {
    return [=](const ad_utility::httpUtils::Url&,
               ad_utility::SharedCancellationHandle,
               const boost::beast::http::verb& method,
               std::string_view postData, std::string_view contentTypeHeader,
               std::string_view acceptHeader) {
      auto g = generateLocationTrace(loc);
      EXPECT_THAT(method, testing::Eq(boost::beast::http::verb::get));
      EXPECT_THAT(contentTypeHeader, testing::Eq(""));
      EXPECT_THAT(acceptHeader, testing::Eq(""));
      EXPECT_THAT(postData, testing::Eq(""));

      if (mockException) {
        std::rethrow_exception(mockException);
      }

      auto randomlySlice =
          [](std::string result) -> cppcoro::generator<ql::span<std::byte>> {
        // Randomly slice the string to make tests more robust.
        std::mt19937 rng{std::random_device{}()};

        const std::string resultStr = result;
        std::uniform_int_distribution<size_t> distribution{
            0, resultStr.length() / 2};

        for (size_t start = 0; start < resultStr.length();) {
          size_t size = distribution(rng);
          std::string resultCopy{resultStr.substr(start, size)};
          co_yield ql::as_writable_bytes(ql::span{resultCopy});
          start += size;
        }
      };
      return HttpOrHttpsResponse{.status_ = status,
                                 .contentType_ = contentType,
                                 .body_ = randomlySlice(predefinedResult)};
    };
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
  EXPECT_THAT(loadURL.getSizeEstimate(), testing::Eq(100'000));
  EXPECT_THAT(loadURL.getCostEstimate(), testing::Eq(1'000'000));
  EXPECT_THAT(loadURL.computeVariableToColumnMap(),
              testing::UnorderedElementsAreArray(VariableToColumnMap{
                  {Variable("?s"), makeAlwaysDefinedColumn(0)},
                  {Variable("?p"), makeAlwaysDefinedColumn(1)},
                  {Variable("?o"), makeAlwaysDefinedColumn(2)}}));
  EXPECT_THAT(loadURL.knownEmptyResult(), testing::IsFalse());
  EXPECT_THAT(loadURL.getChildren(), testing::IsEmpty());
}

TEST_F(LoadURLTest, computeResult) {
  {
    LoadURL loadURL{testQec, pqLoadURL("https://mundhahs.dev"),
                    getResultFunctionFactory(
                        "<x> <b> <c>", boost::beast::http::status::not_found)};
    AD_EXPECT_THROW_WITH_MESSAGE(
        loadURL.computeResultOnlyForTesting(),
        testing::HasSubstr("RDF dataset responded with HTTP status code: 404"));
  }
  {
    LoadURL loadURL{
        testQec, pqLoadURL("https://mundhahs.dev"),
        getResultFunctionFactory("<x> <b> <c>", boost::beast::http::status::ok,
                                 "text/plain")};
    AD_EXPECT_THROW_WITH_MESSAGE(
        loadURL.computeResultOnlyForTesting(),
        testing::HasSubstr("Unsupported media type \"text/plain\"."));
  }
  {
    LoadURL loadURL{testQec, pqLoadURL("https://mundhahs.dev"),
                    getResultFunctionFactory(
                        "<x> <b> <c>", boost::beast::http::status::ok, "")};
    AD_EXPECT_THROW_WITH_MESSAGE(
        loadURL.computeResultOnlyForTesting(),
        testing::HasSubstr("QLever requires the endpoint of a LoadURL to "
                           "return the mediatype."));
  }
  {
    LoadURL loadURL{testQec, pqLoadURL("https://mundhahs.dev"),
                    getResultFunctionFactory("this is not turtle",
                                             boost::beast::http::status::ok,
                                             "text/turtle")};
    AD_EXPECT_THROW_WITH_MESSAGE(
        loadURL.computeResultOnlyForTesting(),
        testing::HasSubstr("Parse error at byte position 0"));
  }
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
  auto clone = loadUrl.clone();
  ASSERT_THAT(clone, testing::Not(testing::Eq(nullptr)));
  EXPECT_THAT(*clone, IsDeepCopy(loadUrl));
  EXPECT_THAT(clone->getDescriptor(), testing::Eq(loadUrl.getDescriptor()));
}
