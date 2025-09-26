// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "DeltaTriplesTestHelpers.h"
#include "engine/ExecuteUpdate.h"
#include "engine/Load.h"
#include "engine/QueryPlanner.h"
#include "parser/SparqlParser.h"
#include "util/GTestHelpers.h"
#include "util/HttpClientTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"
#include "util/RuntimeParametersTestHelpers.h"

namespace {

auto pqLoad = [](std::string url, bool silent = false) {
  return parsedQuery::Load{ad_utility::triple_component::Iri::fromIriref(
                               absl::StrCat("<", url, ">")),
                           silent};
};

// Fixture that sets up a test index and a factory for producing mocks for the
// `getResultFunction` needed by the `Service` operation.
class LoadTest : public ::testing::Test {
 protected:
  // Query execution context (with small test index) and allocator for testing,
  // see `IndexTestHelpers.h`. Note that `getQec` returns a pointer to a static
  // `QueryExecutionContext`, so no need to ever delete `testQec`.
  QueryExecutionContext* testQec = ad_utility::testing::getQec();
  ad_utility::AllocatorWithLimit<Id> testAllocator =
      ad_utility::testing::makeAllocator();
  ad_utility::BlankNodeManager blankNodeManager_;

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

TEST_F(LoadTest, basicMethods) {
  // Create an instance of the operation.
  Load load{testQec, pqLoad("https://mundhahs.dev")};

  // Test the basic methods.
  EXPECT_THAT(load.getDescriptor(), testing::Eq("LOAD <https://mundhahs.dev>"));
  EXPECT_THAT(load.getCacheKey(), testing::StartsWith("LOAD"));
  EXPECT_THAT(load.getResultWidth(), testing::Eq(3));
  EXPECT_THAT(load.getMultiplicity(0), testing::Eq(1));
  EXPECT_THAT(load.getMultiplicity(1), testing::Eq(1));
  EXPECT_THAT(load.getMultiplicity(2), testing::Eq(1));
  EXPECT_THAT(load.getExternallyVisibleVariableColumns(),
              testing::UnorderedElementsAreArray(VariableToColumnMap{
                  {Variable("?s"), makeAlwaysDefinedColumn(0)},
                  {Variable("?p"), makeAlwaysDefinedColumn(1)},
                  {Variable("?o"), makeAlwaysDefinedColumn(2)}}));
  EXPECT_THAT(load.getSizeEstimate(), testing::Eq(100'000));
  EXPECT_THAT(load.getCostEstimate(), testing::Eq(1'000'000));
  EXPECT_THAT(load.knownEmptyResult(), testing::IsFalse());
  EXPECT_THAT(load.getChildren(), testing::IsEmpty());
}

TEST_F(LoadTest, computeResult) {
  auto testSilentBehavior = [this](parsedQuery::Load pq,
                                   SendRequestType sendFunc,
                                   ad_utility::source_location loc =
                                       ad_utility::source_location::current()) {
    auto impl = [this, &pq,
                 &sendFunc](ad_utility::source_location loc =
                                ad_utility::source_location::current()) {
      auto tr = generateLocationTrace(loc);
      Load load{testQec, pq, sendFunc};
      auto res = load.computeResultOnlyForTesting();
      EXPECT_THAT(res.idTable(), testing::IsEmpty());
      EXPECT_THAT(res.localVocab(), testing::IsEmpty());
    };

    auto tr = generateLocationTrace(loc);
    // Not silent, but syntax test mode is activated.
    pq.silent_ = false;
    {
      auto cleanup =
          setRuntimeParameterForTest<&RuntimeParameters::syntaxTestMode_>(true);
      impl();
    }
    // Silent, but syntax test mode is deactivated.
    pq.silent_ = true;
    impl();
  };

  auto expectThrowOnlyIfNotSilent =
      [this, testSilentBehavior](
          parsedQuery::Load pq, SendRequestType sendFunc,
          const testing::Matcher<std::string>& expectedError,
          ad_utility::source_location loc =
              ad_utility::source_location::current()) {
        auto g = generateLocationTrace(loc);
        Load load{testQec, pq, sendFunc};

        AD_EXPECT_THROW_WITH_MESSAGE(load.computeResultOnlyForTesting(),
                                     expectedError);
        testSilentBehavior(pq, sendFunc);
      };
  auto expectThrowAlways =
      [this](parsedQuery::Load pq, SendRequestType sendFunc,
             const testing::Matcher<std::string>& expectedError,
             ad_utility::source_location loc =
                 ad_utility::source_location::current()) {
        auto g = generateLocationTrace(loc);
        Load load{testQec, pq, sendFunc};

        AD_EXPECT_THROW_WITH_MESSAGE(load.computeResultOnlyForTesting(),
                                     expectedError);
        pq.silent_ = true;
        Load silentLoad{testQec, pq, sendFunc};
        AD_EXPECT_THROW_WITH_MESSAGE(silentLoad.computeResultOnlyForTesting(),
                                     expectedError);
      };
  auto expectLoad =
      [this](std::string responseBody, std::string contentType,
             std::vector<std::array<TripleComponent, 3>> expectedIdTable,
             ad_utility::source_location loc =
                 ad_utility::source_location::current()) {
        auto g = generateLocationTrace(loc);

        Load load{
            testQec, pqLoad("https://mundhahs.dev"),
            getResultFunctionFactory(
                responseBody, boost::beast::http::status::ok, contentType)};
        auto res = load.computeResultOnlyForTesting();

        auto& idTable = res.idTable();
        auto& lv = res.localVocab();

        std::vector<std::vector<IntOrId>> idVector;
        for (const auto& row : expectedIdTable) {
          auto& idVecRow = idVector.emplace_back();
          for (auto& field : row) {
            const auto& idx = testQec->getIndex();
            auto idOpt =
                field.toValueId(idx.getVocab(), idx.encodedIriManager());
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
      pqLoad("https://mundhahs.dev"),
      getResultFunctionFactory("<x> <b> <c>",
                               boost::beast::http::status::not_found),
      testing::HasSubstr("RDF dataset responded with HTTP status code: 404"));
  expectThrowOnlyIfNotSilent(
      pqLoad("https://mundhahs.dev"),
      getResultFunctionFactory("<x> <b> <c>", boost::beast::http::status::ok,
                               "foo/bar"),
      testing::HasSubstr(
          "Unsupported `Content-Type` of response: \"foo/bar\""));
  expectThrowOnlyIfNotSilent(
      pqLoad("https://mundhahs.dev"),
      getResultFunctionFactory("<x> <b> <c>", boost::beast::http::status::ok,
                               "text/plain"),
      testing::HasSubstr(
          "Unsupported `Content-Type` of response: \"text/plain\""));
  expectThrowOnlyIfNotSilent(
      pqLoad("https://mundhahs.dev"),
      getResultFunctionFactory("<x> <b> <c>", boost::beast::http::status::ok,
                               ""),
      testing::HasSubstr("QLever requires the `Content-Type` header to be "
                         "set for the HTTP response."));
  expectThrowOnlyIfNotSilent(
      pqLoad("https://mundhahs.dev"),
      getResultFunctionFactory("this is not turtle",
                               boost::beast::http::status::ok, "text/turtle"),
      testing::HasSubstr("Parse error at byte position 0"));
  expectThrowAlways(
      pqLoad("https://mundhahs.dev"),
      getResultFunctionFactory(
          "<x> <y> <z>", boost::beast::http::status::ok, "text/turtle",
          std::make_exception_ptr(ad_utility::CancellationException(
              ad_utility::CancellationState::TIMEOUT))),
      testing::HasSubstr("Operation timed out."));
  expectThrowAlways(
      pqLoad("https://mundhahs.dev"),
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

TEST_F(LoadTest, getCacheKey) {
  {
    auto cleanup =
        setRuntimeParameterForTest<&RuntimeParameters::cacheLoadResults_>(true);

    Load load1{testQec, pqLoad("https://mundhahs.dev")};
    Load load2{testQec, pqLoad("https://mundhahs.dev")};
    Load load3{testQec, pqLoad("https://mundhahs.dev", true)};
    EXPECT_THAT(load1.canResultBeCached(), testing::IsTrue());
    EXPECT_THAT(load2.canResultBeCached(), testing::IsTrue());
    EXPECT_THAT(load3.canResultBeCached(), testing::IsTrue());
    EXPECT_THAT(load1.getCacheKey(), testing::Eq(load2.getCacheKey()));
    EXPECT_THAT(load1.getCacheKey(),
                testing::Not(testing::Eq(load3.getCacheKey())));
    EXPECT_THAT(load1.getCacheKey(),
                testing::Eq("LOAD <https://mundhahs.dev>"));
    EXPECT_THAT(load3.getCacheKey(),
                testing::Eq("LOAD <https://mundhahs.dev> SILENT"));
  }
  {
    auto cleanup =
        setRuntimeParameterForTest<&RuntimeParameters::cacheLoadResults_>(
            false);

    Load load1{testQec, pqLoad("https://mundhahs.dev")};
    Load load2{testQec, pqLoad("https://mundhahs.dev")};
    Load load3{testQec, pqLoad("https://mundhahs.dev", true)};
    EXPECT_THAT(load1.canResultBeCached(), testing::IsFalse());
    EXPECT_THAT(load2.canResultBeCached(), testing::IsFalse());
    EXPECT_THAT(load3.canResultBeCached(), testing::IsFalse());
    EXPECT_THAT(load1.getCacheKey(),
                testing::Not(testing::Eq(load2.getCacheKey())));
    EXPECT_THAT(load1.getCacheKey(),
                testing::Not(testing::Eq(load3.getCacheKey())));
  }
}

TEST_F(LoadTest, clone) {
  Load load{testQec, pqLoad("https://mundhahs.dev")};
  // When the results are not cached, cloning should create a decoupled object.
  // The cache breaker will be different.
  {
    auto cleanup =
        setRuntimeParameterForTest<&RuntimeParameters::cacheLoadResults_>(
            false);
    auto clone = load.clone();
    ASSERT_THAT(clone, testing::Not(testing::Eq(nullptr)));
    EXPECT_THAT(clone->getDescriptor(), testing::Eq(load.getDescriptor()));
    EXPECT_THAT(clone->getCacheKey(),
                testing::Not(testing::Eq(load.getCacheKey())));
  }
  // When the results are cached, we get decoupled object that is the same.
  {
    auto cleanup =
        setRuntimeParameterForTest<&RuntimeParameters::cacheLoadResults_>(true);
    auto clone = load.clone();
    ASSERT_THAT(clone, testing::Not(testing::Eq(nullptr)));
    EXPECT_THAT(clone->getDescriptor(), testing::Eq(load.getDescriptor()));
    EXPECT_THAT(*clone, IsDeepCopy(load));
  }
}

TEST_F(LoadTest, Integration) {
  auto parsedUpdate = SparqlParser::parseUpdate(
      &blankNodeManager_, &testQec->getIndex().encodedIriManager(),
      "LOAD <https://mundhahs.dev>");
  ASSERT_THAT(parsedUpdate, testing::SizeIs(1));
  auto qec =
      ad_utility::testing::getQec(ad_utility::testing::TestIndexConfig{});
  auto cancellationHandle =
      std::make_shared<ad_utility::CancellationHandle<>>();
  QueryPlanner qp(qec, cancellationHandle);
  auto executionTree = qp.createExecutionTree(parsedUpdate[0]);
  Load* load = dynamic_cast<Load*>(executionTree.getRootOperation().get());
  ASSERT_THAT(load, testing::NotNull()) << "Root operation is not a Load";
  load->resetGetResultFunctionForTesting(
      getResultFunctionFactory("<a> <b> <c> . <d> <e> <f>",
                               boost::beast::http::status::ok, "text/turtle"));
  DeltaTriples deltaTriples{qec->getIndex()};
  ExecuteUpdate::executeUpdate(qec->getIndex(), parsedUpdate[0], executionTree,
                               deltaTriples, cancellationHandle);
  EXPECT_THAT(deltaTriples, deltaTriplesTestHelpers::NumTriples(2, 0, 2));
}

}  // namespace
