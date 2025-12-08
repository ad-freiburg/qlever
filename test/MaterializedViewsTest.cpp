// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>

#include "./MaterializedViewsTestHelpers.h"
#include "./util/HttpRequestHelpers.h"
#include "engine/Server.h"
#include "parser/MaterializedViewQuery.h"
#include "rdfTypes/Iri.h"
#include "rdfTypes/Literal.h"
#include "util/CancellationHandle.h"
#include "util/GTestHelpers.h"

namespace {

using namespace materializedViewsTestHelpers;
using namespace ad_utility::testing;
using V = Variable;

}  // namespace

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, NotImplemented) {
  // TODO<ullingerc> Remove this test when materialized views implementation is
  // added
  AD_EXPECT_THROW_WITH_MESSAGE(
      qlv().parseAndPlanQuery(R"(
        PREFIX view: <https://qlever.cs.uni-freiburg.de/materializedView/>
        SELECT * {
          SERVICE view: {
            _:config view:name "testView1" ;
                     view:scan-column ?s ;
                     view:payload-g ?x .
          }
        }
      )"),
      ::testing::HasSubstr(
          "Materialized views are currently not supported yet"));
}

// _____________________________________________________________________________
TEST_F(MaterializedViewsTest, ManualConfigurations) {
  auto plan = qlv().parseAndPlanQuery(simpleWriteQuery_);

  using ViewQuery = parsedQuery::MaterializedViewQuery;
  using V = Variable;
  auto iri = [](const std::string& ref) {
    return ad_utility::triple_component::Iri::fromIriref(ref);
  };
  auto lit = [](const std::string& literal) {
    return ad_utility::triple_component::Literal::fromStringRepresentation(
        literal);
  };

  V placeholderP{"?placeholder_p"};
  V placeholderO{"?placeholder_o"};

  // Request for reading an extra payload column
  {
    ViewQuery query;
    query.addParameter(
        SparqlTriple{iri("<config>"), iri("<name>"), lit("\"testView1\"")});
    query.addParameter(
        SparqlTriple{iri("<config>"), iri("<scan-column>"), V{"?s"}});
    query.addParameter(
        SparqlTriple{iri("<config>"), iri("<payload-g>"), V{"?o"}});
    AD_EXPECT_THROW_WITH_MESSAGE(
        query.addParameter(
            SparqlTriple{iri("<config>"), iri("<blabliblu>"), V{"?o"}}),
        ::testing::HasSubstr("Unknown parameter"));
  }

  // Invalid inputs
  {
    ViewQuery query;
    AD_EXPECT_THROW_WITH_MESSAGE(
        query.addParameter(
            SparqlTriple{iri("<config>"), iri("<blabliblu>"), V{"?o"}}),
        ::testing::HasSubstr("Unknown parameter"));
    AD_EXPECT_THROW_WITH_MESSAGE(
        query.addParameter(
            SparqlTriple{iri("<config>"), iri("<name>"), V{"?o"}}),
        ::testing::HasSubstr("Parameter <name> to materialized view query "
                             "needs to be a literal"));

    query.addParameter(
        SparqlTriple{iri("<config>"), iri("<payload-g>"), V{"?o"}});
    query.addParameter(
        SparqlTriple{iri("<config>"), iri("<name>"), lit("\"testView1\"")});
    query.addParameter(
        SparqlTriple{iri("<config>"), iri("<scan-column>"), V{"?s"}});

    AD_EXPECT_THROW_WITH_MESSAGE(
        query.addParameter(
            SparqlTriple{iri("<config>"), iri("<payload-g>"), V{"?o"}}),
        ::testing::HasSubstr("Each payload column may only be requested once"));

    AD_EXPECT_THROW_WITH_MESSAGE(
        ViewQuery(SparqlTriple{V{"?s"},
                               iri("<https://qlever.cs.uni-freiburg.de/"
                                   "materializedView/testView1>"),
                               V{"?o"}}),
        ::testing::HasSubstr("Special triple for materialized view has an "
                             "invalid predicate"));
  }
}
