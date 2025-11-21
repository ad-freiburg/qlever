// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
//   2011-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#include <boost/program_options.hpp>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "CompilationInfo.h"
#include "engine/Server.h"
#include "global/Constants.h"
#include "global/RuntimeParameters.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ParseableDuration.h"
#include "util/ProgramOptionsHelpers.h"
#include "util/ReadableNumberFacet.h"

using std::size_t;
using std::string;

namespace po = boost::program_options;

// Main function.
int main(int argc, char** argv) {
  // TODO<joka921> This is a hack, because the unit tests currently don't work
  // with the strip-columns feature.
  setRuntimeParameter<&RuntimeParameters::stripColumns_>(true);
  // Copy the git hash and datetime of compilation (which require relinking)
  // to make them accessible to other parts of the code
  qlever::version::copyVersionInfo();
  setlocale(LC_CTYPE, "");

  std::locale loc;
  ad_utility::ReadableNumberFacet facet(1);
  std::locale locWithNumberGrouping(loc, &facet);
  ad_utility::Log::imbue(locWithNumberGrouping);

  // Init variables that may or may not be
  // filled / set depending on the options.
  using ad_utility::NonNegative;

  std::string indexBasename;
  std::string accessToken;
  bool noAccessCheck = false;
  bool text = false;
  unsigned short port;
  NonNegative numSimultaneousQueries = 1;
  bool noPatterns;
  bool onlyPsoAndPosPermutations;
  bool persistUpdates;

  ad_utility::MemorySize memoryMaxSize;

  ad_utility::ParameterToProgramOptionFactory optionFactory{
      &globalRuntimeParameters};

  po::options_description options("Options for ServerMain");
  auto add = [&options](auto&&... args) {
    options.add_options()(AD_FWD(args)...);
  };
  add("help,h", "Produce this help message.");
  // TODO<joka921> Can we output the "required" automatically?
  add("index-basename,i", po::value<std::string>(&indexBasename)->required(),
      "The basename of the index files (required).");
  add("port,p", po::value<unsigned short>(&port)->required(),
      "The port on which HTTP requests are served (required).");
  add("access-token,a", po::value<std::string>(&accessToken)->default_value(""),
      "Access token for restricted API calls (default: no access).");
  add("no-access-check,n",
      po::bool_switch(&noAccessCheck)->default_value(false),
      "If set to true, no access-token check is performed for restricted API "
      "calls (default: false).");
  add("num-simultaneous-queries,j",
      po::value<NonNegative>(&numSimultaneousQueries)->default_value(1),
      "The number of queries that can be processed simultaneously.");
  add("memory-max-size,m",
      po::value<ad_utility::MemorySize>(&memoryMaxSize)
          ->default_value(DEFAULT_MEM_FOR_QUERIES),
      "Limit on the total amount of memory that can be used for "
      "query processing and caching. If exceeded, query will return with "
      "an error, but the engine will not crash.");
  add("cache-max-size,c",
      optionFactory.getProgramOption<&RuntimeParameters::cacheMaxSize_>(),
      "Maximum memory size for all cache entries (pinned and "
      "not pinned). Note that the cache is part of the total memory "
      "limited by --memory-max-size.");
  add("cache-max-size-single-entry,e",
      optionFactory
          .getProgramOption<&RuntimeParameters::cacheMaxSizeSingleEntry_>(),
      "Maximum size for a single cache entry. That is, "
      "results larger than this will not be cached unless pinned.");
  add("cache-max-size-lazy-result,E",
      optionFactory
          .getProgramOption<&RuntimeParameters::cacheMaxSizeLazyResult_>(),
      "Maximum size up to which lazy results will be cached by aggregating "
      "partial results. Caching does cause significant overhead for this "
      "case.");
  add("cache-max-num-entries,k",
      optionFactory.getProgramOption<&RuntimeParameters::cacheMaxNumEntries_>(),
      "Maximum number of entries in the cache. If exceeded, remove "
      "least-recently used non-pinned entries from the cache. Note that "
      "this condition and the size limit specified via --cache-max-size "
      "both have to hold (logical AND).");
  add("no-patterns,P", po::bool_switch(&noPatterns),
      "Disable the use of patterns. If disabled, the special predicate "
      "`ql:has-predicate` is not available.");
  add("text,t", po::bool_switch(&text),
      "Also load the text index. The text index must have been built before "
      "using `IndexBuilderMain` with options `-d` and `- w`.");
  add("only-pso-and-pos-permutations,o",
      po::bool_switch(&onlyPsoAndPosPermutations),
      "Only load the PSO and POS permutations. This disables queries with "
      "predicate variables.");
  add("default-query-timeout,s",
      optionFactory
          .getProgramOption<&RuntimeParameters::defaultQueryTimeout_>(),
      "Set the default timeout in seconds after which queries are cancelled"
      "automatically.");
  add("service-max-value-rows,S",
      optionFactory
          .getProgramOption<&RuntimeParameters::serviceMaxValueRows_>(),
      "The maximal number of result rows to be passed to a SERVICE operation "
      "as a VALUES clause to optimize its computation.");
  add("throw-on-unbound-variables",
      optionFactory
          .getProgramOption<&RuntimeParameters::throwOnUnboundVariables_>(),
      "If set to true, the queries that use GROUP BY, BIND, or ORDER BY with "
      "variables that are unbound in the query throw an exception. These "
      "queries technically are allowed by the SPARQL standard, but typically "
      "are the result of typos and unintended by the user");
  add("request-body-limit",
      optionFactory.getProgramOption<&RuntimeParameters::requestBodyLimit_>(),
      "Set the maximum size for the body of requests the server will process. "
      "Set to zero to disable the limit.");
  add("cache-service-results",
      optionFactory
          .getProgramOption<&RuntimeParameters::cacheServiceResults_>(),
      "SERVICE is not cached because we have to assume that any remote "
      "endpoint might change at any point in time. If you control the "
      "endpoints, you can override this setting. This will disable the sibling "
      "optimization where VALUES are dynamically pushed into `SERVICE`.");
  add("persist-updates", po::bool_switch(&persistUpdates),
      "If set, then SPARQL UPDATES will be persisted on disk. Otherwise they "
      "will be lost when the engine is stopped");
  add("syntax-test-mode",
      optionFactory.getProgramOption<&RuntimeParameters::syntaxTestMode_>(),
      "Make several query patterns that are syntactially valid, but otherwise "
      "erroneous silently into empty results (e.g. LOAD or SERVICE requests to "
      "nonexisting endpoints). This mode should only be used for running the "
      "syntax tests from the W3C SPARQL 1.1 test suite.");
  add("enable-prefilter-on-index-scans",
      optionFactory
          .getProgramOption<&RuntimeParameters::enablePrefilterOnIndexScans_>(),
      "If set to false, the prefilter procedures for FILTER expressions are "
      "disabled.");
  add("spatial-join-max-num-threads",
      optionFactory
          .getProgramOption<&RuntimeParameters::spatialJoinMaxNumThreads_>(),
      "The maximum number of threads to be used for spatial join processing. "
      "If this option is set to `0`, the number of CPU threads will be used.");
  add("spatial-join-prefilter-max-size",
      optionFactory
          .getProgramOption<&RuntimeParameters::spatialJoinPrefilterMaxSize_>(),
      "The maximum size in square coordinates of the aggregated bounding box "
      "of the smaller join partner in a spatial join, such that prefiltering "
      "will be employed. To disable prefiltering for non-point geometries, set "
      "this option to 0.");
  add("materialized-view-writer-memory",
      optionFactory.getProgramOption<
          &RuntimeParameters::materializedViewWriterMemory_>(),
      "Memory limit for sorting rows during the writing of materialized "
      "views.");
  po::variables_map optionsMap;

  try {
    po::store(po::parse_command_line(argc, argv, options), optionsMap);
    if (optionsMap.count("help")) {
      std::cout << options << '\n';
      return EXIT_SUCCESS;
    }
    po::notify(optionsMap);
  } catch (const std::exception& e) {
    std::cerr << "Error in command-line argument: " << e.what() << '\n';
    std::cerr << options << '\n';
    return EXIT_FAILURE;
  }

  AD_LOG_INFO << EMPH_ON << "QLever Server, compiled on "
              << qlever::version::DatetimeOfCompilation << " using git hash "
              << qlever::version::GitShortHash << EMPH_OFF << std::endl;

  try {
    Server server(port, numSimultaneousQueries, memoryMaxSize,
                  std::move(accessToken), noAccessCheck, !noPatterns);
    server.run(indexBasename, text, !noPatterns, !onlyPsoAndPosPermutations,
               persistUpdates);
  } catch (const std::exception& e) {
    // This code should never be reached as all exceptions should be handled
    // within server.run()
    AD_LOG_ERROR << e.what() << std::endl;
    return 1;
  }
  // This should also never be reached as the server threads are not supposed
  // to terminate.
  return 2;
}
