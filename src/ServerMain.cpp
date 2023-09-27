// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
//   2011-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#include <CompilationInfo.h>
#include <engine/Server.h>
#include <global/Constants.h>
#include <util/ProgramOptionsHelpers.h>
#include <util/ReadableNumberFact.h>

#include <boost/program_options.hpp>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

using std::cerr;
using std::cout;
using std::endl;
using std::flush;
using std::size_t;
using std::string;
using std::vector;

namespace po = boost::program_options;

#define EMPH_ON "\033[1m"
#define EMPH_OFF "\033[22m"

// Main function.
int main(int argc, char** argv) {
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
  bool text = false;
  unsigned short port;
  NonNegative numSimultaneousQueries = 1;
  bool noPatterns;
  bool noPatternTrick;
  bool onlyPsoAndPosPermutations;

  NonNegative memoryMaxSizeGb;

  ad_utility::ParameterToProgramOptionFactory optionFactory{
      &RuntimeParameters()};

  po::options_description options("Options for ServerMain");
  auto add = [&options]<typename... Args>(Args&&... args) {
    options.add_options()(std::forward<Args>(args)...);
  };
  add("help,h", "Produce this help message.");
  // TODO<joka921> Can we output the "required" automatically?
  add("index-basename,i", po::value<std::string>(&indexBasename)->required(),
      "The basename of the index files (required).");
  add("port,p", po::value<unsigned short>(&port)->required(),
      "The port on which HTTP requests are served (required).");
  add("access-token,a", po::value<std::string>(&accessToken)->default_value(""),
      "Access token for restricted API calls (default: no access).");
  add("num-simultaneous-queries,j",
      po::value<NonNegative>(&numSimultaneousQueries)->default_value(1),
      "The number of queries that can be processed simultaneously.");
  add("memory-max-size-gb,m",
      po::value<NonNegative>(&memoryMaxSizeGb)
          ->default_value(DEFAULT_MEM_FOR_QUERIES_IN_GB),
      "Limit on the total amount of memory (in GB) that can be used for "
      "query processing and caching. If exceeded, query will return with "
      "an error, but the engine will not crash.");
  add("cache-max-size-gb,c",
      optionFactory.getProgramOption<"cache-max-size-gb">(),
      "Maximum memory size in GB for all cache entries (pinned and "
      "not pinned). Note that the cache is part of the total memory "
      "limited by --memory-max-size-gb.");
  add("cache-max-size-gb-single-entry,e",
      optionFactory.getProgramOption<"cache-max-size-gb-single-entry">(),
      "Maximum size in GB for a single cache entry. That is, "
      "results larger than this will not be cached unless pinned.");
  add("cache-max-num-entries,k",
      optionFactory.getProgramOption<"cache-max-num-entries">(),
      "Maximum number of entries in the cache. If exceeded, remove "
      "least-recently used non-pinned entries from the cache. Note that "
      "this condition and the size limit specified via --cache-max-size-gb "
      "both have to hold (logical AND).");
  add("no-patterns,P", po::bool_switch(&noPatterns),
      "Disable the use of patterns. If disabled, the special predicate "
      "`ql:has-predicate` is not available.");
  add("no-pattern-trick,T", po::bool_switch(&noPatternTrick),
      "Maximum number of entries in the cache. If exceeded, remove "
      "least-recently used entries from the cache if possible. Note that "
      "this condition and the size limit specified via --cache-max-size-gb "
      "both have to hold (logical AND).");
  add("text,t", po::bool_switch(&text),
      "Also load the text index. The text index must have been built before "
      "using `IndexBuilderMain` with options `-d` and `-w`.");
  add("only-pso-and-pos-permutations,o",
      po::bool_switch(&onlyPsoAndPosPermutations),
      "Only load the PSO and POS permutations. This disables queries with "
      "predicate variables.");
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

  LOG(INFO) << EMPH_ON << "QLever Server, compiled on "
            << qlever::version::DatetimeOfCompilation << " using git hash "
            << qlever::version::GitShortHash() << EMPH_OFF << std::endl;

  try {
    Server server(port, numSimultaneousQueries, memoryMaxSizeGb,
                  std::move(accessToken), !noPatternTrick);
    server.run(indexBasename, text, !noPatterns, !onlyPsoAndPosPermutations);
  } catch (const std::exception& e) {
    // This code should never be reached as all exceptions should be handled
    // within server.run()
    LOG(ERROR) << e.what() << std::endl;
    return 1;
  }
  // This should also never be reached as the server threads are not supposed
  // to terminate.
  return 2;
}
