// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include <boost/program_options.hpp>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "engine/Server.h"
#include "global/Constants.h"
#include "util/ProgramOptionsHelpers.h"
#include "util/ReadableNumberFact.h"

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

  string indexBasename;
  bool text = false;
  int port;
  NonNegative numSimultaneousQueries = 1;
  bool noPatterns;
  bool noPatternTrick;
  bool onlyPsoAndPosPermutations;

  NonNegative memoryMaxSizeGb = DEFAULT_MEM_FOR_QUERIES_IN_GB;

  ad_utility::ParameterToProgramOptionFactory optionFactory{
      &RuntimeParameters()};
  auto cacheMaxSizeGbSingleEntry =
      optionFactory.makeParameterOption<"cache-max-size-gb-single-entry">();
  auto cacheMaxNumEntries =
      optionFactory.makeParameterOption<"cache-max-num-entries">();
  auto cacheMaxSizeGb =
      optionFactory.makeParameterOption<"cache-max-size-gb">();

  po::options_description options("Options for ServerMain");
  auto add = [&options]<typename... Args>(Args && ... args) {
    options.add_options()(std::forward<Args>(args)...);
  };
  add("help,h", "Produce help message");
  add("index-basename,i", po::value<std::string>(&indexBasename)->required(),
      "The location of the indexBasename files");
  add("num-simultaneous-queries,j",
      po::value<NonNegative>(&numSimultaneousQueries)->default_value(1),
      "The number of queries that can be processed simultaneously.");
  add("memory-max-size-gb,m", po::value<NonNegative>(&memoryMaxSizeGb),
      "Limit on the total amount of memory (in GB) that can be used for "
      "query processing and caching. If exceeded, query will return with "
      "an error, but the engine will not crash.");
  add("cache-max-size-gb,c", optionFactory.makePoValue(&cacheMaxSizeGb),
      "Maximum memory size in GB for all cache entries (pinned and "
      "non-pinned). Note that the cache is part of the amount of memory "
      "limited by --memory-max-size-gb.");
  add("cache-max-size-gb-single-entry,e",
      optionFactory.makePoValue(&cacheMaxSizeGbSingleEntry),

      "Maximum size in GB for a single cache entry. In other words, "
      "results larger than this will never be cached.");
  add("cache-max-num-entries,k", optionFactory.makePoValue(&cacheMaxNumEntries),
      "Maximum number of entries in the cache. If exceeded, remove "
      "least-recently used entries from the cache if possible. Note that "
      "this condition and the size limit specified via --cache-max-size-gb "
      "both have to hold (logical AND).");
  add("port,p", po::value<int>(&port)->required(),
      "The port on which the http server runs.");
  add("no-patterns,P", po::bool_switch(&noPatterns),
      "Disable the use of patterns. This disables ql:has-predicate.");
  add("no-pattern-trick,T", po::bool_switch(&noPatternTrick),
      "Maximum number of entries in the cache. If exceeded, remove "
      "least-recently used entries from the cache if possible. Note that "
      "this condition and the size limit specified via --cache-max-size-gb "
      "both have to hold (logical AND).");
  add("text,t", po::bool_switch(&text), "Enables the usage of text.");
  add("only-pso-and-pos-permutations,o", po::bool_switch(&onlyPsoAndPosPermutations), "Only load PSO and POS permutations");
  po::variables_map optionsMap;

  try {
    po::store(po::parse_command_line(argc, argv, options), optionsMap);

    if (optionsMap.count("help")) {
      std::cout << options << '\n';
      return EXIT_SUCCESS;
    }

    po::notify(optionsMap);
  } catch (const std::exception& e) {
    std::cerr << "Error in command-line Argument: " << e.what() << '\n';
    std::cerr << options << '\n';
    return EXIT_FAILURE;
  }

  LOG(INFO) << EMPH_ON << "QLever Server, compiled on " << __DATE__ << " "
            << __TIME__ << EMPH_OFF << std::endl;

  try {
    Server server(port, static_cast<int>(numSimultaneousQueries),
                  memoryMaxSizeGb);
    server.run(indexBasename, text, !noPatterns, !noPatternTrick, !onlyPsoAndPosPermutations);
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
