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
  char* locale = setlocale(LC_CTYPE, "");

  std::locale loc;
  ad_utility::ReadableNumberFacet facet(1);
  std::locale locWithNumberGrouping(loc, &facet);
  ad_utility::Log::imbue(locWithNumberGrouping);

  cout << endl
       << EMPH_ON << "ServerMain, version " << __DATE__ << " " << __TIME__
       << EMPH_OFF << endl
       << endl;
  cout << "Set locale LC_CTYPE to: " << locale << endl;

  // Init variables that may or may not be
  // filled / set depending on the options.
  using ad_utility::NonNegative;

  string index;
  bool text = false;
  int port;
  NonNegative numThreads = 1;
  bool noPatterns;
  bool disablePatternTrick;

  NonNegative memLimit = DEFAULT_MEM_FOR_QUERIES_IN_GB;
  using OptNonNeg = std::optional<NonNegative>;
  OptNonNeg cacheMaxSizeGB;
  OptNonNeg cacheMaxSizeGBSingleEntry;
  OptNonNeg cacheMaxNumEntries;

  po::options_description bOptions("Options for ServerMain");
  auto add = [&bOptions]<typename... Args>(Args && ... args) {
    bOptions.add_options()(std::forward<Args>(args)...);
  };
  add("help,h", "produce help message");
  add("index,i", po::value<std::string>(&index)->required(),
      "The location of the index files");
  add("worker-threads,j", po::value<NonNegative>(&numThreads)->default_value(1),
      "Set the number of queries that can be processed simultaneously.");
  add("memory-for_queries,m", po::value<NonNegative>(&memLimit),
      "Limit on the total amount of memory (in GB) that can be used for "
      "query processing and caching. If exceeded, query will return with "
      "an error, but the engine will not crash.");
  add("cache-max-size-gb,c", po::value<OptNonNeg>(&cacheMaxSizeGB),
      "Maximum memory size in GB for all cache entries (pinned and "
      "non-pinned). Note that the cache is part of the amount of memory "
      "limited by --memory-for-queries.");
  add("cache-max-size-gb-single-entry,e",
      po::value<OptNonNeg>(&cacheMaxSizeGBSingleEntry),

      "Maximum size in GB for a single cache entry. In other words, "
      "results larger than this will never be cached.");
  add("cache-max-num-entries,k", po::value<OptNonNeg>(&cacheMaxNumEntries),
      "Maximum number of entries in the cache. If exceeded, remove "
      "least-recently used entries from the cache if possible. Note that "
      "this condition and the size limit specified via --cache-max-size-gb "
      "both have to hold (logical AND).");
  add("port,p", po::value<int>(&port)->required(),
      "The port on which the http server runs.");
  add("no-patterns,P", po::bool_switch(&noPatterns),
      "Disable the use of patterns. This disables ql:has-predicate.");
  add("no-pattern-trick,T", po::bool_switch(&disablePatternTrick),
      "Maximum number of entries in the cache. If exceeded, remove "
      "least-recently used entries from the cache if possible. Note that "
      "this condition and the size limit specified via --cache-max-size-gb "
      "both have to hold (logical AND).");
  add("text,t", po::bool_switch(&text), "Enables the usage of text.");
  po::variables_map vm;

  try {
    po::store(po::parse_command_line(argc, argv, bOptions), vm);

    if (vm.count("help")) {
      std::cout << bOptions << '\n';
      return EXIT_SUCCESS;
    }

    po::notify(vm);
  } catch (const std::exception& e) {
    std::cerr << "Error in command-line Argument: " << e.what() << '\n';
    std::cerr << bOptions << '\n';
    return EXIT_FAILURE;
  }

  // TODO<joka921> Do we want a complex wrapper that directly performs
  // these updates and also supplies the default values?
  if (cacheMaxSizeGB.has_value()) {
    RuntimeParameters().set<"cache-max-size-gb">(cacheMaxSizeGB.value());
  }
  if (cacheMaxSizeGBSingleEntry.has_value()) {
    RuntimeParameters().set<"cache-max-size-gb-single-entry">(
        cacheMaxSizeGBSingleEntry.value());
  }
  if (cacheMaxNumEntries.has_value()) {
    RuntimeParameters().set<"cache-max-num-entries">(
        cacheMaxNumEntries.value());
  }

  try {
    Server server(port, static_cast<int>(numThreads), memLimit);
    server.run(index, text, !noPatterns, !disablePatternTrick);
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
