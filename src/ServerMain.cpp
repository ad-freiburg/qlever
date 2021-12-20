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
  NonNegative cacheMaxSizeGB = DEFAULT_CACHE_MAX_SIZE_GB;
  NonNegative cacheMaxSizeGBSingleEntry =
      DEFAULT_CACHE_MAX_SIZE_GB_SINGLE_ENTRY;
  NonNegative cacheMaxNumEntries = DEFAULT_CACHE_MAX_NUM_ENTRIES;

  po::options_description bOptions("Options for ServerMain");
  auto add = [&bOptions]<typename... Args>(Args && ... args) {
    bOptions.add_options()(std::forward<Args>(args)...);
  };
  add("help,h", "produce help message");
  add("index,i", po::value<std::string>(&index)->required(),
      "The location of the index files");
  add("worker-threads,j", po::value<NonNegative>(&numThreads)->default_value(1),
      "Set the number of queries that can be processed simultaneously.");
  add("memory-for_queries,m",
      po::value<NonNegative>(&cacheMaxSizeGB)
          ->default_value(DEFAULT_CACHE_MAX_SIZE_GB),
      "Limit on the total amount of memory (in GB) that can be used for "
      "query processing and caching. If exceeded, query will return with "
      "an error, but the engine will not crash.");
  add("cache-max-size-gb,c", po::value<NonNegative>(&cacheMaxSizeGB),
      "Maximum memory size in GB for all cache entries (pinned and "
      "non-pinned). Note that the cache is part of the amount of memory "
      "limited by --memory-for-queries.");
  add("cache-max-size-gb-single-entry,e",
      po::value<NonNegative>(&cacheMaxSizeGBSingleEntry),

      "Maximum size in GB for a single cache entry. In other words, "
      "results larger than this will never be cached.");
  add("cache-max-num-entries,k", po::value<NonNegative>(&cacheMaxNumEntries),
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
  // Process command line arguments.
  while (true) {
    int c = getopt_long(argc, argv, "i:p:j:tauhm:lc:e:k:T", options, NULL);
    if (c == -1) break;
    switch (c) {
      case 'i':
        index = optarg;
        break;
      case 'p':
        port = atoi(optarg);
        break;
      case 'P':
        usePatterns = false;
        break;
      case 'T':
        enablePatternTrick = false;
        break;
      case 't':
        text = true;
        break;
      case 'j':
        numThreads = atoi(optarg);
        break;
      case 'm':
        memLimit = atoi(optarg);
        break;
      case 'h':
        printUsage(argv[0]);
        exit(0);
        break;
      case 'l':
        std::cout << "Warning: the -l flag (onDiskLiterals) is deprecated and "
                     "will be ignored for ServerMain. The correct setting for "
                     "this flag is read directly from the index\n";
        break;
      case 'c':
        RuntimeParameters().set<"cache-max-size-gb">(atoi(optarg));
        break;
      case 'e':
        RuntimeParameters().set<"cache-max-size-gb-single-entry">(atoi(optarg));
        break;
      case 'k':
        RuntimeParameters().set<"cache-max-num-entries">(atoi(optarg));
        break;
      default:
        cout << endl
             << "! ERROR in processing options (getopt returned '" << c
             << "' = 0x" << std::setbase(16) << static_cast<int>(c) << ")"
             << endl
             << endl;
        printUsage(argv[0]);
        exit(1);
    }
  }

  try {
    Server server(port, numThreads, memLimit);
    server.run(index, text, usePatterns, enablePatternTrick);
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
