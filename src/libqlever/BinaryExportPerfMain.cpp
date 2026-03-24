// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <algorithm>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>

#include "libqlever/Qlever.h"
#include "util/Timer.h"

// Default query used when no query is provided via CLI.
static const std::string defaultQuery = R"(
PREFIX geo: <http://www.opengis.net/ont/geosparql#>
SELECT ?x WHERE {
 ?x geo:hasGeometry [].
}
)";

int main(int argc, char** argv) {
  // Parse command line arguments.
  if (argc < 2 || argc > 4) {
    std::cerr << "Usage: " << argv[0] << " <index basename> [query] [num-runs]"
              << std::endl;
    std::cerr << "  <index basename>  Path and basename of the QLever index"
              << std::endl;
    std::cerr << "  [query]           SPARQL query string (default: COUNT(*))"
              << std::endl;
    std::cerr << "  [num-runs]        Number of times to run the query "
              << "(default: 5)" << std::endl;
    return 1;
  }
  std::string indexBasename = argv[1];
  std::string query = (argc >= 3) ? argv[2] : defaultQuery;
  size_t numRuns = (argc >= 4) ? std::stoull(argv[3]) : 5;

  // Load the index.
  std::cout << "Loading index with basename \"" << indexBasename << "\" ..."
            << std::endl;
  qlever::EngineConfig engineConfig;
  engineConfig.baseName_ = indexBasename;
  qlever::Qlever qlever{engineConfig};
  std::cout << "Index loaded." << std::endl;
  std::cout << std::endl;

  // Parse and plan the query (measured separately).
  std::cout << "Query: " << query << std::endl;
  ad_utility::Timer planTimer{ad_utility::Timer::Started};
  qlever::Qlever::QueryPlan queryPlan;
  try {
    queryPlan = qlever.parseAndPlanQuery(query);
  } catch (const std::exception& e) {
    std::cerr << "Parsing/planning the query failed: " << e.what() << std::endl;
    return 1;
  }
  auto planTimeMs = planTimer.msecs().count();
  std::cout << "Query planned in " << planTimeMs << " ms" << std::endl;
  std::cout << std::endl;

  // Run the query multiple times and collect timings.
  std::cout << "Running query " << numRuns << " time(s) ..." << std::endl;
  std::vector<double> timingsMs;
  timingsMs.reserve(numRuns);
  for (size_t i = 0; i < numRuns; ++i) {
    ad_utility::Timer timer{ad_utility::Timer::Started};
    try {
      std::string result =
          qlever.query(queryPlan, ad_utility::MediaType::binaryQleverExport);
      auto elapsedMs = timer.msecs().count();
      timingsMs.push_back(static_cast<double>(elapsedMs));
      std::cout << "  Run " << (i + 1) << ": " << elapsedMs << " ms"
                << std::endl;
      // Print the result only for the first run.
      if (i == 0) {
        std::cout << "  Result (first run): " << result.substr(0, 500);
        if (result.size() > 500) {
          std::cout << " ... (" << result.size() << " bytes total)";
        }
        std::cout << std::endl;
      }
    } catch (const std::exception& e) {
      std::cerr << "  Run " << (i + 1) << " failed: " << e.what() << std::endl;
      return 1;
    }
  }
  std::cout << std::endl;

  // Report summary statistics.
  if (!timingsMs.empty()) {
    double minTime = *std::min_element(timingsMs.begin(), timingsMs.end());
    double maxTime = *std::max_element(timingsMs.begin(), timingsMs.end());
    double avgTime = std::accumulate(timingsMs.begin(), timingsMs.end(), 0.0) /
                     static_cast<double>(timingsMs.size());
    std::cout << "Summary (" << numRuns << " runs):" << std::endl;
    std::cout << "  Min: " << minTime << " ms" << std::endl;
    std::cout << "  Max: " << maxTime << " ms" << std::endl;
    std::cout << "  Avg: " << avgTime << " ms" << std::endl;
  }

  return 0;
}
