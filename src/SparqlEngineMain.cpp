// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <getopt.h>
#include <libgen.h>
#include <stdlib.h>
#include <iomanip>
#include <iostream>
#include <string>

#include "engine/QueryPlanner.h"
#include "parser/SparqlParser.h"
#include "util/ReadableNumberFact.h"
#include "util/Timer.h"

using std::cerr;
using std::cout;
using std::endl;
using std::flush;
using std::string;

#define EMPH_ON "\033[1m"
#define EMPH_OFF "\033[22m"

// Available options.
struct option options[] = {{"cost-factors", required_argument, NULL, 'c'},
                           {"help", no_argument, NULL, 'h'},
                           {"index", required_argument, NULL, 'i'},
                           {"interactive", no_argument, NULL, 'I'},
                           {"on-disk-literals", no_argument, NULL, 'l'},
                           {"no-patterns", no_argument, NULL, 'P'},
                           {"queryfile", required_argument, NULL, 'q'},
                           {"text", no_argument, NULL, 't'},
                           {NULL, 0, NULL, 0}};

void processQuery(QueryExecutionContext& qec, const string& query);
void printUsage(char* execName);

void printUsage(char* execName) {
  std::ios coutState(nullptr);
  coutState.copyfmt(cout);
  cout << std::setfill(' ') << std::left;

  cout << "Usage: " << execName << " -i <index> [OPTIONS]" << endl << endl;
  cout << "Options" << endl;
  cout << "  " << std::setw(20) << "c, cost-factors" << std::setw(1) << "    "
       << "Path to a file containing cost factors." << endl;
  cout << "  " << std::setw(20) << "h, help" << std::setw(1) << "    "
       << "Show this help and exit." << endl;
  cout << "  " << std::setw(20) << "i, index" << std::setw(1) << "    "
       << "The location of the index files." << endl;
  cout << "  " << std::setw(20) << "I, interactive" << std::setw(1) << "    "
       << "Use stdin to read the queries." << endl;
  cout << "  " << std::setw(20) << "l, on-disk-literals" << std::setw(1)
       << "    "
       << "Indicates that the literals can be found on disk with the index."
       << endl;
  cout << "  " << std::setw(20) << "no-patterns" << std::setw(1) << "    "
       << "Disable the use of patterns. This disables ql:has-predicate."
       << endl;
  cout << "  " << std::setw(20) << "q, queryfile" << std::setw(1) << "    "
       << "Path to a file containing one query per line." << endl;
  cout << "  " << std::setw(20) << "t, text" << std::setw(1) << "    "
       << "Enables the usage of text." << endl;
  cout.copyfmt(coutState);
}

// Main function.
int main(int argc, char** argv) {
  cout.sync_with_stdio(false);
  char* locale = setlocale(LC_CTYPE, "");

  // std::locale loc;
  // ad_utility::ReadableNumberFacet facet(1);
  // std::locale locWithNumberGrouping(loc, &facet);
  // ad_utility::Log::imbue(locWithNumberGrouping);

  string queryfile;
  string indexName = "";
  string costFactosFileName = "";
  bool text = false;
  bool interactive = false;
  bool onDiskLiterals = false;
  bool usePatterns = false;

  optind = 1;
  // Process command line arguments.
  while (true) {
    int c = getopt_long(argc, argv, "q:Ii:tc:lahu", options, NULL);
    if (c == -1) break;
    switch (c) {
      case 'q':
        queryfile = optarg;
        break;
      case 'I':
        interactive = true;
        break;
      case 'i':
        indexName = optarg;
        break;
      case 't':
        text = true;
        break;
      case 'c':
        costFactosFileName = optarg;
        break;
      case 'l':
        onDiskLiterals = true;
        break;
      case 'h':
        printUsage(argv[0]);
        exit(0);
        break;
      case 'P':
        usePatterns = false;
        break;
      default:
        cout << endl
             << "! ERROR in processing options (getopt returned '" << c
             << "' = 0x" << std::setbase(16) << c << ")" << endl
             << endl;
        printUsage(argv[0]);
        exit(1);
    }
  }

  if (indexName.size() == 0) {
    cerr << "Missing required argument --index (-i)..." << endl;
    printUsage(argv[0]);
    exit(1);
  }

  std::cout << std::endl
            << EMPH_ON << "SparqlEngineMain, version " << __DATE__ << " "
            << __TIME__ << EMPH_OFF << std::endl
            << std::endl;
  cout << "Set locale LC_CTYPE to: " << locale << endl;

  try {
    Engine engine;
    Index index;
    index.setUsePatterns(usePatterns);
    index.setOnDiskLiterals(onDiskLiterals);
    index.createFromOnDiskIndex(indexName);
    if (text) {
      index.addTextFromOnDiskIndex();
    }

    QueryExecutionContext qec(index, engine);
    if (costFactosFileName.size() > 0) {
      qec.readCostFactorsFromTSVFile(costFactosFileName);
    }

    if (queryfile == "") {
      cout << "No query file provided, switching to interactive mode.." << endl;
      interactive = true;
    }

    if (interactive) {
      cout << "Interactive mode... ignoring query." << endl << endl;
      while (true) {
        std::ostringstream os;
        string line;
        cout << "Query: (empty line to end input, empty query to quit)\n";
        while (true) {
          std::getline(std::cin, line);
          if (line == "") {
            break;
          }
          os << line << '\n';
        }
        if (os.str() == "") {
          return 0;
        }
        processQuery(qec, os.str());
      }
    } else {
      std::ifstream qf(queryfile);
      string line;
      while (std::getline(qf, line)) {
        processQuery(qec, line);
      }
    }
  } catch (const std::exception& e) {
    cout << e.what() << std::endl;
  }

  return 0;
}

void processQuery(QueryExecutionContext& qec, const string& query) {
  ad_utility::Timer t;
  t.start();
  SparqlParser sp;
  ParsedQuery pq = sp.parse(query);
  pq.expandPrefixes();
  QueryPlanner qp(&qec);
  ad_utility::Timer timer;
  timer.start();
  auto qet = qp.createExecutionTree(pq);
  timer.stop();
  LOG(INFO) << "Time to create Execution Tree: " << timer.msecs() << "ms\n";
  LOG(INFO) << "Execution Tree: " << qet.asString() << "ms\n";
  size_t limit = MAX_NOF_ROWS_IN_RESULT;
  size_t offset = 0;
  if (pq._limit.size() > 0) {
    limit = static_cast<size_t>(atol(pq._limit.c_str()));
  }
  if (pq._offset.size() > 0) {
    offset = static_cast<size_t>(atol(pq._offset.c_str()));
  }
  qet.writeResultToStream(cout, pq._selectedVariables, limit, offset);
  t.stop();
  std::cout << "\nDone. Time: " << t.usecs() / 1000.0 << " ms\n";
  size_t numMatches = qet.getResult()->size();
  std::cout << "\nNumber of matches (no limit): " << numMatches << "\n";
  size_t effectiveLimit =
      atoi(pq._limit.c_str()) > 0 ? atoi(pq._limit.c_str()) : numMatches;
  std::cout << "\nNumber of matches (limit): "
            << std::min(numMatches, effectiveLimit) << "\n";
}
