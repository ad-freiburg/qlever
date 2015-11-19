// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <stdlib.h>
#include <getopt.h>
#include <string>
#include <iomanip>
#include <iostream>
#include <libgen.h>

#include "util/ReadableNumberFact.h"
#include "parser/SparqlParser.h"
#include "engine/QueryGraph.h"
#include "util/Timer.h"
#include "engine/QueryPlanner.h"

using std::string;
using std::cout;
using std::endl;
using std::flush;
using std::cerr;

#define EMPH_ON  "\033[1m"
#define EMPH_OFF "\033[21m"

// Available options.
struct option options[] = {
    {"queryfile", required_argument, NULL, 'q'},
    {"interactive", no_argument, NULL, 'I'},
    {"index", required_argument, NULL, 'i'},
    {"text", no_argument, NULL, 't'},
    {NULL, 0, NULL, 0}
};

void processQuery(QueryExecutionContext& qec, const string &query);

// Main function.
int main(int argc, char **argv) {
  cout.sync_with_stdio(false);
  std::cout << std::endl << EMPH_ON
      << "SparqlEngineMain, version " << __DATE__
      << " " << __TIME__ << EMPH_OFF << std::endl << std::endl;

  char* locale = setlocale(LC_CTYPE, "en_US.utf8");
  cout << "Set locale LC_CTYPE to: " << locale << endl;


  std::locale loc;
  ad_utility::ReadableNumberFacet facet(1);
  std::locale locWithNumberGrouping(loc, &facet);
  ad_utility::Log::imbue(locWithNumberGrouping);

  string queryfile;
  string indexName = "";
  bool text = false;
  bool interactive = false;

  optind = 1;
  // Process command line arguments.
  while (true) {
    int c = getopt_long(argc, argv, "q:Ii:t", options, NULL);
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
      default:
        cout << endl
            << "! ERROR in processing options (getopt returned '" << c
            << "' = 0x" << std::setbase(16) << c << ")"
            << endl << endl;
        exit(1);
    }
  }

  if (indexName.size() == 0) {
    cout << "Missing required argument --index (-i)..." << endl;
    exit(1);
  }

  try {
    Engine engine;
    Index index;
    index.createFromOnDiskIndex(indexName);
    if (text) {
      index.addTextFromOnDiskIndex();
    }

    QueryExecutionContext qec(index, engine);

    if (queryfile == "") {
      cout << "No queryfile provided, switching to interactive mode.." << endl;
      interactive = true;
    }

    if (interactive) {
      cout << "Interactive mode... ingnoring query." << endl << endl;
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
        if (os.str() == "") { return 0; }
        processQuery(qec, os.str());
      }
    } else {
      std::ifstream qf(queryfile);
      string line;
      while (std::getline(qf, line)) {
        processQuery(qec, line);
      }
    }
  } catch (const std::exception &e) {
    cout << string("Caught exceptions: ") + e.what();
    return 1;
  } catch (ad_semsearch::Exception &e) {
    cout << e.getFullErrorMessage() << std::endl;
  }

  return 0;
}

void processQuery(QueryExecutionContext& qec, const string &query) {
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

  LOG(DEBUG) << "Time to create Execution Tree: " << timer.msecs() << "ms\n";

  timer.start();
  QueryGraph qg(&qec);
  qg.createFromParsedQuery(pq);
  const QueryExecutionTree& qetOld = qg.getExecutionTree();
  timer.stop();

  LOG(INFO) << "Time to create old execution tree: " << timer.msecs() << "ms\n";

  if (qet.asString() == qetOld.asString()) {
    LOG(INFO) << "Execution trees identical!\n";
  } else {
    LOG(INFO) << "Execution Tree:" << qet.asString() << '\n';
    LOG(INFO) << "Old Execution Tree:" << qetOld.asString() << '\n';
  }

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
}
