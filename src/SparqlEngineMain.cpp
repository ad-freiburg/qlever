// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <stdlib.h>
#include <getopt.h>
#include <string>
#include <iomanip>
#include <iostream>
#include <libgen.h>

#include "parser/SparqlParser.h"
#include "engine/IndexMock.h"
#include "engine/QueryGraph.h"
#include "parser/TsvParser.h"

using std::string;
using std::cout;
using std::endl;
using std::flush;
using std::cerr;

#define EMPH_ON  "\033[1m"
#define EMPH_OFF "\033[21m"

// Available options.
struct option options[] = {
    {"query", required_argument, NULL, 'q'},
    {"interactive", no_argument, NULL, 'i'},
    {"index-basename", required_argument, NULL, 'b'},
    {NULL, 0, NULL, 0}
};

void processQuery(QueryExecutionContext& qec, const string &query);

// Main function.
int main(int argc, char **argv) {
  std::cout << std::endl << EMPH_ON
      << "SparqlEngineMain, version " << __DATE__
      << " " << __TIME__ << EMPH_OFF << std::endl << std::endl;

  string query;
  string basename;
  bool interactive = false;

  optind = 1;
  // Process command line arguments.
  while (true) {
    int c = getopt_long(argc, argv, "q:ib:", options, NULL);
    if (c == -1) break;
    switch (c) {
      case 'q':
        query = optarg;
        break;
      case 'i':
        interactive = true;
        break;
      case 'b':
        basename = optarg;
        break;
      default:
        cout << endl
            << "! ERROR in processing options (getopt returned '" << c
            << "' = 0x" << std::setbase(16) << c << ")"
            << endl << endl;
        exit(1);
    }
  }

  if (basename.size() == 0) {
    cout << "Missing required argument --index-basename (-b)..." << endl;
    exit(1);
  }

  try {
    Engine engine;
    IndexMock index(basename);
    QueryExecutionContext qec(index, engine);

    if (query == "") {
      cout << "No query provided, switching to interactive mode.." << endl;
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
      processQuery(qec, query);
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
  LOG(INFO) << "Query is: \"" << query << "\"" << endl << endl << endl;
  SparqlParser sp;
  ParsedQuery pq = sp.parse(query);
  LOG(INFO) << "Parsed format:\n" << pq.asString() << endl;

  LOG(INFO) << "Creating an execution plan..." << endl;
  pq.expandPrefixes();

  QueryGraph qg(&qec);
  qg.createFromParsedQuery(pq);


  const QueryExecutionTree& qet = qg.getExecutionTree();
  LOG(INFO) << "Execution tree:\n" << qet.asString() << endl;

  const ResultTable& res = qet.getResult();
  LOG(INFO) << "Result Width: " << res._nofColumns << endl;
  LOG(INFO) << "As debug string: " << res.asDebugString() << endl;
  LOG(INFO)<< endl;


  LOG(INFO) << "Readable Result is: " << endl;
  size_t limit = MAX_NOF_ROWS_IN_RESULT;
  size_t offset = 0;
  if (pq._limit.size() > 0) {
     limit = static_cast<size_t>(atol(pq._limit.c_str()));
  }
  if (pq._offset.size() > 0) {
    offset = static_cast<size_t>(atol(pq._offset.c_str()));
  }
  qet.writeResultToStream(cout, pq._selectedVariables, limit, offset);
  LOG(INFO) << endl;
}
