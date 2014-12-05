// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <stdlib.h>
#include <getopt.h>
#include <string>
#include <iomanip>
#include <iostream>

#include "parser/SparqlParser.h"

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
    {NULL, 0, NULL, 0}
};

// Main function.
int main(int argc, char **argv) {
  std::cout << std::endl << EMPH_ON
      << "SparqlParsersMain, version " << __DATE__
      << " " << __TIME__ << EMPH_OFF << std::endl << std::endl;

  string query;

  optind = 1;
  // Process command line arguments.
  while (true) {
    int c = getopt_long(argc, argv, "q:", options, NULL);
    if (c == -1) break;
    switch (c) {
      case 'q':
        query = optarg;
        break;
      default:
        cout << endl
            << "! ERROR in processing options (getopt returned '" << c
            << "' = 0x" << std::setbase(16) << c << ")"
            << endl << endl;
        exit(1);
    }
  }

  try {
    cout << "Query is: \"" << query << "\"" << endl << endl << endl;
    SparqlParser sp;
    ParsedQuery pq = sp.parse(query);
    cout << "Parsed format:\n" << pq.asString() << endl;
  } catch (const std::exception& e) {
    cout << string("Caught exceptions: ") + e.what();
    return 1;
  }

  return 0;
}
