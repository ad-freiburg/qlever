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
    {"index", required_argument, NULL, 'i'},
    {NULL, 0, NULL, 0}
};

// Main function.
int main(int argc, char **argv) {
  cout.sync_with_stdio(false);
  std::cout << std::endl << EMPH_ON
      << "WriteIndexListsMain, version " << __DATE__
      << " " << __TIME__ << EMPH_OFF << std::endl << std::endl;

  char* locale = setlocale(LC_CTYPE, "en_US.utf8");
  cout << "Set locale LC_CTYPE to: " << locale << endl;


  std::locale loc;
  ad_utility::ReadableNumberFacet facet(1);
  std::locale locWithNumberGrouping(loc, &facet);
  ad_utility::Log::imbue(locWithNumberGrouping);

  string indexName = "";

  optind = 1;
  // Process command line arguments.
  while (true) {
    int c = getopt_long(argc, argv, "i:", options, NULL);
    if (c == -1) break;
    switch (c) {
      case 'i':
        indexName = optarg;
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
    Index index;
    index.createFromOnDiskIndex(indexName);
    index.addTextFromOnDiskIndex();

    index.dumpAsciiLists();

  } catch (const std::exception &e) {
    cout << string("Caught exceptions: ") + e.what();
    return 1;
  } catch (ad_semsearch::Exception &e) {
    cout << e.getFullErrorMessage() << std::endl;
  }

  return 0;
}
