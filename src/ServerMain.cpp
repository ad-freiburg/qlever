// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <string>
#include <vector>
#include <iomanip>
#include <iostream>

#include "engine/Server.h"
#include "util/ReadableNumberFact.h"

using std::string;
using std::vector;
using std::cout;
using std::endl;
using std::flush;
using std::cerr;

#define EMPH_ON  "\033[1m"
#define EMPH_OFF "\033[22m"

// Available options.
struct option options[] = {
  {"index",            required_argument, NULL, 'i'},
  {"port",             required_argument, NULL, 'p'},
  {"text",             no_argument,       NULL, 't'},
  {"on-disk-literals", no_argument,       NULL, 'l'},
  {"all-permutations", no_argument,       NULL, 'a'},
  {"unopt-optional",   no_argument,       NULL, 'u'},
  {"help",             no_argument,       NULL, 'h'},
  {"worker-threads",   required_argument, NULL, 'j'},
  {NULL,               0,                 NULL, 0}
};

void printUsage(char *execName) {
  std::ios coutState(nullptr);
  coutState.copyfmt(cout);
  cout << std::setfill(' ') << std::left;

  cout << "Usage: " << execName << " -p <PORT> -i <index> [OPTIONS]"
       << endl << endl;
  cout << "Options" << endl;
  cout << "  " << std::setw(20) << "a, all-permutations" << std::setw(1)
       << "    "
       << "Load all six permuations of the index instead of only two." << endl;
  cout << "  " << std::setw(20) << "h, help" << std::setw(1) << "    "
       << "Show this help and exit." << endl;
  cout << "  " << std::setw(20) << "i, index" << std::setw(1) << "    "
       << "The location of the index files." << endl;
  cout << "  " << std::setw(20) << "l, on-disk-literals" << std::setw(1)
       << "    "
       << "Indicates that the literals can be found on disk with the index."
       << endl;
  cout << "  " << std::setw(20) << "p, port" << std::setw(1) << "    "
       << "The port on which to run the web interface." << endl;
  cout << "  " << std::setw(20) << "t, text" << std::setw(1) << "    "
       << "Enables the usage of text." << endl;
  cout << "  " << std::setw(20) << "j, worker-threads" << std::setw(1) << "    "
       << "Sets the number of worker threads to use" << endl;
  cout << "  " << std::setw(20) << "u, unopt-optional" << std::setw(1) << "    "
       << "Always place optional joins at the root of the query execution tree."
       << endl;
  cout.copyfmt(coutState);
}

// Main function.
int main(int argc, char** argv) {
  char* locale = setlocale(LC_CTYPE, "en_US.utf8");

  std::locale loc;
  ad_utility::ReadableNumberFacet facet(1);
  std::locale locWithNumberGrouping(loc, &facet);
  ad_utility::Log::imbue(locWithNumberGrouping);


  // Init variables that may or may not be
  // filled / set depending on the options.
  string index = "";
  bool text = false;
  bool onDiskLiterals = false;
  bool allPermutations = false;
  bool optimizeOptionals = true;
  int port = -1;
  int numThreads = 1;

  optind = 1;
  // Process command line arguments.
  while (true) {
    int c = getopt_long(argc, argv, "i:p:j:tlauh", options, NULL);
    if (c == -1) break;
    switch (c) {
      case 'i':
        index = optarg;
        break;
      case 'p':
        port = atoi(optarg);
        break;
      case 't':
        text = true;
        break;
      case 'l':
        onDiskLiterals = true;
        break;
      case 'a':
        allPermutations = true;
        break;
      case 'j':
        numThreads = atoi(optarg);
        break;
      case 'u':
        optimizeOptionals = false;
        break;
      case 'h':
        printUsage(argv[0]);
        exit(0);
        break;
      default:
        cout << endl
             << "! ERROR in processing options (getopt returned '" << c
             << "' = 0x" << std::setbase(16) << static_cast<int> (c) << ")"
             << endl << endl;
        printUsage(argv[0]);
        exit(1);
    }
  }

  if (index.size() == 0 || port == -1) {
    if (index.size() == 0) {
      cerr << "ERROR: No index specified, but an index is required." << endl;
    }
    if (port == -1) {
      cerr << "ERROR: No port specified, but the port is required." << endl;
    }
    printUsage(argv[0]);
    exit(1);
  }

  cout << endl << EMPH_ON << "ServerMain, version " << __DATE__
       << " " << __TIME__<< EMPH_OFF << endl << endl;
  cout << "Set locale LC_CTYPE to: " << locale << endl;

  try {
    Server server(port, numThreads);
    server.initialize(index, text, allPermutations, onDiskLiterals,
                      optimizeOptionals);
    server.run();
  } catch(const ad_semsearch::Exception& e) {
    LOG(ERROR) << e.getFullErrorMessage() << '\n';
    return 1;
  }
  return 0;
}
