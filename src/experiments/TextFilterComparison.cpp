// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@cs.uni-freiburg.de)

#include <stdlib.h>
#include <getopt.h>
#include <string>
#include <iomanip>
#include <iostream>

#include "../util/ReadableNumberFact.h"
#include "../util/Log.h"
#include "../util/Timer.h"
#include "../engine/Engine.h"
#include "../index/Index.h"
#include "../engine/QueryExecutionContext.h"

using std::string;
using std::cout;
using std::endl;
using std::flush;
using std::cerr;

#define EMPH_ON  "\033[1m"
#define EMPH_OFF "\033[21m"

// Available options.
struct option options[] = {
    {"index",              required_argument, NULL, 'i'},
    {"n",                  required_argument, NULL, 'n'},
    {"k",                  required_argument, NULL, 'k'},
    {"one-class-output",   required_argument, NULL, '1'},
    {"one-classes-output", required_argument, NULL, '2'},
    {NULL, 0,                                 NULL, 0}
};

// Idea of this experiment:
// Find decent factors for text operations with and without filters.
// The experiment does this:
// 1. Pick a single word and
//    a) a single class
//    b) two classes.
//    The picking is according to frequency.
// 2. Create the two (always use the smallest class as filter) trees and execute
//    them k times in an alternating way. Take average times.
// 3. Report both times, and the sizes of the two/three involved lists
//    and the estimates for them. Do that n times.
// 4. A script can use this data to derive further stats
//    (ratios between sizes, visualizations, etc)

void doOneClassExperiment(QueryExecutionContext& context,
                          const vector<std::pair<size_t, Id>>& availableClasses,
                          const vector<std::pair<size_t, Id>>& availableWords,
                          size_t n, size_t k,
                          string basic_string);

void doTwoClassesExperiment(QueryExecutionContext& context,
                            const vector<std::pair<size_t, Id>>& availableClasses,
                            const vector<std::pair<size_t, Id>>& availableWords,
                            size_t n, size_t k,
                            string basic_string);

vector<pair<size_t, Id>> getAvailableClasses(QueryExecutionContext& context);

vector<pair<size_t, Id>> getAvailableWords(QueryExecutionContext& context);

// Main function.
int main(int argc, char** argv) {
  cout.sync_with_stdio(false);
  std::cout << std::endl << EMPH_ON
            << "TextFilterComparison, version " << __DATE__
            << " " << __TIME__ << EMPH_OFF << std::endl << std::endl;

  char* locale = setlocale(LC_CTYPE, "en_US.utf8");
  cout << "Set locale LC_CTYPE to: " << locale << endl;


  std::locale loc;
  ad_utility::ReadableNumberFacet facet(1);
  std::locale locWithNumberGrouping(loc, &facet);
  ad_utility::Log::imbue(locWithNumberGrouping);

  string indexName = "";
  string oneClassOutput = "";
  string twoClassesOutput = "";
  size_t n = 0;
  size_t k = 0;
  bool text = false;
  bool interactive = false;

  optind = 1;
  // Process command line arguments.
  while (true) {
    int c = getopt_long(argc, argv, "i:n:k:1:2:", options, NULL);
    if (c == -1) break;
    switch (c) {
      case 'i':
        indexName = optarg;
        break;
      case '1':
        oneClassOutput = optarg;
        break;
      case '2':
        twoClassesOutput = optarg;
        break;
      case 'n':
        n = size_t(atoi(optarg));
        break;
      case 'k':
        k = size_t(atoi(optarg));
        break;
      default:
        cout << endl
             << "! ERROR in processing options (getopt returned '" << c
             << "' = 0x" << std::setbase(16) << c << ")"
             << endl << endl;
        exit(1);
    }
  }

  // Do perform the experiment.
  ad_utility::Timer t;
  Engine engine;
  Index index;
  index.createFromOnDiskIndex(indexName);
  index.addTextFromOnDiskIndex();
  QueryExecutionContext qec(index, engine);


  // Get available classes and words together with their frequency
  vector<std::pair<size_t, Id>> availableClasses = getAvailableClasses(qec);
  vector<std::pair<size_t, Id>> availableWords = getAvailableWords(qec);

  // Sort by frequency
  std::sort(availableClasses.begin(), availableClasses.end());
  std::sort(availableWords.begin(), availableWords.end());

  doOneClassExperiment(qec, availableClasses, availableWords, n, k,
                       oneClassOutput);
  doTwoClassesExperiment(qec, availableClasses, availableWords, n, k,
                         twoClassesOutput);
}

// _____________________________________________________________________________
vector<pair<size_t, Id>> getAvailableWords(QueryExecutionContext& context) {
  for (size_t i = 0; i < context.getIndex())
}

// _____________________________________________________________________________
vector<pair<size_t, Id>> getAvailableClasses(QueryExecutionContext& context) {

}

// _____________________________________________________________________________
void doOneClassExperiment(QueryExecutionContext& qec,
                          const vector<std::pair<size_t, Id>>& availableClasses,
                          const vector<std::pair<size_t, Id>>& availableWords,
                          size_t n, size_t k,
                          const string& outfile) {

}

// _____________________________________________________________________________
void doTwoClassesExperiment(QueryExecutionContext& qec,
                            const vector<std::pair<size_t, Id>>& availableClasses,
                            const vector<std::pair<size_t, Id>>& availableWords,
                            size_t n, size_t k,
                            const string& outfile) {

}
