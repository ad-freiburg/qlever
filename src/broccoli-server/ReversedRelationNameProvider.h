// Copyright 2013, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold <buchholb>

#ifndef GLOBALS_REVERSEDRELATIONNAMEPROVIDER_H_
#define GLOBALS_REVERSEDRELATIONNAMEPROVIDER_H_

#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <unordered_map>
#include "./Globals.h"
#include "./Conversions.h"

using std::string;
using std::vector;

namespace ad_semsearch {
//! Gets the reversed name of a relation. Take care to use the
//! same map when building the index and running the server.
class ReversedRelationNameProvider {
  public:
  ReversedRelationNameProvider() { init(); }

  explicit ReversedRelationNameProvider(const string& mappingFileName) {
    initFromFile(mappingFileName);
  }

  //! Get the reversed name for the given realtion.
  string reverseRelation(const string& orig) const {
    auto it = _nameMap.find(orig);
    if (it != _nameMap.end()) { return it->second; }
    if (ad_utility::endsWith(orig, REVERSED_RELATION_SUFFIX)) {
      return orig.substr(0, orig.size()
          - std::char_traits<char>::length(REVERSED_RELATION_SUFFIX));
    } else {
      return orig + REVERSED_RELATION_SUFFIX;
    }
  }

  void initFromFile(const string& fileName) {
    init();
    std::ifstream in(fileName.c_str(), std::ios_base::in);
    string line;
    // Extract all pairs in both directions
    vector<string> subjects;
    vector<string> objects;
    while (std::getline(in, line)) {
      vector<string> cols;
      splitTurtleUncheckedButFast(line, &cols);
      string r1 = predicateToBroccoliStyle(cols[0]);
      string r2 = predicateToBroccoliStyle(cols[2]);
      subjects.push_back(r1);
      objects.push_back(r2);
      subjects.push_back(r2);
      objects.push_back(r1);
    }
    // Sort a copy of the subjects to recognize duplicates.
    vector<string> cpy(subjects);
    std::unordered_set<string> dontUse;
    std::sort(cpy.begin(), cpy.end());
    string last;
    for (size_t i = 0; i < cpy.size(); ++i) {
      if (cpy[i] == last) { dontUse.insert(cpy[i]); }
      last = cpy[i];
    }
    // Add every to the map that is not part of the don't use list.
    assert(subjects.size() == objects.size());
    for (size_t i = 0; i < subjects.size(); ++i) {
      if (dontUse.count(subjects[i]) == 0 && dontUse.count(objects[i]) == 0) {
        _nameMap[subjects[i]] = objects[i];
      }
    }
  }

  private:
  std::unordered_map<string, string> _nameMap;
  void init() {
    _nameMap.clear();
    _nameMap[string(RELATION_PREFIX) + OCCURS_IN_RELATION] = string(
        RELATION_PREFIX) + HAS_OCCURRENCE_OF_RELATION;
    _nameMap[string(RELATION_PREFIX) + HAS_OCCURRENCE_OF_RELATION] = string(
        RELATION_PREFIX) + OCCURS_IN_RELATION;
    _nameMap[string(RELATION_PREFIX) + OCCURS_WITH_RELATION] = string(
        RELATION_PREFIX) + OCCURS_WITH_RELATION;
  }
};
}

#endif  // GLOBALS_REVERSEDRELATIONNAMEPROVIDER_H_
