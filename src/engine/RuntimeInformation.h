// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@neptun.uni-freiburg.de)
//
#pragma once

#include <iostream>
#include <string>
#include <vector>

#include "../util/HashMap.h"
#include "../util/Timer.h"

class RuntimeInformation {
 public:
  RuntimeInformation()
      : _descriptor(), _details(), _time(0), _wasCached(false), _children() {}
  void toJson(std::ostream& out) const {
    out << "{";
    out << "\"description\" : \"" << _descriptor << "\",";
    out << "\"time\" : " << _time << ", ";
    out << "\"was_chached\" : " << _wasCached << ", ";
    out << "\"details\" : {";
    auto it = _details.begin();
    while (it != _details.end()) {
      out << "\"" << it->first << "\" : \"" << it->second << "\"";
      ++it;
      if (it != _details.end()) {
        out << ", ";
      }
    }
    out << "}, ";
    out << "\"children\" : [";
    auto it2 = _children.begin();
    while (it2 != _children.end()) {
      // recursively call the childs to json method
      it2->toJson(out);
      ++it2;
      if (it2 != _children.end()) {
        out << ", ";
      }
    }
    out << "]";
    out << "}";
  }

  std::string toString() const {
    std::ostringstream buffer;
    toString(buffer, 0);
    return buffer.str();
  }

  void toString(std::ostream& out, size_t indent) const {
    out << std::string(indent * 2, ' ') << _descriptor << std::endl;
    out << std::string(indent * 2, ' ') << "time: " << _time << "s"
        << std::endl;
    out << std::string(indent * 2, ' ') << "cached: " << _wasCached
        << std::endl;
    for (auto detail : _details) {
      out << std::string((indent + 2) * 2, ' ') << detail.first << ", "
          << detail.second << std::endl;
    }
    for (const RuntimeInformation& child : _children) {
      child.toString(out, indent + 1);
    }
  }

  void setDescriptor(const std::string& descriptor) {
    _descriptor = descriptor;
  }

  void setTime(double time) { _time = time; }
  double getTime() const { return _time; }

  void setWasCached(bool wasCached) { _wasCached = wasCached; }

  void addChild(const RuntimeInformation& r) { _children.push_back(r); }

  void addDetail(const std::string& key, const std::string& value) {
    _details[key] = value;
  }

 private:
  std::string _descriptor;
  ad_utility::HashMap<std::string, std::string> _details;
  double _time;
  bool _wasCached;
  std::vector<RuntimeInformation> _children;
};
