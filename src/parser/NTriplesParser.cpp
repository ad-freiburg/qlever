// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "./NTriplesParser.h"
#include <cassert>
#include <iostream>
#include "../util/Exception.h"
#include "../util/Log.h"

using ad_utility::lstrip;

// _____________________________________________________________________________
NTriplesParser::NTriplesParser(const string& file) : _in(file) {}

// _____________________________________________________________________________
NTriplesParser::~NTriplesParser() { _in.close(); }

// _____________________________________________________________________________
bool NTriplesParser::getLine(array<string, 3>& res) {
  string line;
  if (std::getline(_in, line)) {
    size_t i = 0;
    while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) {
      ++i;
    }
    if (i >= line.size()) {
      AD_THROW(ad_semsearch::Exception::BAD_INPUT,
               "Illegal whitespace only line");
    }
    size_t j = i + 1;
    while (j < line.size() && line[j] != '\t' && line[j] != ' ') {
      ++j;
    }
    if (j >= line.size() ||
        !((line[i] == '<' && line[j - 1] == '>') ||
          (i + 1 < line.size() && line[i] == '_' && line[i + 1] == ':'))) {
      AD_THROW(ad_semsearch::Exception::BAD_INPUT, "Illegal URI in : " + line);
    }
    res[0] = line.substr(i, j - i);
    i = j;
    while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) {
      ++i;
    }
    j = i + 1;
    while (j < line.size() && line[j] != '\t' && line[j] != ' ') {
      ++j;
    }
    if (j >= line.size() || !(line[i] == '<' && line[j - 1] == '>')) {
      AD_THROW(ad_semsearch::Exception::BAD_INPUT, "Illegal URI in : " + line);
    }
    res[1] = line.substr(i, j - i);
    i = j;
    while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) {
      ++i;
    }
    if (i >= line.size()) {
      AD_THROW(ad_semsearch::Exception::BAD_INPUT,
               "Object not followed by space in : " + line);
    }
    if (line[i] == '<') {
      // URI
      j = line.find('>', i + 1);
      if (j == string::npos) {
        AD_THROW(ad_semsearch::Exception::BAD_INPUT,
                 "Illegal URI in : " + line);
      }
      ++j;
    } else if (line[i] == '_' && i + 1 < line.size() && line[i + 1] == ':') {
      // Blank node
      j = i + 1;
      while (j < line.size() && line[j] != '\t' && line[j] != ' ') {
        ++j;
      }
    } else {
      // Literal
      j = line.find('\"', i + 1);
      bool escape = false;
      while (j < line.size()) {
        if (!escape && line[j] == '\\') {
          escape = true;
        } else if (!escape && line[j] == '\"') {
          break;
        } else {
          escape = false;
        }
        ++j;
      }

      if (j == line.size()) {
        AD_THROW(ad_semsearch::Exception::BAD_INPUT,
                 "Illegal literal in : " + line);
      }
      ++j;
      while (j < line.size() && line[j] != ' ' && line[j] != '\t') {
        ++j;
      }
    }
    if (j >= line.size() || !(line[j] == ' ' || line[j] == '\t')) {
      AD_THROW(ad_semsearch::Exception::BAD_INPUT,
               "Object not followed by space in : " + line);
    }
    res[2] = line.substr(i, j - i);
    return true;
  }
  return false;
}
