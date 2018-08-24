// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>

#include "./Tokenizer.h"

// ______________________________________________________
void Tokenizer::skipWhitespaceAndComments() {
  skipWhitespace();
  skipComments();
  skipWhitespace();
}

// _______________________________________________________
std::pair<bool, std::string> Tokenizer::getNextToken(const RE2& reg) {
  std::string match = "";
  bool success = RE2::Consume(&_data, reg, &match);
  if (!success) {
    return {false, {}};
  } else {
    return {true, match};
  }
}

// _______________________________________________________
std::tuple<bool, size_t, std::string> Tokenizer::getNextToken(
    const std::vector<const RE2*>& regs) {
  // TODO<joka921> : write unit tests for this Overload!!
  size_t maxMatchSize = 0;
  size_t maxMatchIndex = 0;
  std::string maxMatch;
  bool success = false;

  // we have to remember the current position of data so we can rewind after a
  // successful match
  const char* beg = _data.begin();
  size_t dataSize = _data.size();

  for (size_t i = 0; i < regs.size(); i++) {
    auto [tmpSuccess, tmpMatch] = getNextToken(*(regs[i]));
    if (tmpSuccess && tmpMatch.size() > maxMatchSize) {
      success = true;
      maxMatchSize = tmpMatch.size();
      maxMatchIndex = i;
      maxMatch = std::move(tmpMatch);
      reset(beg, dataSize);
    }
  }
  // advance for the length of the longest match
  reset(beg + maxMatchSize, dataSize - maxMatchSize);
  return {success, maxMatchIndex, maxMatch};
}
