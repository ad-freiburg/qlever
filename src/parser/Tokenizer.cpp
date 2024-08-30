// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>

#include "./Tokenizer.h"

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

// ______________________________________________________________________________________________________
const RE2& Tokenizer::idToRegex(const TurtleTokenId reg) {
  switch (reg) {
    using enum TurtleTokenId;
    case TurtlePrefix:
      return _tokens.TurtlePrefix;
    case SparqlPrefix:
      return _tokens.SparqlPrefix;
    case TurtleBase:
      return _tokens.TurtleBase;
    case SparqlBase:
      return _tokens.SparqlBase;
    case Dot:
      return _tokens.Dot;
    case Comma:
      return _tokens.Comma;
    case Semicolon:
      return _tokens.Semicolon;
    case OpenSquared:
      return _tokens.OpenSquared;
    case CloseSquared:
      return _tokens.CloseSquared;
    case OpenRound:
      return _tokens.OpenRound;
    case CloseRound:
      return _tokens.CloseRound;
    case A:
      return _tokens.A;
    case DoubleCircumflex:
      return _tokens.DoubleCircumflex;
    case True:
      return _tokens.True;
    case False:
      return _tokens.False;
    case Langtag:
      return _tokens.Langtag;
    case Decimal:
      return _tokens.Decimal;
    case Exponent:
      return _tokens.Exponent;
    case Double:
      return _tokens.Double;
    case Iriref:
      return _tokens.Iriref;
    case IrirefRelaxed:
      return _tokens.IrirefRelaxed;
    case PnameNS:
      return _tokens.PnameNS;
    case PnameLN:
      return _tokens.PnameLN;
    case PnLocal:
      return _tokens.PnLocal;
    case BlankNodeLabel:
      return _tokens.BlankNodeLabel;
    case WsMultiple:
      return _tokens.WsMultiple;
    case Anon:
      return _tokens.Anon;
    case Comment:
      return _tokens.Comment;
    case Integer:
      return _tokens.Integer;
  }
  throw std::runtime_error(
      "Illegal switch value in Tokenizer::getNextToken. This should never "
      "happen");
}
