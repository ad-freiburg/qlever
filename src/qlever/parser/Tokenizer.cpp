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

// ______________________________________________________________________________________________________
const RE2& Tokenizer::idToRegex(const TurtleTokenId reg) {
  switch (reg) {
    case TurtleTokenId::TurtlePrefix:
      return _tokens.TurtlePrefix;
    case TurtleTokenId::SparqlPrefix:
      return _tokens.SparqlPrefix;
    case TurtleTokenId::TurtleBase:
      return _tokens.TurtleBase;
    case TurtleTokenId::SparqlBase:
      return _tokens.SparqlBase;
    case TurtleTokenId::Dot:
      return _tokens.Dot;
    case TurtleTokenId::Comma:
      return _tokens.Comma;
    case TurtleTokenId::Semicolon:
      return _tokens.Semicolon;
    case TurtleTokenId::OpenSquared:
      return _tokens.OpenSquared;
    case TurtleTokenId::CloseSquared:
      return _tokens.CloseSquared;
    case TurtleTokenId::OpenRound:
      return _tokens.OpenRound;
    case TurtleTokenId::CloseRound:
      return _tokens.CloseRound;
    case TurtleTokenId::A:
      return _tokens.A;
    case TurtleTokenId::DoubleCircumflex:
      return _tokens.DoubleCircumflex;
    case TurtleTokenId::True:
      return _tokens.True;
    case TurtleTokenId::False:
      return _tokens.False;
    case TurtleTokenId::Langtag:
      return _tokens.Langtag;
    case TurtleTokenId::Decimal:
      return _tokens.Decimal;
    case TurtleTokenId::Exponent:
      return _tokens.Exponent;
    case TurtleTokenId::Double:
      return _tokens.Double;
    case TurtleTokenId::Iriref:
      return _tokens.Iriref;
    case TurtleTokenId::PnameNS:
      return _tokens.PnameNS;
    case TurtleTokenId::PnameLN:
      return _tokens.PnameLN;
    case TurtleTokenId::PnLocal:
      return _tokens.PnLocal;
    case TurtleTokenId::BlankNodeLabel:
      return _tokens.BlankNodeLabel;
    case TurtleTokenId::WsMultiple:
      return _tokens.WsMultiple;
    case TurtleTokenId::Anon:
      return _tokens.Anon;
    case TurtleTokenId::Comment:
      return _tokens.Comment;
    case TurtleTokenId::Integer:
      return _tokens.Integer;
  }
  throw std::runtime_error(
      "Illegal switch value in Tokenizer::getNextToken. This should never "
      "happen");
}
