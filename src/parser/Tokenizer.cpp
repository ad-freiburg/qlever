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
const RE2& Tokenizer::idToRegex(const TokId reg) {
  switch (reg) {
    case TokId::TurtlePrefix:
      return _tokens.TurtlePrefix;
    case TokId::SparqlPrefix:
      return _tokens.SparqlPrefix;
    case TokId::TurtleBase:
      return _tokens.TurtleBase;
    case TokId::SparqlBase:
      return _tokens.SparqlBase;
    case TokId::Dot:
      return _tokens.Dot;
    case TokId::Comma:
      return _tokens.Comma;
    case TokId::Semicolon:
      return _tokens.Semicolon;
    case TokId::OpenSquared:
      return _tokens.OpenSquared;
    case TokId::CloseSquared:
      return _tokens.CloseSquared;
    case TokId::OpenRound:
      return _tokens.OpenRound;
    case TokId::CloseRound:
      return _tokens.CloseRound;
    case TokId::A:
      return _tokens.A;
    case TokId::DoubleCircumflex:
      return _tokens.DoubleCircumflex;
    case TokId::True:
      return _tokens.True;
    case TokId::False:
      return _tokens.False;
    case TokId::Langtag:
      return _tokens.Langtag;
    case TokId::Decimal:
      return _tokens.Decimal;
    case TokId::Exponent:
      return _tokens.Exponent;
    case TokId::Double:
      return _tokens.Double;
    case TokId::Iriref:
      return _tokens.Iriref;
    case TokId::PnameNS:
      return _tokens.PnameNS;
    case TokId::PnameLN:
      return _tokens.PnameLN;
    case TokId::PnLocal:
      return _tokens.PnLocal;
    case TokId::BlankNodeLabel:
      return _tokens.BlankNodeLabel;
    case TokId::WsMultiple:
      return _tokens.WsMultiple;
    case TokId::Anon:
      return _tokens.Anon;
    case TokId::Comment:
      return _tokens.Comment;
    case TokId::Integer:
      return _tokens.Integer;
  }
  throw std::runtime_error(
      "Illegal switch value in Tokenizer::getNextToken. This should never "
      "happen");
}
