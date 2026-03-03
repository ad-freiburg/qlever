// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_PARSER_TURTLETOKENID_H
#define QLEVER_SRC_PARSER_TURTLETOKENID_H

/// One entry for each Token in the Turtle Grammar. Used to create a unified
/// Interface to the two different Tokenizers
enum class TurtleTokenId : int {
  TurtlePrefix,
  SparqlPrefix,
  TurtleBase,
  SparqlBase,
  Dot,
  Comma,
  Semicolon,
  OpenSquared,
  CloseSquared,
  OpenRound,
  CloseRound,
  A,
  DoubleCircumflex,
  True,
  False,
  Langtag,
  Integer,
  Decimal,
  Exponent,
  Double,
  Iriref,
  IrirefRelaxed,
  PnameNS,
  PnameLN,
  PnLocal,
  BlankNodeLabel,
  WsMultiple,
  Anon,
  Comment
};

#endif  // QLEVER_SRC_PARSER_TURTLETOKENID_H
