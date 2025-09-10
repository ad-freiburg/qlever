// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach(joka921) <johannes.kalmbach@gmail.com>

#ifndef QLEVER_SRC_PARSER_TURTLETOKENID_H
#define QLEVER_SRC_PARSER_TURTLETOKENID_H

#include "backports/usingEnum.h"

/// One entry for each Token in the Turtle Grammar. Used to create a unified
/// Interface to the two different Tokenizers
QL_DEFINE_TYPED_ENUM(TurtleTokenId, int, TurtlePrefix, SparqlPrefix, TurtleBase,
                     SparqlBase, Dot, Comma, Semicolon, OpenSquared,
                     CloseSquared, OpenRound, CloseRound, A, DoubleCircumflex,
                     True, False, Langtag, Integer, Decimal, Exponent, Double,
                     Iriref, IrirefRelaxed, PnameNS, PnameLN, PnLocal,
                     BlankNodeLabel, WsMultiple, Anon, Comment);

#endif  // QLEVER_SRC_PARSER_TURTLETOKENID_H
