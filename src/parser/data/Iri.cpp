//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "parser/data/Iri.h"

#include "ctre/ctre.h"
// ____________________________________________________________________________
Iri::Iri(std::string str) : _string{std::move(str)} {
  AD_CONTRACT_CHECK(ctre::match<"(?:@[a-zA-Z]+(?:-(?:[a-zA-Z]|\\d)+)*@)?"
                                "<[^<>\"{}|^\\\\`\\0- ]*>">(_string));
}
