// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#include <ANTLRInputStream.h>
#include <CommonTokenStream.h>
#include <absl/strings/str_cat.h>
#include <antlr4-runtime.h>

#include "util/ConfigManager/ConfigShorthandVisitor.h"
#include "util/ConfigManager/ConfigUtil.h"
#include "util/ConfigManager/generated/ConfigShorthandLexer.h"
#include "util/ConfigManager/generated/ConfigShorthandParser.h"

namespace ad_utility {
// ____________________________________________________________________________
bool isNameInShortHand(std::string_view str) {
  /*
  The first version was a more general help function, that used the ANTLR
  tokenstream and token ids to compare a string on correctness. Unfortunaly,
  ANTLR 4.13.0 seems to have at least one undeterministic bug, which caused a
  seg fault when used in our main function, but not in our test cases.
  We switched to checking with a special parser rule, after two days of
  headaches and running into the same bug again, and again, regardless of how we
  tried to check the tokens. (We worked our way THROUGH EVERY usable layer.)
  */
  antlr4::ANTLRInputStream input(str);
  ConfigShorthandLexer lexer(&input);
  antlr4::CommonTokenStream tokens(&lexer);
  ConfigShorthandParser parser(&tokens);

  /*
  Parse for the rule `isName`. If we got a valid version, we will have no syntax
  errors.
  */
  return parser.isName() && parser.getNumberOfSyntaxErrors() == 0;
}
}  // namespace ad_utility
