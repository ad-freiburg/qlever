// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

#pragma once

#include <ANTLRInputStream.h>
#include <CommonTokenStream.h>
#include <Lexer.h>

#include <concepts>
#include <string_view>
#include <vector>

#include "util/Algorithm.h"
#include "util/json.h"

/*
@brief Check, if the given string, interpreted with the given lexer, only
contains tokens with same token indexes and in the same order, as the given
indexes.

@param str A string, which content will be transformed into tokens with the
given lexer.
@param tokenTypeIds In `anltr4` every token type has it's own unique index for
identfication. You should find yours as an enum inside your lexer class.

IMPORTANT: Remember, the highest possible lexer rule will always be used by the
lexer to generate a token. That INCLUDES rules, that are just `or` collections
of other lexer rules.
For example: Let's say, those are all of your lexer rules:
```
LITERAL : BOOL | INTEGER;
BOOL : 'true' | 'false';
INTEGER : '-'?[0-9]+;
```
With this grammar, you will NEVER get a token of type `BOOL`, or `INTEGER`,
because `LITERAL` has higher priority and includes all the cases of the other
two.
Input example:
Given the input `true -48`, the lexer would start with trying to find matches
for lexer rule with the highest priority, generate the tokens, and then repeat,
following the order from the lexer rule with the highest priority, to the lexer
rule with the lowes priority.
Priority of the lexer rules is dicated by the order, there are written in the
grammar. In our example that order would be `LITERAL`, `BOOL`, and then
`INTEGER`.
Now, `true -48` could be seen as the tokens `BOOL INTEGER`, but, because the
lexer first looks at `LITERAL`, which is a superset of the two other lexer
rules, they are seen as `LITERAL LITERAL`. After that, it looks for matches for
`BOOL`, but only finds two tokens of `LITERAL`. No `BOOL` tokens are generated.
Same for `INTEGER` after him.
*/
template <std::derived_from<antlr4::Lexer> Lexer>
inline bool stringOnlyContainsSpecifiedTokens(
    std::string_view str, std::vector<size_t> tokenTypeIds) {
  // Create the token stream, with which can read out the generated tokens.
  antlr4::ANTLRInputStream inputStream(str);
  Lexer lexer(&inputStream);
  antlr4::CommonTokenStream tokens(&lexer);

  // Read out all the tokens and put them into the buffer for access. Without
  // this, none of the getter return anything.
  tokens.fill();

  /*
  Get the type of ALL the tokens, that are found in the token stream.
  NOTE: The getter range is INCLUSIVE and the last element in the stream is
  ALWAYS EOF.
  */
  const std::vector<size_t>& allTokenTypeIndexes = ad_utility::transform(
      tokens.get(0, tokens.size() - 2),
      [](const antlr4::Token* token) { return token->getType(); });

  // Is the vecotr of wanted ids equal to the vector of actual ids?
  return tokenTypeIds == allTokenTypeIndexes;
}
