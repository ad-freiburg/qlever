//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#include "util/antlr/ANTLRErrorHandling.h"

namespace ad_utility::antlr_utility::detail {

std::string generateExceptionMessage(antlr4::Token* offendingSymbol,
                                     const std::string& msg) {
  if (!offendingSymbol) {
    return msg;
  } else if (offendingSymbol->getStartIndex() ==
             offendingSymbol->getStopIndex() + 1) {
    // This can only happen at the end of the query when a token is expected,
    // but none is found. The offending token is then empty.
    return absl::StrCat("Unexpected end of input: ", msg);
  } else {
    return absl::StrCat("Token \"", offendingSymbol->getText(), "\": ", msg);
  }
}
}  // namespace ad_utility::antlr_utility::detail
