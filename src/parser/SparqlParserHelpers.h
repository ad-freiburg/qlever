//
// Created by johannes on 16.05.21.
//

#ifndef QLEVER_SPARQLPARSERHELPERS_H
#define QLEVER_SPARQLPARSERHELPERS_H

#include <string>

namespace sparqlParserHelpers {

template<typename ParseResult>
struct ParseResultAndRemainingText {
  ParseResult _parseResult;
  std::string _remainingText;
};


}

#endif  // QLEVER_SPARQLPARSERHELPERS_H
