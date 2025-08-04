//  Copyright 2022-2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SPARQLPARSERHELPERS_H
#define QLEVER_SPARQLPARSERHELPERS_H

#include <memory>
#include <string>

#include "parser/ParsedQuery.h"
#include "parser/ParserAndVisitorBase.h"
#include "sparqlParser/SparqlQleverVisitor.h"
#include "util/BlankNodeManager.h"

namespace sparqlParserHelpers {
// The actual `ParserAndVisitor` class that can be used to fully parse SPARQL
// using the automatically generated parser + the manually written
// `SparqlQLeverVisitor`.
struct ParserAndVisitor : public ParserAndVisitorBase<SparqlQleverVisitor> {
 private:
  // Unescapes unicode sequences like \U01234567 and \u0123 in the input string
  // before beginning with actual parsing as the SPARQL standard mandates.
  static std::string unescapeUnicodeSequences(std::string input);

  using Base = ParserAndVisitorBase<SparqlQleverVisitor>;

 public:
  ParserAndVisitor(
      ad_utility::BlankNodeManager* blankNodeManager,
      const EncodedIriManager* encodedIriManager, std::string input,
      std::optional<ParsedQuery::DatasetClauses> datasetClauses = std::nullopt,
      SparqlQleverVisitor::DisableSomeChecksOnlyForTesting disableSomeChecks =
          SparqlQleverVisitor::DisableSomeChecksOnlyForTesting::False);
  ParserAndVisitor(
      ad_utility::BlankNodeManager* blankNodeManager,
      const EncodedIriManager* encodedIriManager, std::string input,
      SparqlQleverVisitor::PrefixMap prefixes,
      std::optional<ParsedQuery::DatasetClauses> datasetClauses = std::nullopt,
      SparqlQleverVisitor::DisableSomeChecksOnlyForTesting disableSomeChecks =
          SparqlQleverVisitor::DisableSomeChecksOnlyForTesting::False);
};
}  // namespace sparqlParserHelpers

#endif  // QLEVER_SPARQLPARSERHELPERS_H
