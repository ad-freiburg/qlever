// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)
#pragma once

#include <antlr4-runtime.h>

#include "../benchmark/infrastructure/generated/BenchmarkConfigurationShorthandParser.h"
#include "util/json.h"

/*
This visitor will translate the parsed short hand into a `nlohmann::json`
object.
*/
class ToJsonBenchmarkConfigurationShorthandVisitor final {
 public:
  using Parser = BenchmarkConfigurationShorthandParser;

  nlohmann::json visitShortHandString(Parser::ShortHandStringContext* context);

  nlohmann::json visitAssignments(Parser::AssignmentsContext* context);

  nlohmann::json visitAssignment(Parser::AssignmentContext* context);

  nlohmann::json visitObject(Parser::ObjectContext* context);

  nlohmann::json visitList(Parser::ListContext* context);

  nlohmann::json visitContent(Parser::ContentContext* context);
};
