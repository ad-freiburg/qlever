// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)
#pragma once

#include <antlr4-runtime.h>

#include "../benchmark/infrastructure/generated/BenchmarkConfigurationShorthandVisitor.h"

/*
This visitor will translate the parsed short hand into a `nlohmann::json`
object.
*/
class ToJsonBenchmarkConfigurationShorthandVisitor final
    : public BenchmarkConfigurationShorthandVisitor {
 public:
  std::any visitShortHandString(
      BenchmarkConfigurationShorthandParser::ShortHandStringContext* context)
      final;

  std::any visitAssignments(
      BenchmarkConfigurationShorthandParser::AssignmentsContext* context) final;

  std::any visitAssignment(
      BenchmarkConfigurationShorthandParser::AssignmentContext* context) final;

  std::any visitObject(
      BenchmarkConfigurationShorthandParser::ObjectContext* context) final;

  std::any visitList(
      BenchmarkConfigurationShorthandParser::ListContext* context) final;

  std::any visitContent(
      BenchmarkConfigurationShorthandParser::ContentContext* context) final;
};
