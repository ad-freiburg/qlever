
// Generated from BenchmarkConfigurationShorthand.g4 by ANTLR 4.12.0

#pragma once

#include "BenchmarkConfigurationShorthandParser.h"
#include "antlr4-runtime.h"

/**
 * This class defines an abstract visitor for a parse tree
 * produced by BenchmarkConfigurationShorthandParser.
 */
class BenchmarkConfigurationShorthandVisitor
    : public antlr4::tree::AbstractParseTreeVisitor {
 public:
  /**
   * Visit parse trees produced by BenchmarkConfigurationShorthandParser.
   */
  virtual std::any visitShortHandString(
      BenchmarkConfigurationShorthandParser::ShortHandStringContext*
          context) = 0;

  virtual std::any visitAssignments(
      BenchmarkConfigurationShorthandParser::AssignmentsContext* context) = 0;

  virtual std::any visitAssignment(
      BenchmarkConfigurationShorthandParser::AssignmentContext* context) = 0;

  virtual std::any visitObject(
      BenchmarkConfigurationShorthandParser::ObjectContext* context) = 0;

  virtual std::any visitList(
      BenchmarkConfigurationShorthandParser::ListContext* context) = 0;

  virtual std::any visitContent(
      BenchmarkConfigurationShorthandParser::ContentContext* context) = 0;
};
