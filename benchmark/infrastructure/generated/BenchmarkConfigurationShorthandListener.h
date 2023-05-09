
// Generated from BenchmarkConfigurationShorthand.g4 by ANTLR 4.12.0

#pragma once

#include "BenchmarkConfigurationShorthandParser.h"
#include "antlr4-runtime.h"

/**
 * This interface defines an abstract listener for a parse tree produced by
 * BenchmarkConfigurationShorthandParser.
 */
class BenchmarkConfigurationShorthandListener
    : public antlr4::tree::ParseTreeListener {
 public:
  virtual void enterShortHandString(
      BenchmarkConfigurationShorthandParser::ShortHandStringContext* ctx) = 0;
  virtual void exitShortHandString(
      BenchmarkConfigurationShorthandParser::ShortHandStringContext* ctx) = 0;

  virtual void enterAssignments(
      BenchmarkConfigurationShorthandParser::AssignmentsContext* ctx) = 0;
  virtual void exitAssignments(
      BenchmarkConfigurationShorthandParser::AssignmentsContext* ctx) = 0;

  virtual void enterAssignment(
      BenchmarkConfigurationShorthandParser::AssignmentContext* ctx) = 0;
  virtual void exitAssignment(
      BenchmarkConfigurationShorthandParser::AssignmentContext* ctx) = 0;

  virtual void enterObject(
      BenchmarkConfigurationShorthandParser::ObjectContext* ctx) = 0;
  virtual void exitObject(
      BenchmarkConfigurationShorthandParser::ObjectContext* ctx) = 0;

  virtual void enterList(
      BenchmarkConfigurationShorthandParser::ListContext* ctx) = 0;
  virtual void exitList(
      BenchmarkConfigurationShorthandParser::ListContext* ctx) = 0;

  virtual void enterContent(
      BenchmarkConfigurationShorthandParser::ContentContext* ctx) = 0;
  virtual void exitContent(
      BenchmarkConfigurationShorthandParser::ContentContext* ctx) = 0;
};
