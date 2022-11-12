
// Generated from AcceptHeader.g4 by ANTLR 4.9.2

#pragma once

#include "AcceptHeaderParser.h"
#include "antlr4-runtime.h"

/**
 * This interface defines an abstract listener for a parse tree produced by
 * AcceptHeaderParser.
 */
class AcceptHeaderListener : public antlr4::tree::ParseTreeListener {
 public:
  virtual void enterAccept(AcceptHeaderParser::AcceptContext* ctx) = 0;
  virtual void exitAccept(AcceptHeaderParser::AcceptContext* ctx) = 0;

  virtual void enterAcceptWithEof(
      AcceptHeaderParser::AcceptWithEofContext* ctx) = 0;
  virtual void exitAcceptWithEof(
      AcceptHeaderParser::AcceptWithEofContext* ctx) = 0;

  virtual void enterRangeAndParams(
      AcceptHeaderParser::RangeAndParamsContext* ctx) = 0;
  virtual void exitRangeAndParams(
      AcceptHeaderParser::RangeAndParamsContext* ctx) = 0;

  virtual void enterMediaRange(AcceptHeaderParser::MediaRangeContext* ctx) = 0;
  virtual void exitMediaRange(AcceptHeaderParser::MediaRangeContext* ctx) = 0;

  virtual void enterType(AcceptHeaderParser::TypeContext* ctx) = 0;
  virtual void exitType(AcceptHeaderParser::TypeContext* ctx) = 0;

  virtual void enterSubtype(AcceptHeaderParser::SubtypeContext* ctx) = 0;
  virtual void exitSubtype(AcceptHeaderParser::SubtypeContext* ctx) = 0;

  virtual void enterAcceptParams(
      AcceptHeaderParser::AcceptParamsContext* ctx) = 0;
  virtual void exitAcceptParams(
      AcceptHeaderParser::AcceptParamsContext* ctx) = 0;

  virtual void enterWeight(AcceptHeaderParser::WeightContext* ctx) = 0;
  virtual void exitWeight(AcceptHeaderParser::WeightContext* ctx) = 0;

  virtual void enterQvalue(AcceptHeaderParser::QvalueContext* ctx) = 0;
  virtual void exitQvalue(AcceptHeaderParser::QvalueContext* ctx) = 0;

  virtual void enterAcceptExt(AcceptHeaderParser::AcceptExtContext* ctx) = 0;
  virtual void exitAcceptExt(AcceptHeaderParser::AcceptExtContext* ctx) = 0;

  virtual void enterParameter(AcceptHeaderParser::ParameterContext* ctx) = 0;
  virtual void exitParameter(AcceptHeaderParser::ParameterContext* ctx) = 0;

  virtual void enterToken(AcceptHeaderParser::TokenContext* ctx) = 0;
  virtual void exitToken(AcceptHeaderParser::TokenContext* ctx) = 0;

  virtual void enterTchar(AcceptHeaderParser::TcharContext* ctx) = 0;
  virtual void exitTchar(AcceptHeaderParser::TcharContext* ctx) = 0;

  virtual void enterQuotedString(
      AcceptHeaderParser::QuotedStringContext* ctx) = 0;
  virtual void exitQuotedString(
      AcceptHeaderParser::QuotedStringContext* ctx) = 0;

  virtual void enterQuoted_pair(
      AcceptHeaderParser::Quoted_pairContext* ctx) = 0;
  virtual void exitQuoted_pair(AcceptHeaderParser::Quoted_pairContext* ctx) = 0;
};
