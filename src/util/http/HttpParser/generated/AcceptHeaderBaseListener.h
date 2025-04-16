
// Generated from AcceptHeader.g4 by ANTLR 4.11.1

#ifndef QLEVER_SRC_UTIL_HTTP_HTTPPARSER_GENERATED_ACCEPTHEADERBASELISTENER_H
#define QLEVER_SRC_UTIL_HTTP_HTTPPARSER_GENERATED_ACCEPTHEADERBASELISTENER_H

#include "AcceptHeaderListener.h"
#include "antlr4-runtime.h"

/**
 * This class provides an empty implementation of AcceptHeaderListener,
 * which can be extended to create a listener which only needs to handle a
 * subset of the available methods.
 */
class AcceptHeaderBaseListener : public AcceptHeaderListener {
 public:
  virtual void enterAccept(
      AcceptHeaderParser::AcceptContext* /*ctx*/) override {}
  virtual void exitAccept(AcceptHeaderParser::AcceptContext* /*ctx*/) override {
  }

  virtual void enterAcceptWithEof(
      AcceptHeaderParser::AcceptWithEofContext* /*ctx*/) override {}
  virtual void exitAcceptWithEof(
      AcceptHeaderParser::AcceptWithEofContext* /*ctx*/) override {}

  virtual void enterRangeAndParams(
      AcceptHeaderParser::RangeAndParamsContext* /*ctx*/) override {}
  virtual void exitRangeAndParams(
      AcceptHeaderParser::RangeAndParamsContext* /*ctx*/) override {}

  virtual void enterMediaRange(
      AcceptHeaderParser::MediaRangeContext* /*ctx*/) override {}
  virtual void exitMediaRange(
      AcceptHeaderParser::MediaRangeContext* /*ctx*/) override {}

  virtual void enterType(AcceptHeaderParser::TypeContext* /*ctx*/) override {}
  virtual void exitType(AcceptHeaderParser::TypeContext* /*ctx*/) override {}

  virtual void enterSubtype(
      AcceptHeaderParser::SubtypeContext* /*ctx*/) override {}
  virtual void exitSubtype(
      AcceptHeaderParser::SubtypeContext* /*ctx*/) override {}

  virtual void enterAcceptParams(
      AcceptHeaderParser::AcceptParamsContext* /*ctx*/) override {}
  virtual void exitAcceptParams(
      AcceptHeaderParser::AcceptParamsContext* /*ctx*/) override {}

  virtual void enterWeight(
      AcceptHeaderParser::WeightContext* /*ctx*/) override {}
  virtual void exitWeight(AcceptHeaderParser::WeightContext* /*ctx*/) override {
  }

  virtual void enterQvalue(
      AcceptHeaderParser::QvalueContext* /*ctx*/) override {}
  virtual void exitQvalue(AcceptHeaderParser::QvalueContext* /*ctx*/) override {
  }

  virtual void enterAcceptExt(
      AcceptHeaderParser::AcceptExtContext* /*ctx*/) override {}
  virtual void exitAcceptExt(
      AcceptHeaderParser::AcceptExtContext* /*ctx*/) override {}

  virtual void enterParameter(
      AcceptHeaderParser::ParameterContext* /*ctx*/) override {}
  virtual void exitParameter(
      AcceptHeaderParser::ParameterContext* /*ctx*/) override {}

  virtual void enterToken(AcceptHeaderParser::TokenContext* /*ctx*/) override {}
  virtual void exitToken(AcceptHeaderParser::TokenContext* /*ctx*/) override {}

  virtual void enterTchar(AcceptHeaderParser::TcharContext* /*ctx*/) override {}
  virtual void exitTchar(AcceptHeaderParser::TcharContext* /*ctx*/) override {}

  virtual void enterQuotedString(
      AcceptHeaderParser::QuotedStringContext* /*ctx*/) override {}
  virtual void exitQuotedString(
      AcceptHeaderParser::QuotedStringContext* /*ctx*/) override {}

  virtual void enterQuoted_pair(
      AcceptHeaderParser::Quoted_pairContext* /*ctx*/) override {}
  virtual void exitQuoted_pair(
      AcceptHeaderParser::Quoted_pairContext* /*ctx*/) override {}

  virtual void enterEveryRule(antlr4::ParserRuleContext* /*ctx*/) override {}
  virtual void exitEveryRule(antlr4::ParserRuleContext* /*ctx*/) override {}
  virtual void visitTerminal(antlr4::tree::TerminalNode* /*node*/) override {}
  virtual void visitErrorNode(antlr4::tree::ErrorNode* /*node*/) override {}
};

#endif  // QLEVER_SRC_UTIL_HTTP_HTTPPARSER_GENERATED_ACCEPTHEADERBASELISTENER_H
