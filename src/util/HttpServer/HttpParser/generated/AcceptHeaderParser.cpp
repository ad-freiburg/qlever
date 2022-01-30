
// Generated from AcceptHeader.g4 by ANTLR 4.9.2

#include "AcceptHeaderParser.h"

#include "AcceptHeaderListener.h"
#include "AcceptHeaderVisitor.h"

using namespace antlrcpp;
using namespace antlr4;

AcceptHeaderParser::AcceptHeaderParser(TokenStream* input) : Parser(input) {
  _interpreter = new atn::ParserATNSimulator(this, _atn, _decisionToDFA,
                                             _sharedContextCache);
}

AcceptHeaderParser::~AcceptHeaderParser() { delete _interpreter; }

std::string AcceptHeaderParser::getGrammarFileName() const {
  return "AcceptHeader.g4";
}

const std::vector<std::string>& AcceptHeaderParser::getRuleNames() const {
  return _ruleNames;
}

dfa::Vocabulary& AcceptHeaderParser::getVocabulary() const {
  return _vocabulary;
}

//----------------- AcceptContext
//------------------------------------------------------------------

AcceptHeaderParser::AcceptContext::AcceptContext(ParserRuleContext* parent,
                                                 size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<AcceptHeaderParser::RangeAndParamsContext*>
AcceptHeaderParser::AcceptContext::rangeAndParams() {
  return getRuleContexts<AcceptHeaderParser::RangeAndParamsContext>();
}

AcceptHeaderParser::RangeAndParamsContext*
AcceptHeaderParser::AcceptContext::rangeAndParams(size_t i) {
  return getRuleContext<AcceptHeaderParser::RangeAndParamsContext>(i);
}

std::vector<tree::TerminalNode*> AcceptHeaderParser::AcceptContext::OWS() {
  return getTokens(AcceptHeaderParser::OWS);
}

tree::TerminalNode* AcceptHeaderParser::AcceptContext::OWS(size_t i) {
  return getToken(AcceptHeaderParser::OWS, i);
}

size_t AcceptHeaderParser::AcceptContext::getRuleIndex() const {
  return AcceptHeaderParser::RuleAccept;
}

void AcceptHeaderParser::AcceptContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->enterAccept(this);
}

void AcceptHeaderParser::AcceptContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->exitAccept(this);
}

antlrcpp::Any AcceptHeaderParser::AcceptContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<AcceptHeaderVisitor*>(visitor))
    return parserVisitor->visitAccept(this);
  else
    return visitor->visitChildren(this);
}

AcceptHeaderParser::AcceptContext* AcceptHeaderParser::accept() {
  AcceptContext* _localctx =
      _tracker.createInstance<AcceptContext>(_ctx, getState());
  enterRule(_localctx, 0, AcceptHeaderParser::RuleAccept);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(30);
    rangeAndParams();
    setState(47);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == AcceptHeaderParser::T__0

           || _la == AcceptHeaderParser::OWS) {
      setState(34);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == AcceptHeaderParser::OWS) {
        setState(31);
        match(AcceptHeaderParser::OWS);
        setState(36);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(37);
      match(AcceptHeaderParser::T__0);
      setState(41);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == AcceptHeaderParser::OWS) {
        setState(38);
        match(AcceptHeaderParser::OWS);
        setState(43);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(44);
      rangeAndParams();
      setState(49);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AcceptWithEofContext
//------------------------------------------------------------------

AcceptHeaderParser::AcceptWithEofContext::AcceptWithEofContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

AcceptHeaderParser::AcceptContext*
AcceptHeaderParser::AcceptWithEofContext::accept() {
  return getRuleContext<AcceptHeaderParser::AcceptContext>(0);
}

tree::TerminalNode* AcceptHeaderParser::AcceptWithEofContext::EOF() {
  return getToken(AcceptHeaderParser::EOF, 0);
}

size_t AcceptHeaderParser::AcceptWithEofContext::getRuleIndex() const {
  return AcceptHeaderParser::RuleAcceptWithEof;
}

void AcceptHeaderParser::AcceptWithEofContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->enterAcceptWithEof(this);
}

void AcceptHeaderParser::AcceptWithEofContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->exitAcceptWithEof(this);
}

antlrcpp::Any AcceptHeaderParser::AcceptWithEofContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<AcceptHeaderVisitor*>(visitor))
    return parserVisitor->visitAcceptWithEof(this);
  else
    return visitor->visitChildren(this);
}

AcceptHeaderParser::AcceptWithEofContext* AcceptHeaderParser::acceptWithEof() {
  AcceptWithEofContext* _localctx =
      _tracker.createInstance<AcceptWithEofContext>(_ctx, getState());
  enterRule(_localctx, 2, AcceptHeaderParser::RuleAcceptWithEof);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(50);
    accept();
    setState(51);
    match(AcceptHeaderParser::EOF);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- RangeAndParamsContext
//------------------------------------------------------------------

AcceptHeaderParser::RangeAndParamsContext::RangeAndParamsContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

AcceptHeaderParser::MediaRangeContext*
AcceptHeaderParser::RangeAndParamsContext::mediaRange() {
  return getRuleContext<AcceptHeaderParser::MediaRangeContext>(0);
}

AcceptHeaderParser::AcceptParamsContext*
AcceptHeaderParser::RangeAndParamsContext::acceptParams() {
  return getRuleContext<AcceptHeaderParser::AcceptParamsContext>(0);
}

size_t AcceptHeaderParser::RangeAndParamsContext::getRuleIndex() const {
  return AcceptHeaderParser::RuleRangeAndParams;
}

void AcceptHeaderParser::RangeAndParamsContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->enterRangeAndParams(this);
}

void AcceptHeaderParser::RangeAndParamsContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->exitRangeAndParams(this);
}

antlrcpp::Any AcceptHeaderParser::RangeAndParamsContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<AcceptHeaderVisitor*>(visitor))
    return parserVisitor->visitRangeAndParams(this);
  else
    return visitor->visitChildren(this);
}

AcceptHeaderParser::RangeAndParamsContext*
AcceptHeaderParser::rangeAndParams() {
  RangeAndParamsContext* _localctx =
      _tracker.createInstance<RangeAndParamsContext>(_ctx, getState());
  enterRule(_localctx, 4, AcceptHeaderParser::RuleRangeAndParams);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(53);
    mediaRange();
    setState(55);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 3, _ctx)) {
      case 1: {
        setState(54);
        acceptParams();
        break;
      }

      default:
        break;
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MediaRangeContext
//------------------------------------------------------------------

AcceptHeaderParser::MediaRangeContext::MediaRangeContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* AcceptHeaderParser::MediaRangeContext::MediaRangeAll() {
  return getToken(AcceptHeaderParser::MediaRangeAll, 0);
}

std::vector<AcceptHeaderParser::ParameterContext*>
AcceptHeaderParser::MediaRangeContext::parameter() {
  return getRuleContexts<AcceptHeaderParser::ParameterContext>();
}

AcceptHeaderParser::ParameterContext*
AcceptHeaderParser::MediaRangeContext::parameter(size_t i) {
  return getRuleContext<AcceptHeaderParser::ParameterContext>(i);
}

AcceptHeaderParser::TypeContext* AcceptHeaderParser::MediaRangeContext::type() {
  return getRuleContext<AcceptHeaderParser::TypeContext>(0);
}

tree::TerminalNode* AcceptHeaderParser::MediaRangeContext::Slash() {
  return getToken(AcceptHeaderParser::Slash, 0);
}

tree::TerminalNode* AcceptHeaderParser::MediaRangeContext::Star() {
  return getToken(AcceptHeaderParser::Star, 0);
}

AcceptHeaderParser::SubtypeContext*
AcceptHeaderParser::MediaRangeContext::subtype() {
  return getRuleContext<AcceptHeaderParser::SubtypeContext>(0);
}

std::vector<tree::TerminalNode*> AcceptHeaderParser::MediaRangeContext::OWS() {
  return getTokens(AcceptHeaderParser::OWS);
}

tree::TerminalNode* AcceptHeaderParser::MediaRangeContext::OWS(size_t i) {
  return getToken(AcceptHeaderParser::OWS, i);
}

size_t AcceptHeaderParser::MediaRangeContext::getRuleIndex() const {
  return AcceptHeaderParser::RuleMediaRange;
}

void AcceptHeaderParser::MediaRangeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->enterMediaRange(this);
}

void AcceptHeaderParser::MediaRangeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->exitMediaRange(this);
}

antlrcpp::Any AcceptHeaderParser::MediaRangeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<AcceptHeaderVisitor*>(visitor))
    return parserVisitor->visitMediaRange(this);
  else
    return visitor->visitChildren(this);
}

AcceptHeaderParser::MediaRangeContext* AcceptHeaderParser::mediaRange() {
  MediaRangeContext* _localctx =
      _tracker.createInstance<MediaRangeContext>(_ctx, getState());
  enterRule(_localctx, 6, AcceptHeaderParser::RuleMediaRange);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(66);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 4, _ctx)) {
      case 1: {
        setState(57);
        match(AcceptHeaderParser::MediaRangeAll);
        break;
      }

      case 2: {
        setState(58);
        type();
        setState(59);
        match(AcceptHeaderParser::Slash);
        setState(60);
        match(AcceptHeaderParser::Star);
        break;
      }

      case 3: {
        setState(62);
        type();
        setState(63);
        match(AcceptHeaderParser::Slash);
        setState(64);
        subtype();
        break;
      }

      default:
        break;
    }
    setState(84);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 7,
                                                                     _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(71);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == AcceptHeaderParser::OWS) {
          setState(68);
          match(AcceptHeaderParser::OWS);
          setState(73);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(74);
        match(AcceptHeaderParser::T__1);
        setState(78);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == AcceptHeaderParser::OWS) {
          setState(75);
          match(AcceptHeaderParser::OWS);
          setState(80);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
        setState(81);
        parameter();
      }
      setState(86);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                       7, _ctx);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TypeContext
//------------------------------------------------------------------

AcceptHeaderParser::TypeContext::TypeContext(ParserRuleContext* parent,
                                             size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

AcceptHeaderParser::TokenContext* AcceptHeaderParser::TypeContext::token() {
  return getRuleContext<AcceptHeaderParser::TokenContext>(0);
}

size_t AcceptHeaderParser::TypeContext::getRuleIndex() const {
  return AcceptHeaderParser::RuleType;
}

void AcceptHeaderParser::TypeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->enterType(this);
}

void AcceptHeaderParser::TypeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->exitType(this);
}

antlrcpp::Any AcceptHeaderParser::TypeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<AcceptHeaderVisitor*>(visitor))
    return parserVisitor->visitType(this);
  else
    return visitor->visitChildren(this);
}

AcceptHeaderParser::TypeContext* AcceptHeaderParser::type() {
  TypeContext* _localctx =
      _tracker.createInstance<TypeContext>(_ctx, getState());
  enterRule(_localctx, 8, AcceptHeaderParser::RuleType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(87);
    token();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- SubtypeContext
//------------------------------------------------------------------

AcceptHeaderParser::SubtypeContext::SubtypeContext(ParserRuleContext* parent,
                                                   size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

AcceptHeaderParser::TokenContext* AcceptHeaderParser::SubtypeContext::token() {
  return getRuleContext<AcceptHeaderParser::TokenContext>(0);
}

size_t AcceptHeaderParser::SubtypeContext::getRuleIndex() const {
  return AcceptHeaderParser::RuleSubtype;
}

void AcceptHeaderParser::SubtypeContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->enterSubtype(this);
}

void AcceptHeaderParser::SubtypeContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->exitSubtype(this);
}

antlrcpp::Any AcceptHeaderParser::SubtypeContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<AcceptHeaderVisitor*>(visitor))
    return parserVisitor->visitSubtype(this);
  else
    return visitor->visitChildren(this);
}

AcceptHeaderParser::SubtypeContext* AcceptHeaderParser::subtype() {
  SubtypeContext* _localctx =
      _tracker.createInstance<SubtypeContext>(_ctx, getState());
  enterRule(_localctx, 10, AcceptHeaderParser::RuleSubtype);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(89);
    token();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AcceptParamsContext
//------------------------------------------------------------------

AcceptHeaderParser::AcceptParamsContext::AcceptParamsContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

AcceptHeaderParser::WeightContext*
AcceptHeaderParser::AcceptParamsContext::weight() {
  return getRuleContext<AcceptHeaderParser::WeightContext>(0);
}

std::vector<AcceptHeaderParser::AcceptExtContext*>
AcceptHeaderParser::AcceptParamsContext::acceptExt() {
  return getRuleContexts<AcceptHeaderParser::AcceptExtContext>();
}

AcceptHeaderParser::AcceptExtContext*
AcceptHeaderParser::AcceptParamsContext::acceptExt(size_t i) {
  return getRuleContext<AcceptHeaderParser::AcceptExtContext>(i);
}

size_t AcceptHeaderParser::AcceptParamsContext::getRuleIndex() const {
  return AcceptHeaderParser::RuleAcceptParams;
}

void AcceptHeaderParser::AcceptParamsContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->enterAcceptParams(this);
}

void AcceptHeaderParser::AcceptParamsContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->exitAcceptParams(this);
}

antlrcpp::Any AcceptHeaderParser::AcceptParamsContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<AcceptHeaderVisitor*>(visitor))
    return parserVisitor->visitAcceptParams(this);
  else
    return visitor->visitChildren(this);
}

AcceptHeaderParser::AcceptParamsContext* AcceptHeaderParser::acceptParams() {
  AcceptParamsContext* _localctx =
      _tracker.createInstance<AcceptParamsContext>(_ctx, getState());
  enterRule(_localctx, 12, AcceptHeaderParser::RuleAcceptParams);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(91);
    weight();
    setState(95);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 8,
                                                                     _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(92);
        acceptExt();
      }
      setState(97);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                       8, _ctx);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- WeightContext
//------------------------------------------------------------------

AcceptHeaderParser::WeightContext::WeightContext(ParserRuleContext* parent,
                                                 size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* AcceptHeaderParser::WeightContext::QandEqual() {
  return getToken(AcceptHeaderParser::QandEqual, 0);
}

AcceptHeaderParser::QvalueContext* AcceptHeaderParser::WeightContext::qvalue() {
  return getRuleContext<AcceptHeaderParser::QvalueContext>(0);
}

std::vector<tree::TerminalNode*> AcceptHeaderParser::WeightContext::OWS() {
  return getTokens(AcceptHeaderParser::OWS);
}

tree::TerminalNode* AcceptHeaderParser::WeightContext::OWS(size_t i) {
  return getToken(AcceptHeaderParser::OWS, i);
}

size_t AcceptHeaderParser::WeightContext::getRuleIndex() const {
  return AcceptHeaderParser::RuleWeight;
}

void AcceptHeaderParser::WeightContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->enterWeight(this);
}

void AcceptHeaderParser::WeightContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->exitWeight(this);
}

antlrcpp::Any AcceptHeaderParser::WeightContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<AcceptHeaderVisitor*>(visitor))
    return parserVisitor->visitWeight(this);
  else
    return visitor->visitChildren(this);
}

AcceptHeaderParser::WeightContext* AcceptHeaderParser::weight() {
  WeightContext* _localctx =
      _tracker.createInstance<WeightContext>(_ctx, getState());
  enterRule(_localctx, 14, AcceptHeaderParser::RuleWeight);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(101);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == AcceptHeaderParser::OWS) {
      setState(98);
      match(AcceptHeaderParser::OWS);
      setState(103);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(104);
    match(AcceptHeaderParser::T__1);
    setState(108);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == AcceptHeaderParser::OWS) {
      setState(105);
      match(AcceptHeaderParser::OWS);
      setState(110);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(111);
    match(AcceptHeaderParser::QandEqual);
    setState(112);
    qvalue();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- QvalueContext
//------------------------------------------------------------------

AcceptHeaderParser::QvalueContext::QvalueContext(ParserRuleContext* parent,
                                                 size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<tree::TerminalNode*> AcceptHeaderParser::QvalueContext::DIGIT() {
  return getTokens(AcceptHeaderParser::DIGIT);
}

tree::TerminalNode* AcceptHeaderParser::QvalueContext::DIGIT(size_t i) {
  return getToken(AcceptHeaderParser::DIGIT, i);
}

tree::TerminalNode* AcceptHeaderParser::QvalueContext::Dot() {
  return getToken(AcceptHeaderParser::Dot, 0);
}

size_t AcceptHeaderParser::QvalueContext::getRuleIndex() const {
  return AcceptHeaderParser::RuleQvalue;
}

void AcceptHeaderParser::QvalueContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->enterQvalue(this);
}

void AcceptHeaderParser::QvalueContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->exitQvalue(this);
}

antlrcpp::Any AcceptHeaderParser::QvalueContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<AcceptHeaderVisitor*>(visitor))
    return parserVisitor->visitQvalue(this);
  else
    return visitor->visitChildren(this);
}

AcceptHeaderParser::QvalueContext* AcceptHeaderParser::qvalue() {
  QvalueContext* _localctx =
      _tracker.createInstance<QvalueContext>(_ctx, getState());
  enterRule(_localctx, 16, AcceptHeaderParser::RuleQvalue);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(114);
    match(AcceptHeaderParser::DIGIT);
    setState(122);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == AcceptHeaderParser::Dot) {
      setState(115);
      match(AcceptHeaderParser::Dot);
      setState(119);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == AcceptHeaderParser::DIGIT) {
        setState(116);
        match(AcceptHeaderParser::DIGIT);
        setState(121);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AcceptExtContext
//------------------------------------------------------------------

AcceptHeaderParser::AcceptExtContext::AcceptExtContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<AcceptHeaderParser::TokenContext*>
AcceptHeaderParser::AcceptExtContext::token() {
  return getRuleContexts<AcceptHeaderParser::TokenContext>();
}

AcceptHeaderParser::TokenContext* AcceptHeaderParser::AcceptExtContext::token(
    size_t i) {
  return getRuleContext<AcceptHeaderParser::TokenContext>(i);
}

std::vector<tree::TerminalNode*> AcceptHeaderParser::AcceptExtContext::OWS() {
  return getTokens(AcceptHeaderParser::OWS);
}

tree::TerminalNode* AcceptHeaderParser::AcceptExtContext::OWS(size_t i) {
  return getToken(AcceptHeaderParser::OWS, i);
}

AcceptHeaderParser::QuotedStringContext*
AcceptHeaderParser::AcceptExtContext::quotedString() {
  return getRuleContext<AcceptHeaderParser::QuotedStringContext>(0);
}

size_t AcceptHeaderParser::AcceptExtContext::getRuleIndex() const {
  return AcceptHeaderParser::RuleAcceptExt;
}

void AcceptHeaderParser::AcceptExtContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->enterAcceptExt(this);
}

void AcceptHeaderParser::AcceptExtContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->exitAcceptExt(this);
}

antlrcpp::Any AcceptHeaderParser::AcceptExtContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<AcceptHeaderVisitor*>(visitor))
    return parserVisitor->visitAcceptExt(this);
  else
    return visitor->visitChildren(this);
}

AcceptHeaderParser::AcceptExtContext* AcceptHeaderParser::acceptExt() {
  AcceptExtContext* _localctx =
      _tracker.createInstance<AcceptExtContext>(_ctx, getState());
  enterRule(_localctx, 18, AcceptHeaderParser::RuleAcceptExt);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(127);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == AcceptHeaderParser::OWS) {
      setState(124);
      match(AcceptHeaderParser::OWS);
      setState(129);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(130);
    match(AcceptHeaderParser::T__1);
    setState(134);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == AcceptHeaderParser::OWS) {
      setState(131);
      match(AcceptHeaderParser::OWS);
      setState(136);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(137);
    token();
    setState(143);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == AcceptHeaderParser::T__2) {
      setState(138);
      match(AcceptHeaderParser::T__2);
      setState(141);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case AcceptHeaderParser::DIGIT:
        case AcceptHeaderParser::ALPHA:
        case AcceptHeaderParser::Minus:
        case AcceptHeaderParser::Dot:
        case AcceptHeaderParser::Underscore:
        case AcceptHeaderParser::Tilde:
        case AcceptHeaderParser::ExclamationMark:
        case AcceptHeaderParser::DollarSign:
        case AcceptHeaderParser::Hashtag:
        case AcceptHeaderParser::Ampersand:
        case AcceptHeaderParser::Percent:
        case AcceptHeaderParser::SQuote:
        case AcceptHeaderParser::Star:
        case AcceptHeaderParser::Plus:
        case AcceptHeaderParser::Caret:
        case AcceptHeaderParser::BackQuote:
        case AcceptHeaderParser::VBar: {
          setState(139);
          token();
          break;
        }

        case AcceptHeaderParser::DQUOTE: {
          setState(140);
          quotedString();
          break;
        }

        default:
          throw NoViableAltException(this);
      }
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ParameterContext
//------------------------------------------------------------------

AcceptHeaderParser::ParameterContext::ParameterContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<AcceptHeaderParser::TokenContext*>
AcceptHeaderParser::ParameterContext::token() {
  return getRuleContexts<AcceptHeaderParser::TokenContext>();
}

AcceptHeaderParser::TokenContext* AcceptHeaderParser::ParameterContext::token(
    size_t i) {
  return getRuleContext<AcceptHeaderParser::TokenContext>(i);
}

AcceptHeaderParser::QuotedStringContext*
AcceptHeaderParser::ParameterContext::quotedString() {
  return getRuleContext<AcceptHeaderParser::QuotedStringContext>(0);
}

size_t AcceptHeaderParser::ParameterContext::getRuleIndex() const {
  return AcceptHeaderParser::RuleParameter;
}

void AcceptHeaderParser::ParameterContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->enterParameter(this);
}

void AcceptHeaderParser::ParameterContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->exitParameter(this);
}

antlrcpp::Any AcceptHeaderParser::ParameterContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<AcceptHeaderVisitor*>(visitor))
    return parserVisitor->visitParameter(this);
  else
    return visitor->visitChildren(this);
}

AcceptHeaderParser::ParameterContext* AcceptHeaderParser::parameter() {
  ParameterContext* _localctx =
      _tracker.createInstance<ParameterContext>(_ctx, getState());
  enterRule(_localctx, 20, AcceptHeaderParser::RuleParameter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(145);
    token();
    setState(146);
    match(AcceptHeaderParser::T__2);
    setState(149);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case AcceptHeaderParser::DIGIT:
      case AcceptHeaderParser::ALPHA:
      case AcceptHeaderParser::Minus:
      case AcceptHeaderParser::Dot:
      case AcceptHeaderParser::Underscore:
      case AcceptHeaderParser::Tilde:
      case AcceptHeaderParser::ExclamationMark:
      case AcceptHeaderParser::DollarSign:
      case AcceptHeaderParser::Hashtag:
      case AcceptHeaderParser::Ampersand:
      case AcceptHeaderParser::Percent:
      case AcceptHeaderParser::SQuote:
      case AcceptHeaderParser::Star:
      case AcceptHeaderParser::Plus:
      case AcceptHeaderParser::Caret:
      case AcceptHeaderParser::BackQuote:
      case AcceptHeaderParser::VBar: {
        setState(147);
        token();
        break;
      }

      case AcceptHeaderParser::DQUOTE: {
        setState(148);
        quotedString();
        break;
      }

      default:
        throw NoViableAltException(this);
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TokenContext
//------------------------------------------------------------------

AcceptHeaderParser::TokenContext::TokenContext(ParserRuleContext* parent,
                                               size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<AcceptHeaderParser::TcharContext*>
AcceptHeaderParser::TokenContext::tchar() {
  return getRuleContexts<AcceptHeaderParser::TcharContext>();
}

AcceptHeaderParser::TcharContext* AcceptHeaderParser::TokenContext::tchar(
    size_t i) {
  return getRuleContext<AcceptHeaderParser::TcharContext>(i);
}

size_t AcceptHeaderParser::TokenContext::getRuleIndex() const {
  return AcceptHeaderParser::RuleToken;
}

void AcceptHeaderParser::TokenContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->enterToken(this);
}

void AcceptHeaderParser::TokenContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->exitToken(this);
}

antlrcpp::Any AcceptHeaderParser::TokenContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<AcceptHeaderVisitor*>(visitor))
    return parserVisitor->visitToken(this);
  else
    return visitor->visitChildren(this);
}

AcceptHeaderParser::TokenContext* AcceptHeaderParser::token() {
  TokenContext* _localctx =
      _tracker.createInstance<TokenContext>(_ctx, getState());
  enterRule(_localctx, 22, AcceptHeaderParser::RuleToken);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(152);
    _errHandler->sync(this);
    _la = _input->LA(1);
    do {
      setState(151);
      tchar();
      setState(154);
      _errHandler->sync(this);
      _la = _input->LA(1);
    } while ((((_la & ~0x3fULL) == 0) &&
              ((1ULL << _la) & ((1ULL << AcceptHeaderParser::DIGIT) |
                                (1ULL << AcceptHeaderParser::ALPHA) |
                                (1ULL << AcceptHeaderParser::Minus) |
                                (1ULL << AcceptHeaderParser::Dot) |
                                (1ULL << AcceptHeaderParser::Underscore) |
                                (1ULL << AcceptHeaderParser::Tilde) |
                                (1ULL << AcceptHeaderParser::ExclamationMark) |
                                (1ULL << AcceptHeaderParser::DollarSign) |
                                (1ULL << AcceptHeaderParser::Hashtag) |
                                (1ULL << AcceptHeaderParser::Ampersand) |
                                (1ULL << AcceptHeaderParser::Percent) |
                                (1ULL << AcceptHeaderParser::SQuote) |
                                (1ULL << AcceptHeaderParser::Star) |
                                (1ULL << AcceptHeaderParser::Plus) |
                                (1ULL << AcceptHeaderParser::Caret) |
                                (1ULL << AcceptHeaderParser::BackQuote) |
                                (1ULL << AcceptHeaderParser::VBar))) != 0));

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- TcharContext
//------------------------------------------------------------------

AcceptHeaderParser::TcharContext::TcharContext(ParserRuleContext* parent,
                                               size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* AcceptHeaderParser::TcharContext::ExclamationMark() {
  return getToken(AcceptHeaderParser::ExclamationMark, 0);
}

tree::TerminalNode* AcceptHeaderParser::TcharContext::Hashtag() {
  return getToken(AcceptHeaderParser::Hashtag, 0);
}

tree::TerminalNode* AcceptHeaderParser::TcharContext::DollarSign() {
  return getToken(AcceptHeaderParser::DollarSign, 0);
}

tree::TerminalNode* AcceptHeaderParser::TcharContext::Percent() {
  return getToken(AcceptHeaderParser::Percent, 0);
}

tree::TerminalNode* AcceptHeaderParser::TcharContext::Ampersand() {
  return getToken(AcceptHeaderParser::Ampersand, 0);
}

tree::TerminalNode* AcceptHeaderParser::TcharContext::SQuote() {
  return getToken(AcceptHeaderParser::SQuote, 0);
}

tree::TerminalNode* AcceptHeaderParser::TcharContext::Star() {
  return getToken(AcceptHeaderParser::Star, 0);
}

tree::TerminalNode* AcceptHeaderParser::TcharContext::Plus() {
  return getToken(AcceptHeaderParser::Plus, 0);
}

tree::TerminalNode* AcceptHeaderParser::TcharContext::Minus() {
  return getToken(AcceptHeaderParser::Minus, 0);
}

tree::TerminalNode* AcceptHeaderParser::TcharContext::Dot() {
  return getToken(AcceptHeaderParser::Dot, 0);
}

tree::TerminalNode* AcceptHeaderParser::TcharContext::Caret() {
  return getToken(AcceptHeaderParser::Caret, 0);
}

tree::TerminalNode* AcceptHeaderParser::TcharContext::Underscore() {
  return getToken(AcceptHeaderParser::Underscore, 0);
}

tree::TerminalNode* AcceptHeaderParser::TcharContext::BackQuote() {
  return getToken(AcceptHeaderParser::BackQuote, 0);
}

tree::TerminalNode* AcceptHeaderParser::TcharContext::VBar() {
  return getToken(AcceptHeaderParser::VBar, 0);
}

tree::TerminalNode* AcceptHeaderParser::TcharContext::Tilde() {
  return getToken(AcceptHeaderParser::Tilde, 0);
}

tree::TerminalNode* AcceptHeaderParser::TcharContext::DIGIT() {
  return getToken(AcceptHeaderParser::DIGIT, 0);
}

tree::TerminalNode* AcceptHeaderParser::TcharContext::ALPHA() {
  return getToken(AcceptHeaderParser::ALPHA, 0);
}

size_t AcceptHeaderParser::TcharContext::getRuleIndex() const {
  return AcceptHeaderParser::RuleTchar;
}

void AcceptHeaderParser::TcharContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->enterTchar(this);
}

void AcceptHeaderParser::TcharContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->exitTchar(this);
}

antlrcpp::Any AcceptHeaderParser::TcharContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<AcceptHeaderVisitor*>(visitor))
    return parserVisitor->visitTchar(this);
  else
    return visitor->visitChildren(this);
}

AcceptHeaderParser::TcharContext* AcceptHeaderParser::tchar() {
  TcharContext* _localctx =
      _tracker.createInstance<TcharContext>(_ctx, getState());
  enterRule(_localctx, 24, AcceptHeaderParser::RuleTchar);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(156);
    _la = _input->LA(1);
    if (!((((_la & ~0x3fULL) == 0) &&
           ((1ULL << _la) & ((1ULL << AcceptHeaderParser::DIGIT) |
                             (1ULL << AcceptHeaderParser::ALPHA) |
                             (1ULL << AcceptHeaderParser::Minus) |
                             (1ULL << AcceptHeaderParser::Dot) |
                             (1ULL << AcceptHeaderParser::Underscore) |
                             (1ULL << AcceptHeaderParser::Tilde) |
                             (1ULL << AcceptHeaderParser::ExclamationMark) |
                             (1ULL << AcceptHeaderParser::DollarSign) |
                             (1ULL << AcceptHeaderParser::Hashtag) |
                             (1ULL << AcceptHeaderParser::Ampersand) |
                             (1ULL << AcceptHeaderParser::Percent) |
                             (1ULL << AcceptHeaderParser::SQuote) |
                             (1ULL << AcceptHeaderParser::Star) |
                             (1ULL << AcceptHeaderParser::Plus) |
                             (1ULL << AcceptHeaderParser::Caret) |
                             (1ULL << AcceptHeaderParser::BackQuote) |
                             (1ULL << AcceptHeaderParser::VBar))) != 0))) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- QuotedStringContext
//------------------------------------------------------------------

AcceptHeaderParser::QuotedStringContext::QuotedStringContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<tree::TerminalNode*>
AcceptHeaderParser::QuotedStringContext::DQUOTE() {
  return getTokens(AcceptHeaderParser::DQUOTE);
}

tree::TerminalNode* AcceptHeaderParser::QuotedStringContext::DQUOTE(size_t i) {
  return getToken(AcceptHeaderParser::DQUOTE, i);
}

std::vector<tree::TerminalNode*>
AcceptHeaderParser::QuotedStringContext::QDTEXT() {
  return getTokens(AcceptHeaderParser::QDTEXT);
}

tree::TerminalNode* AcceptHeaderParser::QuotedStringContext::QDTEXT(size_t i) {
  return getToken(AcceptHeaderParser::QDTEXT, i);
}

std::vector<AcceptHeaderParser::Quoted_pairContext*>
AcceptHeaderParser::QuotedStringContext::quoted_pair() {
  return getRuleContexts<AcceptHeaderParser::Quoted_pairContext>();
}

AcceptHeaderParser::Quoted_pairContext*
AcceptHeaderParser::QuotedStringContext::quoted_pair(size_t i) {
  return getRuleContext<AcceptHeaderParser::Quoted_pairContext>(i);
}

size_t AcceptHeaderParser::QuotedStringContext::getRuleIndex() const {
  return AcceptHeaderParser::RuleQuotedString;
}

void AcceptHeaderParser::QuotedStringContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->enterQuotedString(this);
}

void AcceptHeaderParser::QuotedStringContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->exitQuotedString(this);
}

antlrcpp::Any AcceptHeaderParser::QuotedStringContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<AcceptHeaderVisitor*>(visitor))
    return parserVisitor->visitQuotedString(this);
  else
    return visitor->visitChildren(this);
}

AcceptHeaderParser::QuotedStringContext* AcceptHeaderParser::quotedString() {
  QuotedStringContext* _localctx =
      _tracker.createInstance<QuotedStringContext>(_ctx, getState());
  enterRule(_localctx, 26, AcceptHeaderParser::RuleQuotedString);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(158);
    match(AcceptHeaderParser::DQUOTE);
    setState(163);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == AcceptHeaderParser::T__3

           || _la == AcceptHeaderParser::QDTEXT) {
      setState(161);
      _errHandler->sync(this);
      switch (_input->LA(1)) {
        case AcceptHeaderParser::QDTEXT: {
          setState(159);
          match(AcceptHeaderParser::QDTEXT);
          break;
        }

        case AcceptHeaderParser::T__3: {
          setState(160);
          quoted_pair();
          break;
        }

        default:
          throw NoViableAltException(this);
      }
      setState(165);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(166);
    match(AcceptHeaderParser::DQUOTE);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- Quoted_pairContext
//------------------------------------------------------------------

AcceptHeaderParser::Quoted_pairContext::Quoted_pairContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* AcceptHeaderParser::Quoted_pairContext::HTAB() {
  return getToken(AcceptHeaderParser::HTAB, 0);
}

tree::TerminalNode* AcceptHeaderParser::Quoted_pairContext::SP() {
  return getToken(AcceptHeaderParser::SP, 0);
}

tree::TerminalNode* AcceptHeaderParser::Quoted_pairContext::VCHAR() {
  return getToken(AcceptHeaderParser::VCHAR, 0);
}

tree::TerminalNode* AcceptHeaderParser::Quoted_pairContext::OBS_TEXT() {
  return getToken(AcceptHeaderParser::OBS_TEXT, 0);
}

size_t AcceptHeaderParser::Quoted_pairContext::getRuleIndex() const {
  return AcceptHeaderParser::RuleQuoted_pair;
}

void AcceptHeaderParser::Quoted_pairContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->enterQuoted_pair(this);
}

void AcceptHeaderParser::Quoted_pairContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener = dynamic_cast<AcceptHeaderListener*>(listener);
  if (parserListener != nullptr) parserListener->exitQuoted_pair(this);
}

antlrcpp::Any AcceptHeaderParser::Quoted_pairContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor = dynamic_cast<AcceptHeaderVisitor*>(visitor))
    return parserVisitor->visitQuoted_pair(this);
  else
    return visitor->visitChildren(this);
}

AcceptHeaderParser::Quoted_pairContext* AcceptHeaderParser::quoted_pair() {
  Quoted_pairContext* _localctx =
      _tracker.createInstance<Quoted_pairContext>(_ctx, getState());
  enterRule(_localctx, 28, AcceptHeaderParser::RuleQuoted_pair);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(168);
    match(AcceptHeaderParser::T__3);
    setState(169);
    _la = _input->LA(1);
    if (!((((_la & ~0x3fULL) == 0) &&
           ((1ULL << _la) & ((1ULL << AcceptHeaderParser::OBS_TEXT) |
                             (1ULL << AcceptHeaderParser::SP) |
                             (1ULL << AcceptHeaderParser::HTAB) |
                             (1ULL << AcceptHeaderParser::VCHAR))) != 0))) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

// Static vars and initialization.
std::vector<dfa::DFA> AcceptHeaderParser::_decisionToDFA;
atn::PredictionContextCache AcceptHeaderParser::_sharedContextCache;

// We own the ATN which in turn owns the ATN states.
atn::ATN AcceptHeaderParser::_atn;
std::vector<uint16_t> AcceptHeaderParser::_serializedATN;

std::vector<std::string> AcceptHeaderParser::_ruleNames = {
    "accept", "acceptWithEof", "rangeAndParams", "mediaRange",
    "type",   "subtype",       "acceptParams",   "weight",
    "qvalue", "acceptExt",     "parameter",      "token",
    "tchar",  "quotedString",  "quoted_pair"};

std::vector<std::string> AcceptHeaderParser::_literalNames = {
    "",    "','", "';'", "'='", "'\\'", "",         "",        "",    "",
    "",    "'-'", "'.'", "'_'", "'~'",  "'\u003F'", "'/'",     "'!'", "':'",
    "'@'", "'$'", "'#'", "'&'", "'%'",  "'''",      "'*'",     "'+'", "'^'",
    "'`'", "'|'", "",    "",    "'\"'", "'\u0020'", "'\u0009'"};

std::vector<std::string> AcceptHeaderParser::_symbolicNames = {
    "",
    "",
    "",
    "",
    "",
    "MediaRangeAll",
    "QandEqual",
    "DIGIT",
    "ALPHA",
    "OWS",
    "Minus",
    "Dot",
    "Underscore",
    "Tilde",
    "QuestionMark",
    "Slash",
    "ExclamationMark",
    "Colon",
    "At",
    "DollarSign",
    "Hashtag",
    "Ampersand",
    "Percent",
    "SQuote",
    "Star",
    "Plus",
    "Caret",
    "BackQuote",
    "VBar",
    "QDTEXT",
    "OBS_TEXT",
    "DQUOTE",
    "SP",
    "HTAB",
    "VCHAR"};

dfa::Vocabulary AcceptHeaderParser::_vocabulary(_literalNames, _symbolicNames);

std::vector<std::string> AcceptHeaderParser::_tokenNames;

AcceptHeaderParser::Initializer::Initializer() {
  for (size_t i = 0; i < _symbolicNames.size(); ++i) {
    std::string name = _vocabulary.getLiteralName(i);
    if (name.empty()) {
      name = _vocabulary.getSymbolicName(i);
    }

    if (name.empty()) {
      _tokenNames.push_back("<INVALID>");
    } else {
      _tokenNames.push_back(name);
    }
  }

  static const uint16_t serializedATNSegment0[] = {
      0x3,  0x608b, 0xa72a, 0x8133, 0xb9ed, 0x417c, 0x3be7, 0x7786, 0x5964,
      0x3,  0x24,   0xae,   0x4,    0x2,    0x9,    0x2,    0x4,    0x3,
      0x9,  0x3,    0x4,    0x4,    0x9,    0x4,    0x4,    0x5,    0x9,
      0x5,  0x4,    0x6,    0x9,    0x6,    0x4,    0x7,    0x9,    0x7,
      0x4,  0x8,    0x9,    0x8,    0x4,    0x9,    0x9,    0x9,    0x4,
      0xa,  0x9,    0xa,    0x4,    0xb,    0x9,    0xb,    0x4,    0xc,
      0x9,  0xc,    0x4,    0xd,    0x9,    0xd,    0x4,    0xe,    0x9,
      0xe,  0x4,    0xf,    0x9,    0xf,    0x4,    0x10,   0x9,    0x10,
      0x3,  0x2,    0x3,    0x2,    0x7,    0x2,    0x23,   0xa,    0x2,
      0xc,  0x2,    0xe,    0x2,    0x26,   0xb,    0x2,    0x3,    0x2,
      0x3,  0x2,    0x7,    0x2,    0x2a,   0xa,    0x2,    0xc,    0x2,
      0xe,  0x2,    0x2d,   0xb,    0x2,    0x3,    0x2,    0x7,    0x2,
      0x30, 0xa,    0x2,    0xc,    0x2,    0xe,    0x2,    0x33,   0xb,
      0x2,  0x3,    0x3,    0x3,    0x3,    0x3,    0x3,    0x3,    0x4,
      0x3,  0x4,    0x5,    0x4,    0x3a,   0xa,    0x4,    0x3,    0x5,
      0x3,  0x5,    0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x3,
      0x5,  0x3,    0x5,    0x3,    0x5,    0x3,    0x5,    0x5,    0x5,
      0x45, 0xa,    0x5,    0x3,    0x5,    0x7,    0x5,    0x48,   0xa,
      0x5,  0xc,    0x5,    0xe,    0x5,    0x4b,   0xb,    0x5,    0x3,
      0x5,  0x3,    0x5,    0x7,    0x5,    0x4f,   0xa,    0x5,    0xc,
      0x5,  0xe,    0x5,    0x52,   0xb,    0x5,    0x3,    0x5,    0x7,
      0x5,  0x55,   0xa,    0x5,    0xc,    0x5,    0xe,    0x5,    0x58,
      0xb,  0x5,    0x3,    0x6,    0x3,    0x6,    0x3,    0x7,    0x3,
      0x7,  0x3,    0x8,    0x3,    0x8,    0x7,    0x8,    0x60,   0xa,
      0x8,  0xc,    0x8,    0xe,    0x8,    0x63,   0xb,    0x8,    0x3,
      0x9,  0x7,    0x9,    0x66,   0xa,    0x9,    0xc,    0x9,    0xe,
      0x9,  0x69,   0xb,    0x9,    0x3,    0x9,    0x3,    0x9,    0x7,
      0x9,  0x6d,   0xa,    0x9,    0xc,    0x9,    0xe,    0x9,    0x70,
      0xb,  0x9,    0x3,    0x9,    0x3,    0x9,    0x3,    0x9,    0x3,
      0xa,  0x3,    0xa,    0x3,    0xa,    0x7,    0xa,    0x78,   0xa,
      0xa,  0xc,    0xa,    0xe,    0xa,    0x7b,   0xb,    0xa,    0x5,
      0xa,  0x7d,   0xa,    0xa,    0x3,    0xb,    0x7,    0xb,    0x80,
      0xa,  0xb,    0xc,    0xb,    0xe,    0xb,    0x83,   0xb,    0xb,
      0x3,  0xb,    0x3,    0xb,    0x7,    0xb,    0x87,   0xa,    0xb,
      0xc,  0xb,    0xe,    0xb,    0x8a,   0xb,    0xb,    0x3,    0xb,
      0x3,  0xb,    0x3,    0xb,    0x3,    0xb,    0x5,    0xb,    0x90,
      0xa,  0xb,    0x5,    0xb,    0x92,   0xa,    0xb,    0x3,    0xc,
      0x3,  0xc,    0x3,    0xc,    0x3,    0xc,    0x5,    0xc,    0x98,
      0xa,  0xc,    0x3,    0xd,    0x6,    0xd,    0x9b,   0xa,    0xd,
      0xd,  0xd,    0xe,    0xd,    0x9c,   0x3,    0xe,    0x3,    0xe,
      0x3,  0xf,    0x3,    0xf,    0x3,    0xf,    0x7,    0xf,    0xa4,
      0xa,  0xf,    0xc,    0xf,    0xe,    0xf,    0xa7,   0xb,    0xf,
      0x3,  0xf,    0x3,    0xf,    0x3,    0x10,   0x3,    0x10,   0x3,
      0x10, 0x3,    0x10,   0x2,    0x2,    0x11,   0x2,    0x4,    0x6,
      0x8,  0xa,    0xc,    0xe,    0x10,   0x12,   0x14,   0x16,   0x18,
      0x1a, 0x1c,   0x1e,   0x2,    0x4,    0x6,    0x2,    0x9,    0xa,
      0xc,  0xf,    0x12,   0x12,   0x15,   0x1e,   0x4,    0x2,    0x20,
      0x20, 0x22,   0x24,   0x2,    0xb4,   0x2,    0x20,   0x3,    0x2,
      0x2,  0x2,    0x4,    0x34,   0x3,    0x2,    0x2,    0x2,    0x6,
      0x37, 0x3,    0x2,    0x2,    0x2,    0x8,    0x44,   0x3,    0x2,
      0x2,  0x2,    0xa,    0x59,   0x3,    0x2,    0x2,    0x2,    0xc,
      0x5b, 0x3,    0x2,    0x2,    0x2,    0xe,    0x5d,   0x3,    0x2,
      0x2,  0x2,    0x10,   0x67,   0x3,    0x2,    0x2,    0x2,    0x12,
      0x74, 0x3,    0x2,    0x2,    0x2,    0x14,   0x81,   0x3,    0x2,
      0x2,  0x2,    0x16,   0x93,   0x3,    0x2,    0x2,    0x2,    0x18,
      0x9a, 0x3,    0x2,    0x2,    0x2,    0x1a,   0x9e,   0x3,    0x2,
      0x2,  0x2,    0x1c,   0xa0,   0x3,    0x2,    0x2,    0x2,    0x1e,
      0xaa, 0x3,    0x2,    0x2,    0x2,    0x20,   0x31,   0x5,    0x6,
      0x4,  0x2,    0x21,   0x23,   0x7,    0xb,    0x2,    0x2,    0x22,
      0x21, 0x3,    0x2,    0x2,    0x2,    0x23,   0x26,   0x3,    0x2,
      0x2,  0x2,    0x24,   0x22,   0x3,    0x2,    0x2,    0x2,    0x24,
      0x25, 0x3,    0x2,    0x2,    0x2,    0x25,   0x27,   0x3,    0x2,
      0x2,  0x2,    0x26,   0x24,   0x3,    0x2,    0x2,    0x2,    0x27,
      0x2b, 0x7,    0x3,    0x2,    0x2,    0x28,   0x2a,   0x7,    0xb,
      0x2,  0x2,    0x29,   0x28,   0x3,    0x2,    0x2,    0x2,    0x2a,
      0x2d, 0x3,    0x2,    0x2,    0x2,    0x2b,   0x29,   0x3,    0x2,
      0x2,  0x2,    0x2b,   0x2c,   0x3,    0x2,    0x2,    0x2,    0x2c,
      0x2e, 0x3,    0x2,    0x2,    0x2,    0x2d,   0x2b,   0x3,    0x2,
      0x2,  0x2,    0x2e,   0x30,   0x5,    0x6,    0x4,    0x2,    0x2f,
      0x24, 0x3,    0x2,    0x2,    0x2,    0x30,   0x33,   0x3,    0x2,
      0x2,  0x2,    0x31,   0x2f,   0x3,    0x2,    0x2,    0x2,    0x31,
      0x32, 0x3,    0x2,    0x2,    0x2,    0x32,   0x3,    0x3,    0x2,
      0x2,  0x2,    0x33,   0x31,   0x3,    0x2,    0x2,    0x2,    0x34,
      0x35, 0x5,    0x2,    0x2,    0x2,    0x35,   0x36,   0x7,    0x2,
      0x2,  0x3,    0x36,   0x5,    0x3,    0x2,    0x2,    0x2,    0x37,
      0x39, 0x5,    0x8,    0x5,    0x2,    0x38,   0x3a,   0x5,    0xe,
      0x8,  0x2,    0x39,   0x38,   0x3,    0x2,    0x2,    0x2,    0x39,
      0x3a, 0x3,    0x2,    0x2,    0x2,    0x3a,   0x7,    0x3,    0x2,
      0x2,  0x2,    0x3b,   0x45,   0x7,    0x7,    0x2,    0x2,    0x3c,
      0x3d, 0x5,    0xa,    0x6,    0x2,    0x3d,   0x3e,   0x7,    0x11,
      0x2,  0x2,    0x3e,   0x3f,   0x7,    0x1a,   0x2,    0x2,    0x3f,
      0x45, 0x3,    0x2,    0x2,    0x2,    0x40,   0x41,   0x5,    0xa,
      0x6,  0x2,    0x41,   0x42,   0x7,    0x11,   0x2,    0x2,    0x42,
      0x43, 0x5,    0xc,    0x7,    0x2,    0x43,   0x45,   0x3,    0x2,
      0x2,  0x2,    0x44,   0x3b,   0x3,    0x2,    0x2,    0x2,    0x44,
      0x3c, 0x3,    0x2,    0x2,    0x2,    0x44,   0x40,   0x3,    0x2,
      0x2,  0x2,    0x45,   0x56,   0x3,    0x2,    0x2,    0x2,    0x46,
      0x48, 0x7,    0xb,    0x2,    0x2,    0x47,   0x46,   0x3,    0x2,
      0x2,  0x2,    0x48,   0x4b,   0x3,    0x2,    0x2,    0x2,    0x49,
      0x47, 0x3,    0x2,    0x2,    0x2,    0x49,   0x4a,   0x3,    0x2,
      0x2,  0x2,    0x4a,   0x4c,   0x3,    0x2,    0x2,    0x2,    0x4b,
      0x49, 0x3,    0x2,    0x2,    0x2,    0x4c,   0x50,   0x7,    0x4,
      0x2,  0x2,    0x4d,   0x4f,   0x7,    0xb,    0x2,    0x2,    0x4e,
      0x4d, 0x3,    0x2,    0x2,    0x2,    0x4f,   0x52,   0x3,    0x2,
      0x2,  0x2,    0x50,   0x4e,   0x3,    0x2,    0x2,    0x2,    0x50,
      0x51, 0x3,    0x2,    0x2,    0x2,    0x51,   0x53,   0x3,    0x2,
      0x2,  0x2,    0x52,   0x50,   0x3,    0x2,    0x2,    0x2,    0x53,
      0x55, 0x5,    0x16,   0xc,    0x2,    0x54,   0x49,   0x3,    0x2,
      0x2,  0x2,    0x55,   0x58,   0x3,    0x2,    0x2,    0x2,    0x56,
      0x54, 0x3,    0x2,    0x2,    0x2,    0x56,   0x57,   0x3,    0x2,
      0x2,  0x2,    0x57,   0x9,    0x3,    0x2,    0x2,    0x2,    0x58,
      0x56, 0x3,    0x2,    0x2,    0x2,    0x59,   0x5a,   0x5,    0x18,
      0xd,  0x2,    0x5a,   0xb,    0x3,    0x2,    0x2,    0x2,    0x5b,
      0x5c, 0x5,    0x18,   0xd,    0x2,    0x5c,   0xd,    0x3,    0x2,
      0x2,  0x2,    0x5d,   0x61,   0x5,    0x10,   0x9,    0x2,    0x5e,
      0x60, 0x5,    0x14,   0xb,    0x2,    0x5f,   0x5e,   0x3,    0x2,
      0x2,  0x2,    0x60,   0x63,   0x3,    0x2,    0x2,    0x2,    0x61,
      0x5f, 0x3,    0x2,    0x2,    0x2,    0x61,   0x62,   0x3,    0x2,
      0x2,  0x2,    0x62,   0xf,    0x3,    0x2,    0x2,    0x2,    0x63,
      0x61, 0x3,    0x2,    0x2,    0x2,    0x64,   0x66,   0x7,    0xb,
      0x2,  0x2,    0x65,   0x64,   0x3,    0x2,    0x2,    0x2,    0x66,
      0x69, 0x3,    0x2,    0x2,    0x2,    0x67,   0x65,   0x3,    0x2,
      0x2,  0x2,    0x67,   0x68,   0x3,    0x2,    0x2,    0x2,    0x68,
      0x6a, 0x3,    0x2,    0x2,    0x2,    0x69,   0x67,   0x3,    0x2,
      0x2,  0x2,    0x6a,   0x6e,   0x7,    0x4,    0x2,    0x2,    0x6b,
      0x6d, 0x7,    0xb,    0x2,    0x2,    0x6c,   0x6b,   0x3,    0x2,
      0x2,  0x2,    0x6d,   0x70,   0x3,    0x2,    0x2,    0x2,    0x6e,
      0x6c, 0x3,    0x2,    0x2,    0x2,    0x6e,   0x6f,   0x3,    0x2,
      0x2,  0x2,    0x6f,   0x71,   0x3,    0x2,    0x2,    0x2,    0x70,
      0x6e, 0x3,    0x2,    0x2,    0x2,    0x71,   0x72,   0x7,    0x8,
      0x2,  0x2,    0x72,   0x73,   0x5,    0x12,   0xa,    0x2,    0x73,
      0x11, 0x3,    0x2,    0x2,    0x2,    0x74,   0x7c,   0x7,    0x9,
      0x2,  0x2,    0x75,   0x79,   0x7,    0xd,    0x2,    0x2,    0x76,
      0x78, 0x7,    0x9,    0x2,    0x2,    0x77,   0x76,   0x3,    0x2,
      0x2,  0x2,    0x78,   0x7b,   0x3,    0x2,    0x2,    0x2,    0x79,
      0x77, 0x3,    0x2,    0x2,    0x2,    0x79,   0x7a,   0x3,    0x2,
      0x2,  0x2,    0x7a,   0x7d,   0x3,    0x2,    0x2,    0x2,    0x7b,
      0x79, 0x3,    0x2,    0x2,    0x2,    0x7c,   0x75,   0x3,    0x2,
      0x2,  0x2,    0x7c,   0x7d,   0x3,    0x2,    0x2,    0x2,    0x7d,
      0x13, 0x3,    0x2,    0x2,    0x2,    0x7e,   0x80,   0x7,    0xb,
      0x2,  0x2,    0x7f,   0x7e,   0x3,    0x2,    0x2,    0x2,    0x80,
      0x83, 0x3,    0x2,    0x2,    0x2,    0x81,   0x7f,   0x3,    0x2,
      0x2,  0x2,    0x81,   0x82,   0x3,    0x2,    0x2,    0x2,    0x82,
      0x84, 0x3,    0x2,    0x2,    0x2,    0x83,   0x81,   0x3,    0x2,
      0x2,  0x2,    0x84,   0x88,   0x7,    0x4,    0x2,    0x2,    0x85,
      0x87, 0x7,    0xb,    0x2,    0x2,    0x86,   0x85,   0x3,    0x2,
      0x2,  0x2,    0x87,   0x8a,   0x3,    0x2,    0x2,    0x2,    0x88,
      0x86, 0x3,    0x2,    0x2,    0x2,    0x88,   0x89,   0x3,    0x2,
      0x2,  0x2,    0x89,   0x8b,   0x3,    0x2,    0x2,    0x2,    0x8a,
      0x88, 0x3,    0x2,    0x2,    0x2,    0x8b,   0x91,   0x5,    0x18,
      0xd,  0x2,    0x8c,   0x8f,   0x7,    0x5,    0x2,    0x2,    0x8d,
      0x90, 0x5,    0x18,   0xd,    0x2,    0x8e,   0x90,   0x5,    0x1c,
      0xf,  0x2,    0x8f,   0x8d,   0x3,    0x2,    0x2,    0x2,    0x8f,
      0x8e, 0x3,    0x2,    0x2,    0x2,    0x90,   0x92,   0x3,    0x2,
      0x2,  0x2,    0x91,   0x8c,   0x3,    0x2,    0x2,    0x2,    0x91,
      0x92, 0x3,    0x2,    0x2,    0x2,    0x92,   0x15,   0x3,    0x2,
      0x2,  0x2,    0x93,   0x94,   0x5,    0x18,   0xd,    0x2,    0x94,
      0x97, 0x7,    0x5,    0x2,    0x2,    0x95,   0x98,   0x5,    0x18,
      0xd,  0x2,    0x96,   0x98,   0x5,    0x1c,   0xf,    0x2,    0x97,
      0x95, 0x3,    0x2,    0x2,    0x2,    0x97,   0x96,   0x3,    0x2,
      0x2,  0x2,    0x98,   0x17,   0x3,    0x2,    0x2,    0x2,    0x99,
      0x9b, 0x5,    0x1a,   0xe,    0x2,    0x9a,   0x99,   0x3,    0x2,
      0x2,  0x2,    0x9b,   0x9c,   0x3,    0x2,    0x2,    0x2,    0x9c,
      0x9a, 0x3,    0x2,    0x2,    0x2,    0x9c,   0x9d,   0x3,    0x2,
      0x2,  0x2,    0x9d,   0x19,   0x3,    0x2,    0x2,    0x2,    0x9e,
      0x9f, 0x9,    0x2,    0x2,    0x2,    0x9f,   0x1b,   0x3,    0x2,
      0x2,  0x2,    0xa0,   0xa5,   0x7,    0x21,   0x2,    0x2,    0xa1,
      0xa4, 0x7,    0x1f,   0x2,    0x2,    0xa2,   0xa4,   0x5,    0x1e,
      0x10, 0x2,    0xa3,   0xa1,   0x3,    0x2,    0x2,    0x2,    0xa3,
      0xa2, 0x3,    0x2,    0x2,    0x2,    0xa4,   0xa7,   0x3,    0x2,
      0x2,  0x2,    0xa5,   0xa3,   0x3,    0x2,    0x2,    0x2,    0xa5,
      0xa6, 0x3,    0x2,    0x2,    0x2,    0xa6,   0xa8,   0x3,    0x2,
      0x2,  0x2,    0xa7,   0xa5,   0x3,    0x2,    0x2,    0x2,    0xa8,
      0xa9, 0x7,    0x21,   0x2,    0x2,    0xa9,   0x1d,   0x3,    0x2,
      0x2,  0x2,    0xaa,   0xab,   0x7,    0x6,    0x2,    0x2,    0xab,
      0xac, 0x9,    0x3,    0x2,    0x2,    0xac,   0x1f,   0x3,    0x2,
      0x2,  0x2,    0x17,   0x24,   0x2b,   0x31,   0x39,   0x44,   0x49,
      0x50, 0x56,   0x61,   0x67,   0x6e,   0x79,   0x7c,   0x81,   0x88,
      0x8f, 0x91,   0x97,   0x9c,   0xa3,   0xa5,
  };

  _serializedATN.insert(
      _serializedATN.end(), serializedATNSegment0,
      serializedATNSegment0 +
          sizeof(serializedATNSegment0) / sizeof(serializedATNSegment0[0]));

  atn::ATNDeserializer deserializer;
  _atn = deserializer.deserialize(_serializedATN);

  size_t count = _atn.getNumberOfDecisions();
  _decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) {
    _decisionToDFA.emplace_back(_atn.getDecisionState(i), i);
  }
}

AcceptHeaderParser::Initializer AcceptHeaderParser::_init;
