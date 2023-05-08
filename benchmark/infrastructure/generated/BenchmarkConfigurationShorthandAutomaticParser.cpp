
// Generated from BenchmarkConfigurationShorthandAutomatic.g4 by ANTLR 4.12.0

#include "BenchmarkConfigurationShorthandAutomaticParser.h"

#include "BenchmarkConfigurationShorthandAutomaticVisitor.h"

using namespace antlrcpp;

using namespace antlr4;

namespace {

struct BenchmarkConfigurationShorthandAutomaticParserStaticData final {
  BenchmarkConfigurationShorthandAutomaticParserStaticData(
      std::vector<std::string> ruleNames, std::vector<std::string> literalNames,
      std::vector<std::string> symbolicNames)
      : ruleNames(std::move(ruleNames)),
        literalNames(std::move(literalNames)),
        symbolicNames(std::move(symbolicNames)),
        vocabulary(this->literalNames, this->symbolicNames) {}

  BenchmarkConfigurationShorthandAutomaticParserStaticData(
      const BenchmarkConfigurationShorthandAutomaticParserStaticData&) = delete;
  BenchmarkConfigurationShorthandAutomaticParserStaticData(
      BenchmarkConfigurationShorthandAutomaticParserStaticData&&) = delete;
  BenchmarkConfigurationShorthandAutomaticParserStaticData& operator=(
      const BenchmarkConfigurationShorthandAutomaticParserStaticData&) = delete;
  BenchmarkConfigurationShorthandAutomaticParserStaticData& operator=(
      BenchmarkConfigurationShorthandAutomaticParserStaticData&&) = delete;

  std::vector<antlr4::dfa::DFA> decisionToDFA;
  antlr4::atn::PredictionContextCache sharedContextCache;
  const std::vector<std::string> ruleNames;
  const std::vector<std::string> literalNames;
  const std::vector<std::string> symbolicNames;
  const antlr4::dfa::Vocabulary vocabulary;
  antlr4::atn::SerializedATNView serializedATN;
  std::unique_ptr<antlr4::atn::ATN> atn;
};

::antlr4::internal::OnceFlag
    benchmarkconfigurationshorthandautomaticParserOnceFlag;
BenchmarkConfigurationShorthandAutomaticParserStaticData*
    benchmarkconfigurationshorthandautomaticParserStaticData = nullptr;

void benchmarkconfigurationshorthandautomaticParserInitialize() {
  assert(benchmarkconfigurationshorthandautomaticParserStaticData == nullptr);
  auto staticData = std::make_unique<
      BenchmarkConfigurationShorthandAutomaticParserStaticData>(
      std::vector<std::string>{"assignments", "assignment", "list"},
      std::vector<std::string>{"", "'='", "';'", "'['", "','", "']'"},
      std::vector<std::string>{"", "", "", "", "", "", "LITERAL", "BOOL",
                               "INTEGER", "FLOAT", "STRING", "NAME",
                               "WHITESPACE"});
  static const int32_t serializedATNSegment[] = {
      4, 1, 12, 34, 2, 0,  7, 0,  2,  1,  7,  1, 2,  2,  7,  2,  1,  0,
      5, 0, 8,  8,  0, 10, 0, 12, 0,  11, 9,  0, 1,  0,  1,  0,  1,  1,
      1, 1, 1,  1,  1, 1,  3, 1,  19, 8,  1,  1, 1,  1,  1,  1,  2,  1,
      2, 1, 2,  5,  2, 26, 8, 2,  10, 2,  12, 2, 29, 9,  2,  1,  2,  1,
      2, 1, 2,  1,  2, 0,  0, 3,  0,  2,  4,  0, 0,  33, 0,  9,  1,  0,
      0, 0, 2,  14, 1, 0,  0, 0,  4,  22, 1,  0, 0,  0,  6,  8,  3,  2,
      1, 0, 7,  6,  1, 0,  0, 0,  8,  11, 1,  0, 0,  0,  9,  7,  1,  0,
      0, 0, 9,  10, 1, 0,  0, 0,  10, 12, 1,  0, 0,  0,  11, 9,  1,  0,
      0, 0, 12, 13, 5, 0,  0, 1,  13, 1,  1,  0, 0,  0,  14, 15, 5,  11,
      0, 0, 15, 18, 5, 1,  0, 0,  16, 19, 5,  6, 0,  0,  17, 19, 3,  4,
      2, 0, 18, 16, 1, 0,  0, 0,  18, 17, 1,  0, 0,  0,  19, 20, 1,  0,
      0, 0, 20, 21, 5, 2,  0, 0,  21, 3,  1,  0, 0,  0,  22, 27, 5,  3,
      0, 0, 23, 24, 5, 6,  0, 0,  24, 26, 5,  4, 0,  0,  25, 23, 1,  0,
      0, 0, 26, 29, 1, 0,  0, 0,  27, 25, 1,  0, 0,  0,  27, 28, 1,  0,
      0, 0, 28, 30, 1, 0,  0, 0,  29, 27, 1,  0, 0,  0,  30, 31, 5,  6,
      0, 0, 31, 32, 5, 5,  0, 0,  32, 5,  1,  0, 0,  0,  3,  9,  18, 27};
  staticData->serializedATN = antlr4::atn::SerializedATNView(
      serializedATNSegment,
      sizeof(serializedATNSegment) / sizeof(serializedATNSegment[0]));

  antlr4::atn::ATNDeserializer deserializer;
  staticData->atn = deserializer.deserialize(staticData->serializedATN);

  const size_t count = staticData->atn->getNumberOfDecisions();
  staticData->decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) {
    staticData->decisionToDFA.emplace_back(staticData->atn->getDecisionState(i),
                                           i);
  }
  benchmarkconfigurationshorthandautomaticParserStaticData =
      staticData.release();
}

}  // namespace

BenchmarkConfigurationShorthandAutomaticParser::
    BenchmarkConfigurationShorthandAutomaticParser(TokenStream* input)
    : BenchmarkConfigurationShorthandAutomaticParser(
          input, antlr4::atn::ParserATNSimulatorOptions()) {}

BenchmarkConfigurationShorthandAutomaticParser::
    BenchmarkConfigurationShorthandAutomaticParser(
        TokenStream* input,
        const antlr4::atn::ParserATNSimulatorOptions& options)
    : Parser(input) {
  BenchmarkConfigurationShorthandAutomaticParser::initialize();
  _interpreter = new atn::ParserATNSimulator(
      this, *benchmarkconfigurationshorthandautomaticParserStaticData->atn,
      benchmarkconfigurationshorthandautomaticParserStaticData->decisionToDFA,
      benchmarkconfigurationshorthandautomaticParserStaticData
          ->sharedContextCache,
      options);
}

BenchmarkConfigurationShorthandAutomaticParser::
    ~BenchmarkConfigurationShorthandAutomaticParser() {
  delete _interpreter;
}

const atn::ATN& BenchmarkConfigurationShorthandAutomaticParser::getATN() const {
  return *benchmarkconfigurationshorthandautomaticParserStaticData->atn;
}

std::string BenchmarkConfigurationShorthandAutomaticParser::getGrammarFileName()
    const {
  return "BenchmarkConfigurationShorthandAutomatic.g4";
}

const std::vector<std::string>&
BenchmarkConfigurationShorthandAutomaticParser::getRuleNames() const {
  return benchmarkconfigurationshorthandautomaticParserStaticData->ruleNames;
}

const dfa::Vocabulary&
BenchmarkConfigurationShorthandAutomaticParser::getVocabulary() const {
  return benchmarkconfigurationshorthandautomaticParserStaticData->vocabulary;
}

antlr4::atn::SerializedATNView
BenchmarkConfigurationShorthandAutomaticParser::getSerializedATN() const {
  return benchmarkconfigurationshorthandautomaticParserStaticData
      ->serializedATN;
}

//----------------- AssignmentsContext
//------------------------------------------------------------------

BenchmarkConfigurationShorthandAutomaticParser::AssignmentsContext::
    AssignmentsContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode*
BenchmarkConfigurationShorthandAutomaticParser::AssignmentsContext::EOF() {
  return getToken(BenchmarkConfigurationShorthandAutomaticParser::EOF, 0);
}

std::vector<BenchmarkConfigurationShorthandAutomaticParser::AssignmentContext*>
BenchmarkConfigurationShorthandAutomaticParser::AssignmentsContext::
    assignment() {
  return getRuleContexts<
      BenchmarkConfigurationShorthandAutomaticParser::AssignmentContext>();
}

BenchmarkConfigurationShorthandAutomaticParser::AssignmentContext*
BenchmarkConfigurationShorthandAutomaticParser::AssignmentsContext::assignment(
    size_t i) {
  return getRuleContext<
      BenchmarkConfigurationShorthandAutomaticParser::AssignmentContext>(i);
}

size_t BenchmarkConfigurationShorthandAutomaticParser::AssignmentsContext::
    getRuleIndex() const {
  return BenchmarkConfigurationShorthandAutomaticParser::RuleAssignments;
}

std::any
BenchmarkConfigurationShorthandAutomaticParser::AssignmentsContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor =
          dynamic_cast<BenchmarkConfigurationShorthandAutomaticVisitor*>(
              visitor))
    return parserVisitor->visitAssignments(this);
  else
    return visitor->visitChildren(this);
}

BenchmarkConfigurationShorthandAutomaticParser::AssignmentsContext*
BenchmarkConfigurationShorthandAutomaticParser::assignments() {
  AssignmentsContext* _localctx =
      _tracker.createInstance<AssignmentsContext>(_ctx, getState());
  enterRule(_localctx, 0,
            BenchmarkConfigurationShorthandAutomaticParser::RuleAssignments);
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
    setState(9);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == BenchmarkConfigurationShorthandAutomaticParser::NAME) {
      setState(6);
      antlrcpp::downCast<AssignmentsContext*>(_localctx)->assignmentContext =
          assignment();
      antlrcpp::downCast<AssignmentsContext*>(_localctx)
          ->listOfAssignments.push_back(
              antlrcpp::downCast<AssignmentsContext*>(_localctx)
                  ->assignmentContext);
      setState(11);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(12);
    match(BenchmarkConfigurationShorthandAutomaticParser::EOF);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AssignmentContext
//------------------------------------------------------------------

BenchmarkConfigurationShorthandAutomaticParser::AssignmentContext::
    AssignmentContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode*
BenchmarkConfigurationShorthandAutomaticParser::AssignmentContext::NAME() {
  return getToken(BenchmarkConfigurationShorthandAutomaticParser::NAME, 0);
}

tree::TerminalNode*
BenchmarkConfigurationShorthandAutomaticParser::AssignmentContext::LITERAL() {
  return getToken(BenchmarkConfigurationShorthandAutomaticParser::LITERAL, 0);
}

BenchmarkConfigurationShorthandAutomaticParser::ListContext*
BenchmarkConfigurationShorthandAutomaticParser::AssignmentContext::list() {
  return getRuleContext<
      BenchmarkConfigurationShorthandAutomaticParser::ListContext>(0);
}

size_t BenchmarkConfigurationShorthandAutomaticParser::AssignmentContext::
    getRuleIndex() const {
  return BenchmarkConfigurationShorthandAutomaticParser::RuleAssignment;
}

std::any
BenchmarkConfigurationShorthandAutomaticParser::AssignmentContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor =
          dynamic_cast<BenchmarkConfigurationShorthandAutomaticVisitor*>(
              visitor))
    return parserVisitor->visitAssignment(this);
  else
    return visitor->visitChildren(this);
}

BenchmarkConfigurationShorthandAutomaticParser::AssignmentContext*
BenchmarkConfigurationShorthandAutomaticParser::assignment() {
  AssignmentContext* _localctx =
      _tracker.createInstance<AssignmentContext>(_ctx, getState());
  enterRule(_localctx, 2,
            BenchmarkConfigurationShorthandAutomaticParser::RuleAssignment);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(14);
    match(BenchmarkConfigurationShorthandAutomaticParser::NAME);
    setState(15);
    match(BenchmarkConfigurationShorthandAutomaticParser::T__0);
    setState(18);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case BenchmarkConfigurationShorthandAutomaticParser::LITERAL: {
        setState(16);
        match(BenchmarkConfigurationShorthandAutomaticParser::LITERAL);
        break;
      }

      case BenchmarkConfigurationShorthandAutomaticParser::T__2: {
        setState(17);
        list();
        break;
      }

      default:
        throw NoViableAltException(this);
    }
    setState(20);
    match(BenchmarkConfigurationShorthandAutomaticParser::T__1);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ListContext
//------------------------------------------------------------------

BenchmarkConfigurationShorthandAutomaticParser::ListContext::ListContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<tree::TerminalNode*>
BenchmarkConfigurationShorthandAutomaticParser::ListContext::LITERAL() {
  return getTokens(BenchmarkConfigurationShorthandAutomaticParser::LITERAL);
}

tree::TerminalNode*
BenchmarkConfigurationShorthandAutomaticParser::ListContext::LITERAL(size_t i) {
  return getToken(BenchmarkConfigurationShorthandAutomaticParser::LITERAL, i);
}

size_t
BenchmarkConfigurationShorthandAutomaticParser::ListContext::getRuleIndex()
    const {
  return BenchmarkConfigurationShorthandAutomaticParser::RuleList;
}

std::any BenchmarkConfigurationShorthandAutomaticParser::ListContext::accept(
    tree::ParseTreeVisitor* visitor) {
  if (auto parserVisitor =
          dynamic_cast<BenchmarkConfigurationShorthandAutomaticVisitor*>(
              visitor))
    return parserVisitor->visitList(this);
  else
    return visitor->visitChildren(this);
}

BenchmarkConfigurationShorthandAutomaticParser::ListContext*
BenchmarkConfigurationShorthandAutomaticParser::list() {
  ListContext* _localctx =
      _tracker.createInstance<ListContext>(_ctx, getState());
  enterRule(_localctx, 4,
            BenchmarkConfigurationShorthandAutomaticParser::RuleList);

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
    setState(22);
    match(BenchmarkConfigurationShorthandAutomaticParser::T__2);
    setState(27);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 2,
                                                                     _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(23);
        antlrcpp::downCast<ListContext*>(_localctx)->literalToken =
            match(BenchmarkConfigurationShorthandAutomaticParser::LITERAL);
        antlrcpp::downCast<ListContext*>(_localctx)->listElement.push_back(
            antlrcpp::downCast<ListContext*>(_localctx)->literalToken);
        setState(24);
        match(BenchmarkConfigurationShorthandAutomaticParser::T__3);
      }
      setState(29);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                       2, _ctx);
    }
    setState(30);
    antlrcpp::downCast<ListContext*>(_localctx)->literalToken =
        match(BenchmarkConfigurationShorthandAutomaticParser::LITERAL);
    antlrcpp::downCast<ListContext*>(_localctx)->listElement.push_back(
        antlrcpp::downCast<ListContext*>(_localctx)->literalToken);
    setState(31);
    match(BenchmarkConfigurationShorthandAutomaticParser::T__4);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

void BenchmarkConfigurationShorthandAutomaticParser::initialize() {
  ::antlr4::internal::call_once(
      benchmarkconfigurationshorthandautomaticParserOnceFlag,
      benchmarkconfigurationshorthandautomaticParserInitialize);
}
