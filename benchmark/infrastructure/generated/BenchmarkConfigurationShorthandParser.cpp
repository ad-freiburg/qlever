
// Generated from BenchmarkConfigurationShorthand.g4 by ANTLR 4.12.0

#include "BenchmarkConfigurationShorthandParser.h"

#include "BenchmarkConfigurationShorthandListener.h"

using namespace antlrcpp;

using namespace antlr4;

namespace {

struct BenchmarkConfigurationShorthandParserStaticData final {
  BenchmarkConfigurationShorthandParserStaticData(
      std::vector<std::string> ruleNames, std::vector<std::string> literalNames,
      std::vector<std::string> symbolicNames)
      : ruleNames(std::move(ruleNames)),
        literalNames(std::move(literalNames)),
        symbolicNames(std::move(symbolicNames)),
        vocabulary(this->literalNames, this->symbolicNames) {}

  BenchmarkConfigurationShorthandParserStaticData(
      const BenchmarkConfigurationShorthandParserStaticData&) = delete;
  BenchmarkConfigurationShorthandParserStaticData(
      BenchmarkConfigurationShorthandParserStaticData&&) = delete;
  BenchmarkConfigurationShorthandParserStaticData& operator=(
      const BenchmarkConfigurationShorthandParserStaticData&) = delete;
  BenchmarkConfigurationShorthandParserStaticData& operator=(
      BenchmarkConfigurationShorthandParserStaticData&&) = delete;

  std::vector<antlr4::dfa::DFA> decisionToDFA;
  antlr4::atn::PredictionContextCache sharedContextCache;
  const std::vector<std::string> ruleNames;
  const std::vector<std::string> literalNames;
  const std::vector<std::string> symbolicNames;
  const antlr4::dfa::Vocabulary vocabulary;
  antlr4::atn::SerializedATNView serializedATN;
  std::unique_ptr<antlr4::atn::ATN> atn;
};

::antlr4::internal::OnceFlag benchmarkconfigurationshorthandParserOnceFlag;
BenchmarkConfigurationShorthandParserStaticData*
    benchmarkconfigurationshorthandParserStaticData = nullptr;

void benchmarkconfigurationshorthandParserInitialize() {
  assert(benchmarkconfigurationshorthandParserStaticData == nullptr);
  auto staticData =
      std::make_unique<BenchmarkConfigurationShorthandParserStaticData>(
          std::vector<std::string>{"shortHandString", "assignments",
                                   "assignment", "object", "list", "content"},
          std::vector<std::string>{"", "','", "':'", "'{'", "'}'", "'['",
                                   "']'"},
          std::vector<std::string>{"", "", "", "", "", "", "", "LITERAL",
                                   "BOOL", "INTEGER", "FLOAT", "STRING", "NAME",
                                   "WHITESPACE"});
  static const int32_t serializedATNSegment[] = {
      4, 1, 13, 51, 2,  0, 7,  0, 2, 1,  7,  1,  2,  2,  7,  2,  2,  3,
      7, 3, 2,  4,  7,  4, 2,  5, 7, 5,  1,  0,  1,  0,  1,  0,  1,  1,
      1, 1, 1,  1,  5,  1, 19, 8, 1, 10, 1,  12, 1,  22, 9,  1,  1,  1,
      1, 1, 1,  2,  1,  2, 1,  2, 1, 2,  1,  3,  1,  3,  1,  3,  1,  3,
      1, 4, 1,  4,  1,  4, 1,  4, 5, 4,  38, 8,  4,  10, 4,  12, 4,  41,
      9, 4, 1,  4,  1,  4, 1,  4, 1, 5,  1,  5,  1,  5,  3,  5,  49, 8,
      5, 1, 5,  0,  0,  6, 0,  2, 4, 6,  8,  10, 0,  0,  48, 0,  12, 1,
      0, 0, 0,  2,  20, 1, 0,  0, 0, 4,  25, 1,  0,  0,  0,  6,  29, 1,
      0, 0, 0,  8,  33, 1, 0,  0, 0, 10, 48, 1,  0,  0,  0,  12, 13, 3,
      2, 1, 0,  13, 14, 5, 0,  0, 1, 14, 1,  1,  0,  0,  0,  15, 16, 3,
      4, 2, 0,  16, 17, 5, 1,  0, 0, 17, 19, 1,  0,  0,  0,  18, 15, 1,
      0, 0, 0,  19, 22, 1, 0,  0, 0, 20, 18, 1,  0,  0,  0,  20, 21, 1,
      0, 0, 0,  21, 23, 1, 0,  0, 0, 22, 20, 1,  0,  0,  0,  23, 24, 3,
      4, 2, 0,  24, 3,  1, 0,  0, 0, 25, 26, 5,  12, 0,  0,  26, 27, 5,
      2, 0, 0,  27, 28, 3, 10, 5, 0, 28, 5,  1,  0,  0,  0,  29, 30, 5,
      3, 0, 0,  30, 31, 3, 2,  1, 0, 31, 32, 5,  4,  0,  0,  32, 7,  1,
      0, 0, 0,  33, 39, 5, 5,  0, 0, 34, 35, 3,  10, 5,  0,  35, 36, 5,
      1, 0, 0,  36, 38, 1, 0,  0, 0, 37, 34, 1,  0,  0,  0,  38, 41, 1,
      0, 0, 0,  39, 37, 1, 0,  0, 0, 39, 40, 1,  0,  0,  0,  40, 42, 1,
      0, 0, 0,  41, 39, 1, 0,  0, 0, 42, 43, 3,  10, 5,  0,  43, 44, 5,
      6, 0, 0,  44, 9,  1, 0,  0, 0, 45, 49, 5,  7,  0,  0,  46, 49, 3,
      8, 4, 0,  47, 49, 3, 6,  3, 0, 48, 45, 1,  0,  0,  0,  48, 46, 1,
      0, 0, 0,  48, 47, 1, 0,  0, 0, 49, 11, 1,  0,  0,  0,  3,  20, 39,
      48};
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
  benchmarkconfigurationshorthandParserStaticData = staticData.release();
}

}  // namespace

BenchmarkConfigurationShorthandParser::BenchmarkConfigurationShorthandParser(
    TokenStream* input)
    : BenchmarkConfigurationShorthandParser(
          input, antlr4::atn::ParserATNSimulatorOptions()) {}

BenchmarkConfigurationShorthandParser::BenchmarkConfigurationShorthandParser(
    TokenStream* input, const antlr4::atn::ParserATNSimulatorOptions& options)
    : Parser(input) {
  BenchmarkConfigurationShorthandParser::initialize();
  _interpreter = new atn::ParserATNSimulator(
      this, *benchmarkconfigurationshorthandParserStaticData->atn,
      benchmarkconfigurationshorthandParserStaticData->decisionToDFA,
      benchmarkconfigurationshorthandParserStaticData->sharedContextCache,
      options);
}

BenchmarkConfigurationShorthandParser::
    ~BenchmarkConfigurationShorthandParser() {
  delete _interpreter;
}

const atn::ATN& BenchmarkConfigurationShorthandParser::getATN() const {
  return *benchmarkconfigurationshorthandParserStaticData->atn;
}

std::string BenchmarkConfigurationShorthandParser::getGrammarFileName() const {
  return "BenchmarkConfigurationShorthand.g4";
}

const std::vector<std::string>&
BenchmarkConfigurationShorthandParser::getRuleNames() const {
  return benchmarkconfigurationshorthandParserStaticData->ruleNames;
}

const dfa::Vocabulary& BenchmarkConfigurationShorthandParser::getVocabulary()
    const {
  return benchmarkconfigurationshorthandParserStaticData->vocabulary;
}

antlr4::atn::SerializedATNView
BenchmarkConfigurationShorthandParser::getSerializedATN() const {
  return benchmarkconfigurationshorthandParserStaticData->serializedATN;
}

//----------------- ShortHandStringContext
//------------------------------------------------------------------

BenchmarkConfigurationShorthandParser::ShortHandStringContext::
    ShortHandStringContext(ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

BenchmarkConfigurationShorthandParser::AssignmentsContext*
BenchmarkConfigurationShorthandParser::ShortHandStringContext::assignments() {
  return getRuleContext<
      BenchmarkConfigurationShorthandParser::AssignmentsContext>(0);
}

tree::TerminalNode*
BenchmarkConfigurationShorthandParser::ShortHandStringContext::EOF() {
  return getToken(BenchmarkConfigurationShorthandParser::EOF, 0);
}

size_t
BenchmarkConfigurationShorthandParser::ShortHandStringContext::getRuleIndex()
    const {
  return BenchmarkConfigurationShorthandParser::RuleShortHandString;
}

void BenchmarkConfigurationShorthandParser::ShortHandStringContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener =
      dynamic_cast<BenchmarkConfigurationShorthandListener*>(listener);
  if (parserListener != nullptr) parserListener->enterShortHandString(this);
}

void BenchmarkConfigurationShorthandParser::ShortHandStringContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener =
      dynamic_cast<BenchmarkConfigurationShorthandListener*>(listener);
  if (parserListener != nullptr) parserListener->exitShortHandString(this);
}

BenchmarkConfigurationShorthandParser::ShortHandStringContext*
BenchmarkConfigurationShorthandParser::shortHandString() {
  ShortHandStringContext* _localctx =
      _tracker.createInstance<ShortHandStringContext>(_ctx, getState());
  enterRule(_localctx, 0,
            BenchmarkConfigurationShorthandParser::RuleShortHandString);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(12);
    assignments();
    setState(13);
    match(BenchmarkConfigurationShorthandParser::EOF);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AssignmentsContext
//------------------------------------------------------------------

BenchmarkConfigurationShorthandParser::AssignmentsContext::AssignmentsContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<BenchmarkConfigurationShorthandParser::AssignmentContext*>
BenchmarkConfigurationShorthandParser::AssignmentsContext::assignment() {
  return getRuleContexts<
      BenchmarkConfigurationShorthandParser::AssignmentContext>();
}

BenchmarkConfigurationShorthandParser::AssignmentContext*
BenchmarkConfigurationShorthandParser::AssignmentsContext::assignment(
    size_t i) {
  return getRuleContext<
      BenchmarkConfigurationShorthandParser::AssignmentContext>(i);
}

size_t BenchmarkConfigurationShorthandParser::AssignmentsContext::getRuleIndex()
    const {
  return BenchmarkConfigurationShorthandParser::RuleAssignments;
}

void BenchmarkConfigurationShorthandParser::AssignmentsContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener =
      dynamic_cast<BenchmarkConfigurationShorthandListener*>(listener);
  if (parserListener != nullptr) parserListener->enterAssignments(this);
}

void BenchmarkConfigurationShorthandParser::AssignmentsContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener =
      dynamic_cast<BenchmarkConfigurationShorthandListener*>(listener);
  if (parserListener != nullptr) parserListener->exitAssignments(this);
}

BenchmarkConfigurationShorthandParser::AssignmentsContext*
BenchmarkConfigurationShorthandParser::assignments() {
  AssignmentsContext* _localctx =
      _tracker.createInstance<AssignmentsContext>(_ctx, getState());
  enterRule(_localctx, 2,
            BenchmarkConfigurationShorthandParser::RuleAssignments);

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
    setState(20);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 0,
                                                                     _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(15);
        antlrcpp::downCast<AssignmentsContext*>(_localctx)->assignmentContext =
            assignment();
        antlrcpp::downCast<AssignmentsContext*>(_localctx)
            ->listOfAssignments.push_back(
                antlrcpp::downCast<AssignmentsContext*>(_localctx)
                    ->assignmentContext);
        setState(16);
        match(BenchmarkConfigurationShorthandParser::T__0);
      }
      setState(22);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                       0, _ctx);
    }
    setState(23);
    antlrcpp::downCast<AssignmentsContext*>(_localctx)->assignmentContext =
        assignment();
    antlrcpp::downCast<AssignmentsContext*>(_localctx)
        ->listOfAssignments.push_back(
            antlrcpp::downCast<AssignmentsContext*>(_localctx)
                ->assignmentContext);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- AssignmentContext
//------------------------------------------------------------------

BenchmarkConfigurationShorthandParser::AssignmentContext::AssignmentContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode*
BenchmarkConfigurationShorthandParser::AssignmentContext::NAME() {
  return getToken(BenchmarkConfigurationShorthandParser::NAME, 0);
}

BenchmarkConfigurationShorthandParser::ContentContext*
BenchmarkConfigurationShorthandParser::AssignmentContext::content() {
  return getRuleContext<BenchmarkConfigurationShorthandParser::ContentContext>(
      0);
}

size_t BenchmarkConfigurationShorthandParser::AssignmentContext::getRuleIndex()
    const {
  return BenchmarkConfigurationShorthandParser::RuleAssignment;
}

void BenchmarkConfigurationShorthandParser::AssignmentContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener =
      dynamic_cast<BenchmarkConfigurationShorthandListener*>(listener);
  if (parserListener != nullptr) parserListener->enterAssignment(this);
}

void BenchmarkConfigurationShorthandParser::AssignmentContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener =
      dynamic_cast<BenchmarkConfigurationShorthandListener*>(listener);
  if (parserListener != nullptr) parserListener->exitAssignment(this);
}

BenchmarkConfigurationShorthandParser::AssignmentContext*
BenchmarkConfigurationShorthandParser::assignment() {
  AssignmentContext* _localctx =
      _tracker.createInstance<AssignmentContext>(_ctx, getState());
  enterRule(_localctx, 4,
            BenchmarkConfigurationShorthandParser::RuleAssignment);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(25);
    match(BenchmarkConfigurationShorthandParser::NAME);
    setState(26);
    match(BenchmarkConfigurationShorthandParser::T__1);
    setState(27);
    content();

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ObjectContext
//------------------------------------------------------------------

BenchmarkConfigurationShorthandParser::ObjectContext::ObjectContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

BenchmarkConfigurationShorthandParser::AssignmentsContext*
BenchmarkConfigurationShorthandParser::ObjectContext::assignments() {
  return getRuleContext<
      BenchmarkConfigurationShorthandParser::AssignmentsContext>(0);
}

size_t BenchmarkConfigurationShorthandParser::ObjectContext::getRuleIndex()
    const {
  return BenchmarkConfigurationShorthandParser::RuleObject;
}

void BenchmarkConfigurationShorthandParser::ObjectContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener =
      dynamic_cast<BenchmarkConfigurationShorthandListener*>(listener);
  if (parserListener != nullptr) parserListener->enterObject(this);
}

void BenchmarkConfigurationShorthandParser::ObjectContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener =
      dynamic_cast<BenchmarkConfigurationShorthandListener*>(listener);
  if (parserListener != nullptr) parserListener->exitObject(this);
}

BenchmarkConfigurationShorthandParser::ObjectContext*
BenchmarkConfigurationShorthandParser::object() {
  ObjectContext* _localctx =
      _tracker.createInstance<ObjectContext>(_ctx, getState());
  enterRule(_localctx, 6, BenchmarkConfigurationShorthandParser::RuleObject);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(29);
    match(BenchmarkConfigurationShorthandParser::T__2);
    setState(30);
    assignments();
    setState(31);
    match(BenchmarkConfigurationShorthandParser::T__3);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ListContext
//------------------------------------------------------------------

BenchmarkConfigurationShorthandParser::ListContext::ListContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

std::vector<BenchmarkConfigurationShorthandParser::ContentContext*>
BenchmarkConfigurationShorthandParser::ListContext::content() {
  return getRuleContexts<
      BenchmarkConfigurationShorthandParser::ContentContext>();
}

BenchmarkConfigurationShorthandParser::ContentContext*
BenchmarkConfigurationShorthandParser::ListContext::content(size_t i) {
  return getRuleContext<BenchmarkConfigurationShorthandParser::ContentContext>(
      i);
}

size_t BenchmarkConfigurationShorthandParser::ListContext::getRuleIndex()
    const {
  return BenchmarkConfigurationShorthandParser::RuleList;
}

void BenchmarkConfigurationShorthandParser::ListContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener =
      dynamic_cast<BenchmarkConfigurationShorthandListener*>(listener);
  if (parserListener != nullptr) parserListener->enterList(this);
}

void BenchmarkConfigurationShorthandParser::ListContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener =
      dynamic_cast<BenchmarkConfigurationShorthandListener*>(listener);
  if (parserListener != nullptr) parserListener->exitList(this);
}

BenchmarkConfigurationShorthandParser::ListContext*
BenchmarkConfigurationShorthandParser::list() {
  ListContext* _localctx =
      _tracker.createInstance<ListContext>(_ctx, getState());
  enterRule(_localctx, 8, BenchmarkConfigurationShorthandParser::RuleList);

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
    setState(33);
    match(BenchmarkConfigurationShorthandParser::T__4);
    setState(39);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 1,
                                                                     _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(34);
        antlrcpp::downCast<ListContext*>(_localctx)->contentContext = content();
        antlrcpp::downCast<ListContext*>(_localctx)->listElement.push_back(
            antlrcpp::downCast<ListContext*>(_localctx)->contentContext);
        setState(35);
        match(BenchmarkConfigurationShorthandParser::T__0);
      }
      setState(41);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input,
                                                                       1, _ctx);
    }
    setState(42);
    antlrcpp::downCast<ListContext*>(_localctx)->contentContext = content();
    antlrcpp::downCast<ListContext*>(_localctx)->listElement.push_back(
        antlrcpp::downCast<ListContext*>(_localctx)->contentContext);
    setState(43);
    match(BenchmarkConfigurationShorthandParser::T__5);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- ContentContext
//------------------------------------------------------------------

BenchmarkConfigurationShorthandParser::ContentContext::ContentContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode*
BenchmarkConfigurationShorthandParser::ContentContext::LITERAL() {
  return getToken(BenchmarkConfigurationShorthandParser::LITERAL, 0);
}

BenchmarkConfigurationShorthandParser::ListContext*
BenchmarkConfigurationShorthandParser::ContentContext::list() {
  return getRuleContext<BenchmarkConfigurationShorthandParser::ListContext>(0);
}

BenchmarkConfigurationShorthandParser::ObjectContext*
BenchmarkConfigurationShorthandParser::ContentContext::object() {
  return getRuleContext<BenchmarkConfigurationShorthandParser::ObjectContext>(
      0);
}

size_t BenchmarkConfigurationShorthandParser::ContentContext::getRuleIndex()
    const {
  return BenchmarkConfigurationShorthandParser::RuleContent;
}

void BenchmarkConfigurationShorthandParser::ContentContext::enterRule(
    tree::ParseTreeListener* listener) {
  auto parserListener =
      dynamic_cast<BenchmarkConfigurationShorthandListener*>(listener);
  if (parserListener != nullptr) parserListener->enterContent(this);
}

void BenchmarkConfigurationShorthandParser::ContentContext::exitRule(
    tree::ParseTreeListener* listener) {
  auto parserListener =
      dynamic_cast<BenchmarkConfigurationShorthandListener*>(listener);
  if (parserListener != nullptr) parserListener->exitContent(this);
}

BenchmarkConfigurationShorthandParser::ContentContext*
BenchmarkConfigurationShorthandParser::content() {
  ContentContext* _localctx =
      _tracker.createInstance<ContentContext>(_ctx, getState());
  enterRule(_localctx, 10, BenchmarkConfigurationShorthandParser::RuleContent);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(48);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case BenchmarkConfigurationShorthandParser::LITERAL: {
        enterOuterAlt(_localctx, 1);
        setState(45);
        match(BenchmarkConfigurationShorthandParser::LITERAL);
        break;
      }

      case BenchmarkConfigurationShorthandParser::T__4: {
        enterOuterAlt(_localctx, 2);
        setState(46);
        list();
        break;
      }

      case BenchmarkConfigurationShorthandParser::T__2: {
        enterOuterAlt(_localctx, 3);
        setState(47);
        object();
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

void BenchmarkConfigurationShorthandParser::initialize() {
  ::antlr4::internal::call_once(
      benchmarkconfigurationshorthandParserOnceFlag,
      benchmarkconfigurationshorthandParserInitialize);
}
