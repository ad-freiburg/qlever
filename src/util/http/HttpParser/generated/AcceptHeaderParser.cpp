
// Generated from AcceptHeader.g4 by ANTLR 4.11.1

#include "AcceptHeaderParser.h"

#include "AcceptHeaderListener.h"
#include "AcceptHeaderVisitor.h"

using namespace antlrcpp;

using namespace antlr4;

namespace {

struct AcceptHeaderParserStaticData final {
  AcceptHeaderParserStaticData(std::vector<std::string> ruleNames,
                               std::vector<std::string> literalNames,
                               std::vector<std::string> symbolicNames)
      : ruleNames(std::move(ruleNames)),
        literalNames(std::move(literalNames)),
        symbolicNames(std::move(symbolicNames)),
        vocabulary(this->literalNames, this->symbolicNames) {}

  AcceptHeaderParserStaticData(const AcceptHeaderParserStaticData&) = delete;
  AcceptHeaderParserStaticData(AcceptHeaderParserStaticData&&) = delete;
  AcceptHeaderParserStaticData& operator=(const AcceptHeaderParserStaticData&) =
      delete;
  AcceptHeaderParserStaticData& operator=(AcceptHeaderParserStaticData&&) =
      delete;

  std::vector<antlr4::dfa::DFA> decisionToDFA;
  antlr4::atn::PredictionContextCache sharedContextCache;
  const std::vector<std::string> ruleNames;
  const std::vector<std::string> literalNames;
  const std::vector<std::string> symbolicNames;
  const antlr4::dfa::Vocabulary vocabulary;
  antlr4::atn::SerializedATNView serializedATN;
  std::unique_ptr<antlr4::atn::ATN> atn;
};

::antlr4::internal::OnceFlag acceptheaderParserOnceFlag;
AcceptHeaderParserStaticData* acceptheaderParserStaticData = nullptr;

void acceptheaderParserInitialize() {
  assert(acceptheaderParserStaticData == nullptr);
  auto staticData = std::make_unique<AcceptHeaderParserStaticData>(
      std::vector<std::string>{"accept", "acceptWithEof", "rangeAndParams",
                               "mediaRange", "type", "subtype", "acceptParams",
                               "weight", "qvalue", "acceptExt", "parameter",
                               "token", "tchar", "quotedString", "quoted_pair"},
      std::vector<std::string>{
          "",          "','", "';'", "'='",  "'\\'",      "",         "",
          "",          "",    "",    "'-'",  "'.'",       "'_'",      "'~'",
          "'\\u003F'", "'/'", "'!'", "':'",  "'@'",       "'$'",      "'#'",
          "'&'",       "'%'", "'''", "'*'",  "'+'",       "'^'",      "'`'",
          "'|'",       "",    "",    "'\"'", "'\\u0020'", "'\\u0009'"},
      std::vector<std::string>{"",
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
                               "VCHAR"});
  static const int32_t serializedATNSegment[] = {
      4,   1,   34,  172, 2,   0,   7,   0,   2,   1,   7,   1,   2,   2,   7,
      2,   2,   3,   7,   3,   2,   4,   7,   4,   2,   5,   7,   5,   2,   6,
      7,   6,   2,   7,   7,   7,   2,   8,   7,   8,   2,   9,   7,   9,   2,
      10,  7,   10,  2,   11,  7,   11,  2,   12,  7,   12,  2,   13,  7,   13,
      2,   14,  7,   14,  1,   0,   1,   0,   5,   0,   33,  8,   0,   10,  0,
      12,  0,   36,  9,   0,   1,   0,   1,   0,   5,   0,   40,  8,   0,   10,
      0,   12,  0,   43,  9,   0,   1,   0,   5,   0,   46,  8,   0,   10,  0,
      12,  0,   49,  9,   0,   1,   1,   1,   1,   1,   1,   1,   2,   1,   2,
      3,   2,   56,  8,   2,   1,   3,   1,   3,   1,   3,   1,   3,   1,   3,
      1,   3,   1,   3,   1,   3,   1,   3,   3,   3,   67,  8,   3,   1,   3,
      5,   3,   70,  8,   3,   10,  3,   12,  3,   73,  9,   3,   1,   3,   1,
      3,   5,   3,   77,  8,   3,   10,  3,   12,  3,   80,  9,   3,   1,   3,
      5,   3,   83,  8,   3,   10,  3,   12,  3,   86,  9,   3,   1,   4,   1,
      4,   1,   5,   1,   5,   1,   6,   1,   6,   5,   6,   94,  8,   6,   10,
      6,   12,  6,   97,  9,   6,   1,   7,   5,   7,   100, 8,   7,   10,  7,
      12,  7,   103, 9,   7,   1,   7,   1,   7,   5,   7,   107, 8,   7,   10,
      7,   12,  7,   110, 9,   7,   1,   7,   1,   7,   1,   7,   1,   8,   1,
      8,   1,   8,   5,   8,   118, 8,   8,   10,  8,   12,  8,   121, 9,   8,
      3,   8,   123, 8,   8,   1,   9,   5,   9,   126, 8,   9,   10,  9,   12,
      9,   129, 9,   9,   1,   9,   1,   9,   5,   9,   133, 8,   9,   10,  9,
      12,  9,   136, 9,   9,   1,   9,   1,   9,   1,   9,   1,   9,   3,   9,
      142, 8,   9,   3,   9,   144, 8,   9,   1,   10,  1,   10,  1,   10,  1,
      10,  3,   10,  150, 8,   10,  1,   11,  4,   11,  153, 8,   11,  11,  11,
      12,  11,  154, 1,   12,  1,   12,  1,   13,  1,   13,  1,   13,  5,   13,
      162, 8,   13,  10,  13,  12,  13,  165, 9,   13,  1,   13,  1,   13,  1,
      14,  1,   14,  1,   14,  1,   14,  0,   0,   15,  0,   2,   4,   6,   8,
      10,  12,  14,  16,  18,  20,  22,  24,  26,  28,  0,   2,   4,   0,   7,
      8,   10,  13,  16,  16,  19,  28,  2,   0,   30,  30,  32,  34,  178, 0,
      30,  1,   0,   0,   0,   2,   50,  1,   0,   0,   0,   4,   53,  1,   0,
      0,   0,   6,   66,  1,   0,   0,   0,   8,   87,  1,   0,   0,   0,   10,
      89,  1,   0,   0,   0,   12,  91,  1,   0,   0,   0,   14,  101, 1,   0,
      0,   0,   16,  114, 1,   0,   0,   0,   18,  127, 1,   0,   0,   0,   20,
      145, 1,   0,   0,   0,   22,  152, 1,   0,   0,   0,   24,  156, 1,   0,
      0,   0,   26,  158, 1,   0,   0,   0,   28,  168, 1,   0,   0,   0,   30,
      47,  3,   4,   2,   0,   31,  33,  5,   9,   0,   0,   32,  31,  1,   0,
      0,   0,   33,  36,  1,   0,   0,   0,   34,  32,  1,   0,   0,   0,   34,
      35,  1,   0,   0,   0,   35,  37,  1,   0,   0,   0,   36,  34,  1,   0,
      0,   0,   37,  41,  5,   1,   0,   0,   38,  40,  5,   9,   0,   0,   39,
      38,  1,   0,   0,   0,   40,  43,  1,   0,   0,   0,   41,  39,  1,   0,
      0,   0,   41,  42,  1,   0,   0,   0,   42,  44,  1,   0,   0,   0,   43,
      41,  1,   0,   0,   0,   44,  46,  3,   4,   2,   0,   45,  34,  1,   0,
      0,   0,   46,  49,  1,   0,   0,   0,   47,  45,  1,   0,   0,   0,   47,
      48,  1,   0,   0,   0,   48,  1,   1,   0,   0,   0,   49,  47,  1,   0,
      0,   0,   50,  51,  3,   0,   0,   0,   51,  52,  5,   0,   0,   1,   52,
      3,   1,   0,   0,   0,   53,  55,  3,   6,   3,   0,   54,  56,  3,   12,
      6,   0,   55,  54,  1,   0,   0,   0,   55,  56,  1,   0,   0,   0,   56,
      5,   1,   0,   0,   0,   57,  67,  5,   5,   0,   0,   58,  59,  3,   8,
      4,   0,   59,  60,  5,   15,  0,   0,   60,  61,  5,   24,  0,   0,   61,
      67,  1,   0,   0,   0,   62,  63,  3,   8,   4,   0,   63,  64,  5,   15,
      0,   0,   64,  65,  3,   10,  5,   0,   65,  67,  1,   0,   0,   0,   66,
      57,  1,   0,   0,   0,   66,  58,  1,   0,   0,   0,   66,  62,  1,   0,
      0,   0,   67,  84,  1,   0,   0,   0,   68,  70,  5,   9,   0,   0,   69,
      68,  1,   0,   0,   0,   70,  73,  1,   0,   0,   0,   71,  69,  1,   0,
      0,   0,   71,  72,  1,   0,   0,   0,   72,  74,  1,   0,   0,   0,   73,
      71,  1,   0,   0,   0,   74,  78,  5,   2,   0,   0,   75,  77,  5,   9,
      0,   0,   76,  75,  1,   0,   0,   0,   77,  80,  1,   0,   0,   0,   78,
      76,  1,   0,   0,   0,   78,  79,  1,   0,   0,   0,   79,  81,  1,   0,
      0,   0,   80,  78,  1,   0,   0,   0,   81,  83,  3,   20,  10,  0,   82,
      71,  1,   0,   0,   0,   83,  86,  1,   0,   0,   0,   84,  82,  1,   0,
      0,   0,   84,  85,  1,   0,   0,   0,   85,  7,   1,   0,   0,   0,   86,
      84,  1,   0,   0,   0,   87,  88,  3,   22,  11,  0,   88,  9,   1,   0,
      0,   0,   89,  90,  3,   22,  11,  0,   90,  11,  1,   0,   0,   0,   91,
      95,  3,   14,  7,   0,   92,  94,  3,   18,  9,   0,   93,  92,  1,   0,
      0,   0,   94,  97,  1,   0,   0,   0,   95,  93,  1,   0,   0,   0,   95,
      96,  1,   0,   0,   0,   96,  13,  1,   0,   0,   0,   97,  95,  1,   0,
      0,   0,   98,  100, 5,   9,   0,   0,   99,  98,  1,   0,   0,   0,   100,
      103, 1,   0,   0,   0,   101, 99,  1,   0,   0,   0,   101, 102, 1,   0,
      0,   0,   102, 104, 1,   0,   0,   0,   103, 101, 1,   0,   0,   0,   104,
      108, 5,   2,   0,   0,   105, 107, 5,   9,   0,   0,   106, 105, 1,   0,
      0,   0,   107, 110, 1,   0,   0,   0,   108, 106, 1,   0,   0,   0,   108,
      109, 1,   0,   0,   0,   109, 111, 1,   0,   0,   0,   110, 108, 1,   0,
      0,   0,   111, 112, 5,   6,   0,   0,   112, 113, 3,   16,  8,   0,   113,
      15,  1,   0,   0,   0,   114, 122, 5,   7,   0,   0,   115, 119, 5,   11,
      0,   0,   116, 118, 5,   7,   0,   0,   117, 116, 1,   0,   0,   0,   118,
      121, 1,   0,   0,   0,   119, 117, 1,   0,   0,   0,   119, 120, 1,   0,
      0,   0,   120, 123, 1,   0,   0,   0,   121, 119, 1,   0,   0,   0,   122,
      115, 1,   0,   0,   0,   122, 123, 1,   0,   0,   0,   123, 17,  1,   0,
      0,   0,   124, 126, 5,   9,   0,   0,   125, 124, 1,   0,   0,   0,   126,
      129, 1,   0,   0,   0,   127, 125, 1,   0,   0,   0,   127, 128, 1,   0,
      0,   0,   128, 130, 1,   0,   0,   0,   129, 127, 1,   0,   0,   0,   130,
      134, 5,   2,   0,   0,   131, 133, 5,   9,   0,   0,   132, 131, 1,   0,
      0,   0,   133, 136, 1,   0,   0,   0,   134, 132, 1,   0,   0,   0,   134,
      135, 1,   0,   0,   0,   135, 137, 1,   0,   0,   0,   136, 134, 1,   0,
      0,   0,   137, 143, 3,   22,  11,  0,   138, 141, 5,   3,   0,   0,   139,
      142, 3,   22,  11,  0,   140, 142, 3,   26,  13,  0,   141, 139, 1,   0,
      0,   0,   141, 140, 1,   0,   0,   0,   142, 144, 1,   0,   0,   0,   143,
      138, 1,   0,   0,   0,   143, 144, 1,   0,   0,   0,   144, 19,  1,   0,
      0,   0,   145, 146, 3,   22,  11,  0,   146, 149, 5,   3,   0,   0,   147,
      150, 3,   22,  11,  0,   148, 150, 3,   26,  13,  0,   149, 147, 1,   0,
      0,   0,   149, 148, 1,   0,   0,   0,   150, 21,  1,   0,   0,   0,   151,
      153, 3,   24,  12,  0,   152, 151, 1,   0,   0,   0,   153, 154, 1,   0,
      0,   0,   154, 152, 1,   0,   0,   0,   154, 155, 1,   0,   0,   0,   155,
      23,  1,   0,   0,   0,   156, 157, 7,   0,   0,   0,   157, 25,  1,   0,
      0,   0,   158, 163, 5,   31,  0,   0,   159, 162, 5,   29,  0,   0,   160,
      162, 3,   28,  14,  0,   161, 159, 1,   0,   0,   0,   161, 160, 1,   0,
      0,   0,   162, 165, 1,   0,   0,   0,   163, 161, 1,   0,   0,   0,   163,
      164, 1,   0,   0,   0,   164, 166, 1,   0,   0,   0,   165, 163, 1,   0,
      0,   0,   166, 167, 5,   31,  0,   0,   167, 27,  1,   0,   0,   0,   168,
      169, 5,   4,   0,   0,   169, 170, 7,   1,   0,   0,   170, 29,  1,   0,
      0,   0,   21,  34,  41,  47,  55,  66,  71,  78,  84,  95,  101, 108, 119,
      122, 127, 134, 141, 143, 149, 154, 161, 163};
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
  acceptheaderParserStaticData = staticData.release();
}

}  // namespace

AcceptHeaderParser::AcceptHeaderParser(TokenStream* input)
    : AcceptHeaderParser(input, antlr4::atn::ParserATNSimulatorOptions()) {}

AcceptHeaderParser::AcceptHeaderParser(
    TokenStream* input, const antlr4::atn::ParserATNSimulatorOptions& options)
    : Parser(input) {
  AcceptHeaderParser::initialize();
  _interpreter = new atn::ParserATNSimulator(
      this, *acceptheaderParserStaticData->atn,
      acceptheaderParserStaticData->decisionToDFA,
      acceptheaderParserStaticData->sharedContextCache, options);
}

AcceptHeaderParser::~AcceptHeaderParser() { delete _interpreter; }

const atn::ATN& AcceptHeaderParser::getATN() const {
  return *acceptheaderParserStaticData->atn;
}

std::string AcceptHeaderParser::getGrammarFileName() const {
  return "AcceptHeader.g4";
}

const std::vector<std::string>& AcceptHeaderParser::getRuleNames() const {
  return acceptheaderParserStaticData->ruleNames;
}

const dfa::Vocabulary& AcceptHeaderParser::getVocabulary() const {
  return acceptheaderParserStaticData->vocabulary;
}

antlr4::atn::SerializedATNView AcceptHeaderParser::getSerializedATN() const {
  return acceptheaderParserStaticData->serializedATN;
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

std::any AcceptHeaderParser::AcceptContext::accept(
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

std::any AcceptHeaderParser::AcceptWithEofContext::accept(
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

std::any AcceptHeaderParser::RangeAndParamsContext::accept(
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

std::any AcceptHeaderParser::MediaRangeContext::accept(
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

std::any AcceptHeaderParser::TypeContext::accept(
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

std::any AcceptHeaderParser::SubtypeContext::accept(
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

std::any AcceptHeaderParser::AcceptParamsContext::accept(
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

std::any AcceptHeaderParser::WeightContext::accept(
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

std::any AcceptHeaderParser::QvalueContext::accept(
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

std::any AcceptHeaderParser::AcceptExtContext::accept(
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

std::any AcceptHeaderParser::ParameterContext::accept(
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

std::any AcceptHeaderParser::TokenContext::accept(
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
    } while (((_la & ~0x3fULL) == 0) && ((1ULL << _la) & 536427904) != 0);

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

std::any AcceptHeaderParser::TcharContext::accept(
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
    if (!(((_la & ~0x3fULL) == 0) && ((1ULL << _la) & 536427904) != 0)) {
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

std::any AcceptHeaderParser::QuotedStringContext::accept(
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

std::any AcceptHeaderParser::Quoted_pairContext::accept(
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
    if (!(((_la & ~0x3fULL) == 0) && ((1ULL << _la) & 31138512896) != 0)) {
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

void AcceptHeaderParser::initialize() {
  ::antlr4::internal::call_once(acceptheaderParserOnceFlag,
                                acceptheaderParserInitialize);
}
