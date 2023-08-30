
// Generated from MemorySizeLanguage.g4 by ANTLR 4.13.0

#include "MemorySizeLanguageParser.h"

using namespace antlrcpp;

using namespace antlr4;

namespace {

struct MemorySizeLanguageParserStaticData final {
  MemorySizeLanguageParserStaticData(std::vector<std::string> ruleNames,
                                     std::vector<std::string> literalNames,
                                     std::vector<std::string> symbolicNames)
      : ruleNames(std::move(ruleNames)),
        literalNames(std::move(literalNames)),
        symbolicNames(std::move(symbolicNames)),
        vocabulary(this->literalNames, this->symbolicNames) {}

  MemorySizeLanguageParserStaticData(
      const MemorySizeLanguageParserStaticData&) = delete;
  MemorySizeLanguageParserStaticData(MemorySizeLanguageParserStaticData&&) =
      delete;
  MemorySizeLanguageParserStaticData& operator=(
      const MemorySizeLanguageParserStaticData&) = delete;
  MemorySizeLanguageParserStaticData& operator=(
      MemorySizeLanguageParserStaticData&&) = delete;

  std::vector<antlr4::dfa::DFA> decisionToDFA;
  antlr4::atn::PredictionContextCache sharedContextCache;
  const std::vector<std::string> ruleNames;
  const std::vector<std::string> literalNames;
  const std::vector<std::string> symbolicNames;
  const antlr4::dfa::Vocabulary vocabulary;
  antlr4::atn::SerializedATNView serializedATN;
  std::unique_ptr<antlr4::atn::ATN> atn;
};

::antlr4::internal::OnceFlag memorysizelanguageParserOnceFlag;
#if ANTLR4_USE_THREAD_LOCAL_CACHE
static thread_local
#endif
    MemorySizeLanguageParserStaticData* memorysizelanguageParserStaticData =
        nullptr;

void memorysizelanguageParserInitialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  if (memorysizelanguageParserStaticData != nullptr) {
    return;
  }
#else
  assert(memorysizelanguageParserStaticData == nullptr);
#endif
  auto staticData = std::make_unique<MemorySizeLanguageParserStaticData>(
      std::vector<std::string>{"memorySizeString", "pureByteSize",
                               "memoryUnitSize"},
      std::vector<std::string>{},
      std::vector<std::string>{"", "MEMORY_UNIT", "BYTE", "UNSIGNED_INTEGER",
                               "FLOAT", "WHITESPACE"});
  static const int32_t serializedATNSegment[] = {
      4,  1,  5,  19, 2,  0,  7,  0, 2,  1,  7,  1, 2, 2, 7,  2,  1,  0,  1, 0,
      3,  0,  9,  8,  0,  1,  0,  1, 0,  1,  1,  1, 1, 1, 1,  1,  2,  1,  2, 1,
      2,  1,  2,  0,  0,  3,  0,  2, 4,  0,  1,  1, 0, 3, 4,  16, 0,  8,  1, 0,
      0,  0,  2,  12, 1,  0,  0,  0, 4,  15, 1,  0, 0, 0, 6,  9,  3,  2,  1, 0,
      7,  9,  3,  4,  2,  0,  8,  6, 1,  0,  0,  0, 8, 7, 1,  0,  0,  0,  9, 10,
      1,  0,  0,  0,  10, 11, 5,  0, 0,  1,  11, 1, 1, 0, 0,  0,  12, 13, 5, 3,
      0,  0,  13, 14, 5,  2,  0,  0, 14, 3,  1,  0, 0, 0, 15, 16, 7,  0,  0, 0,
      16, 17, 5,  1,  0,  0,  17, 5, 1,  0,  0,  0, 1, 8};
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
  memorysizelanguageParserStaticData = staticData.release();
}

}  // namespace

MemorySizeLanguageParser::MemorySizeLanguageParser(TokenStream* input)
    : MemorySizeLanguageParser(input,
                               antlr4::atn::ParserATNSimulatorOptions()) {}

MemorySizeLanguageParser::MemorySizeLanguageParser(
    TokenStream* input, const antlr4::atn::ParserATNSimulatorOptions& options)
    : Parser(input) {
  MemorySizeLanguageParser::initialize();
  _interpreter = new atn::ParserATNSimulator(
      this, *memorysizelanguageParserStaticData->atn,
      memorysizelanguageParserStaticData->decisionToDFA,
      memorysizelanguageParserStaticData->sharedContextCache, options);
}

MemorySizeLanguageParser::~MemorySizeLanguageParser() { delete _interpreter; }

const atn::ATN& MemorySizeLanguageParser::getATN() const {
  return *memorysizelanguageParserStaticData->atn;
}

std::string MemorySizeLanguageParser::getGrammarFileName() const {
  return "MemorySizeLanguage.g4";
}

const std::vector<std::string>& MemorySizeLanguageParser::getRuleNames() const {
  return memorysizelanguageParserStaticData->ruleNames;
}

const dfa::Vocabulary& MemorySizeLanguageParser::getVocabulary() const {
  return memorysizelanguageParserStaticData->vocabulary;
}

antlr4::atn::SerializedATNView MemorySizeLanguageParser::getSerializedATN()
    const {
  return memorysizelanguageParserStaticData->serializedATN;
}

//----------------- MemorySizeStringContext
//------------------------------------------------------------------

MemorySizeLanguageParser::MemorySizeStringContext::MemorySizeStringContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode* MemorySizeLanguageParser::MemorySizeStringContext::EOF() {
  return getToken(MemorySizeLanguageParser::EOF, 0);
}

MemorySizeLanguageParser::PureByteSizeContext*
MemorySizeLanguageParser::MemorySizeStringContext::pureByteSize() {
  return getRuleContext<MemorySizeLanguageParser::PureByteSizeContext>(0);
}

MemorySizeLanguageParser::MemoryUnitSizeContext*
MemorySizeLanguageParser::MemorySizeStringContext::memoryUnitSize() {
  return getRuleContext<MemorySizeLanguageParser::MemoryUnitSizeContext>(0);
}

size_t MemorySizeLanguageParser::MemorySizeStringContext::getRuleIndex() const {
  return MemorySizeLanguageParser::RuleMemorySizeString;
}

MemorySizeLanguageParser::MemorySizeStringContext*
MemorySizeLanguageParser::memorySizeString() {
  MemorySizeStringContext* _localctx =
      _tracker.createInstance<MemorySizeStringContext>(_ctx, getState());
  enterRule(_localctx, 0, MemorySizeLanguageParser::RuleMemorySizeString);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(8);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(
        _input, 0, _ctx)) {
      case 1: {
        setState(6);
        pureByteSize();
        break;
      }

      case 2: {
        setState(7);
        memoryUnitSize();
        break;
      }

      default:
        break;
    }
    setState(10);
    match(MemorySizeLanguageParser::EOF);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- PureByteSizeContext
//------------------------------------------------------------------

MemorySizeLanguageParser::PureByteSizeContext::PureByteSizeContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode*
MemorySizeLanguageParser::PureByteSizeContext::UNSIGNED_INTEGER() {
  return getToken(MemorySizeLanguageParser::UNSIGNED_INTEGER, 0);
}

tree::TerminalNode* MemorySizeLanguageParser::PureByteSizeContext::BYTE() {
  return getToken(MemorySizeLanguageParser::BYTE, 0);
}

size_t MemorySizeLanguageParser::PureByteSizeContext::getRuleIndex() const {
  return MemorySizeLanguageParser::RulePureByteSize;
}

MemorySizeLanguageParser::PureByteSizeContext*
MemorySizeLanguageParser::pureByteSize() {
  PureByteSizeContext* _localctx =
      _tracker.createInstance<PureByteSizeContext>(_ctx, getState());
  enterRule(_localctx, 2, MemorySizeLanguageParser::RulePureByteSize);

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
    match(MemorySizeLanguageParser::UNSIGNED_INTEGER);
    setState(13);
    match(MemorySizeLanguageParser::BYTE);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- MemoryUnitSizeContext
//------------------------------------------------------------------

MemorySizeLanguageParser::MemoryUnitSizeContext::MemoryUnitSizeContext(
    ParserRuleContext* parent, size_t invokingState)
    : ParserRuleContext(parent, invokingState) {}

tree::TerminalNode*
MemorySizeLanguageParser::MemoryUnitSizeContext::MEMORY_UNIT() {
  return getToken(MemorySizeLanguageParser::MEMORY_UNIT, 0);
}

tree::TerminalNode*
MemorySizeLanguageParser::MemoryUnitSizeContext::UNSIGNED_INTEGER() {
  return getToken(MemorySizeLanguageParser::UNSIGNED_INTEGER, 0);
}

tree::TerminalNode* MemorySizeLanguageParser::MemoryUnitSizeContext::FLOAT() {
  return getToken(MemorySizeLanguageParser::FLOAT, 0);
}

size_t MemorySizeLanguageParser::MemoryUnitSizeContext::getRuleIndex() const {
  return MemorySizeLanguageParser::RuleMemoryUnitSize;
}

MemorySizeLanguageParser::MemoryUnitSizeContext*
MemorySizeLanguageParser::memoryUnitSize() {
  MemoryUnitSizeContext* _localctx =
      _tracker.createInstance<MemoryUnitSizeContext>(_ctx, getState());
  enterRule(_localctx, 4, MemorySizeLanguageParser::RuleMemoryUnitSize);
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
    setState(15);
    _la = _input->LA(1);
    if (!(_la == MemorySizeLanguageParser::UNSIGNED_INTEGER

          || _la == MemorySizeLanguageParser::FLOAT)) {
      _errHandler->recoverInline(this);
    } else {
      _errHandler->reportMatch(this);
      consume();
    }
    setState(16);
    match(MemorySizeLanguageParser::MEMORY_UNIT);

  } catch (RecognitionException& e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

void MemorySizeLanguageParser::initialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  memorysizelanguageParserInitialize();
#else
  ::antlr4::internal::call_once(memorysizelanguageParserOnceFlag,
                                memorysizelanguageParserInitialize);
#endif
}
