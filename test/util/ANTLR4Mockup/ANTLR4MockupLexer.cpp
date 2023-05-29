
// Generated from ANTLR4Mockup.g4 by ANTLR 4.13.0

#include "ANTLR4MockupLexer.h"

using namespace antlr4;

using namespace antlr4;

namespace {

struct ANTLR4MockupLexerStaticData final {
  ANTLR4MockupLexerStaticData(std::vector<std::string> ruleNames,
                              std::vector<std::string> channelNames,
                              std::vector<std::string> modeNames,
                              std::vector<std::string> literalNames,
                              std::vector<std::string> symbolicNames)
      : ruleNames(std::move(ruleNames)),
        channelNames(std::move(channelNames)),
        modeNames(std::move(modeNames)),
        literalNames(std::move(literalNames)),
        symbolicNames(std::move(symbolicNames)),
        vocabulary(this->literalNames, this->symbolicNames) {}

  ANTLR4MockupLexerStaticData(const ANTLR4MockupLexerStaticData&) = delete;
  ANTLR4MockupLexerStaticData(ANTLR4MockupLexerStaticData&&) = delete;
  ANTLR4MockupLexerStaticData& operator=(const ANTLR4MockupLexerStaticData&) =
      delete;
  ANTLR4MockupLexerStaticData& operator=(ANTLR4MockupLexerStaticData&&) =
      delete;

  std::vector<antlr4::dfa::DFA> decisionToDFA;
  antlr4::atn::PredictionContextCache sharedContextCache;
  const std::vector<std::string> ruleNames;
  const std::vector<std::string> channelNames;
  const std::vector<std::string> modeNames;
  const std::vector<std::string> literalNames;
  const std::vector<std::string> symbolicNames;
  const antlr4::dfa::Vocabulary vocabulary;
  antlr4::atn::SerializedATNView serializedATN;
  std::unique_ptr<antlr4::atn::ATN> atn;
};

::antlr4::internal::OnceFlag antlr4mockuplexerLexerOnceFlag;
#if ANTLR4_USE_THREAD_LOCAL_CACHE
static thread_local
#endif
    ANTLR4MockupLexerStaticData* antlr4mockuplexerLexerStaticData = nullptr;

void antlr4mockuplexerLexerInitialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  if (antlr4mockuplexerLexerStaticData != nullptr) {
    return;
  }
#else
  assert(antlr4mockuplexerLexerStaticData == nullptr);
#endif
  auto staticData = std::make_unique<ANTLR4MockupLexerStaticData>(
      std::vector<std::string>{"BOOL", "INTEGER", "FLOAT", "STRING",
                               "WHITESPACE"},
      std::vector<std::string>{"DEFAULT_TOKEN_CHANNEL", "HIDDEN"},
      std::vector<std::string>{"DEFAULT_MODE"}, std::vector<std::string>{},
      std::vector<std::string>{"", "BOOL", "INTEGER", "FLOAT", "STRING",
                               "WHITESPACE"});
  static const int32_t serializedATNSegment[] = {
      4,  0,  5,  50, 6,  -1,  2,  0,  7,  0,  2,  1,   7,  1,  2,  2,  7,  2,
      2,  3,  7,  3,  2,  4,   7,  4,  1,  0,  1,  0,   1,  0,  1,  0,  1,  0,
      1,  0,  1,  0,  1,  0,   1,  0,  3,  0,  21, 8,   0,  1,  1,  3,  1,  24,
      8,  1,  1,  1,  4,  1,   27, 8,  1,  11, 1,  12,  1,  28, 1,  2,  1,  2,
      1,  2,  4,  2,  34, 8,   2,  11, 2,  12, 2,  35,  1,  3,  1,  3,  5,  3,
      40, 8,  3,  10, 3,  12,  3,  43, 9,  3,  1,  3,   1,  3,  1,  4,  1,  4,
      1,  4,  1,  4,  1,  41,  0,  5,  1,  1,  3,  2,   5,  3,  7,  4,  9,  5,
      1,  0,  2,  1,  0,  48,  57, 2,  0,  9,  9,  32,  32, 54, 0,  1,  1,  0,
      0,  0,  0,  3,  1,  0,   0,  0,  0,  5,  1,  0,   0,  0,  0,  7,  1,  0,
      0,  0,  0,  9,  1,  0,   0,  0,  1,  20, 1,  0,   0,  0,  3,  23, 1,  0,
      0,  0,  5,  30, 1,  0,   0,  0,  7,  37, 1,  0,   0,  0,  9,  46, 1,  0,
      0,  0,  11, 12, 5,  116, 0,  0,  12, 13, 5,  114, 0,  0,  13, 14, 5,  117,
      0,  0,  14, 21, 5,  101, 0,  0,  15, 16, 5,  102, 0,  0,  16, 17, 5,  97,
      0,  0,  17, 18, 5,  108, 0,  0,  18, 19, 5,  115, 0,  0,  19, 21, 5,  101,
      0,  0,  20, 11, 1,  0,   0,  0,  20, 15, 1,  0,   0,  0,  21, 2,  1,  0,
      0,  0,  22, 24, 5,  45,  0,  0,  23, 22, 1,  0,   0,  0,  23, 24, 1,  0,
      0,  0,  24, 26, 1,  0,   0,  0,  25, 27, 7,  0,   0,  0,  26, 25, 1,  0,
      0,  0,  27, 28, 1,  0,   0,  0,  28, 26, 1,  0,   0,  0,  28, 29, 1,  0,
      0,  0,  29, 4,  1,  0,   0,  0,  30, 31, 3,  3,   1,  0,  31, 33, 5,  46,
      0,  0,  32, 34, 7,  0,   0,  0,  33, 32, 1,  0,   0,  0,  34, 35, 1,  0,
      0,  0,  35, 33, 1,  0,   0,  0,  35, 36, 1,  0,   0,  0,  36, 6,  1,  0,
      0,  0,  37, 41, 5,  34,  0,  0,  38, 40, 9,  0,   0,  0,  39, 38, 1,  0,
      0,  0,  40, 43, 1,  0,   0,  0,  41, 42, 1,  0,   0,  0,  41, 39, 1,  0,
      0,  0,  42, 44, 1,  0,   0,  0,  43, 41, 1,  0,   0,  0,  44, 45, 5,  34,
      0,  0,  45, 8,  1,  0,   0,  0,  46, 47, 7,  1,   0,  0,  47, 48, 1,  0,
      0,  0,  48, 49, 6,  4,   0,  0,  49, 10, 1,  0,   0,  0,  6,  0,  20, 23,
      28, 35, 41, 1,  6,  0,   0};
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
  antlr4mockuplexerLexerStaticData = staticData.release();
}

}  // namespace

ANTLR4MockupLexer::ANTLR4MockupLexer(CharStream* input) : Lexer(input) {
  ANTLR4MockupLexer::initialize();
  _interpreter = new atn::LexerATNSimulator(
      this, *antlr4mockuplexerLexerStaticData->atn,
      antlr4mockuplexerLexerStaticData->decisionToDFA,
      antlr4mockuplexerLexerStaticData->sharedContextCache);
}

ANTLR4MockupLexer::~ANTLR4MockupLexer() { delete _interpreter; }

std::string ANTLR4MockupLexer::getGrammarFileName() const {
  return "ANTLR4Mockup.g4";
}

const std::vector<std::string>& ANTLR4MockupLexer::getRuleNames() const {
  return antlr4mockuplexerLexerStaticData->ruleNames;
}

const std::vector<std::string>& ANTLR4MockupLexer::getChannelNames() const {
  return antlr4mockuplexerLexerStaticData->channelNames;
}

const std::vector<std::string>& ANTLR4MockupLexer::getModeNames() const {
  return antlr4mockuplexerLexerStaticData->modeNames;
}

const dfa::Vocabulary& ANTLR4MockupLexer::getVocabulary() const {
  return antlr4mockuplexerLexerStaticData->vocabulary;
}

antlr4::atn::SerializedATNView ANTLR4MockupLexer::getSerializedATN() const {
  return antlr4mockuplexerLexerStaticData->serializedATN;
}

const atn::ATN& ANTLR4MockupLexer::getATN() const {
  return *antlr4mockuplexerLexerStaticData->atn;
}

void ANTLR4MockupLexer::initialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  antlr4mockuplexerLexerInitialize();
#else
  ::antlr4::internal::call_once(antlr4mockuplexerLexerOnceFlag,
                                antlr4mockuplexerLexerInitialize);
#endif
}
