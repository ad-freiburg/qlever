
// Generated from MemorySizeLanguage.g4 by ANTLR 4.13.0

#include "MemorySizeLanguageLexer.h"

using namespace antlr4;

using namespace antlr4;

namespace {

struct MemorySizeLanguageLexerStaticData final {
  MemorySizeLanguageLexerStaticData(std::vector<std::string> ruleNames,
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

  MemorySizeLanguageLexerStaticData(const MemorySizeLanguageLexerStaticData&) =
      delete;
  MemorySizeLanguageLexerStaticData(MemorySizeLanguageLexerStaticData&&) =
      delete;
  MemorySizeLanguageLexerStaticData& operator=(
      const MemorySizeLanguageLexerStaticData&) = delete;
  MemorySizeLanguageLexerStaticData& operator=(
      MemorySizeLanguageLexerStaticData&&) = delete;

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

::antlr4::internal::OnceFlag memorysizelanguagelexerLexerOnceFlag;
#if ANTLR4_USE_THREAD_LOCAL_CACHE
static thread_local
#endif
    MemorySizeLanguageLexerStaticData* memorysizelanguagelexerLexerStaticData =
        nullptr;

void memorysizelanguagelexerLexerInitialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  if (memorysizelanguagelexerLexerStaticData != nullptr) {
    return;
  }
#else
  assert(memorysizelanguagelexerLexerStaticData == nullptr);
#endif
  auto staticData = std::make_unique<MemorySizeLanguageLexerStaticData>(
      std::vector<std::string>{"T__0", "MEMORY_UNIT", "UNSIGNED_INTEGER",
                               "FLOAT", "WHITESPACE"},
      std::vector<std::string>{"DEFAULT_TOKEN_CHANNEL", "HIDDEN"},
      std::vector<std::string>{"DEFAULT_MODE"},
      std::vector<std::string>{"", "'B'"},
      std::vector<std::string>{"", "", "MEMORY_UNIT", "UNSIGNED_INTEGER",
                               "FLOAT", "WHITESPACE"});
  static const int32_t serializedATNSegment[] = {
      4,  0,   5, 36, 6,  -1, 2,  0,  7,  0, 2,  1,  7,  1,  2,  2,  7,  2,
      2,  3,   7, 3,  2,  4,  7,  4,  1,  0, 1,  0,  1,  1,  1,  1,  1,  1,
      1,  1,   1, 1,  1,  1,  1,  1,  1,  1, 3,  1,  22, 8,  1,  1,  2,  4,
      2,  25,  8, 2,  11, 2,  12, 2,  26, 1, 3,  1,  3,  1,  3,  1,  3,  1,
      4,  1,   4, 1,  4,  1,  4,  0,  0,  5, 1,  1,  3,  2,  5,  3,  7,  4,
      9,  5,   1, 0,  2,  1,  0,  48, 57, 2, 0,  9,  9,  32, 32, 39, 0,  1,
      1,  0,   0, 0,  0,  3,  1,  0,  0,  0, 0,  5,  1,  0,  0,  0,  0,  7,
      1,  0,   0, 0,  0,  9,  1,  0,  0,  0, 1,  11, 1,  0,  0,  0,  3,  21,
      1,  0,   0, 0,  5,  24, 1,  0,  0,  0, 7,  28, 1,  0,  0,  0,  9,  32,
      1,  0,   0, 0,  11, 12, 5,  66, 0,  0, 12, 2,  1,  0,  0,  0,  13, 14,
      5,  107, 0, 0,  14, 22, 5,  66, 0,  0, 15, 16, 5,  77, 0,  0,  16, 22,
      5,  66,  0, 0,  17, 18, 5,  71, 0,  0, 18, 22, 5,  66, 0,  0,  19, 20,
      5,  84,  0, 0,  20, 22, 5,  66, 0,  0, 21, 13, 1,  0,  0,  0,  21, 15,
      1,  0,   0, 0,  21, 17, 1,  0,  0,  0, 21, 19, 1,  0,  0,  0,  22, 4,
      1,  0,   0, 0,  23, 25, 7,  0,  0,  0, 24, 23, 1,  0,  0,  0,  25, 26,
      1,  0,   0, 0,  26, 24, 1,  0,  0,  0, 26, 27, 1,  0,  0,  0,  27, 6,
      1,  0,   0, 0,  28, 29, 3,  5,  2,  0, 29, 30, 5,  46, 0,  0,  30, 31,
      3,  5,   2, 0,  31, 8,  1,  0,  0,  0, 32, 33, 7,  1,  0,  0,  33, 34,
      1,  0,   0, 0,  34, 35, 6,  4,  0,  0, 35, 10, 1,  0,  0,  0,  3,  0,
      21, 26,  1, 6,  0,  0};
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
  memorysizelanguagelexerLexerStaticData = staticData.release();
}

}  // namespace

MemorySizeLanguageLexer::MemorySizeLanguageLexer(CharStream* input)
    : Lexer(input) {
  MemorySizeLanguageLexer::initialize();
  _interpreter = new atn::LexerATNSimulator(
      this, *memorysizelanguagelexerLexerStaticData->atn,
      memorysizelanguagelexerLexerStaticData->decisionToDFA,
      memorysizelanguagelexerLexerStaticData->sharedContextCache);
}

MemorySizeLanguageLexer::~MemorySizeLanguageLexer() { delete _interpreter; }

std::string MemorySizeLanguageLexer::getGrammarFileName() const {
  return "MemorySizeLanguage.g4";
}

const std::vector<std::string>& MemorySizeLanguageLexer::getRuleNames() const {
  return memorysizelanguagelexerLexerStaticData->ruleNames;
}

const std::vector<std::string>& MemorySizeLanguageLexer::getChannelNames()
    const {
  return memorysizelanguagelexerLexerStaticData->channelNames;
}

const std::vector<std::string>& MemorySizeLanguageLexer::getModeNames() const {
  return memorysizelanguagelexerLexerStaticData->modeNames;
}

const dfa::Vocabulary& MemorySizeLanguageLexer::getVocabulary() const {
  return memorysizelanguagelexerLexerStaticData->vocabulary;
}

antlr4::atn::SerializedATNView MemorySizeLanguageLexer::getSerializedATN()
    const {
  return memorysizelanguagelexerLexerStaticData->serializedATN;
}

const atn::ATN& MemorySizeLanguageLexer::getATN() const {
  return *memorysizelanguagelexerLexerStaticData->atn;
}

void MemorySizeLanguageLexer::initialize() {
#if ANTLR4_USE_THREAD_LOCAL_CACHE
  memorysizelanguagelexerLexerInitialize();
#else
  ::antlr4::internal::call_once(memorysizelanguagelexerLexerOnceFlag,
                                memorysizelanguagelexerLexerInitialize);
#endif
}
