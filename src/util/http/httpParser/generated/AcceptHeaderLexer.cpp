
// Generated from AcceptHeader.g4 by ANTLR 4.11.1

#include "AcceptHeaderLexer.h"

using namespace antlr4;

using namespace antlr4;

namespace {

struct AcceptHeaderLexerStaticData final {
  AcceptHeaderLexerStaticData(std::vector<std::string> ruleNames,
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

  AcceptHeaderLexerStaticData(const AcceptHeaderLexerStaticData&) = delete;
  AcceptHeaderLexerStaticData(AcceptHeaderLexerStaticData&&) = delete;
  AcceptHeaderLexerStaticData& operator=(const AcceptHeaderLexerStaticData&) =
      delete;
  AcceptHeaderLexerStaticData& operator=(AcceptHeaderLexerStaticData&&) =
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

::antlr4::internal::OnceFlag acceptheaderlexerLexerOnceFlag;
AcceptHeaderLexerStaticData* acceptheaderlexerLexerStaticData = nullptr;

void acceptheaderlexerLexerInitialize() {
  assert(acceptheaderlexerLexerStaticData == nullptr);
  auto staticData = std::make_unique<AcceptHeaderLexerStaticData>(
      std::vector<std::string>{
          "T__0",          "T__1",         "T__2",       "T__3",
          "MediaRangeAll", "QandEqual",    "DIGIT",      "ALPHA",
          "OWS",           "Minus",        "Dot",        "Underscore",
          "Tilde",         "QuestionMark", "Slash",      "ExclamationMark",
          "Colon",         "At",           "DollarSign", "Hashtag",
          "Ampersand",     "Percent",      "SQuote",     "Star",
          "Plus",          "Caret",        "BackQuote",  "VBar",
          "QDTEXT",        "OBS_TEXT",     "DQUOTE",     "SP",
          "HTAB",          "VCHAR"},
      std::vector<std::string>{"DEFAULT_TOKEN_CHANNEL", "HIDDEN"},
      std::vector<std::string>{"DEFAULT_MODE"},
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
      4,   0,   34,  146, 6,   -1,  2,   0,   7,   0,  2,   1,   7,  1,   2,
      2,   7,   2,   2,   3,   7,   3,   2,   4,   7,  4,   2,   5,  7,   5,
      2,   6,   7,   6,   2,   7,   7,   7,   2,   8,  7,   8,   2,  9,   7,
      9,   2,   10,  7,   10,  2,   11,  7,   11,  2,  12,  7,   12, 2,   13,
      7,   13,  2,   14,  7,   14,  2,   15,  7,   15, 2,   16,  7,  16,  2,
      17,  7,   17,  2,   18,  7,   18,  2,   19,  7,  19,  2,   20, 7,   20,
      2,   21,  7,   21,  2,   22,  7,   22,  2,   23, 7,   23,  2,  24,  7,
      24,  2,   25,  7,   25,  2,   26,  7,   26,  2,  27,  7,   27, 2,   28,
      7,   28,  2,   29,  7,   29,  2,   30,  7,   30, 2,   31,  7,  31,  2,
      32,  7,   32,  2,   33,  7,   33,  1,   0,   1,  0,   1,   1,  1,   1,
      1,   2,   1,   2,   1,   3,   1,   3,   1,   4,  1,   4,   1,  4,   1,
      4,   1,   5,   1,   5,   1,   5,   1,   6,   1,  6,   1,   7,  1,   7,
      1,   8,   1,   8,   3,   8,   91,  8,   8,   1,  9,   1,   9,  1,   10,
      1,   10,  1,   11,  1,   11,  1,   12,  1,   12, 1,   13,  1,  13,  1,
      14,  1,   14,  1,   15,  1,   15,  1,   16,  1,  16,  1,   17, 1,   17,
      1,   18,  1,   18,  1,   19,  1,   19,  1,   20, 1,   20,  1,  21,  1,
      21,  1,   22,  1,   22,  1,   23,  1,   23,  1,  24,  1,   24, 1,   25,
      1,   25,  1,   26,  1,   26,  1,   27,  1,   27, 1,   28,  1,  28,  1,
      28,  1,   28,  3,   28,  135, 8,   28,  1,   29, 1,   29,  1,  30,  1,
      30,  1,   31,  1,   31,  1,   32,  1,   32,  1,  33,  1,   33, 0,   0,
      34,  1,   1,   3,   2,   5,   3,   7,   4,   9,  5,   11,  6,  13,  7,
      15,  8,   17,  9,   19,  10,  21,  11,  23,  12, 25,  13,  27, 14,  29,
      15,  31,  16,  33,  17,  35,  18,  37,  19,  39, 20,  41,  21, 43,  22,
      45,  23,  47,  24,  49,  25,  51,  26,  53,  27, 55,  28,  57, 29,  59,
      30,  61,  31,  63,  32,  65,  33,  67,  34,  1,  0,   3,   1,  0,   48,
      57,  2,   0,   65,  90,  97,  122, 3,   0,   33, 33,  39,  91, 93,  126,
      149, 0,   1,   1,   0,   0,   0,   0,   3,   1,  0,   0,   0,  0,   5,
      1,   0,   0,   0,   0,   7,   1,   0,   0,   0,  0,   9,   1,  0,   0,
      0,   0,   11,  1,   0,   0,   0,   0,   13,  1,  0,   0,   0,  0,   15,
      1,   0,   0,   0,   0,   17,  1,   0,   0,   0,  0,   19,  1,  0,   0,
      0,   0,   21,  1,   0,   0,   0,   0,   23,  1,  0,   0,   0,  0,   25,
      1,   0,   0,   0,   0,   27,  1,   0,   0,   0,  0,   29,  1,  0,   0,
      0,   0,   31,  1,   0,   0,   0,   0,   33,  1,  0,   0,   0,  0,   35,
      1,   0,   0,   0,   0,   37,  1,   0,   0,   0,  0,   39,  1,  0,   0,
      0,   0,   41,  1,   0,   0,   0,   0,   43,  1,  0,   0,   0,  0,   45,
      1,   0,   0,   0,   0,   47,  1,   0,   0,   0,  0,   49,  1,  0,   0,
      0,   0,   51,  1,   0,   0,   0,   0,   53,  1,  0,   0,   0,  0,   55,
      1,   0,   0,   0,   0,   57,  1,   0,   0,   0,  0,   59,  1,  0,   0,
      0,   0,   61,  1,   0,   0,   0,   0,   63,  1,  0,   0,   0,  0,   65,
      1,   0,   0,   0,   0,   67,  1,   0,   0,   0,  1,   69,  1,  0,   0,
      0,   3,   71,  1,   0,   0,   0,   5,   73,  1,  0,   0,   0,  7,   75,
      1,   0,   0,   0,   9,   77,  1,   0,   0,   0,  11,  81,  1,  0,   0,
      0,   13,  84,  1,   0,   0,   0,   15,  86,  1,  0,   0,   0,  17,  90,
      1,   0,   0,   0,   19,  92,  1,   0,   0,   0,  21,  94,  1,  0,   0,
      0,   23,  96,  1,   0,   0,   0,   25,  98,  1,  0,   0,   0,  27,  100,
      1,   0,   0,   0,   29,  102, 1,   0,   0,   0,  31,  104, 1,  0,   0,
      0,   33,  106, 1,   0,   0,   0,   35,  108, 1,  0,   0,   0,  37,  110,
      1,   0,   0,   0,   39,  112, 1,   0,   0,   0,  41,  114, 1,  0,   0,
      0,   43,  116, 1,   0,   0,   0,   45,  118, 1,  0,   0,   0,  47,  120,
      1,   0,   0,   0,   49,  122, 1,   0,   0,   0,  51,  124, 1,  0,   0,
      0,   53,  126, 1,   0,   0,   0,   55,  128, 1,  0,   0,   0,  57,  134,
      1,   0,   0,   0,   59,  136, 1,   0,   0,   0,  61,  138, 1,  0,   0,
      0,   63,  140, 1,   0,   0,   0,   65,  142, 1,  0,   0,   0,  67,  144,
      1,   0,   0,   0,   69,  70,  5,   44,  0,   0,  70,  2,   1,  0,   0,
      0,   71,  72,  5,   59,  0,   0,   72,  4,   1,  0,   0,   0,  73,  74,
      5,   61,  0,   0,   74,  6,   1,   0,   0,   0,  75,  76,  5,  92,  0,
      0,   76,  8,   1,   0,   0,   0,   77,  78,  3,  47,  23,  0,  78,  79,
      5,   47,  0,   0,   79,  80,  3,   47,  23,  0,  80,  10,  1,  0,   0,
      0,   81,  82,  5,   113, 0,   0,   82,  83,  5,  61,  0,   0,  83,  12,
      1,   0,   0,   0,   84,  85,  7,   0,   0,   0,  85,  14,  1,  0,   0,
      0,   86,  87,  7,   1,   0,   0,   87,  16,  1,  0,   0,   0,  88,  91,
      3,   63,  31,  0,   89,  91,  3,   65,  32,  0,  90,  88,  1,  0,   0,
      0,   90,  89,  1,   0,   0,   0,   91,  18,  1,  0,   0,   0,  92,  93,
      5,   45,  0,   0,   93,  20,  1,   0,   0,   0,  94,  95,  5,  46,  0,
      0,   95,  22,  1,   0,   0,   0,   96,  97,  5,  95,  0,   0,  97,  24,
      1,   0,   0,   0,   98,  99,  5,   126, 0,   0,  99,  26,  1,  0,   0,
      0,   100, 101, 5,   63,  0,   0,   101, 28,  1,  0,   0,   0,  102, 103,
      5,   47,  0,   0,   103, 30,  1,   0,   0,   0,  104, 105, 5,  33,  0,
      0,   105, 32,  1,   0,   0,   0,   106, 107, 5,  58,  0,   0,  107, 34,
      1,   0,   0,   0,   108, 109, 5,   64,  0,   0,  109, 36,  1,  0,   0,
      0,   110, 111, 5,   36,  0,   0,   111, 38,  1,  0,   0,   0,  112, 113,
      5,   35,  0,   0,   113, 40,  1,   0,   0,   0,  114, 115, 5,  38,  0,
      0,   115, 42,  1,   0,   0,   0,   116, 117, 5,  37,  0,   0,  117, 44,
      1,   0,   0,   0,   118, 119, 5,   39,  0,   0,  119, 46,  1,  0,   0,
      0,   120, 121, 5,   42,  0,   0,   121, 48,  1,  0,   0,   0,  122, 123,
      5,   43,  0,   0,   123, 50,  1,   0,   0,   0,  124, 125, 5,  94,  0,
      0,   125, 52,  1,   0,   0,   0,   126, 127, 5,  96,  0,   0,  127, 54,
      1,   0,   0,   0,   128, 129, 5,   124, 0,   0,  129, 56,  1,  0,   0,
      0,   130, 135, 3,   65,  32,  0,   131, 135, 3,  63,  31,  0,  132, 135,
      7,   2,   0,   0,   133, 135, 3,   59,  29,  0,  134, 130, 1,  0,   0,
      0,   134, 131, 1,   0,   0,   0,   134, 132, 1,  0,   0,   0,  134, 133,
      1,   0,   0,   0,   135, 58,  1,   0,   0,   0,  136, 137, 2,  128, 255,
      0,   137, 60,  1,   0,   0,   0,   138, 139, 5,  34,  0,   0,  139, 62,
      1,   0,   0,   0,   140, 141, 5,   32,  0,   0,  141, 64,  1,  0,   0,
      0,   142, 143, 5,   9,   0,   0,   143, 66,  1,  0,   0,   0,  144, 145,
      2,   33,  126, 0,   145, 68,  1,   0,   0,   0,  3,   0,   90, 134, 0};
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
  acceptheaderlexerLexerStaticData = staticData.release();
}

}  // namespace

AcceptHeaderLexer::AcceptHeaderLexer(CharStream* input) : Lexer(input) {
  AcceptHeaderLexer::initialize();
  _interpreter = new atn::LexerATNSimulator(
      this, *acceptheaderlexerLexerStaticData->atn,
      acceptheaderlexerLexerStaticData->decisionToDFA,
      acceptheaderlexerLexerStaticData->sharedContextCache);
}

AcceptHeaderLexer::~AcceptHeaderLexer() { delete _interpreter; }

std::string AcceptHeaderLexer::getGrammarFileName() const {
  return "AcceptHeader.g4";
}

const std::vector<std::string>& AcceptHeaderLexer::getRuleNames() const {
  return acceptheaderlexerLexerStaticData->ruleNames;
}

const std::vector<std::string>& AcceptHeaderLexer::getChannelNames() const {
  return acceptheaderlexerLexerStaticData->channelNames;
}

const std::vector<std::string>& AcceptHeaderLexer::getModeNames() const {
  return acceptheaderlexerLexerStaticData->modeNames;
}

const dfa::Vocabulary& AcceptHeaderLexer::getVocabulary() const {
  return acceptheaderlexerLexerStaticData->vocabulary;
}

antlr4::atn::SerializedATNView AcceptHeaderLexer::getSerializedATN() const {
  return acceptheaderlexerLexerStaticData->serializedATN;
}

const atn::ATN& AcceptHeaderLexer::getATN() const {
  return *acceptheaderlexerLexerStaticData->atn;
}

void AcceptHeaderLexer::initialize() {
  ::antlr4::internal::call_once(acceptheaderlexerLexerOnceFlag,
                                acceptheaderlexerLexerInitialize);
}
