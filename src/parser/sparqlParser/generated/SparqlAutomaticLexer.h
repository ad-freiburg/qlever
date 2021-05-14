
// Generated from SparqlAutomatic.g4 by ANTLR 4.9.2

#pragma once


#include "antlr4-runtime.h"




class  SparqlAutomaticLexer : public antlr4::Lexer {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, 
    T__7 = 8, T__8 = 9, T__9 = 10, T__10 = 11, T__11 = 12, T__12 = 13, T__13 = 14, 
    T__14 = 15, T__15 = 16, T__16 = 17, T__17 = 18, T__18 = 19, T__19 = 20, 
    T__20 = 21, BASE = 22, PREFIX = 23, SELECT = 24, DISTINCT = 25, REDUCED = 26, 
    AS = 27, CONSTRUCT = 28, WHERE = 29, DESCRIBE = 30, ASK = 31, FROM = 32, 
    NAMED = 33, GROUPBY = 34, GROUP_CONCAT = 35, HAVING = 36, ORDERBY = 37, 
    ASC = 38, DESC = 39, LIMIT = 40, OFFSET = 41, VALUES = 42, LOAD = 43, 
    SILENT = 44, CLEAR = 45, DROP = 46, CREATE = 47, ADD = 48, DATA = 49, 
    MOVE = 50, COPY = 51, INSERT = 52, DELETE = 53, WITH = 54, USING = 55, 
    DEFAULT = 56, GRAPH = 57, ALL = 58, OPTIONAL = 59, SERVICE = 60, BIND = 61, 
    UNDEF = 62, MINUS = 63, UNION = 64, FILTER = 65, NOT = 66, IN = 67, 
    STR = 68, LANG = 69, LANGMATCHES = 70, DATATYPE = 71, BOUND = 72, IRI = 73, 
    URI = 74, BNODE = 75, RAND = 76, ABS = 77, CEIL = 78, FLOOR = 79, ROUND = 80, 
    CONCAT = 81, STRLEN = 82, UCASE = 83, LCASE = 84, ENCODE = 85, FOR = 86, 
    CONTAINS = 87, STRSTARTS = 88, STRENDS = 89, STRBEFORE = 90, STRAFTER = 91, 
    YEAR = 92, MONTH = 93, DAY = 94, HOURS = 95, MINUTES = 96, SECONDS = 97, 
    TIMEZONE = 98, TZ = 99, NOW = 100, UUID = 101, STRUUID = 102, SHA1 = 103, 
    SHA256 = 104, SHA384 = 105, SHA512 = 106, MD5 = 107, COALESCE = 108, 
    IF = 109, STRLANG = 110, STRDT = 111, SAMETERM = 112, ISIRI = 113, ISURI = 114, 
    ISBLANK = 115, ISLITERAL = 116, ISNUMERIC = 117, REGEX = 118, SUBSTR = 119, 
    REPLACE = 120, EXISTS = 121, COUNT = 122, SUM = 123, MIN = 124, MAX = 125, 
    AVG = 126, SAMPLE = 127, SEPARATOR = 128, IRI_REF = 129, PNAME_NS = 130, 
    PNAME_LN = 131, BLANK_NODE_LABEL = 132, VAR1 = 133, VAR2 = 134, LANGTAG = 135, 
    INTEGER = 136, DECIMAL = 137, DOUBLE = 138, INTEGER_POSITIVE = 139, 
    DECIMAL_POSITIVE = 140, DOUBLE_POSITIVE = 141, INTEGER_NEGATIVE = 142, 
    DECIMAL_NEGATIVE = 143, DOUBLE_NEGATIVE = 144, EXPONENT = 145, STRING_LITERAL1 = 146, 
    STRING_LITERAL2 = 147, STRING_LITERAL_LONG1 = 148, STRING_LITERAL_LONG2 = 149, 
    ECHAR = 150, NIL = 151, ANON = 152, PN_CHARS_U = 153, VARNAME = 154, 
    PN_PREFIX = 155, PN_LOCAL = 156, PLX = 157, PERCENT = 158, HEX = 159, 
    PN_LOCAL_ESC = 160, MULTIPLY = 161, DIVIDE = 162, PLUS = 163, MINUSSIGN = 164, 
    EQUALS = 165, NEQUALS = 166, NEGATE = 167, LESS = 168, GREATER = 169, 
    LESS_EQUAL = 170, GREATER_EQUAL = 171, WS = 172, COMMENTS = 173
  };

  explicit SparqlAutomaticLexer(antlr4::CharStream *input);
  ~SparqlAutomaticLexer();

  virtual std::string getGrammarFileName() const override;
  virtual const std::vector<std::string>& getRuleNames() const override;

  virtual const std::vector<std::string>& getChannelNames() const override;
  virtual const std::vector<std::string>& getModeNames() const override;
  virtual const std::vector<std::string>& getTokenNames() const override; // deprecated, use vocabulary instead
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

  virtual const std::vector<uint16_t> getSerializedATN() const override;
  virtual const antlr4::atn::ATN& getATN() const override;

private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;
  static std::vector<std::string> _channelNames;
  static std::vector<std::string> _modeNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;


  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};

