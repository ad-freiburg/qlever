
// Generated from Sparql.g4 by ANTLR 4.9.2

#pragma once


#include "antlr4-runtime.h"




class  SparqlLexer : public antlr4::Lexer {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, 
    T__7 = 8, T__8 = 9, T__9 = 10, T__10 = 11, T__11 = 12, T__12 = 13, T__13 = 14, 
    T__14 = 15, T__15 = 16, T__16 = 17, T__17 = 18, T__18 = 19, T__19 = 20, 
    T__20 = 21, T__21 = 22, T__22 = 23, T__23 = 24, T__24 = 25, T__25 = 26, 
    T__26 = 27, T__27 = 28, T__28 = 29, T__29 = 30, T__30 = 31, BASE = 32, 
    PREFIX = 33, SELECT = 34, DISTINCT = 35, REDUCED = 36, AS = 37, CONSTRUCT = 38, 
    WHERE = 39, DESCRIBE = 40, ASK = 41, FROM = 42, NAMED = 43, GROUPBY = 44, 
    GROUP_CONCAT = 45, HAVING = 46, ORDERBY = 47, ASC = 48, DESC = 49, LIMIT = 50, 
    OFFSET = 51, VALUES = 52, LOAD = 53, SILENT = 54, CLEAR = 55, DROP = 56, 
    CREATE = 57, ADD = 58, DATA = 59, MOVE = 60, COPY = 61, INSERT = 62, 
    DELETE = 63, WITH = 64, USING = 65, DEFAULT = 66, GRAPH = 67, ALL = 68, 
    OPTIONAL = 69, SERVICE = 70, BIND = 71, UNDEF = 72, MINUS = 73, UNION = 74, 
    FILTER = 75, NOT = 76, IN = 77, STR = 78, LANG = 79, LANGMATCHES = 80, 
    DATATYPE = 81, BOUND = 82, IRI = 83, URI = 84, BNODE = 85, RAND = 86, 
    ABS = 87, CEIL = 88, FLOOR = 89, ROUND = 90, CONCAT = 91, STRLEN = 92, 
    UCASE = 93, LCASE = 94, ENCODE = 95, FOR = 96, CONTAINS = 97, STRSTARTS = 98, 
    STRENDS = 99, STRBEFORE = 100, STRAFTER = 101, YEAR = 102, MONTH = 103, 
    DAY = 104, HOURS = 105, MINUTES = 106, SECONDS = 107, TIMEZONE = 108, 
    TZ = 109, NOW = 110, UUID = 111, STRUUID = 112, SHA1 = 113, SHA256 = 114, 
    SHA384 = 115, SHA512 = 116, MD5 = 117, COALESCE = 118, IF = 119, STRLANG = 120, 
    STRDT = 121, SAMETERM = 122, ISIRI = 123, ISURI = 124, ISBLANK = 125, 
    ISLITERAL = 126, ISNUMERIC = 127, REGEX = 128, SUBSTR = 129, REPLACE = 130, 
    EXISTS = 131, COUNT = 132, SUM = 133, MIN = 134, MAX = 135, AVG = 136, 
    SAMPLE = 137, SEPARATOR = 138, IRI_REF = 139, PNAME_NS = 140, BLANK_NODE_LABEL = 141, 
    VAR1 = 142, VAR2 = 143, LANGTAG = 144, INTEGER = 145, DECIMAL = 146, 
    DOUBLE = 147, INTEGER_POSITIVE = 148, DECIMAL_POSITIVE = 149, DOUBLE_POSITIVE = 150, 
    INTEGER_NEGATIVE = 151, DECIMAL_NEGATIVE = 152, DOUBLE_NEGATIVE = 153, 
    EXPONENT = 154, STRING_LITERAL1 = 155, STRING_LITERAL2 = 156, STRING_LITERAL_LONG1 = 157, 
    STRING_LITERAL_LONG2 = 158, ECHAR = 159, NIL = 160, ANON = 161, PN_CHARS_U = 162, 
    VARNAME = 163, PN_PREFIX = 164, PN_LOCAL = 165, PLX = 166, PERCENT = 167, 
    HEX = 168, PN_LOCAL_ESC = 169, WS = 170, COMMENTS = 171
  };

  explicit SparqlLexer(antlr4::CharStream *input);
  ~SparqlLexer();

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

