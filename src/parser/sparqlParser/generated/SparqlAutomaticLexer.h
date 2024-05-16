
// Generated from SparqlAutomatic.g4 by ANTLR 4.11.1

#pragma once


#include "antlr4-runtime.h"




class  SparqlAutomaticLexer : public antlr4::Lexer {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, 
    T__7 = 8, T__8 = 9, T__9 = 10, T__10 = 11, T__11 = 12, T__12 = 13, T__13 = 14, 
    T__14 = 15, T__15 = 16, T__16 = 17, T__17 = 18, T__18 = 19, T__19 = 20, 
    T__20 = 21, T__21 = 22, T__22 = 23, T__23 = 24, T__24 = 25, T__25 = 26, 
    T__26 = 27, T__27 = 28, T__28 = 29, BASE = 30, PREFIX = 31, SELECT = 32, 
    DISTINCT = 33, REDUCED = 34, AS = 35, CONSTRUCT = 36, WHERE = 37, DESCRIBE = 38, 
    ASK = 39, FROM = 40, NAMED = 41, GROUPBY = 42, GROUP_CONCAT = 43, HAVING = 44, 
    ORDERBY = 45, INTERNALSORTBY = 46, ASC = 47, DESC = 48, LIMIT = 49, 
    OFFSET = 50, TEXTLIMIT = 51, VALUES = 52, LOAD = 53, SILENT = 54, CLEAR = 55, 
    DROP = 56, CREATE = 57, ADD = 58, DATA = 59, MOVE = 60, COPY = 61, INSERT = 62, 
    DELETE = 63, WITH = 64, USING = 65, DEFAULT = 66, GRAPH = 67, ALL = 68, 
    OPTIONAL = 69, SERVICE = 70, BIND = 71, UNDEF = 72, MINUS = 73, UNION = 74, 
    FILTER = 75, NOT = 76, IN = 77, STR = 78, LANG = 79, LANGMATCHES = 80, 
    DATATYPE = 81, BOUND = 82, IRI = 83, URI = 84, BNODE = 85, RAND = 86, 
    ABS = 87, CEIL = 88, FLOOR = 89, ROUND = 90, CONCAT = 91, STRLEN = 92, 
    UCASE = 93, LCASE = 94, ENCODE_FOR_URI = 95, FOR = 96, CONTAINS = 97, 
    STRSTARTS = 98, STRENDS = 99, STRBEFORE = 100, STRAFTER = 101, YEAR = 102, 
    MONTH = 103, DAY = 104, HOURS = 105, MINUTES = 106, SECONDS = 107, TIMEZONE = 108, 
    TZ = 109, NOW = 110, UUID = 111, STRUUID = 112, SHA1 = 113, SHA256 = 114, 
    SHA384 = 115, SHA512 = 116, MD5 = 117, COALESCE = 118, IF = 119, STRLANG = 120, 
    STRDT = 121, SAMETERM = 122, ISIRI = 123, ISURI = 124, ISBLANK = 125, 
    ISLITERAL = 126, ISNUMERIC = 127, REGEX = 128, SUBSTR = 129, REPLACE = 130, 
    EXISTS = 131, COUNT = 132, SUM = 133, MIN = 134, MAX = 135, AVG = 136, 
    SAMPLE = 137, SEPARATOR = 138, IRI_REF = 139, PNAME_NS = 140, PNAME_LN = 141, 
    BLANK_NODE_LABEL = 142, VAR1 = 143, VAR2 = 144, LANGTAG = 145, PREFIX_LANGTAG = 146, 
    INTEGER = 147, DECIMAL = 148, DOUBLE = 149, INTEGER_POSITIVE = 150, 
    DECIMAL_POSITIVE = 151, DOUBLE_POSITIVE = 152, INTEGER_NEGATIVE = 153, 
    DECIMAL_NEGATIVE = 154, DOUBLE_NEGATIVE = 155, EXPONENT = 156, STRING_LITERAL1 = 157, 
    STRING_LITERAL2 = 158, STRING_LITERAL_LONG1 = 159, STRING_LITERAL_LONG2 = 160, 
    ECHAR = 161, NIL = 162, ANON = 163, PN_CHARS_U = 164, VARNAME = 165, 
    PN_PREFIX = 166, PN_LOCAL = 167, PLX = 168, PERCENT = 169, HEX = 170, 
    PN_LOCAL_ESC = 171, WS = 172, COMMENTS = 173
  };

  explicit SparqlAutomaticLexer(antlr4::CharStream *input);

  ~SparqlAutomaticLexer() override;


  std::string getGrammarFileName() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const std::vector<std::string>& getChannelNames() const override;

  const std::vector<std::string>& getModeNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  const antlr4::atn::ATN& getATN() const override;

  // By default the static state used to implement the lexer is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:

  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

};

