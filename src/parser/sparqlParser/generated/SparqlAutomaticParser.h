
// Generated from SparqlAutomatic.g4 by ANTLR 4.9.2

#pragma once

#include "antlr4-runtime.h"

class SparqlAutomaticParser : public antlr4::Parser {
 public:
  enum {
    T__0 = 1,
    T__1 = 2,
    T__2 = 3,
    T__3 = 4,
    T__4 = 5,
    T__5 = 6,
    T__6 = 7,
    T__7 = 8,
    T__8 = 9,
    T__9 = 10,
    T__10 = 11,
    T__11 = 12,
    T__12 = 13,
    T__13 = 14,
    T__14 = 15,
    T__15 = 16,
    T__16 = 17,
    T__17 = 18,
    T__18 = 19,
    T__19 = 20,
    T__20 = 21,
    T__21 = 22,
    T__22 = 23,
    T__23 = 24,
    T__24 = 25,
    T__25 = 26,
    T__26 = 27,
    T__27 = 28,
    T__28 = 29,
    T__29 = 30,
    BASE = 31,
    PREFIX = 32,
    SELECT = 33,
    DISTINCT = 34,
    REDUCED = 35,
    AS = 36,
    CONSTRUCT = 37,
    WHERE = 38,
    DESCRIBE = 39,
    ASK = 40,
    FROM = 41,
    NAMED = 42,
    GROUPBY = 43,
    GROUP_CONCAT = 44,
    HAVING = 45,
    ORDERBY = 46,
    ASC = 47,
    DESC = 48,
    LIMIT = 49,
    OFFSET = 50,
    TEXTLIMIT = 51,
    VALUES = 52,
    LOAD = 53,
    SILENT = 54,
    CLEAR = 55,
    DROP = 56,
    CREATE = 57,
    ADD = 58,
    DATA = 59,
    MOVE = 60,
    COPY = 61,
    INSERT = 62,
    DELETE = 63,
    WITH = 64,
    USING = 65,
    DEFAULT = 66,
    GRAPH = 67,
    ALL = 68,
    OPTIONAL = 69,
    SERVICE = 70,
    BIND = 71,
    UNDEF = 72,
    MINUS = 73,
    UNION = 74,
    FILTER = 75,
    NOT = 76,
    IN = 77,
    STR = 78,
    LANG = 79,
    LANGMATCHES = 80,
    DATATYPE = 81,
    BOUND = 82,
    IRI = 83,
    URI = 84,
    BNODE = 85,
    RAND = 86,
    ABS = 87,
    CEIL = 88,
    FLOOR = 89,
    ROUND = 90,
    CONCAT = 91,
    STRLEN = 92,
    UCASE = 93,
    LCASE = 94,
    ENCODE = 95,
    FOR = 96,
    CONTAINS = 97,
    STRSTARTS = 98,
    STRENDS = 99,
    STRBEFORE = 100,
    STRAFTER = 101,
    YEAR = 102,
    MONTH = 103,
    DAY = 104,
    HOURS = 105,
    MINUTES = 106,
    SECONDS = 107,
    TIMEZONE = 108,
    TZ = 109,
    NOW = 110,
    UUID = 111,
    STRUUID = 112,
    SHA1 = 113,
    SHA256 = 114,
    SHA384 = 115,
    SHA512 = 116,
    MD5 = 117,
    COALESCE = 118,
    IF = 119,
    STRLANG = 120,
    STRDT = 121,
    SAMETERM = 122,
    ISIRI = 123,
    ISURI = 124,
    ISBLANK = 125,
    ISLITERAL = 126,
    ISNUMERIC = 127,
    REGEX = 128,
    SUBSTR = 129,
    REPLACE = 130,
    EXISTS = 131,
    COUNT = 132,
    SUM = 133,
    MIN = 134,
    MAX = 135,
    AVG = 136,
    SAMPLE = 137,
    SEPARATOR = 138,
    IRI_REF = 139,
    PNAME_NS = 140,
    PNAME_LN = 141,
    BLANK_NODE_LABEL = 142,
    VAR1 = 143,
    VAR2 = 144,
    LANGTAG = 145,
    INTEGER = 146,
    DECIMAL = 147,
    DOUBLE = 148,
    INTEGER_POSITIVE = 149,
    DECIMAL_POSITIVE = 150,
    DOUBLE_POSITIVE = 151,
    INTEGER_NEGATIVE = 152,
    DECIMAL_NEGATIVE = 153,
    DOUBLE_NEGATIVE = 154,
    EXPONENT = 155,
    STRING_LITERAL1 = 156,
    STRING_LITERAL2 = 157,
    STRING_LITERAL_LONG1 = 158,
    STRING_LITERAL_LONG2 = 159,
    ECHAR = 160,
    NIL = 161,
    ANON = 162,
    PN_CHARS_U = 163,
    VARNAME = 164,
    PN_PREFIX = 165,
    PN_LOCAL = 166,
    PLX = 167,
    PERCENT = 168,
    HEX = 169,
    PN_LOCAL_ESC = 170,
    WS = 171,
    COMMENTS = 172
  };

  enum {
    RuleQuery = 0,
    RulePrologue = 1,
    RuleBaseDecl = 2,
    RulePrefixDecl = 3,
    RuleSelectQuery = 4,
    RuleSubSelect = 5,
    RuleSelectClause = 6,
    RuleAlias = 7,
    RuleAliasWithouBrackes = 8,
    RuleConstructQuery = 9,
    RuleDescribeQuery = 10,
    RuleAskQuery = 11,
    RuleDatasetClause = 12,
    RuleDefaultGraphClause = 13,
    RuleNamedGraphClause = 14,
    RuleSourceSelector = 15,
    RuleWhereClause = 16,
    RuleSolutionModifier = 17,
    RuleGroupClause = 18,
    RuleGroupCondition = 19,
    RuleHavingClause = 20,
    RuleHavingCondition = 21,
    RuleOrderClause = 22,
    RuleOrderCondition = 23,
    RuleLimitOffsetClauses = 24,
    RuleLimitClause = 25,
    RuleOffsetClause = 26,
    RuleTextLimitClause = 27,
    RuleValuesClause = 28,
    RuleTriplesTemplate = 29,
    RuleGroupGraphPattern = 30,
    RuleGroupGraphPatternSub = 31,
    RuleTriplesBlock = 32,
    RuleGraphPatternNotTriples = 33,
    RuleOptionalGraphPattern = 34,
    RuleGraphGraphPattern = 35,
    RuleServiceGraphPattern = 36,
    RuleBind = 37,
    RuleInlineData = 38,
    RuleDataBlock = 39,
    RuleInlineDataOneVar = 40,
    RuleInlineDataFull = 41,
    RuleDataBlockSingle = 42,
    RuleDataBlockValue = 43,
    RuleMinusGraphPattern = 44,
    RuleGroupOrUnionGraphPattern = 45,
    RuleFilterR = 46,
    RuleConstraint = 47,
    RuleFunctionCall = 48,
    RuleArgList = 49,
    RuleExpressionList = 50,
    RuleConstructTemplate = 51,
    RuleConstructTriples = 52,
    RuleTriplesSameSubject = 53,
    RulePropertyList = 54,
    RulePropertyListNotEmpty = 55,
    RuleVerb = 56,
    RuleObjectList = 57,
    RuleObjectR = 58,
    RuleTriplesSameSubjectPath = 59,
    RulePropertyListPath = 60,
    RulePropertyListPathNotEmpty = 61,
    RuleVerbPath = 62,
    RuleVerbSimple = 63,
    RuleVerbPathOrSimple = 64,
    RuleObjectListPath = 65,
    RuleObjectPath = 66,
    RulePath = 67,
    RulePathAlternative = 68,
    RulePathSequence = 69,
    RulePathElt = 70,
    RulePathEltOrInverse = 71,
    RulePathMod = 72,
    RulePathPrimary = 73,
    RulePathNegatedPropertySet = 74,
    RulePathOneInPropertySet = 75,
    RuleInteger = 76,
    RuleTriplesNode = 77,
    RuleBlankNodePropertyList = 78,
    RuleTriplesNodePath = 79,
    RuleBlankNodePropertyListPath = 80,
    RuleCollection = 81,
    RuleCollectionPath = 82,
    RuleGraphNode = 83,
    RuleGraphNodePath = 84,
    RuleVarOrTerm = 85,
    RuleVarOrIri = 86,
    RuleVar = 87,
    RuleGraphTerm = 88,
    RuleExpression = 89,
    RuleConditionalOrExpression = 90,
    RuleConditionalAndExpression = 91,
    RuleValueLogical = 92,
    RuleRelationalExpression = 93,
    RuleNumericExpression = 94,
    RuleAdditiveExpression = 95,
    RuleStrangeMultiplicativeSubexprOfAdditive = 96,
    RuleMultiplicativeExpression = 97,
    RuleUnaryExpression = 98,
    RulePrimaryExpression = 99,
    RuleBrackettedExpression = 100,
    RuleBuiltInCall = 101,
    RuleRegexExpression = 102,
    RuleSubstringExpression = 103,
    RuleStrReplaceExpression = 104,
    RuleExistsFunc = 105,
    RuleNotExistsFunc = 106,
    RuleAggregate = 107,
    RuleIriOrFunction = 108,
    RuleRdfLiteral = 109,
    RuleNumericLiteral = 110,
    RuleNumericLiteralUnsigned = 111,
    RuleNumericLiteralPositive = 112,
    RuleNumericLiteralNegative = 113,
    RuleBooleanLiteral = 114,
    RuleString = 115,
    RuleIri = 116,
    RulePrefixedName = 117,
    RuleBlankNode = 118,
    RuleIriref = 119,
    RulePnameLn = 120,
    RulePnameNs = 121
  };

  explicit SparqlAutomaticParser(antlr4::TokenStream* input);
  ~SparqlAutomaticParser();

  virtual std::string getGrammarFileName() const override;
  virtual const antlr4::atn::ATN& getATN() const override { return _atn; };
  virtual const std::vector<std::string>& getTokenNames() const override {
    return _tokenNames;
  };  // deprecated: use vocabulary instead.
  virtual const std::vector<std::string>& getRuleNames() const override;
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

  class QueryContext;
  class PrologueContext;
  class BaseDeclContext;
  class PrefixDeclContext;
  class SelectQueryContext;
  class SubSelectContext;
  class SelectClauseContext;
  class AliasContext;
  class AliasWithouBrackesContext;
  class ConstructQueryContext;
  class DescribeQueryContext;
  class AskQueryContext;
  class DatasetClauseContext;
  class DefaultGraphClauseContext;
  class NamedGraphClauseContext;
  class SourceSelectorContext;
  class WhereClauseContext;
  class SolutionModifierContext;
  class GroupClauseContext;
  class GroupConditionContext;
  class HavingClauseContext;
  class HavingConditionContext;
  class OrderClauseContext;
  class OrderConditionContext;
  class LimitOffsetClausesContext;
  class LimitClauseContext;
  class OffsetClauseContext;
  class TextLimitClauseContext;
  class ValuesClauseContext;
  class TriplesTemplateContext;
  class GroupGraphPatternContext;
  class GroupGraphPatternSubContext;
  class TriplesBlockContext;
  class GraphPatternNotTriplesContext;
  class OptionalGraphPatternContext;
  class GraphGraphPatternContext;
  class ServiceGraphPatternContext;
  class BindContext;
  class InlineDataContext;
  class DataBlockContext;
  class InlineDataOneVarContext;
  class InlineDataFullContext;
  class DataBlockSingleContext;
  class DataBlockValueContext;
  class MinusGraphPatternContext;
  class GroupOrUnionGraphPatternContext;
  class FilterRContext;
  class ConstraintContext;
  class FunctionCallContext;
  class ArgListContext;
  class ExpressionListContext;
  class ConstructTemplateContext;
  class ConstructTriplesContext;
  class TriplesSameSubjectContext;
  class PropertyListContext;
  class PropertyListNotEmptyContext;
  class VerbContext;
  class ObjectListContext;
  class ObjectRContext;
  class TriplesSameSubjectPathContext;
  class PropertyListPathContext;
  class PropertyListPathNotEmptyContext;
  class VerbPathContext;
  class VerbSimpleContext;
  class VerbPathOrSimpleContext;
  class ObjectListPathContext;
  class ObjectPathContext;
  class PathContext;
  class PathAlternativeContext;
  class PathSequenceContext;
  class PathEltContext;
  class PathEltOrInverseContext;
  class PathModContext;
  class PathPrimaryContext;
  class PathNegatedPropertySetContext;
  class PathOneInPropertySetContext;
  class IntegerContext;
  class TriplesNodeContext;
  class BlankNodePropertyListContext;
  class TriplesNodePathContext;
  class BlankNodePropertyListPathContext;
  class CollectionContext;
  class CollectionPathContext;
  class GraphNodeContext;
  class GraphNodePathContext;
  class VarOrTermContext;
  class VarOrIriContext;
  class VarContext;
  class GraphTermContext;
  class ExpressionContext;
  class ConditionalOrExpressionContext;
  class ConditionalAndExpressionContext;
  class ValueLogicalContext;
  class RelationalExpressionContext;
  class NumericExpressionContext;
  class AdditiveExpressionContext;
  class StrangeMultiplicativeSubexprOfAdditiveContext;
  class MultiplicativeExpressionContext;
  class UnaryExpressionContext;
  class PrimaryExpressionContext;
  class BrackettedExpressionContext;
  class BuiltInCallContext;
  class RegexExpressionContext;
  class SubstringExpressionContext;
  class StrReplaceExpressionContext;
  class ExistsFuncContext;
  class NotExistsFuncContext;
  class AggregateContext;
  class IriOrFunctionContext;
  class RdfLiteralContext;
  class NumericLiteralContext;
  class NumericLiteralUnsignedContext;
  class NumericLiteralPositiveContext;
  class NumericLiteralNegativeContext;
  class BooleanLiteralContext;
  class StringContext;
  class IriContext;
  class PrefixedNameContext;
  class BlankNodeContext;
  class IrirefContext;
  class PnameLnContext;
  class PnameNsContext;

  class QueryContext : public antlr4::ParserRuleContext {
   public:
    QueryContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PrologueContext* prologue();
    ValuesClauseContext* valuesClause();
    antlr4::tree::TerminalNode* EOF();
    SelectQueryContext* selectQuery();
    ConstructQueryContext* constructQuery();
    DescribeQueryContext* describeQuery();
    AskQueryContext* askQuery();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  QueryContext* query();

  class PrologueContext : public antlr4::ParserRuleContext {
   public:
    PrologueContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<BaseDeclContext*> baseDecl();
    BaseDeclContext* baseDecl(size_t i);
    std::vector<PrefixDeclContext*> prefixDecl();
    PrefixDeclContext* prefixDecl(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PrologueContext* prologue();

  class BaseDeclContext : public antlr4::ParserRuleContext {
   public:
    BaseDeclContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* BASE();
    IrirefContext* iriref();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  BaseDeclContext* baseDecl();

  class PrefixDeclContext : public antlr4::ParserRuleContext {
   public:
    PrefixDeclContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* PREFIX();
    antlr4::tree::TerminalNode* PNAME_NS();
    IrirefContext* iriref();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PrefixDeclContext* prefixDecl();

  class SelectQueryContext : public antlr4::ParserRuleContext {
   public:
    SelectQueryContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SelectClauseContext* selectClause();
    WhereClauseContext* whereClause();
    SolutionModifierContext* solutionModifier();
    std::vector<DatasetClauseContext*> datasetClause();
    DatasetClauseContext* datasetClause(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SelectQueryContext* selectQuery();

  class SubSelectContext : public antlr4::ParserRuleContext {
   public:
    SubSelectContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SelectClauseContext* selectClause();
    WhereClauseContext* whereClause();
    SolutionModifierContext* solutionModifier();
    ValuesClauseContext* valuesClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SubSelectContext* subSelect();

  class SelectClauseContext : public antlr4::ParserRuleContext {
   public:
    SelectClauseContext(antlr4::ParserRuleContext* parent,
                        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* SELECT();
    antlr4::tree::TerminalNode* DISTINCT();
    antlr4::tree::TerminalNode* REDUCED();
    std::vector<VarContext*> var();
    VarContext* var(size_t i);
    std::vector<AliasContext*> alias();
    AliasContext* alias(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SelectClauseContext* selectClause();

  class AliasContext : public antlr4::ParserRuleContext {
   public:
    AliasContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    AliasWithouBrackesContext* aliasWithouBrackes();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AliasContext* alias();

  class AliasWithouBrackesContext : public antlr4::ParserRuleContext {
   public:
    AliasWithouBrackesContext(antlr4::ParserRuleContext* parent,
                              size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* AS();
    VarContext* var();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AliasWithouBrackesContext* aliasWithouBrackes();

  class ConstructQueryContext : public antlr4::ParserRuleContext {
   public:
    ConstructQueryContext(antlr4::ParserRuleContext* parent,
                          size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* CONSTRUCT();
    ConstructTemplateContext* constructTemplate();
    WhereClauseContext* whereClause();
    SolutionModifierContext* solutionModifier();
    antlr4::tree::TerminalNode* WHERE();
    std::vector<DatasetClauseContext*> datasetClause();
    DatasetClauseContext* datasetClause(size_t i);
    TriplesTemplateContext* triplesTemplate();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ConstructQueryContext* constructQuery();

  class DescribeQueryContext : public antlr4::ParserRuleContext {
   public:
    DescribeQueryContext(antlr4::ParserRuleContext* parent,
                         size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* DESCRIBE();
    SolutionModifierContext* solutionModifier();
    std::vector<DatasetClauseContext*> datasetClause();
    DatasetClauseContext* datasetClause(size_t i);
    WhereClauseContext* whereClause();
    std::vector<VarOrIriContext*> varOrIri();
    VarOrIriContext* varOrIri(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  DescribeQueryContext* describeQuery();

  class AskQueryContext : public antlr4::ParserRuleContext {
   public:
    AskQueryContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* ASK();
    WhereClauseContext* whereClause();
    SolutionModifierContext* solutionModifier();
    std::vector<DatasetClauseContext*> datasetClause();
    DatasetClauseContext* datasetClause(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AskQueryContext* askQuery();

  class DatasetClauseContext : public antlr4::ParserRuleContext {
   public:
    DatasetClauseContext(antlr4::ParserRuleContext* parent,
                         size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* FROM();
    DefaultGraphClauseContext* defaultGraphClause();
    NamedGraphClauseContext* namedGraphClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  DatasetClauseContext* datasetClause();

  class DefaultGraphClauseContext : public antlr4::ParserRuleContext {
   public:
    DefaultGraphClauseContext(antlr4::ParserRuleContext* parent,
                              size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SourceSelectorContext* sourceSelector();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  DefaultGraphClauseContext* defaultGraphClause();

  class NamedGraphClauseContext : public antlr4::ParserRuleContext {
   public:
    NamedGraphClauseContext(antlr4::ParserRuleContext* parent,
                            size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* NAMED();
    SourceSelectorContext* sourceSelector();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NamedGraphClauseContext* namedGraphClause();

  class SourceSelectorContext : public antlr4::ParserRuleContext {
   public:
    SourceSelectorContext(antlr4::ParserRuleContext* parent,
                          size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IriContext* iri();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SourceSelectorContext* sourceSelector();

  class WhereClauseContext : public antlr4::ParserRuleContext {
   public:
    WhereClauseContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    GroupGraphPatternContext* groupGraphPattern();
    antlr4::tree::TerminalNode* WHERE();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  WhereClauseContext* whereClause();

  class SolutionModifierContext : public antlr4::ParserRuleContext {
   public:
    SolutionModifierContext(antlr4::ParserRuleContext* parent,
                            size_t invokingState);
    virtual size_t getRuleIndex() const override;
    GroupClauseContext* groupClause();
    HavingClauseContext* havingClause();
    OrderClauseContext* orderClause();
    LimitOffsetClausesContext* limitOffsetClauses();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SolutionModifierContext* solutionModifier();

  class GroupClauseContext : public antlr4::ParserRuleContext {
   public:
    GroupClauseContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* GROUPBY();
    std::vector<GroupConditionContext*> groupCondition();
    GroupConditionContext* groupCondition(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GroupClauseContext* groupClause();

  class GroupConditionContext : public antlr4::ParserRuleContext {
   public:
    GroupConditionContext(antlr4::ParserRuleContext* parent,
                          size_t invokingState);
    virtual size_t getRuleIndex() const override;
    BuiltInCallContext* builtInCall();
    FunctionCallContext* functionCall();
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* AS();
    VarContext* var();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GroupConditionContext* groupCondition();

  class HavingClauseContext : public antlr4::ParserRuleContext {
   public:
    HavingClauseContext(antlr4::ParserRuleContext* parent,
                        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* HAVING();
    std::vector<HavingConditionContext*> havingCondition();
    HavingConditionContext* havingCondition(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  HavingClauseContext* havingClause();

  class HavingConditionContext : public antlr4::ParserRuleContext {
   public:
    HavingConditionContext(antlr4::ParserRuleContext* parent,
                           size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ConstraintContext* constraint();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  HavingConditionContext* havingCondition();

  class OrderClauseContext : public antlr4::ParserRuleContext {
   public:
    OrderClauseContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* ORDERBY();
    std::vector<OrderConditionContext*> orderCondition();
    OrderConditionContext* orderCondition(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  OrderClauseContext* orderClause();

  class OrderConditionContext : public antlr4::ParserRuleContext {
   public:
    OrderConditionContext(antlr4::ParserRuleContext* parent,
                          size_t invokingState);
    virtual size_t getRuleIndex() const override;
    BrackettedExpressionContext* brackettedExpression();
    antlr4::tree::TerminalNode* ASC();
    antlr4::tree::TerminalNode* DESC();
    ConstraintContext* constraint();
    VarContext* var();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  OrderConditionContext* orderCondition();

  class LimitOffsetClausesContext : public antlr4::ParserRuleContext {
   public:
    LimitOffsetClausesContext(antlr4::ParserRuleContext* parent,
                              size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LimitClauseContext* limitClause();
    OffsetClauseContext* offsetClause();
    TextLimitClauseContext* textLimitClause();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  LimitOffsetClausesContext* limitOffsetClauses();

  class LimitClauseContext : public antlr4::ParserRuleContext {
   public:
    LimitClauseContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LIMIT();
    IntegerContext* integer();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  LimitClauseContext* limitClause();

  class OffsetClauseContext : public antlr4::ParserRuleContext {
   public:
    OffsetClauseContext(antlr4::ParserRuleContext* parent,
                        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* OFFSET();
    IntegerContext* integer();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  OffsetClauseContext* offsetClause();

  class TextLimitClauseContext : public antlr4::ParserRuleContext {
   public:
    TextLimitClauseContext(antlr4::ParserRuleContext* parent,
                           size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* TEXTLIMIT();
    IntegerContext* integer();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TextLimitClauseContext* textLimitClause();

  class ValuesClauseContext : public antlr4::ParserRuleContext {
   public:
    ValuesClauseContext(antlr4::ParserRuleContext* parent,
                        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* VALUES();
    DataBlockContext* dataBlock();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ValuesClauseContext* valuesClause();

  class TriplesTemplateContext : public antlr4::ParserRuleContext {
   public:
    TriplesTemplateContext(antlr4::ParserRuleContext* parent,
                           size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TriplesSameSubjectContext* triplesSameSubject();
    TriplesTemplateContext* triplesTemplate();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TriplesTemplateContext* triplesTemplate();

  class GroupGraphPatternContext : public antlr4::ParserRuleContext {
   public:
    GroupGraphPatternContext(antlr4::ParserRuleContext* parent,
                             size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SubSelectContext* subSelect();
    GroupGraphPatternSubContext* groupGraphPatternSub();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GroupGraphPatternContext* groupGraphPattern();

  class GroupGraphPatternSubContext : public antlr4::ParserRuleContext {
   public:
    GroupGraphPatternSubContext(antlr4::ParserRuleContext* parent,
                                size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<TriplesBlockContext*> triplesBlock();
    TriplesBlockContext* triplesBlock(size_t i);
    std::vector<GraphPatternNotTriplesContext*> graphPatternNotTriples();
    GraphPatternNotTriplesContext* graphPatternNotTriples(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GroupGraphPatternSubContext* groupGraphPatternSub();

  class TriplesBlockContext : public antlr4::ParserRuleContext {
   public:
    TriplesBlockContext(antlr4::ParserRuleContext* parent,
                        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TriplesSameSubjectPathContext* triplesSameSubjectPath();
    TriplesBlockContext* triplesBlock();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TriplesBlockContext* triplesBlock();

  class GraphPatternNotTriplesContext : public antlr4::ParserRuleContext {
   public:
    GraphPatternNotTriplesContext(antlr4::ParserRuleContext* parent,
                                  size_t invokingState);
    virtual size_t getRuleIndex() const override;
    GroupOrUnionGraphPatternContext* groupOrUnionGraphPattern();
    OptionalGraphPatternContext* optionalGraphPattern();
    MinusGraphPatternContext* minusGraphPattern();
    GraphGraphPatternContext* graphGraphPattern();
    ServiceGraphPatternContext* serviceGraphPattern();
    FilterRContext* filterR();
    BindContext* bind();
    InlineDataContext* inlineData();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GraphPatternNotTriplesContext* graphPatternNotTriples();

  class OptionalGraphPatternContext : public antlr4::ParserRuleContext {
   public:
    OptionalGraphPatternContext(antlr4::ParserRuleContext* parent,
                                size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* OPTIONAL();
    GroupGraphPatternContext* groupGraphPattern();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  OptionalGraphPatternContext* optionalGraphPattern();

  class GraphGraphPatternContext : public antlr4::ParserRuleContext {
   public:
    GraphGraphPatternContext(antlr4::ParserRuleContext* parent,
                             size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* GRAPH();
    VarOrIriContext* varOrIri();
    GroupGraphPatternContext* groupGraphPattern();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GraphGraphPatternContext* graphGraphPattern();

  class ServiceGraphPatternContext : public antlr4::ParserRuleContext {
   public:
    ServiceGraphPatternContext(antlr4::ParserRuleContext* parent,
                               size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* SERVICE();
    VarOrIriContext* varOrIri();
    GroupGraphPatternContext* groupGraphPattern();
    antlr4::tree::TerminalNode* SILENT();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ServiceGraphPatternContext* serviceGraphPattern();

  class BindContext : public antlr4::ParserRuleContext {
   public:
    BindContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* BIND();
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* AS();
    VarContext* var();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  BindContext* bind();

  class InlineDataContext : public antlr4::ParserRuleContext {
   public:
    InlineDataContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* VALUES();
    DataBlockContext* dataBlock();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  InlineDataContext* inlineData();

  class DataBlockContext : public antlr4::ParserRuleContext {
   public:
    DataBlockContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    InlineDataOneVarContext* inlineDataOneVar();
    InlineDataFullContext* inlineDataFull();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  DataBlockContext* dataBlock();

  class InlineDataOneVarContext : public antlr4::ParserRuleContext {
   public:
    InlineDataOneVarContext(antlr4::ParserRuleContext* parent,
                            size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VarContext* var();
    std::vector<DataBlockValueContext*> dataBlockValue();
    DataBlockValueContext* dataBlockValue(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  InlineDataOneVarContext* inlineDataOneVar();

  class InlineDataFullContext : public antlr4::ParserRuleContext {
   public:
    InlineDataFullContext(antlr4::ParserRuleContext* parent,
                          size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* NIL();
    std::vector<DataBlockSingleContext*> dataBlockSingle();
    DataBlockSingleContext* dataBlockSingle(size_t i);
    std::vector<VarContext*> var();
    VarContext* var(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  InlineDataFullContext* inlineDataFull();

  class DataBlockSingleContext : public antlr4::ParserRuleContext {
   public:
    DataBlockSingleContext(antlr4::ParserRuleContext* parent,
                           size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* NIL();
    std::vector<DataBlockValueContext*> dataBlockValue();
    DataBlockValueContext* dataBlockValue(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  DataBlockSingleContext* dataBlockSingle();

  class DataBlockValueContext : public antlr4::ParserRuleContext {
   public:
    DataBlockValueContext(antlr4::ParserRuleContext* parent,
                          size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IriContext* iri();
    RdfLiteralContext* rdfLiteral();
    NumericLiteralContext* numericLiteral();
    BooleanLiteralContext* booleanLiteral();
    antlr4::tree::TerminalNode* UNDEF();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  DataBlockValueContext* dataBlockValue();

  class MinusGraphPatternContext : public antlr4::ParserRuleContext {
   public:
    MinusGraphPatternContext(antlr4::ParserRuleContext* parent,
                             size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* MINUS();
    GroupGraphPatternContext* groupGraphPattern();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  MinusGraphPatternContext* minusGraphPattern();

  class GroupOrUnionGraphPatternContext : public antlr4::ParserRuleContext {
   public:
    GroupOrUnionGraphPatternContext(antlr4::ParserRuleContext* parent,
                                    size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<GroupGraphPatternContext*> groupGraphPattern();
    GroupGraphPatternContext* groupGraphPattern(size_t i);
    std::vector<antlr4::tree::TerminalNode*> UNION();
    antlr4::tree::TerminalNode* UNION(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GroupOrUnionGraphPatternContext* groupOrUnionGraphPattern();

  class FilterRContext : public antlr4::ParserRuleContext {
   public:
    FilterRContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* FILTER();
    ConstraintContext* constraint();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  FilterRContext* filterR();

  class ConstraintContext : public antlr4::ParserRuleContext {
   public:
    ConstraintContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    BrackettedExpressionContext* brackettedExpression();
    BuiltInCallContext* builtInCall();
    FunctionCallContext* functionCall();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ConstraintContext* constraint();

  class FunctionCallContext : public antlr4::ParserRuleContext {
   public:
    FunctionCallContext(antlr4::ParserRuleContext* parent,
                        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IriContext* iri();
    ArgListContext* argList();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  FunctionCallContext* functionCall();

  class ArgListContext : public antlr4::ParserRuleContext {
   public:
    ArgListContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* NIL();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    antlr4::tree::TerminalNode* DISTINCT();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ArgListContext* argList();

  class ExpressionListContext : public antlr4::ParserRuleContext {
   public:
    ExpressionListContext(antlr4::ParserRuleContext* parent,
                          size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* NIL();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ExpressionListContext* expressionList();

  class ConstructTemplateContext : public antlr4::ParserRuleContext {
   public:
    ConstructTemplateContext(antlr4::ParserRuleContext* parent,
                             size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ConstructTriplesContext* constructTriples();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ConstructTemplateContext* constructTemplate();

  class ConstructTriplesContext : public antlr4::ParserRuleContext {
   public:
    ConstructTriplesContext(antlr4::ParserRuleContext* parent,
                            size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TriplesSameSubjectContext* triplesSameSubject();
    ConstructTriplesContext* constructTriples();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ConstructTriplesContext* constructTriples();

  class TriplesSameSubjectContext : public antlr4::ParserRuleContext {
   public:
    TriplesSameSubjectContext(antlr4::ParserRuleContext* parent,
                              size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VarOrTermContext* varOrTerm();
    PropertyListNotEmptyContext* propertyListNotEmpty();
    TriplesNodeContext* triplesNode();
    PropertyListContext* propertyList();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TriplesSameSubjectContext* triplesSameSubject();

  class PropertyListContext : public antlr4::ParserRuleContext {
   public:
    PropertyListContext(antlr4::ParserRuleContext* parent,
                        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PropertyListNotEmptyContext* propertyListNotEmpty();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PropertyListContext* propertyList();

  class PropertyListNotEmptyContext : public antlr4::ParserRuleContext {
   public:
    PropertyListNotEmptyContext(antlr4::ParserRuleContext* parent,
                                size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<VerbContext*> verb();
    VerbContext* verb(size_t i);
    std::vector<ObjectListContext*> objectList();
    ObjectListContext* objectList(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PropertyListNotEmptyContext* propertyListNotEmpty();

  class VerbContext : public antlr4::ParserRuleContext {
   public:
    VerbContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VarOrIriContext* varOrIri();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  VerbContext* verb();

  class ObjectListContext : public antlr4::ParserRuleContext {
   public:
    ObjectListContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ObjectRContext*> objectR();
    ObjectRContext* objectR(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ObjectListContext* objectList();

  class ObjectRContext : public antlr4::ParserRuleContext {
   public:
    ObjectRContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    GraphNodeContext* graphNode();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ObjectRContext* objectR();

  class TriplesSameSubjectPathContext : public antlr4::ParserRuleContext {
   public:
    TriplesSameSubjectPathContext(antlr4::ParserRuleContext* parent,
                                  size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VarOrTermContext* varOrTerm();
    PropertyListPathNotEmptyContext* propertyListPathNotEmpty();
    TriplesNodePathContext* triplesNodePath();
    PropertyListPathContext* propertyListPath();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TriplesSameSubjectPathContext* triplesSameSubjectPath();

  class PropertyListPathContext : public antlr4::ParserRuleContext {
   public:
    PropertyListPathContext(antlr4::ParserRuleContext* parent,
                            size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PropertyListPathNotEmptyContext* propertyListPathNotEmpty();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PropertyListPathContext* propertyListPath();

  class PropertyListPathNotEmptyContext : public antlr4::ParserRuleContext {
   public:
    PropertyListPathNotEmptyContext(antlr4::ParserRuleContext* parent,
                                    size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<VerbPathOrSimpleContext*> verbPathOrSimple();
    VerbPathOrSimpleContext* verbPathOrSimple(size_t i);
    ObjectListPathContext* objectListPath();
    std::vector<ObjectListContext*> objectList();
    ObjectListContext* objectList(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PropertyListPathNotEmptyContext* propertyListPathNotEmpty();

  class VerbPathContext : public antlr4::ParserRuleContext {
   public:
    VerbPathContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PathContext* path();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  VerbPathContext* verbPath();

  class VerbSimpleContext : public antlr4::ParserRuleContext {
   public:
    VerbSimpleContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VarContext* var();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  VerbSimpleContext* verbSimple();

  class VerbPathOrSimpleContext : public antlr4::ParserRuleContext {
   public:
    VerbPathOrSimpleContext(antlr4::ParserRuleContext* parent,
                            size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VerbPathContext* verbPath();
    VerbSimpleContext* verbSimple();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  VerbPathOrSimpleContext* verbPathOrSimple();

  class ObjectListPathContext : public antlr4::ParserRuleContext {
   public:
    ObjectListPathContext(antlr4::ParserRuleContext* parent,
                          size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ObjectPathContext*> objectPath();
    ObjectPathContext* objectPath(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ObjectListPathContext* objectListPath();

  class ObjectPathContext : public antlr4::ParserRuleContext {
   public:
    ObjectPathContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    GraphNodePathContext* graphNodePath();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ObjectPathContext* objectPath();

  class PathContext : public antlr4::ParserRuleContext {
   public:
    PathContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PathAlternativeContext* pathAlternative();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PathContext* path();

  class PathAlternativeContext : public antlr4::ParserRuleContext {
   public:
    PathAlternativeContext(antlr4::ParserRuleContext* parent,
                           size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<PathSequenceContext*> pathSequence();
    PathSequenceContext* pathSequence(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PathAlternativeContext* pathAlternative();

  class PathSequenceContext : public antlr4::ParserRuleContext {
   public:
    PathSequenceContext(antlr4::ParserRuleContext* parent,
                        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<PathEltOrInverseContext*> pathEltOrInverse();
    PathEltOrInverseContext* pathEltOrInverse(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PathSequenceContext* pathSequence();

  class PathEltContext : public antlr4::ParserRuleContext {
   public:
    PathEltContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PathPrimaryContext* pathPrimary();
    PathModContext* pathMod();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PathEltContext* pathElt();

  class PathEltOrInverseContext : public antlr4::ParserRuleContext {
   public:
    antlr4::Token* negationOperator = nullptr;
    PathEltOrInverseContext(antlr4::ParserRuleContext* parent,
                            size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PathEltContext* pathElt();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PathEltOrInverseContext* pathEltOrInverse();

  class PathModContext : public antlr4::ParserRuleContext {
   public:
    PathModContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PathModContext* pathMod();

  class PathPrimaryContext : public antlr4::ParserRuleContext {
   public:
    PathPrimaryContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IriContext* iri();
    PathNegatedPropertySetContext* pathNegatedPropertySet();
    PathContext* path();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PathPrimaryContext* pathPrimary();

  class PathNegatedPropertySetContext : public antlr4::ParserRuleContext {
   public:
    PathNegatedPropertySetContext(antlr4::ParserRuleContext* parent,
                                  size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<PathOneInPropertySetContext*> pathOneInPropertySet();
    PathOneInPropertySetContext* pathOneInPropertySet(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PathNegatedPropertySetContext* pathNegatedPropertySet();

  class PathOneInPropertySetContext : public antlr4::ParserRuleContext {
   public:
    PathOneInPropertySetContext(antlr4::ParserRuleContext* parent,
                                size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IriContext* iri();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PathOneInPropertySetContext* pathOneInPropertySet();

  class IntegerContext : public antlr4::ParserRuleContext {
   public:
    IntegerContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* INTEGER();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  IntegerContext* integer();

  class TriplesNodeContext : public antlr4::ParserRuleContext {
   public:
    TriplesNodeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CollectionContext* collection();
    BlankNodePropertyListContext* blankNodePropertyList();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TriplesNodeContext* triplesNode();

  class BlankNodePropertyListContext : public antlr4::ParserRuleContext {
   public:
    BlankNodePropertyListContext(antlr4::ParserRuleContext* parent,
                                 size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PropertyListNotEmptyContext* propertyListNotEmpty();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  BlankNodePropertyListContext* blankNodePropertyList();

  class TriplesNodePathContext : public antlr4::ParserRuleContext {
   public:
    TriplesNodePathContext(antlr4::ParserRuleContext* parent,
                           size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CollectionPathContext* collectionPath();
    BlankNodePropertyListPathContext* blankNodePropertyListPath();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TriplesNodePathContext* triplesNodePath();

  class BlankNodePropertyListPathContext : public antlr4::ParserRuleContext {
   public:
    BlankNodePropertyListPathContext(antlr4::ParserRuleContext* parent,
                                     size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PropertyListPathNotEmptyContext* propertyListPathNotEmpty();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  BlankNodePropertyListPathContext* blankNodePropertyListPath();

  class CollectionContext : public antlr4::ParserRuleContext {
   public:
    CollectionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<GraphNodeContext*> graphNode();
    GraphNodeContext* graphNode(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  CollectionContext* collection();

  class CollectionPathContext : public antlr4::ParserRuleContext {
   public:
    CollectionPathContext(antlr4::ParserRuleContext* parent,
                          size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<GraphNodePathContext*> graphNodePath();
    GraphNodePathContext* graphNodePath(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  CollectionPathContext* collectionPath();

  class GraphNodeContext : public antlr4::ParserRuleContext {
   public:
    GraphNodeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VarOrTermContext* varOrTerm();
    TriplesNodeContext* triplesNode();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GraphNodeContext* graphNode();

  class GraphNodePathContext : public antlr4::ParserRuleContext {
   public:
    GraphNodePathContext(antlr4::ParserRuleContext* parent,
                         size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VarOrTermContext* varOrTerm();
    TriplesNodePathContext* triplesNodePath();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GraphNodePathContext* graphNodePath();

  class VarOrTermContext : public antlr4::ParserRuleContext {
   public:
    VarOrTermContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VarContext* var();
    GraphTermContext* graphTerm();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  VarOrTermContext* varOrTerm();

  class VarOrIriContext : public antlr4::ParserRuleContext {
   public:
    VarOrIriContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VarContext* var();
    IriContext* iri();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  VarOrIriContext* varOrIri();

  class VarContext : public antlr4::ParserRuleContext {
   public:
    VarContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* VAR1();
    antlr4::tree::TerminalNode* VAR2();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  VarContext* var();

  class GraphTermContext : public antlr4::ParserRuleContext {
   public:
    GraphTermContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IriContext* iri();
    RdfLiteralContext* rdfLiteral();
    NumericLiteralContext* numericLiteral();
    BooleanLiteralContext* booleanLiteral();
    BlankNodeContext* blankNode();
    antlr4::tree::TerminalNode* NIL();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GraphTermContext* graphTerm();

  class ExpressionContext : public antlr4::ParserRuleContext {
   public:
    ExpressionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ConditionalOrExpressionContext* conditionalOrExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ExpressionContext* expression();

  class ConditionalOrExpressionContext : public antlr4::ParserRuleContext {
   public:
    ConditionalOrExpressionContext(antlr4::ParserRuleContext* parent,
                                   size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ConditionalAndExpressionContext*> conditionalAndExpression();
    ConditionalAndExpressionContext* conditionalAndExpression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ConditionalOrExpressionContext* conditionalOrExpression();

  class ConditionalAndExpressionContext : public antlr4::ParserRuleContext {
   public:
    ConditionalAndExpressionContext(antlr4::ParserRuleContext* parent,
                                    size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ValueLogicalContext*> valueLogical();
    ValueLogicalContext* valueLogical(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ConditionalAndExpressionContext* conditionalAndExpression();

  class ValueLogicalContext : public antlr4::ParserRuleContext {
   public:
    ValueLogicalContext(antlr4::ParserRuleContext* parent,
                        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    RelationalExpressionContext* relationalExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ValueLogicalContext* valueLogical();

  class RelationalExpressionContext : public antlr4::ParserRuleContext {
   public:
    RelationalExpressionContext(antlr4::ParserRuleContext* parent,
                                size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<NumericExpressionContext*> numericExpression();
    NumericExpressionContext* numericExpression(size_t i);
    antlr4::tree::TerminalNode* IN();
    ExpressionListContext* expressionList();
    antlr4::tree::TerminalNode* NOT();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  RelationalExpressionContext* relationalExpression();

  class NumericExpressionContext : public antlr4::ParserRuleContext {
   public:
    NumericExpressionContext(antlr4::ParserRuleContext* parent,
                             size_t invokingState);
    virtual size_t getRuleIndex() const override;
    AdditiveExpressionContext* additiveExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NumericExpressionContext* numericExpression();

  class AdditiveExpressionContext : public antlr4::ParserRuleContext {
   public:
    AdditiveExpressionContext(antlr4::ParserRuleContext* parent,
                              size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<MultiplicativeExpressionContext*> multiplicativeExpression();
    MultiplicativeExpressionContext* multiplicativeExpression(size_t i);
    std::vector<StrangeMultiplicativeSubexprOfAdditiveContext*>
    strangeMultiplicativeSubexprOfAdditive();
    StrangeMultiplicativeSubexprOfAdditiveContext*
    strangeMultiplicativeSubexprOfAdditive(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AdditiveExpressionContext* additiveExpression();

  class StrangeMultiplicativeSubexprOfAdditiveContext
      : public antlr4::ParserRuleContext {
   public:
    StrangeMultiplicativeSubexprOfAdditiveContext(
        antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NumericLiteralPositiveContext* numericLiteralPositive();
    NumericLiteralNegativeContext* numericLiteralNegative();
    std::vector<UnaryExpressionContext*> unaryExpression();
    UnaryExpressionContext* unaryExpression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  StrangeMultiplicativeSubexprOfAdditiveContext*
  strangeMultiplicativeSubexprOfAdditive();

  class MultiplicativeExpressionContext : public antlr4::ParserRuleContext {
   public:
    MultiplicativeExpressionContext(antlr4::ParserRuleContext* parent,
                                    size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<UnaryExpressionContext*> unaryExpression();
    UnaryExpressionContext* unaryExpression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  MultiplicativeExpressionContext* multiplicativeExpression();

  class UnaryExpressionContext : public antlr4::ParserRuleContext {
   public:
    UnaryExpressionContext(antlr4::ParserRuleContext* parent,
                           size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PrimaryExpressionContext* primaryExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  UnaryExpressionContext* unaryExpression();

  class PrimaryExpressionContext : public antlr4::ParserRuleContext {
   public:
    PrimaryExpressionContext(antlr4::ParserRuleContext* parent,
                             size_t invokingState);
    virtual size_t getRuleIndex() const override;
    BrackettedExpressionContext* brackettedExpression();
    BuiltInCallContext* builtInCall();
    IriOrFunctionContext* iriOrFunction();
    RdfLiteralContext* rdfLiteral();
    NumericLiteralContext* numericLiteral();
    BooleanLiteralContext* booleanLiteral();
    VarContext* var();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PrimaryExpressionContext* primaryExpression();

  class BrackettedExpressionContext : public antlr4::ParserRuleContext {
   public:
    BrackettedExpressionContext(antlr4::ParserRuleContext* parent,
                                size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExpressionContext* expression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  BrackettedExpressionContext* brackettedExpression();

  class BuiltInCallContext : public antlr4::ParserRuleContext {
   public:
    BuiltInCallContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    AggregateContext* aggregate();
    antlr4::tree::TerminalNode* STR();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);
    antlr4::tree::TerminalNode* LANG();
    antlr4::tree::TerminalNode* LANGMATCHES();
    antlr4::tree::TerminalNode* DATATYPE();
    antlr4::tree::TerminalNode* BOUND();
    VarContext* var();
    antlr4::tree::TerminalNode* IRI();
    antlr4::tree::TerminalNode* URI();
    antlr4::tree::TerminalNode* BNODE();
    antlr4::tree::TerminalNode* NIL();
    antlr4::tree::TerminalNode* RAND();
    antlr4::tree::TerminalNode* ABS();
    antlr4::tree::TerminalNode* CEIL();
    antlr4::tree::TerminalNode* FLOOR();
    antlr4::tree::TerminalNode* ROUND();
    antlr4::tree::TerminalNode* CONCAT();
    ExpressionListContext* expressionList();
    SubstringExpressionContext* substringExpression();
    antlr4::tree::TerminalNode* STRLEN();
    StrReplaceExpressionContext* strReplaceExpression();
    antlr4::tree::TerminalNode* UCASE();
    antlr4::tree::TerminalNode* LCASE();
    antlr4::tree::TerminalNode* ENCODE();
    antlr4::tree::TerminalNode* FOR();
    antlr4::tree::TerminalNode* CONTAINS();
    antlr4::tree::TerminalNode* STRSTARTS();
    antlr4::tree::TerminalNode* STRENDS();
    antlr4::tree::TerminalNode* STRBEFORE();
    antlr4::tree::TerminalNode* STRAFTER();
    antlr4::tree::TerminalNode* YEAR();
    antlr4::tree::TerminalNode* MONTH();
    antlr4::tree::TerminalNode* DAY();
    antlr4::tree::TerminalNode* HOURS();
    antlr4::tree::TerminalNode* MINUTES();
    antlr4::tree::TerminalNode* SECONDS();
    antlr4::tree::TerminalNode* TIMEZONE();
    antlr4::tree::TerminalNode* TZ();
    antlr4::tree::TerminalNode* NOW();
    antlr4::tree::TerminalNode* UUID();
    antlr4::tree::TerminalNode* STRUUID();
    antlr4::tree::TerminalNode* MD5();
    antlr4::tree::TerminalNode* SHA1();
    antlr4::tree::TerminalNode* SHA256();
    antlr4::tree::TerminalNode* SHA384();
    antlr4::tree::TerminalNode* SHA512();
    antlr4::tree::TerminalNode* COALESCE();
    antlr4::tree::TerminalNode* IF();
    antlr4::tree::TerminalNode* STRLANG();
    antlr4::tree::TerminalNode* STRDT();
    antlr4::tree::TerminalNode* SAMETERM();
    antlr4::tree::TerminalNode* ISIRI();
    antlr4::tree::TerminalNode* ISURI();
    antlr4::tree::TerminalNode* ISBLANK();
    antlr4::tree::TerminalNode* ISLITERAL();
    antlr4::tree::TerminalNode* ISNUMERIC();
    RegexExpressionContext* regexExpression();
    ExistsFuncContext* existsFunc();
    NotExistsFuncContext* notExistsFunc();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  BuiltInCallContext* builtInCall();

  class RegexExpressionContext : public antlr4::ParserRuleContext {
   public:
    RegexExpressionContext(antlr4::ParserRuleContext* parent,
                           size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* REGEX();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  RegexExpressionContext* regexExpression();

  class SubstringExpressionContext : public antlr4::ParserRuleContext {
   public:
    SubstringExpressionContext(antlr4::ParserRuleContext* parent,
                               size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* SUBSTR();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SubstringExpressionContext* substringExpression();

  class StrReplaceExpressionContext : public antlr4::ParserRuleContext {
   public:
    StrReplaceExpressionContext(antlr4::ParserRuleContext* parent,
                                size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* REPLACE();
    std::vector<ExpressionContext*> expression();
    ExpressionContext* expression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  StrReplaceExpressionContext* strReplaceExpression();

  class ExistsFuncContext : public antlr4::ParserRuleContext {
   public:
    ExistsFuncContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* EXISTS();
    GroupGraphPatternContext* groupGraphPattern();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ExistsFuncContext* existsFunc();

  class NotExistsFuncContext : public antlr4::ParserRuleContext {
   public:
    NotExistsFuncContext(antlr4::ParserRuleContext* parent,
                         size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* NOT();
    antlr4::tree::TerminalNode* EXISTS();
    GroupGraphPatternContext* groupGraphPattern();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NotExistsFuncContext* notExistsFunc();

  class AggregateContext : public antlr4::ParserRuleContext {
   public:
    AggregateContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* COUNT();
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* DISTINCT();
    antlr4::tree::TerminalNode* SUM();
    antlr4::tree::TerminalNode* MIN();
    antlr4::tree::TerminalNode* MAX();
    antlr4::tree::TerminalNode* AVG();
    antlr4::tree::TerminalNode* SAMPLE();
    antlr4::tree::TerminalNode* GROUP_CONCAT();
    antlr4::tree::TerminalNode* SEPARATOR();
    StringContext* string();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AggregateContext* aggregate();

  class IriOrFunctionContext : public antlr4::ParserRuleContext {
   public:
    IriOrFunctionContext(antlr4::ParserRuleContext* parent,
                         size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IriContext* iri();
    ArgListContext* argList();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  IriOrFunctionContext* iriOrFunction();

  class RdfLiteralContext : public antlr4::ParserRuleContext {
   public:
    RdfLiteralContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    StringContext* string();
    antlr4::tree::TerminalNode* LANGTAG();
    IriContext* iri();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  RdfLiteralContext* rdfLiteral();

  class NumericLiteralContext : public antlr4::ParserRuleContext {
   public:
    NumericLiteralContext(antlr4::ParserRuleContext* parent,
                          size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NumericLiteralUnsignedContext* numericLiteralUnsigned();
    NumericLiteralPositiveContext* numericLiteralPositive();
    NumericLiteralNegativeContext* numericLiteralNegative();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NumericLiteralContext* numericLiteral();

  class NumericLiteralUnsignedContext : public antlr4::ParserRuleContext {
   public:
    NumericLiteralUnsignedContext(antlr4::ParserRuleContext* parent,
                                  size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* INTEGER();
    antlr4::tree::TerminalNode* DECIMAL();
    antlr4::tree::TerminalNode* DOUBLE();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NumericLiteralUnsignedContext* numericLiteralUnsigned();

  class NumericLiteralPositiveContext : public antlr4::ParserRuleContext {
   public:
    NumericLiteralPositiveContext(antlr4::ParserRuleContext* parent,
                                  size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* INTEGER_POSITIVE();
    antlr4::tree::TerminalNode* DECIMAL_POSITIVE();
    antlr4::tree::TerminalNode* DOUBLE_POSITIVE();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NumericLiteralPositiveContext* numericLiteralPositive();

  class NumericLiteralNegativeContext : public antlr4::ParserRuleContext {
   public:
    NumericLiteralNegativeContext(antlr4::ParserRuleContext* parent,
                                  size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* INTEGER_NEGATIVE();
    antlr4::tree::TerminalNode* DECIMAL_NEGATIVE();
    antlr4::tree::TerminalNode* DOUBLE_NEGATIVE();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NumericLiteralNegativeContext* numericLiteralNegative();

  class BooleanLiteralContext : public antlr4::ParserRuleContext {
   public:
    BooleanLiteralContext(antlr4::ParserRuleContext* parent,
                          size_t invokingState);
    virtual size_t getRuleIndex() const override;

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  BooleanLiteralContext* booleanLiteral();

  class StringContext : public antlr4::ParserRuleContext {
   public:
    StringContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* STRING_LITERAL1();
    antlr4::tree::TerminalNode* STRING_LITERAL2();
    antlr4::tree::TerminalNode* STRING_LITERAL_LONG1();
    antlr4::tree::TerminalNode* STRING_LITERAL_LONG2();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  StringContext* string();

  class IriContext : public antlr4::ParserRuleContext {
   public:
    IriContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IrirefContext* iriref();
    PrefixedNameContext* prefixedName();
    antlr4::tree::TerminalNode* LANGTAG();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  IriContext* iri();

  class PrefixedNameContext : public antlr4::ParserRuleContext {
   public:
    PrefixedNameContext(antlr4::ParserRuleContext* parent,
                        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PnameLnContext* pnameLn();
    PnameNsContext* pnameNs();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PrefixedNameContext* prefixedName();

  class BlankNodeContext : public antlr4::ParserRuleContext {
   public:
    BlankNodeContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* BLANK_NODE_LABEL();
    antlr4::tree::TerminalNode* ANON();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  BlankNodeContext* blankNode();

  class IrirefContext : public antlr4::ParserRuleContext {
   public:
    IrirefContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* IRI_REF();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  IrirefContext* iriref();

  class PnameLnContext : public antlr4::ParserRuleContext {
   public:
    PnameLnContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* PNAME_LN();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PnameLnContext* pnameLn();

  class PnameNsContext : public antlr4::ParserRuleContext {
   public:
    PnameNsContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* PNAME_NS();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual antlrcpp::Any accept(
        antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PnameNsContext* pnameNs();

 private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;

  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};
