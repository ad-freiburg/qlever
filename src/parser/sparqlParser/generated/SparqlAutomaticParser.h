
// Generated from SparqlAutomatic.g4 by ANTLR 4.11.1

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
    INTERNALSORTBY = 47,
    ASC = 48,
    DESC = 49,
    LIMIT = 50,
    OFFSET = 51,
    TEXTLIMIT = 52,
    VALUES = 53,
    LOAD = 54,
    SILENT = 55,
    CLEAR = 56,
    DROP = 57,
    CREATE = 58,
    ADD = 59,
    DATA = 60,
    MOVE = 61,
    COPY = 62,
    INSERT = 63,
    DELETE = 64,
    WITH = 65,
    USING = 66,
    DEFAULT = 67,
    GRAPH = 68,
    ALL = 69,
    OPTIONAL = 70,
    SERVICE = 71,
    BIND = 72,
    UNDEF = 73,
    MINUS = 74,
    UNION = 75,
    FILTER = 76,
    NOT = 77,
    IN = 78,
    STR = 79,
    LANG = 80,
    LANGMATCHES = 81,
    DATATYPE = 82,
    BOUND = 83,
    IRI = 84,
    URI = 85,
    BNODE = 86,
    RAND = 87,
    ABS = 88,
    CEIL = 89,
    FLOOR = 90,
    ROUND = 91,
    CONCAT = 92,
    STRLEN = 93,
    UCASE = 94,
    LCASE = 95,
    ENCODE = 96,
    FOR = 97,
    CONTAINS = 98,
    STRSTARTS = 99,
    STRENDS = 100,
    STRBEFORE = 101,
    STRAFTER = 102,
    YEAR = 103,
    MONTH = 104,
    DAY = 105,
    HOURS = 106,
    MINUTES = 107,
    SECONDS = 108,
    TIMEZONE = 109,
    TZ = 110,
    NOW = 111,
    UUID = 112,
    STRUUID = 113,
    SHA1 = 114,
    SHA256 = 115,
    SHA384 = 116,
    SHA512 = 117,
    MD5 = 118,
    COALESCE = 119,
    IF = 120,
    STRLANG = 121,
    STRDT = 122,
    SAMETERM = 123,
    ISIRI = 124,
    ISURI = 125,
    ISBLANK = 126,
    ISLITERAL = 127,
    ISNUMERIC = 128,
    REGEX = 129,
    SUBSTR = 130,
    REPLACE = 131,
    EXISTS = 132,
    COUNT = 133,
    SUM = 134,
    MIN = 135,
    MAX = 136,
    AVG = 137,
    SAMPLE = 138,
    SEPARATOR = 139,
    IRI_REF = 140,
    PNAME_NS = 141,
    PNAME_LN = 142,
    BLANK_NODE_LABEL = 143,
    VAR1 = 144,
    VAR2 = 145,
    LANGTAG = 146,
    PREFIX_LANGTAG = 147,
    INTEGER = 148,
    DECIMAL = 149,
    DOUBLE = 150,
    INTEGER_POSITIVE = 151,
    DECIMAL_POSITIVE = 152,
    DOUBLE_POSITIVE = 153,
    INTEGER_NEGATIVE = 154,
    DECIMAL_NEGATIVE = 155,
    DOUBLE_NEGATIVE = 156,
    EXPONENT = 157,
    STRING_LITERAL1 = 158,
    STRING_LITERAL2 = 159,
    STRING_LITERAL_LONG1 = 160,
    STRING_LITERAL_LONG2 = 161,
    ECHAR = 162,
    NIL = 163,
    ANON = 164,
    PN_CHARS_U = 165,
    VARNAME = 166,
    PN_PREFIX = 167,
    PN_LOCAL = 168,
    PLX = 169,
    PERCENT = 170,
    HEX = 171,
    PN_LOCAL_ESC = 172,
    WS = 173,
    COMMENTS = 174
  };

  enum {
    RuleQuery = 0,
    RulePrologue = 1,
    RuleBaseDecl = 2,
    RulePrefixDecl = 3,
    RuleSelectQuery = 4,
    RuleSubSelect = 5,
    RuleSelectClause = 6,
    RuleVarOrAlias = 7,
    RuleAlias = 8,
    RuleAliasWithoutBrackets = 9,
    RuleConstructQuery = 10,
    RuleDescribeQuery = 11,
    RuleAskQuery = 12,
    RuleDatasetClause = 13,
    RuleDefaultGraphClause = 14,
    RuleNamedGraphClause = 15,
    RuleSourceSelector = 16,
    RuleWhereClause = 17,
    RuleSolutionModifier = 18,
    RuleGroupClause = 19,
    RuleGroupCondition = 20,
    RuleHavingClause = 21,
    RuleHavingCondition = 22,
    RuleOrderClause = 23,
    RuleOrderCondition = 24,
    RuleLimitOffsetClauses = 25,
    RuleLimitClause = 26,
    RuleOffsetClause = 27,
    RuleTextLimitClause = 28,
    RuleValuesClause = 29,
    RuleTriplesTemplate = 30,
    RuleGroupGraphPattern = 31,
    RuleGroupGraphPatternSub = 32,
    RuleGraphPatternNotTriplesAndMaybeTriples = 33,
    RuleTriplesBlock = 34,
    RuleGraphPatternNotTriples = 35,
    RuleOptionalGraphPattern = 36,
    RuleGraphGraphPattern = 37,
    RuleServiceGraphPattern = 38,
    RuleBind = 39,
    RuleInlineData = 40,
    RuleDataBlock = 41,
    RuleInlineDataOneVar = 42,
    RuleInlineDataFull = 43,
    RuleDataBlockSingle = 44,
    RuleDataBlockValue = 45,
    RuleMinusGraphPattern = 46,
    RuleGroupOrUnionGraphPattern = 47,
    RuleFilterR = 48,
    RuleConstraint = 49,
    RuleFunctionCall = 50,
    RuleArgList = 51,
    RuleExpressionList = 52,
    RuleConstructTemplate = 53,
    RuleConstructTriples = 54,
    RuleTriplesSameSubject = 55,
    RulePropertyList = 56,
    RulePropertyListNotEmpty = 57,
    RuleVerb = 58,
    RuleObjectList = 59,
    RuleObjectR = 60,
    RuleTriplesSameSubjectPath = 61,
    RulePropertyListPath = 62,
    RulePropertyListPathNotEmpty = 63,
    RuleVerbPath = 64,
    RuleVerbSimple = 65,
    RuleTupleWithoutPath = 66,
    RuleTupleWithPath = 67,
    RuleVerbPathOrSimple = 68,
    RuleObjectListPath = 69,
    RuleObjectPath = 70,
    RulePath = 71,
    RulePathAlternative = 72,
    RulePathSequence = 73,
    RulePathElt = 74,
    RulePathEltOrInverse = 75,
    RulePathMod = 76,
    RulePathPrimary = 77,
    RulePathNegatedPropertySet = 78,
    RulePathOneInPropertySet = 79,
    RuleInteger = 80,
    RuleTriplesNode = 81,
    RuleBlankNodePropertyList = 82,
    RuleTriplesNodePath = 83,
    RuleBlankNodePropertyListPath = 84,
    RuleCollection = 85,
    RuleCollectionPath = 86,
    RuleGraphNode = 87,
    RuleGraphNodePath = 88,
    RuleVarOrTerm = 89,
    RuleVarOrIri = 90,
    RuleVar = 91,
    RuleGraphTerm = 92,
    RuleExpression = 93,
    RuleConditionalOrExpression = 94,
    RuleConditionalAndExpression = 95,
    RuleValueLogical = 96,
    RuleRelationalExpression = 97,
    RuleNumericExpression = 98,
    RuleAdditiveExpression = 99,
    RuleMultiplicativeExpressionWithSign = 100,
    RulePlusSubexpression = 101,
    RuleMinusSubexpression = 102,
    RuleMultiplicativeExpressionWithLeadingSignButNoSpace = 103,
    RuleMultiplicativeExpression = 104,
    RuleMultiplyOrDivideExpression = 105,
    RuleMultiplyExpression = 106,
    RuleDivideExpression = 107,
    RuleUnaryExpression = 108,
    RulePrimaryExpression = 109,
    RuleBrackettedExpression = 110,
    RuleBuiltInCall = 111,
    RuleRegexExpression = 112,
    RuleLangExpression = 113,
    RuleSubstringExpression = 114,
    RuleStrReplaceExpression = 115,
    RuleExistsFunc = 116,
    RuleNotExistsFunc = 117,
    RuleAggregate = 118,
    RuleIriOrFunction = 119,
    RuleRdfLiteral = 120,
    RuleNumericLiteral = 121,
    RuleNumericLiteralUnsigned = 122,
    RuleNumericLiteralPositive = 123,
    RuleNumericLiteralNegative = 124,
    RuleBooleanLiteral = 125,
    RuleString = 126,
    RuleIri = 127,
    RulePrefixedName = 128,
    RuleBlankNode = 129,
    RuleIriref = 130,
    RulePnameLn = 131,
    RulePnameNs = 132
  };

  explicit SparqlAutomaticParser(antlr4::TokenStream* input);

  SparqlAutomaticParser(antlr4::TokenStream* input,
                        const antlr4::atn::ParserATNSimulatorOptions& options);

  ~SparqlAutomaticParser() override;

  std::string getGrammarFileName() const override;

  const antlr4::atn::ATN& getATN() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  class QueryContext;
  class PrologueContext;
  class BaseDeclContext;
  class PrefixDeclContext;
  class SelectQueryContext;
  class SubSelectContext;
  class SelectClauseContext;
  class VarOrAliasContext;
  class AliasContext;
  class AliasWithoutBracketsContext;
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
  class GraphPatternNotTriplesAndMaybeTriplesContext;
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
  class TupleWithoutPathContext;
  class TupleWithPathContext;
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
  class MultiplicativeExpressionWithSignContext;
  class PlusSubexpressionContext;
  class MinusSubexpressionContext;
  class MultiplicativeExpressionWithLeadingSignButNoSpaceContext;
  class MultiplicativeExpressionContext;
  class MultiplyOrDivideExpressionContext;
  class MultiplyExpressionContext;
  class DivideExpressionContext;
  class UnaryExpressionContext;
  class PrimaryExpressionContext;
  class BrackettedExpressionContext;
  class BuiltInCallContext;
  class RegexExpressionContext;
  class LangExpressionContext;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SubSelectContext* subSelect();

  class SelectClauseContext : public antlr4::ParserRuleContext {
   public:
    antlr4::Token* asterisk = nullptr;
    SelectClauseContext(antlr4::ParserRuleContext* parent,
                        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* SELECT();
    antlr4::tree::TerminalNode* DISTINCT();
    antlr4::tree::TerminalNode* REDUCED();
    std::vector<VarOrAliasContext*> varOrAlias();
    VarOrAliasContext* varOrAlias(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  SelectClauseContext* selectClause();

  class VarOrAliasContext : public antlr4::ParserRuleContext {
   public:
    VarOrAliasContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VarContext* var();
    AliasContext* alias();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  VarOrAliasContext* varOrAlias();

  class AliasContext : public antlr4::ParserRuleContext {
   public:
    AliasContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    AliasWithoutBracketsContext* aliasWithoutBrackets();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AliasContext* alias();

  class AliasWithoutBracketsContext : public antlr4::ParserRuleContext {
   public:
    AliasWithoutBracketsContext(antlr4::ParserRuleContext* parent,
                                size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExpressionContext* expression();
    antlr4::tree::TerminalNode* AS();
    VarContext* var();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AliasWithoutBracketsContext* aliasWithoutBrackets();

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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  HavingConditionContext* havingCondition();

  class OrderClauseContext : public antlr4::ParserRuleContext {
   public:
    antlr4::Token* orderBy = nullptr;
    antlr4::Token* internalSortBy = nullptr;
    OrderClauseContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* ORDERBY();
    antlr4::tree::TerminalNode* INTERNALSORTBY();
    std::vector<OrderConditionContext*> orderCondition();
    OrderConditionContext* orderCondition(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GroupGraphPatternContext* groupGraphPattern();

  class GroupGraphPatternSubContext : public antlr4::ParserRuleContext {
   public:
    GroupGraphPatternSubContext(antlr4::ParserRuleContext* parent,
                                size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TriplesBlockContext* triplesBlock();
    std::vector<GraphPatternNotTriplesAndMaybeTriplesContext*>
    graphPatternNotTriplesAndMaybeTriples();
    GraphPatternNotTriplesAndMaybeTriplesContext*
    graphPatternNotTriplesAndMaybeTriples(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GroupGraphPatternSubContext* groupGraphPatternSub();

  class GraphPatternNotTriplesAndMaybeTriplesContext
      : public antlr4::ParserRuleContext {
   public:
    GraphPatternNotTriplesAndMaybeTriplesContext(
        antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    GraphPatternNotTriplesContext* graphPatternNotTriples();
    TriplesBlockContext* triplesBlock();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GraphPatternNotTriplesAndMaybeTriplesContext*
  graphPatternNotTriplesAndMaybeTriples();

  class TriplesBlockContext : public antlr4::ParserRuleContext {
   public:
    TriplesBlockContext(antlr4::ParserRuleContext* parent,
                        size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TriplesSameSubjectPathContext* triplesSameSubjectPath();
    TriplesBlockContext* triplesBlock();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PropertyListNotEmptyContext* propertyListNotEmpty();

  class VerbContext : public antlr4::ParserRuleContext {
   public:
    VerbContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VarOrIriContext* varOrIri();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ObjectListContext* objectList();

  class ObjectRContext : public antlr4::ParserRuleContext {
   public:
    ObjectRContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    GraphNodeContext* graphNode();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PropertyListPathContext* propertyListPath();

  class PropertyListPathNotEmptyContext : public antlr4::ParserRuleContext {
   public:
    PropertyListPathNotEmptyContext(antlr4::ParserRuleContext* parent,
                                    size_t invokingState);
    virtual size_t getRuleIndex() const override;
    TupleWithPathContext* tupleWithPath();
    std::vector<TupleWithoutPathContext*> tupleWithoutPath();
    TupleWithoutPathContext* tupleWithoutPath(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PropertyListPathNotEmptyContext* propertyListPathNotEmpty();

  class VerbPathContext : public antlr4::ParserRuleContext {
   public:
    VerbPathContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PathContext* path();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  VerbPathContext* verbPath();

  class VerbSimpleContext : public antlr4::ParserRuleContext {
   public:
    VerbSimpleContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VarContext* var();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  VerbSimpleContext* verbSimple();

  class TupleWithoutPathContext : public antlr4::ParserRuleContext {
   public:
    TupleWithoutPathContext(antlr4::ParserRuleContext* parent,
                            size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VerbPathOrSimpleContext* verbPathOrSimple();
    ObjectListContext* objectList();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TupleWithoutPathContext* tupleWithoutPath();

  class TupleWithPathContext : public antlr4::ParserRuleContext {
   public:
    TupleWithPathContext(antlr4::ParserRuleContext* parent,
                         size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VerbPathOrSimpleContext* verbPathOrSimple();
    ObjectListPathContext* objectListPath();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  TupleWithPathContext* tupleWithPath();

  class VerbPathOrSimpleContext : public antlr4::ParserRuleContext {
   public:
    VerbPathOrSimpleContext(antlr4::ParserRuleContext* parent,
                            size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VerbPathContext* verbPath();
    VerbSimpleContext* verbSimple();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ObjectListPathContext* objectListPath();

  class ObjectPathContext : public antlr4::ParserRuleContext {
   public:
    ObjectPathContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    GraphNodePathContext* graphNodePath();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  ObjectPathContext* objectPath();

  class PathContext : public antlr4::ParserRuleContext {
   public:
    PathContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PathAlternativeContext* pathAlternative();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PathEltOrInverseContext* pathEltOrInverse();

  class PathModContext : public antlr4::ParserRuleContext {
   public:
    PathModContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PathOneInPropertySetContext* pathOneInPropertySet();

  class IntegerContext : public antlr4::ParserRuleContext {
   public:
    IntegerContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* INTEGER();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  GraphTermContext* graphTerm();

  class ExpressionContext : public antlr4::ParserRuleContext {
   public:
    ExpressionContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ConditionalOrExpressionContext* conditionalOrExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NumericExpressionContext* numericExpression();

  class AdditiveExpressionContext : public antlr4::ParserRuleContext {
   public:
    AdditiveExpressionContext(antlr4::ParserRuleContext* parent,
                              size_t invokingState);
    virtual size_t getRuleIndex() const override;
    MultiplicativeExpressionContext* multiplicativeExpression();
    std::vector<MultiplicativeExpressionWithSignContext*>
    multiplicativeExpressionWithSign();
    MultiplicativeExpressionWithSignContext* multiplicativeExpressionWithSign(
        size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  AdditiveExpressionContext* additiveExpression();

  class MultiplicativeExpressionWithSignContext
      : public antlr4::ParserRuleContext {
   public:
    MultiplicativeExpressionWithSignContext(antlr4::ParserRuleContext* parent,
                                            size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PlusSubexpressionContext* plusSubexpression();
    MinusSubexpressionContext* minusSubexpression();
    MultiplicativeExpressionWithLeadingSignButNoSpaceContext*
    multiplicativeExpressionWithLeadingSignButNoSpace();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  MultiplicativeExpressionWithSignContext* multiplicativeExpressionWithSign();

  class PlusSubexpressionContext : public antlr4::ParserRuleContext {
   public:
    PlusSubexpressionContext(antlr4::ParserRuleContext* parent,
                             size_t invokingState);
    virtual size_t getRuleIndex() const override;
    MultiplicativeExpressionContext* multiplicativeExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PlusSubexpressionContext* plusSubexpression();

  class MinusSubexpressionContext : public antlr4::ParserRuleContext {
   public:
    MinusSubexpressionContext(antlr4::ParserRuleContext* parent,
                              size_t invokingState);
    virtual size_t getRuleIndex() const override;
    MultiplicativeExpressionContext* multiplicativeExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  MinusSubexpressionContext* minusSubexpression();

  class MultiplicativeExpressionWithLeadingSignButNoSpaceContext
      : public antlr4::ParserRuleContext {
   public:
    MultiplicativeExpressionWithLeadingSignButNoSpaceContext(
        antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NumericLiteralPositiveContext* numericLiteralPositive();
    NumericLiteralNegativeContext* numericLiteralNegative();
    std::vector<MultiplyOrDivideExpressionContext*>
    multiplyOrDivideExpression();
    MultiplyOrDivideExpressionContext* multiplyOrDivideExpression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  MultiplicativeExpressionWithLeadingSignButNoSpaceContext*
  multiplicativeExpressionWithLeadingSignButNoSpace();

  class MultiplicativeExpressionContext : public antlr4::ParserRuleContext {
   public:
    MultiplicativeExpressionContext(antlr4::ParserRuleContext* parent,
                                    size_t invokingState);
    virtual size_t getRuleIndex() const override;
    UnaryExpressionContext* unaryExpression();
    std::vector<MultiplyOrDivideExpressionContext*>
    multiplyOrDivideExpression();
    MultiplyOrDivideExpressionContext* multiplyOrDivideExpression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  MultiplicativeExpressionContext* multiplicativeExpression();

  class MultiplyOrDivideExpressionContext : public antlr4::ParserRuleContext {
   public:
    MultiplyOrDivideExpressionContext(antlr4::ParserRuleContext* parent,
                                      size_t invokingState);
    virtual size_t getRuleIndex() const override;
    MultiplyExpressionContext* multiplyExpression();
    DivideExpressionContext* divideExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  MultiplyOrDivideExpressionContext* multiplyOrDivideExpression();

  class MultiplyExpressionContext : public antlr4::ParserRuleContext {
   public:
    MultiplyExpressionContext(antlr4::ParserRuleContext* parent,
                              size_t invokingState);
    virtual size_t getRuleIndex() const override;
    UnaryExpressionContext* unaryExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  MultiplyExpressionContext* multiplyExpression();

  class DivideExpressionContext : public antlr4::ParserRuleContext {
   public:
    DivideExpressionContext(antlr4::ParserRuleContext* parent,
                            size_t invokingState);
    virtual size_t getRuleIndex() const override;
    UnaryExpressionContext* unaryExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  DivideExpressionContext* divideExpression();

  class UnaryExpressionContext : public antlr4::ParserRuleContext {
   public:
    UnaryExpressionContext(antlr4::ParserRuleContext* parent,
                           size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PrimaryExpressionContext* primaryExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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
    LangExpressionContext* langExpression();
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  RegexExpressionContext* regexExpression();

  class LangExpressionContext : public antlr4::ParserRuleContext {
   public:
    LangExpressionContext(antlr4::ParserRuleContext* parent,
                          size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* LANG();
    ExpressionContext* expression();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  LangExpressionContext* langExpression();

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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  NumericLiteralNegativeContext* numericLiteralNegative();

  class BooleanLiteralContext : public antlr4::ParserRuleContext {
   public:
    BooleanLiteralContext(antlr4::ParserRuleContext* parent,
                          size_t invokingState);
    virtual size_t getRuleIndex() const override;

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  StringContext* string();

  class IriContext : public antlr4::ParserRuleContext {
   public:
    IriContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IrirefContext* iriref();
    PrefixedNameContext* prefixedName();
    antlr4::tree::TerminalNode* PREFIX_LANGTAG();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
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

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  BlankNodeContext* blankNode();

  class IrirefContext : public antlr4::ParserRuleContext {
   public:
    IrirefContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* IRI_REF();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  IrirefContext* iriref();

  class PnameLnContext : public antlr4::ParserRuleContext {
   public:
    PnameLnContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* PNAME_LN();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PnameLnContext* pnameLn();

  class PnameNsContext : public antlr4::ParserRuleContext {
   public:
    PnameNsContext(antlr4::ParserRuleContext* parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode* PNAME_NS();

    virtual void enterRule(antlr4::tree::ParseTreeListener* listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener* listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor* visitor) override;
  };

  PnameNsContext* pnameNs();

  // By default the static state used to implement the parser is lazily
  // initialized during the first call to the constructor. You can call this
  // function if you wish to initialize the static state ahead of time.
  static void initialize();

 private:
};
