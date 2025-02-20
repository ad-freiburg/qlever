/*
 * Copyright 2007 the original author or authors.
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 * disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 * disclaimer in the documentation and/or other materials provided with the distribution.
 *
 * Neither the name of the author or authors nor the names of its contributors may be used to endorse or promote
 * products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * @author Simone Tripodi   (simone)
 * @author Michele Mostarda (michele)

 * Ported to Antlr4 by Tom Everett
 * Extended for use with QLever by Niklas Schnelle (schnelle@informatik.uni-freiburg.de)
 * and Johannes Kalmbach (kalmbacj@informatik.uni-freiburg.de)
 */


grammar SparqlAutomatic;

// query and update are disjoint in the grammar;
// add a common parent for easier parsing
queryOrUpdate: (query | update) EOF
    ;

query
    : prologue (selectQuery | constructQuery | describeQuery | askQuery) valuesClause
    ;

prologue
    : (baseDecl | prefixDecl)*
    ;

baseDecl
    : BASE iriref
    ;

prefixDecl
    : PREFIX PNAME_NS iriref
    ;

selectQuery : selectClause datasetClause* whereClause solutionModifier;

subSelect : selectClause whereClause solutionModifier valuesClause;

selectClause
    : SELECT ( DISTINCT | REDUCED)? ( varOrAlias+ | asterisk='*' )
    // extracted the alias rule for easier visiting.
    ;

varOrAlias: var | alias; // NOT part of sparql, extracted from selectClause to
// preserve the order of the selections

alias: ( '(' aliasWithoutBrackets ')' ) ;  // NOT part of sparql, for easier antlr parsing

aliasWithoutBrackets : ( expression AS var ); // Needed for interaction between old and new parser

constructQuery : CONSTRUCT ( constructTemplate datasetClause* whereClause solutionModifier | datasetClause* WHERE '{' triplesTemplate? '}' solutionModifier );

describeQuery : DESCRIBE ( varOrIri+ | '*' ) datasetClause* whereClause? solutionModifier;

askQuery : ASK datasetClause*  whereClause solutionModifier;

datasetClause : FROM ( defaultGraphClause | namedGraphClause ) ;

defaultGraphClause
    : sourceSelector
    ;

namedGraphClause
    : NAMED sourceSelector
    ;

sourceSelector
    : iri
    ;

whereClause
    : WHERE? groupGraphPattern
    ;

solutionModifier
    : groupClause? havingClause? orderClause? limitOffsetClauses?
    ;

groupClause: GROUPBY groupCondition+ ;

groupCondition : 	builtInCall | functionCall | '(' expression ( AS var )? ')' | var;

havingClause: HAVING havingCondition+;

havingCondition : constraint;

orderClause
    : (orderBy = ORDERBY | internalSortBy = INTERNALSORTBY) orderCondition+
    ;

orderCondition
    : ( ( ASC | DESC ) brackettedExpression )
    | ( constraint | var )
    ;

// This is an extension to the SPARQL standard.
// We're adding an additional modifier to the limitOffsetClauses. This
// modifier `TEXTLIMIT` is used in QLever's full-text index. The order of the
// solution modifiers does not matter, but each of them may occur at most
// once. This behavior is the same as in standard SPARQL.
limitOffsetClauses
    : limitClause offsetClause? textLimitClause?
    | limitClause textLimitClause? offsetClause?
    | offsetClause limitClause? textLimitClause?
    | offsetClause textLimitClause? limitClause?
    | textLimitClause offsetClause? limitClause?
    | textLimitClause limitClause? offsetClause?
    ;


limitClause
    : LIMIT integer
    ;

offsetClause
    : OFFSET integer
    ;

textLimitClause
    : TEXTLIMIT integer
    ;

valuesClause : ( VALUES dataBlock )?;

update: prologue (update1 (';' update)? )? ;

update1: load | clear | drop | add | move | copy | create | insertData | deleteData | deleteWhere | modify ;

load: LOAD SILENT? iri (INTO graphRef)? ;

clear: CLEAR SILENT? graphRefAll ;

drop: DROP SILENT? graphRefAll ;

create: CREATE SILENT? graphRef ;

add: ADD SILENT? graphOrDefault TO graphOrDefault ;

move: MOVE SILENT? graphOrDefault TO graphOrDefault ;

copy: COPY SILENT? graphOrDefault TO graphOrDefault ;

insertData: INSERT DATA quadData ;

deleteData: DELETE DATA quadData ;

deleteWhere: DELETE WHERE quadPattern ;

modify: (WITH iri)? ( deleteClause insertClause? | insertClause ) usingClause* WHERE groupGraphPattern ;

deleteClause: DELETE quadPattern ;

insertClause: INSERT quadPattern ;

usingClause: USING (iri | NAMED iri) ;

graphOrDefault: DEFAULT | GRAPH? iri ;

graphRef: GRAPH iri ;

graphRefAll: graphRef | DEFAULT | NAMED | ALL ;

quadPattern: '{' quads '}' ;

quadData: '{' quads '}' ;

quads: triplesTemplate? ( quadsNotTriples '.'? triplesTemplate? )* ;

quadsNotTriples: GRAPH varOrIri '{' triplesTemplate? '}' ;

triplesTemplate: triplesSameSubject ( '.' triplesTemplate? )?;


// Corresponds to GraphPattern.
groupGraphPattern
    : '{' ( subSelect | groupGraphPatternSub )'}'
    ;

groupGraphPatternSub
    : triplesBlock? graphPatternNotTriplesAndMaybeTriples*
    ;

/* Helper rules to make parsing of groupGraphPatternSub easier. */
graphPatternNotTriplesAndMaybeTriples
    : graphPatternNotTriples '.'? triplesBlock?
    ;

triplesBlock
    : triplesSameSubjectPath ( '.' triplesBlock? )?
    ;

// Corresponds to GraphPatternOperation.
graphPatternNotTriples
    : groupOrUnionGraphPattern | optionalGraphPattern | minusGraphPattern | graphGraphPattern | serviceGraphPattern | filterR | bind | inlineData
    ;

optionalGraphPattern
    : OPTIONAL groupGraphPattern
    ;

graphGraphPattern
    : GRAPH varOrIri groupGraphPattern
    ;

serviceGraphPattern
    : SERVICE SILENT? varOrIri groupGraphPattern
    ;

bind : BIND '(' expression AS var ')' ;

inlineData : VALUES dataBlock ;

dataBlock : inlineDataOneVar | inlineDataFull ;

inlineDataOneVar : var '{' dataBlockValue* '}' ;

inlineDataFull : ( NIL | '(' var* ')' ) '{' dataBlockSingle* '}';  // factored out dataBlockSingle for antlr

dataBlockSingle:( '(' dataBlockValue* ')' | NIL );  // helper rule for antlr

dataBlockValue : iri | rdfLiteral | numericLiteral | booleanLiteral | UNDEF ;

minusGraphPattern : MINUS groupGraphPattern;

groupOrUnionGraphPattern
    : groupGraphPattern ( UNION groupGraphPattern )*
    ;


filterR
    : FILTER constraint
    ;

constraint
    : brackettedExpression | builtInCall | functionCall
    ;

functionCall
    : iri argList;

argList
    : NIL | '(' DISTINCT? expression ( ',' expression )* ')'
    ;

expressionList : NIL | '(' expression ( ',' expression )* ')';

constructTemplate
    : '{' constructTriples? '}'
    ;

constructTriples
    : triplesSameSubject ( '.' constructTriples? )?
    ;

triplesSameSubject
    : varOrTerm propertyListNotEmpty | triplesNode propertyList
    ;

propertyList
    : propertyListNotEmpty?
    ;

propertyListNotEmpty
    : verb objectList ( ';' ( verb objectList )? )*
    ;

verb
    : varOrIri | 'a' ;

objectList
    : objectR ( ',' objectR )*
    ;

objectR : graphNode;


triplesSameSubjectPath
    : varOrTerm propertyListPathNotEmpty | triplesNodePath propertyListPath
    ;

propertyListPath
    : propertyListPathNotEmpty?
    ;

propertyListPathNotEmpty
    : tupleWithPath ( ';' ( tupleWithoutPath )? )*
    ;

verbPath
    : path
    ;

verbSimple
    : var
    ;

/* Helper rules to make parsing of propertyListPathNotEmpty easier. */
tupleWithoutPath
    : verbPathOrSimple objectList
    ;

tupleWithPath
    : verbPathOrSimple objectListPath
    ;

/*
* We need an extra rule for this since otherwise ANTLR gives us no easy way to
* treat the verbPaths/verbSimples above as a single list in the correct order as we lose
* the order between the separate verbPath/verbSimple lists.
*/
verbPathOrSimple
    : (verbPath | verbSimple)
    ;
/* property paths */
objectListPath
    : objectPath ( ',' objectPath )*
    ;

objectPath
    : graphNodePath
    ;

path
    : pathAlternative
    ;

pathAlternative
    : pathSequence ( '|' pathSequence )*
    ;

pathSequence
    : pathEltOrInverse ( '/' pathEltOrInverse )*
    ;

pathElt
    : pathPrimary pathMod?
    ;
pathEltOrInverse
/* Name the '^' to be able to detect it easily. */
    : pathElt | negationOperator='^' pathElt
    ;

pathMod
    : '+' | '*' | '?' | '{' stepsMin ( ',' ( stepsMax )? )? '}'
    ;

stepsMin: integer ;

stepsMax: integer ;


pathPrimary
    : iri | 'a' | '!' pathNegatedPropertySet | '(' path ')'
    ;

pathNegatedPropertySet
    : pathOneInPropertySet | '(' ( pathOneInPropertySet ( '|' pathOneInPropertySet )* )? ')'
    ;

pathOneInPropertySet
    : iri | 'a' | '^' ( iri | 'a' )
    ;

/* property paths end */

integer : INTEGER;

triplesNode
    : collection
    | blankNodePropertyList
    ;

blankNodePropertyList
    : '[' propertyListNotEmpty ']'
    ;

triplesNodePath
    : collectionPath
    | blankNodePropertyListPath
    ;

blankNodePropertyListPath
    : '[' propertyListPathNotEmpty ']'
    ;

collection
    : '(' graphNode+ ')'
    ;
collectionPath
    : '(' graphNodePath+ ')'
    ;

graphNode
    : varOrTerm | triplesNode
    ;

graphNodePath
    : varOrTerm | triplesNodePath
    ;

varOrTerm
    : var
    | graphTerm
    ;

varOrIri
    : var | iri
    ;

var
    : VAR1
    | VAR2
    ;






graphTerm
    : iri
    | rdfLiteral
    | numericLiteral
    | booleanLiteral
    | blankNode
    | NIL
    ;

expression
    : conditionalOrExpression
    ;

conditionalOrExpression
    : conditionalAndExpression ( '||' conditionalAndExpression )*
    ;

conditionalAndExpression
    : valueLogical ( '&&' valueLogical )*
    ;

valueLogical
    : relationalExpression
    ;

relationalExpression
    : numericExpression ( '=' numericExpression | '!=' numericExpression |  '<' numericExpression | '>' numericExpression | '<=' numericExpression | '>=' numericExpression | IN expressionList | (notToken = NOT) IN expressionList)?
    ;

numericExpression
    : additiveExpression
    ;

// The  rule `multiplicativeExpressionWithSign` and the simple alias rules `plusSubexpression` and `minusSubexpression`
// are not part of the SPARQL standard, but are added for easier identification of the signs in an `additiveExpression`.
additiveExpression :
    multiplicativeExpression multiplicativeExpressionWithSign*
    ;

multiplicativeExpressionWithSign :  '+' plusSubexpression | '-' minusSubexpression |  multiplicativeExpressionWithLeadingSignButNoSpace;


plusSubexpression : multiplicativeExpression;
minusSubexpression : multiplicativeExpression;

// A multiplicative expression with an explicit `+` or `-` sign in front of it, but without a space after that sign.
multiplicativeExpressionWithLeadingSignButNoSpace:
( numericLiteralPositive | numericLiteralNegative ) multiplyOrDivideExpression*
;

multiplicativeExpression
    : unaryExpression multiplyOrDivideExpression*
    ;

// Helper rules that combine a `*` or `/` with the expression after it
multiplyOrDivideExpression : multiplyExpression | divideExpression;
multiplyExpression : '*' unaryExpression;
divideExpression : '/' unaryExpression;

unaryExpression
    :  '!' primaryExpression
    | '+' primaryExpression
    | '-' primaryExpression
    | primaryExpression
    ;

primaryExpression
    : brackettedExpression | builtInCall | iriOrFunction | rdfLiteral | numericLiteral | booleanLiteral | var;

brackettedExpression
    : '(' expression ')'
    ;


builtInCall	  :  	  aggregate
| STR '(' expression ')'
| langExpression
| LANGMATCHES '(' expression ',' expression ')'
| DATATYPE '(' expression ')'
| BOUND '(' var ')'
| IRI '(' expression ')'
| URI '(' expression ')'
| BNODE ( '(' expression ')' | NIL )
| RAND NIL
| ABS '(' expression ')'
| CEIL '(' expression ')'
| FLOOR '(' expression ')'
| ROUND '(' expression ')'
| CONCAT expressionList
| substringExpression
| STRLEN '(' expression ')'
| strReplaceExpression
| UCASE '(' expression ')'
| LCASE '(' expression ')'
| ENCODE_FOR_URI '(' expression ')'
| CONTAINS '(' expression ',' expression ')'
| STRSTARTS '(' expression ',' expression ')'
| STRENDS '(' expression ',' expression ')'
| STRBEFORE '(' expression ',' expression ')'
| STRAFTER '(' expression ',' expression ')'
| YEAR '(' expression ')'
| MONTH '(' expression ')'
| DAY '(' expression ')'
| HOURS '(' expression ')'
| MINUTES '(' expression ')'
| SECONDS '(' expression ')'
| TIMEZONE '(' expression ')'
| TZ '(' expression ')'
| NOW NIL
| UUID NIL
| STRUUID NIL
| MD5 '(' expression ')'
| SHA1 '(' expression ')'
| SHA256 '(' expression ')'
| SHA384 '(' expression ')'
| SHA512 '(' expression ')'
| COALESCE expressionList
| IF '(' expression ',' expression ',' expression ')'
| STRLANG '(' expression ',' expression ')'
| STRDT '(' expression ',' expression ')'
| SAMETERM '(' expression ',' expression ')'
| ISIRI '(' expression ')'
| ISURI '(' expression ')'
| ISBLANK '(' expression ')'
| ISLITERAL '(' expression ')'
| ISNUMERIC '(' expression ')'
| regexExpression
| existsFunc
| notExistsFunc;

regexExpression	  :  	REGEX '(' expression ',' expression ( ',' expression )? ')';
langExpression : LANG '(' expression ')';
substringExpression	  :  	SUBSTR '(' expression ',' expression ( ',' expression )? ')' ;
strReplaceExpression	  :  	REPLACE '(' expression ',' expression ',' expression ( ',' expression )? ')';
existsFunc	  :  	EXISTS groupGraphPattern;
notExistsFunc	  :  	NOT EXISTS groupGraphPattern;

aggregate :  COUNT '(' DISTINCT? ( '*' | expression ) ')'
           | SUM '(' DISTINCT? expression ')'
           | MIN '(' DISTINCT? expression ')'
           | MAX '(' DISTINCT? expression ')'
           | AVG '(' DISTINCT? expression ')'
           | STDEV '(' DISTINCT? expression ')'
           | SAMPLE '(' DISTINCT? expression ')'
           | GROUP_CONCAT '(' DISTINCT? expression ( ';' SEPARATOR '=' string )? ')'  ;


iriOrFunction
    : iri argList?
    ;

rdfLiteral
    : string ( LANGTAG | ( '^^' iri ) )?
    ;

numericLiteral
    : numericLiteralUnsigned | numericLiteralPositive | numericLiteralNegative
    ;

numericLiteralUnsigned
    : INTEGER
    | DECIMAL
    | DOUBLE
    ;

numericLiteralPositive
    : INTEGER_POSITIVE
    | DECIMAL_POSITIVE
    | DOUBLE_POSITIVE
    ;

numericLiteralNegative
    : INTEGER_NEGATIVE
    | DECIMAL_NEGATIVE
    | DOUBLE_NEGATIVE
    ;

booleanLiteral
    : 'true'
    | 'false'
    ;

string
    : STRING_LITERAL1
    | STRING_LITERAL2
    | STRING_LITERAL_LONG1 | STRING_LITERAL_LONG2
    /* | STRING_LITERAL_LONG('0'..'9') | STRING_LITERAL_LONG('0'..'9')*/
    ;

iri
    : (PREFIX_LANGTAG)? (iriref | prefixedName)
    ;

prefixedName
    : pnameLn
    | pnameNs
    ;

blankNode
    : BLANK_NODE_LABEL
    | ANON
    ;

iriref : IRI_REF;

pnameLn

    : PNAME_LN
    ;

pnameNs : PNAME_NS;


BASE : B A S E;
PREFIX : P R E F I X;
//PREFIX : 'PREFIX';
SELECT : S E L E C T;
DISTINCT : D I S T I N C T;
REDUCED : R E D U C E D;
AS : A S;
CONSTRUCT : C O N S T R U C T;
WHERE : W H E R E;
DESCRIBE : D E S C R I B E;
ASK : A S K;
FROM : F R O M;
NAMED : N A M E D;
GROUPBY : G R O U P WS+ B Y;

GROUP_CONCAT : G R O U P '_' C O N C A T;
HAVING : H A V I N G;
ORDERBY: O R D E R WS+ B Y;
INTERNALSORTBY: I N T E R N A L WS+ S O R T WS+ B Y;
ASC : A S C;
DESC : D E S C;
LIMIT : L I M I T;
OFFSET : O F F S E T;
TEXTLIMIT: T E X T L I M I T;
VALUES : V A L U E S;
LOAD : L O A D;
SILENT : S I L E N T;
INTO: I N T O;
CLEAR : C L E A R;
DROP : D R O P;
CREATE: C R E A T E;
ADD: A D D;
TO: T O;
DATA: D A T A;
MOVE: M O V E;
COPY: C O P Y;
INSERT : I N S E R T;
DELETE : D E L E T E;
WITH: W I T H;
USING: U S I N G;
DEFAULT : D E F A U L T;
GRAPH: G R A P H;
ALL : A L L;
OPTIONAL : O P T I O N A L;
SERVICE : S E R V I C E;
BIND: B I N D;
UNDEF: U N D E F;
MINUS : M I N U S;
UNION: U N I O N;
FILTER : F I L T E R;
NOT : N O T;
IN : I N ;
STR : S T R;
LANG : L A N G;
LANGMATCHES : L A N G M A T C H E S;
DATATYPE : D A T A T Y P E;
BOUND : B O U N D;
IRI : I R I;
URI : U R I;
BNODE : B N O D E;
RAND : R A N D;
ABS : A B S;
CEIL : C E I L;
FLOOR : F L O O R;
ROUND : R O U N D;
CONCAT : C O N C A T;
STRLEN : S T R L E N;
UCASE : U C A S E;
LCASE : L C A S E;
ENCODE_FOR_URI : E N C O D E '_' F O R '_' U R I;
FOR : F O R;
CONTAINS : C O N T A I N S;
STRSTARTS : S T R S T A R T S;
STRENDS : S T R E N D S;
STRBEFORE : S T R B E F O R E;
STRAFTER : S T R A F T E R;
YEAR : Y E A R;
MONTH : M O N T H;
DAY : D A Y;
HOURS : H O U R S;
MINUTES : M I N U T E S;
SECONDS : S E C O N D S;
TIMEZONE : T I M E Z O N E;
TZ : T Z;
NOW : N O W;
UUID : U U I D;
STRUUID : S T R U U I D;
SHA1 : S H A '1';
SHA256 : S H A '256';
SHA384 : S H A '384';
SHA512 : S H A '512';
MD5 : M D '5';
COALESCE : C O A L E S C E;
IF : I F;
STRLANG : S T R L A N G;
STRDT : S T R D T;
SAMETERM : S A M E T E R M;
ISIRI : I S I R I;
ISURI : I S U R I;
ISBLANK : I S B L A N K;
ISLITERAL : I S L I T E R A L;
ISNUMERIC : I S N U M E R I C;
REGEX : R E G E X;
SUBSTR : S U B S T R;
REPLACE : R E P L A C E;
EXISTS : E X I S T S;
COUNT : C O U N T;
SUM : S U M;
MIN : M I N;
MAX : M A X;
AVG : A V G;
STDEV : S T D E V ;
SAMPLE : S A M P L E;
SEPARATOR : S E P A R A T O R;

// LEXER RULES

IRI_REF
    : '<'  ~('<' | '>' | '"' | '{' | '}' | '|' | '^' | '\\' | '`'| '\u0000'..'\u0020')* '>'
    ;

PNAME_NS
    : PN_PREFIX? ':'
    ;
PNAME_LN : PNAME_NS PN_LOCAL;


BLANK_NODE_LABEL
    : '_:' ( PN_CHARS_U | DIGIT ) ((PN_CHARS|'.')* PN_CHARS)?
    ;

VAR1
    : '?' VARNAME
    ;

VAR2
    : '$' VARNAME
    ;

LANGTAG
    : '@' ('a'..'z' | 'A' .. 'Z')+ ('-' ('a'..'z' | 'A' .. 'Z' | DIGIT)+)*
    ;

// The PREFIX_LANGTAG is an extension of the SPARQL standard that allows IRIs
// like @en@rdfs:label which are used in QLever's implementation of language
// filters.
PREFIX_LANGTAG
    : LANGTAG '@'
    ;

INTEGER
    : DIGIT+
    ;

DECIMAL
    : DIGIT* '.' DIGIT+
    ;

DOUBLE
    : DIGIT+ '.' DIGIT* EXPONENT
    | '.' DIGIT+ EXPONENT
    | DIGIT+ EXPONENT
    ;

INTEGER_POSITIVE
    : '+' INTEGER
    ;

DECIMAL_POSITIVE
    : '+' DECIMAL
    ;

DOUBLE_POSITIVE
    : '+' DOUBLE
    ;

INTEGER_NEGATIVE
    : '-' INTEGER
    ;

DECIMAL_NEGATIVE
    : '-' DECIMAL
    ;

DOUBLE_NEGATIVE
    : '-' DOUBLE
    ;

EXPONENT
    : ('e'|'E') ('+'|'-')? DIGIT+
    ;

STRING_LITERAL1
    : '\'' ( ~('\u0027' | '\u005C' | '\u000A' | '\u000D') | ECHAR )* '\''
    ;

STRING_LITERAL2
    : '"'  ( ~('\u0022' | '\u005C' | '\u000A' | '\u000D') | ECHAR )* '"'
    ;

STRING_LITERAL_LONG1
    : '\'\'\'' ( ( '\'' | '\'\'' )? (~('\'' | '\\') | ECHAR ) )* '\'\'\''
    ;

STRING_LITERAL_LONG2
    : '"""' ( ( '"' | '""' )? ( ~('"' | '\\') | ECHAR ) )* '"""'
    ;

ECHAR
    : '\\' ('t' | 'b' | 'n' | 'r' | 'f' | '"' | '\'' | '\\')
    ;

NIL
    : '(' WS* ')'
    ;

ANON
    : '[' WS* ']'
    ;

PN_CHARS_U
    : PN_CHARS_BASE | '_'
    ;

VARNAME
    : ( PN_CHARS_U | DIGIT ) ( PN_CHARS_U | DIGIT | '\u00B7' | ('\u0300'..'\u036F') | ('\u203F'..'\u2040') )*
    ;

fragment
PN_CHARS
    : PN_CHARS_U
    | '-'
    | DIGIT
    | '\u00B7'
    | '\u0300'..'\u036F'
    | '\u203F'..'\u2040'
    ;

PN_PREFIX
    : PN_CHARS_BASE ((PN_CHARS|'.')* PN_CHARS)?
    ;

PN_LOCAL
    : ( PN_CHARS_U | ':' | DIGIT | PLX ) ((PN_CHARS|'.' | ':' | PLX)* (PN_CHARS | ':' | PLX))?
    ;

PLX:
    PERCENT | PN_LOCAL_ESC;

PERCENT :
    '%' HEX HEX;

HEX:
  DIGIT | 'A'..'F' | 'a'..'f';

PN_LOCAL_ESC :
    '\\' ( '_' | '~' | '.' | '-' | '!' | '$' | '&' | '\'' | '(' | ')' | '*' | '+' | ',' | ';' | '=' | '/' | '?' | '#' | '@' | '%' );




fragment
PN_CHARS_BASE
    : 'A'..'Z'
    | 'a'..'z'
    | '\u00C0'..'\u00D6'
    | '\u00D8'..'\u00F6'
    | '\u00F8'..'\u02FF'
    | '\u0370'..'\u037D'
    | '\u037F'..'\u1FFF'
    | '\u200C'..'\u200D'
    | '\u2070'..'\u218F'
    | '\u2C00'..'\u2FEF'
    | '\u3001'..'\uD7FF'
    | '\uF900'..'\uFDCF'
    | '\uFDF0'..'\uFFFD'
    // not supported by antlr4 | '\U00010000'..'\U000EFFFF'
    ;

fragment
DIGIT
    : '0'..'9'
    ;

WS
    : (' '
    | '\t'
    | '\n'
    | '\r')+ ->skip
    ;

COMMENTS
    : '#' ~( '\r' | '\n')* ->skip
    ;








// todo: builtin call

fragment A : ('a' | 'A');
fragment B : ('b' | 'B');
fragment C : ('c' | 'C');
fragment D : ('d' | 'D');
fragment E : ('e' | 'E');
fragment F : ('f' | 'F');
fragment G : ('g' | 'G');
fragment H : ('h' | 'H');
fragment I : ('i' | 'I');
fragment J : ('j' | 'J');
fragment K : ('k' | 'K');
fragment L : ('l' | 'L');
fragment M : ('m' | 'M');
fragment N : ('n' | 'N');
fragment O : ('o' | 'O');
fragment P : ('p' | 'P');
fragment Q : ('q' | 'Q');
fragment R : ('r' | 'R');
fragment S : ('s' | 'S');
fragment T : ('t' | 'T');
fragment U : ('u' | 'U');
fragment V : ('v' | 'V');
fragment W : ('w' | 'W');
fragment X : ('x' | 'X');
fragment Y : ('y' | 'Y');
fragment Z : ('z' | 'Z');


/*
fragment A : [aA]; // match either an 'a' or 'A'
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];

*/