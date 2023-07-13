// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023, schlegea@informatik.uni-freiburg.de)

grammar MemoryDefinitionLanguage;

// Parser rules

memoryDefinitionString : (pureByteDefinition | memoryUnitDefinition) EOF;
pureByteDefinition : UNSIGNED_INTEGER BYTE;
memoryUnitDefinition : (UNSIGNED_INTEGER | FLOAT) MEMORY_UNIT;

// Lexer rules

// So that everything is case insensitive.
fragment A : [aA];
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

// Keywords.
MEMORY_UNIT : K B | M B | G B | T B | P B;
BYTE: B Y T E | B ;

// The literals.
UNSIGNED_INTEGER : [0-9]+;
FLOAT : UNSIGNED_INTEGER'.'UNSIGNED_INTEGER;

WHITESPACE : ('\t' | ' ') -> skip ;
