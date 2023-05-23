// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (May of 2023, schlegea@informatik.uni-freiburg.de)

grammar ANTLR4Mockup;

// Parser rules
// We don't have any, because we have nothing, that needs them for tests.

// Lexer rules

// The literals.
BOOL : 'true' | 'false';
INTEGER : '-'?[0-9]+;
FLOAT : INTEGER'.'[0-9]+;
STRING : '"' .*? '"';

// To get of uneeded spaces between words/tokens.
WHITESPACE : ('\t' | ' ') -> skip ;
