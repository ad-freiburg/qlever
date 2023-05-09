// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (March of 2023, schlegea@informatik.uni-freiburg.de)

grammar BenchmarkConfigurationShorthand;

// Parser rules

// Every string of the shorthand should just be a bunch of assignments, one after the other.
shortHandString : assignments EOF;
assignments : (listOfAssignments+=assignment ',')* listOfAssignments+=assignment;
assignment : NAME ':' content;
object : '{' assignments '}';
list : '['(listElement+=content',')* listElement+=content']';
content : LITERAL|list|object;

// Lexer rules

// The literals.
LITERAL : BOOL | INTEGER | FLOAT | STRING;
BOOL : 'true' | 'false';
INTEGER : '-'?[0-9]+;
FLOAT : INTEGER'.'[0-9]+;
STRING : '"' .*? '"';

NAME : [a-zA-Z0-9\-_]+;

WHITESPACE : ('\t' | ' ') -> skip ;
