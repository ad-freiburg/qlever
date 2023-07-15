// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023, schlegea@informatik.uni-freiburg.de)

grammar MemorySizeLanguage;

// Parser rules

memorySizeString : (pureByteSize | memoryUnitSize) EOF;
pureByteSize : UNSIGNED_INTEGER 'B';
memoryUnitSize : (UNSIGNED_INTEGER | FLOAT) MEMORY_UNIT;

// Lexer rules

// Keywords.
MEMORY_UNIT : 'kB' | 'MB' | 'GB' | 'TB';

// The literals.
UNSIGNED_INTEGER : [0-9]+;
FLOAT : UNSIGNED_INTEGER'.'UNSIGNED_INTEGER;

WHITESPACE : ('\t' | ' ') -> skip ;
