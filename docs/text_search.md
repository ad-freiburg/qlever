# Text Search Feature Documentation for SPARQL Engine

## Overview

The Text Search feature provides additional control over text search compared to ql:contains-word and ql:contains-entity. It allows binding the score and prefix match of the searches to self defined variables. This feature is accessed using the `SERVICE` keyword and the service IRI `<https://qlever.cs.uni-freiburg.de/textSearch/>`.

## Full Syntax

The complete structure of a Text Search with every predicate used is as follows:

```sparql
PREFIX textSearch: <https://qlever.cs.uni-freiburg.de/textSearch/>

SELECT * WHERE {
	SERVICE textSearch: {
		?t textSearch:text-search [textSearch:contains-word "prefix*" ; textSearch:bind-match ?prefix_match ; textSearch:bind-score ?prefix_score] .
		?t textSearch:text-search [textSearch:contains-entity ?entity ; textSearch:bind-score ?entity_score] .
	}
}
```

Can be also expressed as:

```sparql
PREFIX textSearch: <https://qlever.cs.uni-freiburg.de/textSearch/>

SELECT * WHERE {
	SERVICE textSearch: {
		?t textSearch:text-search ?config1 .           # Bind first config to text variable
		?config1 textSearch:contains-word "prefix*" .  # Specify word or prefix to search in text
		?config1 textSearch:bind-match ?prefix_match . # Bind prefix completion variable
		?config1 textSearch:bind-score ?prefix_score . # Bind score variable
		?t textSearch:text-search ?config2 .           # Bind second config to text variable
		?config2 textSearch:contains-entity ?entity .  # Specify entity to search in text
		?config2 textSearch:bind-score ?entity_score . # Bind score variable
	}
}
```


### Parameters

- **textSearch:text-search**: Connects a text variable to a configuration for text search. One text variable can be connected to multiple configurations leading to multiple searches on the same text where the searches are later joined. One config shouldn't be linked to multiple text variables.
- **textSearch:contains-word**: Defines the word or prefix used in the text search. Doesn't support multiple words separated by spaces like `ql:contains-word` does. It is instead intended to use multiple searches. 
- **textSearch:contains-entity**: Defines the entity used in text search. Can be a literal or IRI for fixed entities or a variable to get matches for multiple entities. An entity search on a text variable needs at least one word search on the same variable. 
- **textSearch:bind-match** (optional): Binds prefix completion to a variable that can be used outside of the `SERVICE`. If not specified the column will be omitted. Should only be used in a word search configuration where the word is a prefix.
- **textSearch:bind-score** (optional): Binds score to a variable that can be used outside of the `SERVICE`. If not specified the column will be omitted.

A configuration is either a word search configuration specified with `textSearch:contains-word` or an entity search configuration with `textSearch:contains-entity`. 

### Example 1:  Simple word search

A simple word search:

```sparql
PREFIX textSearch: <https://qlever.cs.uni-freiburg.de/textSearch/>

SELECT * WHERE {
	SERVICE textSearch: {
		?t textSearch:text-search [ textSearch:contains-word "word" ] .
	}
}
```

The output columns are: `?t`

### Example 2: Prefix search

A simple prefix search where the prefix variable is bound:

```sparql
PREFIX textSearch: <https://qlever.cs.uni-freiburg.de/textSearch/>

SELECT * WHERE {
	SERVICE textSearch: {
		?t textSearch:text-search [ textSearch:contains-word "prefix*" ; textSearch:bind-match ?prefix_match ] .
	}
}
```

The output columns are: `?t`, `?prefix_match`

### Example 3: Score word search with filter

A word search where the score variable is bound and later used to filter the result:

```sparql
PREFIX textSearch: <https://qlever.cs.uni-freiburg.de/textSearch/>

SELECT * WHERE {
	SERVICE textSearch: {
		?t textSearch:text-search [ textSearch:contains-word "word" ; textSearch:bind-score ?score ] .
	}
	FILTER(?score > 1)
}
```

The output columns are: `?t`, `?score`

### Example 4: Simple entity search

A simple entity search, where an entity is contained in a text together with a word:

```sparql
PREFIX textSearch: <https://qlever.cs.uni-freiburg.de/textSearch/>

SELECT * WHERE {
	SERVICE textSearch: {
		?t textSearch:text-search [ textSearch:contains-word "word" ] .
		?t textSearch:text-search [ textSearch:contains-entity ?e ] .
	}
}
```

The output columns are: `?t`, `?e`

### Example 5: Multiple word and or prefix search

A text search with multiple word and prefix searches:

```sparql
PREFIX textSearch: <https://qlever.cs.uni-freiburg.de/textSearch/>

SELECT * WHERE {
	SERVICE textSearch: {
		?t textSearch:text-search [ textSearch:contains-word "word" ] .
		?t textSearch:text-search [ textSearch:contains-word "prefix*" ] .
		?t textSearch:text-search [ textSearch:contains-word "term" ] .
	}
}
```

The output columns are: `?t`

## Error Handling 

The Text Search feature will throw errors in the following scenarios:

- **No Basic Graph Pattern**: If the inner syntax of the `SERVICE` isn't only triples, an error will be raised.
- **Faulty Triples**: If predicates aren't used with subjects and objects of correct type, an error will be raised. E.g. `textSearch:contains-word` with a variable as object. 
- **Multiple Occurrences of Predicates**: If predicates occur multiple times in one configuration, an error will be raised.
- **Config linked to multiple Text Variables**: If manually defining a configuration variable and linking that to different text variables, an error will be raised. It is good practice to use the [ ] syntax and thus not manually defining the configuration variables.
- **Missing necessary Predicates**: If predicate `textSearch:text-search` is missing inside the `SERVICE`, an error will be raised. If a configuration doesn't contain exactly one occurrence of either `textSearch:contains-word` or `textSearch:contains-entity`, an error will be raised.
- **Word and Entity Search in one Configuration**: If one configuration contains both predicates `textSearch:contains-word` and `textSearch:contains-entity` , an error will be raised.
- **Entity Search on a Text without Word Search**: If a configuration with `textSearch:contains-entity` is connected to a text variable that is not connected to a configuration with `textSearch:contains-word`, an error will be raised.
- **Empty Word Search**: If the object of `textSearch:contains-word` is an empty literal, an error will be raised.

For detailed tests of errors see QueryPlannerTest.cpp, specifically the TextSearchService test.
