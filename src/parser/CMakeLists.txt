
add_subdirectory(sparqlParser)
add_subdirectory(data)

# These files are needed by `index`, but the `parser` library below itself
# depends on `index`, so the dependency chain is
# `parser` -> `index` -> `basicParser`. They therefore have to live in their own
# library instead of in `parser`.
# NOTE: `TripleComponent.cpp` only implements `operator<<` and `toString()`. The
# conversions that require an index live in `index/TripleComponentConversions.h`.
add_library(basicParser Tokenizer.cpp LiteralOrIri.cpp WordsAndDocsFileParser.cpp AsyncBlockSource.cpp
        TripleComponent.cpp PropertyPath.cpp PayloadVariables.cpp)
qlever_target_link_libraries(basicParser qlever_util rdfTypes parserData)

# The parser for the RDF input formats. It is used by `index` (which builds an
# index from RDF input files), so it must not depend on `index` itself. It needs
# the `vocabulary` library for the `EncodedIriManager`.
add_library(rdfParser RdfParser.cpp AsyncFileBlockDriver.cpp)
qlever_target_link_libraries(rdfParser basicParser vocabulary localVocab)

# TODO: Consider moving further files that don't depend on the engine
# (sparqlExpressions, index) into `basicParser` or a new intermediate library to
# clean up the dependency graph. The remaining files all reach `index` or
# `sparqlExpressions`, mostly through `parser/Alias.h` (which includes
# `SparqlExpressionPimpl.h`) and through `GraphPatternOperation.h` (which
# includes `PathQuery.h` and `TextSearchQuery.h`, and thus even `engine`).
add_library(parser
        sparqlParser/SparqlQleverVisitor.cpp
        SparqlParser.cpp
        ParsedQuery.cpp
        SparqlParserHelpers.cpp
        GraphPatternOperation.cpp
        MagicServiceQuery.cpp
        NamedCachedResult.cpp
        PathQuery.cpp
        SpatialQuery.cpp
        data/SparqlFilter.cpp
        SelectClause.cpp
        GraphPatternOperation.cpp
        GraphPattern.cpp
        DatasetClauses.cpp
        TextSearchQuery.cpp
        MaterializedViewQuery.cpp
        Quads.cpp
        UpdateTriples.cpp
        MaterializedViewQuery.cpp
        GraphPatternAnalysis.cpp
        ExternalValuesQuery.cpp
        VariableCounter.cpp
)
qlever_target_link_libraries(parser basicParser sparqlParser parserData sparqlExpressions rdfEscaping global qlever_util index rdfTypes)
