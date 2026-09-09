
add_subdirectory(sparqlParser)
add_subdirectory(data)

# These files are needed by `index`, but the `sparqlParser` library below itself
# depends on `index`, so the dependency chain is
# `sparqlParser` -> `index` -> `basicParser`. They therefore have to live in
# their own library instead of in `sparqlParser`.
add_library(basicParser Tokenizer.cpp LiteralOrIri.cpp WordsAndDocsFileParser.cpp AsyncBlockSource.cpp
        TripleComponent.cpp PropertyPath.cpp PayloadVariables.cpp)
qlever_target_link_libraries(basicParser qlever_util rdfTypes parserData)

# The parser for the RDF input formats. It is used by `index` (which builds an
# index from RDF input files), so it must not depend on `index` itself. It needs
# the `vocabulary` library for the `EncodedIriManager`.
set(RDF_PARSER_SOURCES RdfParser.cpp AsyncFileBlockDriver.cpp)
# `RdfAsyncParallelParser` uses `ad_utility::AsyncResourcePool`, which needs
# `boost::asio::experimental::concurrent_channel` (Boost 1.80 or newer), whereas
# the `REDUCED_FEATURE_SET_FOR_CPP17` build targets Boost 1.71. Nothing inside
# `src` uses that parser yet (it is only exercised by the unit tests), so it is
# simply excluded there.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    list(APPEND RDF_PARSER_SOURCES RdfAsyncParallelParser.cpp)
endif ()
add_library(rdfParser ${RDF_PARSER_SOURCES})
qlever_target_link_libraries(rdfParser basicParser vocabulary localVocab)

add_library(sparqlParser
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
qlever_target_link_libraries(sparqlParser basicParser antlrParser parserData sparqlExpressions rdfEscaping global qlever_util index rdfTypes)
