add_subdirectory(ConfigManager)
add_subdirectory(MemorySize)
add_subdirectory(http)
add_library(util GeoSparqlHelpers.cpp antlr/ANTLRErrorHandling.cpp ParseException.cpp Conversions.cpp Date.cpp DateYearDuration.cpp Duration.cpp antlr/GenerateAntlrExceptionMetadata.cpp CancellationHandle.cpp StringUtils.cpp LazyJsonParser.cpp BlankNodeManager.cpp
        Rtree.h
        Rtree.cpp
        RtreeBuild.cpp
        RtreeFileReader.cpp
        RtreeFileReader.h
        RtreeBasicGeometry.h
        RtreeNode.cpp
        RtreeNode.h
        RtreeSorter.cpp RtreeSorter.h)
qlever_target_link_libraries(util Boost::serialization ${STXXL_LIBRARIES})
qlever_target_link_libraries(util re2::re2 s2)
