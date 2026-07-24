add_subdirectory(ConfigManager)
add_subdirectory(MemorySize)
add_subdirectory(http)
add_library(qlever_util ParseableDuration.cpp GeoSparqlHelpers.cpp UnitOfMeasurement.cpp antlr/ANTLRErrorHandling.cpp ParseException.cpp Conversions.cpp Date.cpp DateYearDuration.cpp Duration.cpp antlr/GenerateAntlrExceptionMetadata.cpp CancellationHandle.cpp StringUtils.cpp LazyJsonParser.cpp BlankNodeManager.cpp IoUringManager.cpp FilesystemHelpers.cpp QueryEventLog.cpp ResourceMonitor.cpp)
qlever_target_link_libraries(qlever_util re2::re2 s2 pb_util pb_util_geo antlr4_static)

add_subdirectory(metrics)
