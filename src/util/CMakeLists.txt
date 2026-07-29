add_subdirectory(ConfigManager)
add_subdirectory(MemorySize)
add_subdirectory(http)

# The lowest layer: string helpers that depend on nothing but the standard
# library and ICU. Kept separate so that leaf targets (for example
# `LocaleManagerTest`) can use them without pulling in all of `qlever_util`.
add_library(stringUtils StringUtils.cpp)
qlever_target_link_libraries(stringUtils)

# The error handling for the ANTLR-generated parsers. This has to be separate
# from `qlever_util`, because `rdfTypes` (which `qlever_util` depends on) needs
# it, and a single library would therefore be a cyclic dependency.
add_library(antlrErrorHandling antlr/ANTLRErrorHandling.cpp antlr/GenerateAntlrExceptionMetadata.cpp ParseException.cpp)
qlever_target_link_libraries(antlrErrorHandling stringUtils antlr4_umbrella)

# The general-purpose utilities. Everything in here may depend on `rdfTypes`,
# so nothing that `rdfTypes` itself needs must be added to this library.
add_library(qlever_util ParseableDuration.cpp UnitOfMeasurement.cpp Conversions.cpp Date.cpp DateYearDuration.cpp Duration.cpp CancellationHandle.cpp LazyJsonParser.cpp BlankNodeManager.cpp IoUringManager.cpp FilesystemHelpers.cpp QueryEventLog.cpp ResourceMonitor.cpp)
qlever_target_link_libraries(qlever_util antlrErrorHandling s2 pb_util pb_util_geo pb_util_json rdfTypes antlr4_umbrella)

add_subdirectory(metrics)
