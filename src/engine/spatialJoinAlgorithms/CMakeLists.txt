target_sources(engine PRIVATE
        SpatialJoinAlgorithmBase.cpp
        RtreeEntryAlgorithm.cpp
        BaselineAlgorithm.cpp S2GeometryAlgorithm.cpp
        S2PointPolylineAlgorithm.cpp BoundingBoxAlgorithm.cpp
        LibspatialjoinAlgorithm.cpp)

# Since GCC 15, the inline storage of the `absl::InlinedVector` inside the
# priority queue of `S2ClosestPointQueryBase` is wrongly reported as definitely
# used uninitialized. For why this cannot be suppressed at the include, see
# `src/util/CompilerWarnings.h`; the same suppression is needed for the `s2`
# target itself, see the top-level `CMakeLists.txt`.
#
# NOTE: `TARGET_DIRECTORY` is required, because source file properties are
# scoped to a directory, but the `engine` target is defined in the parent
# directory. Without it the property would silently have no effect.
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set_source_files_properties(S2GeometryAlgorithm.cpp
            TARGET_DIRECTORY engine
            PROPERTIES COMPILE_OPTIONS "-Wno-uninitialized")
endif ()
