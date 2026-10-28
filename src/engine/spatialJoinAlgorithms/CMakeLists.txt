target_sources(engine PRIVATE
        SpatialJoinAlgorithmBase.cpp
        RtreeEntryAlgorithm.cpp
        BaselineAlgorithm.cpp S2GeometryAlgorithm.cpp
        S2PointPolylineAlgorithm.cpp BoundingBoxAlgorithm.cpp
        LibspatialjoinAlgorithm.cpp)

# GCC 15 and GCC 16 wrongly report the inline storage of the
# `absl::InlinedVector` inside the priority queue of `S2ClosestPointQueryBase`
# as definitely used uninitialized. The same suppression is needed for the `s2`
# target itself, see the top-level `CMakeLists.txt`. The suppression is limited
# to the affected versions, so that the warning stays active for older ones.
#
# NOTE: `TARGET_DIRECTORY` is required, because source file properties are
# scoped to a directory, but the `engine` target is defined in the parent
# directory. Without it the property would silently have no effect.
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND
    CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "15")
    set_source_files_properties(S2GeometryAlgorithm.cpp
            TARGET_DIRECTORY engine
            PROPERTIES COMPILE_OPTIONS "-Wno-uninitialized")
endif ()
