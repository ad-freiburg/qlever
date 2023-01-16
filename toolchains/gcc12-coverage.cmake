# Toolchain that uses G++ 12
set(CMAKE_C_COMPILER gcc-12)
set(CMAKE_CXX_COMPILER g++-12)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Og -g --coverage -fkeep-inline-functions -fkeep-static-functions")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} --coverage")

set(
        COVERAGE_TRACE_COMMAND
        lcov -c -q
        -o "${PROJECT_BINARY_DIR}/coverage.info"
        -d "${PROJECT_BINARY_DIR}"
        --include "${PROJECT_SOURCE_DIR}/*"
        CACHE STRING
        "; separated command to generate a trace for the 'coverage' target"
)

set(
        COVERAGE_HTML_COMMAND
        genhtml --legend -f -q
        "${PROJECT_BINARY_DIR}/coverage.info"
        -p "${PROJECT_SOURCE_DIR}"
        -o "${PROJECT_BINARY_DIR}/coverage_html"
        CACHE STRING
        "; separated command to generate an HTML report for the 'coverage' target"
)

# ---- Coverage target ----

add_custom_target(
        coverage
        COMMAND ${COVERAGE_TRACE_COMMAND}
        COMMAND ${COVERAGE_HTML_COMMAND}
        COMMENT "Generating coverage report"
        VERBATIM
)
