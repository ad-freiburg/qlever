# Get the project version from git describe.
# Format : {most recent tag } - {commits since tag } - g{short hash }
# or just the short hash if no tags.
execute_process(
    COMMAND git describe --tags --always
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    OUTPUT_VARIABLE PROJECT_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
)

if ((NOT DEFINED PROJECT_VERSION) OR (PROJECT_VERSION STREQUAL ""))
    set(PROJECT_VERSION "0.0.0-unknown")
endif()
