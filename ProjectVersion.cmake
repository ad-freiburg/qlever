# A small CMake script that writes the current version (from git tags) to a
# header file ProjectVersion.h

# Try to run `git describe`
execute_process(
  COMMAND git describe --tags --dirty
  OUTPUT_VARIABLE PROJECT_VERSION
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Fallback if `git describe` failed or returned empty
if ((NOT DEFINED PROJECT_VERSION) OR (PROJECT_VERSION STREQUAL ""))
  set(PROJECT_VERSION "0.0.0-unknown")
endif()

message(STATUS "QLever PROJECT_VERSION is ${PROJECT_VERSION}")

set(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}")

# Escape the version string into a quoted literal
set(QLEVER_PROJECTVERSION "\"${PROJECT_VERSION}\"")

# Create a header file with include guards and a define
set(CONTENTS "#ifndef QLEVER_SRC_PROJECTVERSION_H
#define QLEVER_SRC_PROJECTVERSION_H

#define QLEVER_SRC_PROJECTVERSION ${QLEVER_PROJECTVERSION}

#endif // QLEVER_SRC_PROJECTVERSION_H
")

file(WRITE ${CMAKE_CURRENT_SOURCE_DIR}/ProjectVersion.h "${CONTENTS}")
