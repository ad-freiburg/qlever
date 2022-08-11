# get the current time, remove the trailing newline and add quotes
execute_process(COMMAND date OUTPUT_VARIABLE DATETIME_OF_COMPILATION)
string(REPLACE "\n" "" DATETIME_OF_COMPILATION "${DATETIME_OF_COMPILATION}")
set(DATETIME_OF_COMPILATION "\"${DATETIME_OF_COMPILATION}\"")
message(STATUS "DATE is ${DATETIME_OF_COMPILATION}" )

# get the current git hash
execute_process(COMMAND git log -1 --format="%H" OUTPUT_VARIABLE GIT_HASH)
if ((NOT DEFINED GIT_HASH) OR (GIT_HASH STREQUAL ""))
    set(GIT_HASH "\"Qlever is not inside a git repository\"")
endif()
message(STATUS "Git hash is ${GIT_HASH}")

set(CONSTANTS "const char* GitHash = ${GIT_HASH};
const char* Datetime = ${DATETIME_OF_COMPILATION}")

file(WRITE ${CMAKE_CURRENT_SOURCE_DIR}/src/GitHash.h "${CONSTANTS}")

