# Auto-detected extensions (e.g. custom SPARQL functions). Each subdirectory is
# an OBJECT library, linked into the binaries by the top-level CMakeLists.
file(GLOB entries CONFIGURE_DEPENDS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
     ${CMAKE_CURRENT_SOURCE_DIR}/*)
foreach (entry ${entries})
    if (IS_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/${entry} AND
        EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${entry}/CMakeLists.txt)
        add_subdirectory(${entry})
    endif ()
endforeach ()
