# Auto-detected tests for the extensions under `src/extensions`.
file(GLOB entries CONFIGURE_DEPENDS RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
     ${CMAKE_CURRENT_SOURCE_DIR}/*)
foreach (entry ${entries})
    if (IS_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/${entry} AND
        EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${entry}/CMakeLists.txt)
        add_subdirectory(${entry})
    endif ()
endforeach ()
