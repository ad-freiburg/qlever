find_package(OpenSSL REQUIRED)

message("archive tool is ${CMAKE_AR}")

# Dummy output which is never actually produced. Anything that depends
# on this will always be rebuilt.
add_custom_command(OUTPUT always_rebuild COMMAND cmake -E echo)
set(CUSTOM_FLAGS "-Wno-sign-conversion ${CMAKE_CXX_FLAGS} -Wno-unused-parameter -Wno-sign-compare -Wno-sign-conversion")
set(CUSTOM_C_FLAGS "${CMAKE_C_FLAGS} -Wno-sign-conversion")
add_custom_command(
  OUTPUT "${CMAKE_CURRENT_LIST_DIR}/src/uSockets/uSockets.a"
  #      OUTPUT build_usockets
  COMMAND ${CMAKE_COMMAND} -E env WITH_OPENSSL=1 CXX=${CMAKE_CXX_COMPILER} CC=${CMAKE_C_COMPILER} CFLAGS=${CUSTOM_C_FLAGS} CXXFLAGS=${CUSTOM_FLAGS} AR=${CMAKE_AR} make
  WORKING_DIRECTORY "${CMAKE_CURRENT_LIST_DIR}/src/uSockets" DEPENDS always_rebuild)

add_custom_target(uSocketsBuild ALL
  DEPENDS "${CMAKE_CURRENT_LIST_DIR}/src/uSockets/uSockets.a" always_rebuild
  )

add_library(uSockets STATIC IMPORTED)
add_dependencies(uSockets uSocketsBuild)
target_link_libraries(uSockets INTERFACE OpenSSL::SSL)
set_target_properties(uSockets PROPERTIES
  IMPORTED_LOCATION "${CMAKE_CURRENT_LIST_DIR}/src/uSockets/uSockets.a")

# Create a cmake target for using the external project
add_library(uWebSockets INTERFACE)
target_include_directories(uWebSockets INTERFACE
    ${CMAKE_CURRENT_LIST_DIR}/src/src
    ${CMAKE_CURRENT_LIST_DIR}/src/uSockets/src)
target_link_libraries(uWebSockets INTERFACE z uSockets)
