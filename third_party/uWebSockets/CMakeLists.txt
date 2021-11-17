find_package(OpenSSL REQUIRED)

add_custom_command(
  OUTPUT "${CMAKE_CURRENT_LIST_DIR}/src/uSockets/uSockets.a"
  COMMAND ${CMAKE_COMMAND} -E env WITH_OPENSSL=1 CXX=${CMAKE_CXX_COMPILER} CC=${CMAKE_C_COMPILER} make
  WORKING_DIRECTORY "${CMAKE_CURRENT_LIST_DIR}/src")

add_custom_target(uSocketsBuild ALL
  DEPENDS "${CMAKE_CURRENT_LIST_DIR}/src/uSockets/uSockets.a"
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


