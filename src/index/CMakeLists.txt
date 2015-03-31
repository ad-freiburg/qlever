################################
# STXXL
################################

# include the STXXL library
add_subdirectory(stxxl)
# apply STXXL CXXFLAGS
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${STXXL_CXX_FLAGS}")
# add STXXL includes path
include_directories(${STXXL_INCLUDE_DIRS})

################################
# LOCAL
################################

add_library(index
              Index.h Index.cpp
              Vocabulary.h Vocabulary.cpp
              IndexMetaData.h IndexMetaData.cpp
              StxxlSortFunctors.h
            )

target_link_libraries(index parser ${STXXL_LIBRARIES})