
add_library(index
              Index.h Index.cpp
              Vocabulary.h Vocabulary.cpp
              IndexMetaData.h IndexMetaData.cpp
              StxxlSortFunctors.h
            )

target_link_libraries(index parser ${STXXL_LIBRARIES})