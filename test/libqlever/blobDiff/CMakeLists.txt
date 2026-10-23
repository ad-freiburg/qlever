addLinkAndDiscoverTestNoLibs(BlobDiffTest qlever testUtil)
addLinkAndDiscoverTestNoLibs(BlobDiffConfigTest qlever testUtil)
addLinkAndDiscoverTestNoLibs(BlobDiffQueriesTest qlever testUtil)
addLinkAndDiscoverTestNoLibs(TurtleDeltaTest qlever testUtil)
addLinkAndDiscoverTestNoLibs(BlobDiffProducerTest qlever testUtil)
target_compile_definitions(BlobDiffConfigTest PRIVATE
        QLEVER_LIBQLEVER_TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/data")
target_compile_definitions(BlobDiffProducerTest PRIVATE
        QLEVER_LIBQLEVER_TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/data")
