addLinkAndDiscoverTestNoLibs(BlobDiffTest qlever testUtil)
addLinkAndDiscoverTestNoLibs(BlobDiffProducerTest qlever testUtil)
target_compile_definitions(BlobDiffProducerTest PRIVATE
        QLEVER_LIBQLEVER_TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/data")
