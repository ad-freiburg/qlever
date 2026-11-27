addLinkAndDiscoverTestNoLibs(BlobDiffTest qlever testUtil)
addLinkAndDiscoverTestNoLibs(BlobDiffConfigTest qlever testUtil)
addLinkAndDiscoverTestNoLibs(BlobDiffQueriesTest qlever testUtil)
# The test reads `data/snapshot-config.yaml` from the source tree. The macro
# holds the absolute path of the `data` directory, so the test does not depend
# on the directory from which it is run.
target_compile_definitions(BlobDiffConfigTest PRIVATE
        QLEVER_LIBQLEVER_TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/data")
