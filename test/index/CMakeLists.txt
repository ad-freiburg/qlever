add_subdirectory(vocabulary)
addLinkAndDiscoverTest(PatternCreatorTest index)
addLinkAndDiscoverTest(ScanSpecificationTest index)
addLinkAndDiscoverTestNoLibs(KeyOrderTest)
addLinkAndDiscoverTestNoLibs(EncodedIriManagerTest parser)
addLinkAndDiscoverTest(GraphNameManagerTest index)
# When the `server` library is not built (Emscripten, see
# `src/engine/CMakeLists.txt`), the server-integration test is compiled out
# via `#ifndef __EMSCRIPTEN__` in the test file and all other tests still run.
if (TARGET server)
    addLinkAndDiscoverTest(IndexRebuilderTest index server)
else ()
    addLinkAndDiscoverTest(IndexRebuilderTest index)
endif ()
addLinkAndDiscoverTest(InputFileSpecificationTest parser index)
addLinkAndDiscoverTest(VocabularyMergerImplTest index)
