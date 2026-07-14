add_subdirectory(vocabulary)
addLinkAndDiscoverTest(PatternCreatorTest index)
addLinkAndDiscoverTest(ScanSpecificationTest index)
addLinkAndDiscoverTestNoLibs(KeyOrderTest)
addLinkAndDiscoverTestNoLibs(EncodedIriManagerTest parser)
addLinkAndDiscoverTest(GraphNameManagerTest index)
# `IndexRebuilderTest` links the `http`/`server` CMake targets, which are
# entirely excluded under `REDUCED_FEATURE_SET_FOR_CPP17`.
if (NOT REDUCED_FEATURE_SET_FOR_CPP17)
    addLinkAndDiscoverTest(IndexRebuilderTest index server)
endif ()
addLinkAndDiscoverTest(InputFileSpecificationTest parser index)
addLinkAndDiscoverTest(VocabularyMergerImplTest index)
