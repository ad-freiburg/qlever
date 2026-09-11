add_library(blobConverter LegacyEncodedIriManager.cpp LegacyBlobReader.cpp BlobConverter.cpp)
qlever_target_link_libraries(blobConverter qlever engine index)

if (NOT _EMSCRIPTEN_NO_INDEXBUILDER_AND_SERVER)
    add_executable(qlever-blob-converter BlobConverterMain.cpp)
    qlever_target_link_libraries(qlever-blob-converter blobConverter Boost::program_options)
endif ()
