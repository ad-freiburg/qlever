add_library(testUtil IdTableHelpers.cpp ValidatorHelpers.cpp RandomTestHelpers.cpp BenchmarkMeasurementContainerHelpers.cpp IndexTestHelpers.cpp)
# Bundle the large engine and index static libraries into testUtil.so so that
# each test binary only needs to link against this single shared library.
# $<LINK_LIBRARY:WHOLE_ARCHIVE,...> both establishes the build dependency on
# these targets and emits the platform-appropriate --whole-archive flags.
qlever_target_link_libraries(testUtil PUBLIC qlever-shared-complete)
#target_link_libraries(testUtil PRIVATE absl::compare absl::flat_hash_map
#        absl::flat_hash_set absl::strings absl::str_format absl::base absl::log ICU::uc
#        ICU::i18n OpenSSL::SSL OpenSSL::Crypto GTest::gtest GTest::gmock fsst nlohmann_json::nlohmann_json Boost::container)
#target_link_libraries(testUtil INTERFACE re2 s2 spatialjoin-dev pb_util pb_util_geo)
# TODO<c++20> Once there is more support for it, we should be able to do this cheaper with modules.
#qlever_target_link_libraries(testUtil global gmock_main re2)
