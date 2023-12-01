//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "../IndexTestHelpers.h"

#include "./GTestHelpers.h"
#include "index/IndexImpl.h"

namespace ad_utility::testing {
Index makeTestIndex(const std::string& indexBasename,
                    std::optional<std::string> turtleInput,
                    bool loadAllPermutations, bool usePatterns,
                    bool usePrefixCompression,
                    ad_utility::MemorySize blocksizePermutations) {
  // Ignore the (irrelevant) log output of the index building and loading during
  // these tests.
  static std::ostringstream ignoreLogStream;
  ad_utility::setGlobalLoggingStream(&ignoreLogStream);
  std::string inputFilename = indexBasename + ".ttl";
  if (!turtleInput.has_value()) {
    turtleInput =
        "<x> <label> \"alpha\" . <x> <label> \"älpha\" . <x> <label> \"A\" . "
        "<x> "
        "<label> \"Beta\". <x> <is-a> <y>. <y> <is-a> <x>. <z> <label> "
        "\"zz\"@en";
  }

  FILE_BUFFER_SIZE() = 1000;
  std::fstream f(inputFilename, std::ios_base::out);
  f << turtleInput.value();
  f.close();
  {
    Index index = makeIndexWithTestSettings();
    // This is enough for 2 triples per block. This is deliberately chosen as a
    // small value, s.t. the tiny knowledge graphs from unit tests also contain
    // multiple blocks. Should this value or the semantics of it (how many
    // triples it may store) ever change, then some unit tests might have to be
    // adapted.
    index.blocksizePermutationsPerColumn() = blocksizePermutations;
    index.setOnDiskBase(indexBasename);
    index.usePatterns() = usePatterns;
    index.setPrefixCompression(usePrefixCompression);
    index.loadAllPermutations() = loadAllPermutations;
    index.createFromFile(inputFilename);
  }
  if (!usePatterns || !loadAllPermutations) {
    // If we have no patterns, or only two permutations, then check the graceful
    // fallback even if the options were not explicitly specified during the
    // loading of the server.
    Index index{ad_utility::makeUnlimitedAllocator<Id>()};
    index.usePatterns() = true;
    index.loadAllPermutations() = true;
    EXPECT_NO_THROW(index.createFromOnDiskIndex(indexBasename));
    EXPECT_EQ(index.loadAllPermutations(), loadAllPermutations);
    EXPECT_EQ(index.usePatterns(), usePatterns);
  }
  Index index{ad_utility::makeUnlimitedAllocator<Id>()};
  index.usePatterns() = usePatterns;
  index.loadAllPermutations() = loadAllPermutations;
  index.createFromOnDiskIndex(indexBasename);
  ad_utility::setGlobalLoggingStream(&std::cout);

  // TODO<joka921> Factor this out into a separate function.
  if (usePatterns && loadAllPermutations) {
    auto checkConsistency = [&](Id predicate, source_location l =
                                                  source_location::current()) {
      auto tr = generateLocationTrace(l);
      auto cancellationDummy =
          std::make_shared<ad_utility::CancellationHandle>();
      auto scanResult =
          index.scan(predicate, std::nullopt, Permutation::PSO,
                     std::array{ColumnIndex{2}}, cancellationDummy);
      ASSERT_EQ(scanResult.numColumns(), 3u);
      for (const auto& row : scanResult) {
        auto subject = row[0].getVocabIndex().get();
        auto patternIdx = row[2].getInt();
        if (subject >= index.getHasPattern().size()) {
          EXPECT_EQ(patternIdx, NO_PATTERN);
        } else {
          EXPECT_EQ(patternIdx, index.getHasPattern()[subject])
              << subject << ' '
              << index.idToOptionalString(row[0].getVocabIndex()).value() << ' '
              << index.getHasPattern().size() << ' ' << NO_PATTERN;
        }
      }
    };
    const auto& predicates = index.getImpl().PSO().metaData().data();
    for (const auto& predicate : predicates) {
      checkConsistency(predicate.col0Id_);
    }
  }
  return index;
}
}  // namespace ad_utility::testing
