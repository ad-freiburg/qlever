//  Copyright 2026 The QLever Authors, in particular:
//
//  2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
//  UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <emscripten/bind.h>

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "libqlever/Qlever.h"

namespace {
using namespace emscripten;

// JS numbers are doubles, so take the number of megabytes as `double` and
// convert to an exact `MemorySize`.
ad_utility::MemorySize memorySizeFromMegabytes(double numMegabytes) {
  return ad_utility::MemorySize::megabytes(numMegabytes);
}
}  // namespace

// Emscripten (embind) bindings for `libqlever`, so that QLever can be used
// from JavaScript when compiled to WebAssembly. This file is only compiled
// for the `qleverwasm` target.
//
// The resulting module exposes the configuration structs from `Qlever.h`
// (`IndexBuilderConfig`, `EngineConfig`, ...) as well as the `Qlever` class
// itself. Typical usage from JavaScript (inside a Web Worker, as index
// building and querying are blocking calls):
//
//   const qlever = await createQleverModule();
//   qlever.FS.writeFile("/input.ttl", turtleData);
//   const config = new qlever.IndexBuilderConfig();
//   config.baseName = "/index";
//   const file = new qlever.InputFileSpecification();
//   file.filename = "/input.ttl";
//   file.filetype = qlever.Filetype.Turtle;
//   config.inputFiles.push_back(file);
//   qlever.Qlever.buildIndex(config);
//   const engine = new qlever.Qlever(new qlever.EngineConfig(config));
//   const result = engine.query("SELECT * { ?s ?p ?o }");
EMSCRIPTEN_BINDINGS(qlever) {
  // Helper types used by the configuration structs below.
  register_optional<std::string>();
  register_vector<std::string>("StringVector");
  register_vector<qlever::InputFileSpecification>(
      "InputFileSpecificationVector");

  enum_<qlever::Filetype>("Filetype")
      .value("Turtle", qlever::Filetype::Turtle)
      .value("NQuad", qlever::Filetype::NQuad);

  enum_<ad_utility::VocabularyType::Enum>("VocabularyType")
      .value("InMemoryUncompressed",
             ad_utility::VocabularyType::Enum::InMemoryUncompressed)
      .value("OnDiskUncompressed",
             ad_utility::VocabularyType::Enum::OnDiskUncompressed)
      .value("InMemoryCompressed",
             ad_utility::VocabularyType::Enum::InMemoryCompressed)
      .value("OnDiskCompressed",
             ad_utility::VocabularyType::Enum::OnDiskCompressed)
      .value("OnDiskCompressedGeoSplit",
             ad_utility::VocabularyType::Enum::OnDiskCompressedGeoSplit);

  // The formats in which query results can be exported.
  enum_<ad_utility::MediaType>("MediaType")
      .value("textPlain", ad_utility::MediaType::textPlain)
      .value("json", ad_utility::MediaType::json)
      .value("sparqlJson", ad_utility::MediaType::sparqlJson)
      .value("sparqlXml", ad_utility::MediaType::sparqlXml)
      .value("qleverJson", ad_utility::MediaType::qleverJson)
      .value("tsv", ad_utility::MediaType::tsv)
      .value("csv", ad_utility::MediaType::csv)
      .value("turtle", ad_utility::MediaType::turtle)
      .value("ntriples", ad_utility::MediaType::ntriples)
      .value("octetStream", ad_utility::MediaType::octetStream);

  class_<qlever::InputFileSpecification>("InputFileSpecification")
      .constructor<>()
      // `source_` is a variant of a filename and a byte-source factory; only
      // the filename alternative is meaningful from JavaScript.
      .property(
          "filename",
          optional_override([](const qlever::InputFileSpecification& self) {
            return self.filename();
          }),
          optional_override(
              [](qlever::InputFileSpecification& self, std::string filename) {
                self.source_ = std::move(filename);
              }))
      .property("filetype", &qlever::InputFileSpecification::filetype_)
      .property("defaultGraph", &qlever::InputFileSpecification::defaultGraph_)
      .property("parseInParallel",
                &qlever::InputFileSpecification::parseInParallel_);

  class_<qlever::CommonConfig>("CommonConfig")
      .property("baseName", &qlever::CommonConfig::baseName_)
      .property("kbIndexName", &qlever::CommonConfig::kbIndexName_)
      .property("noPatterns", &qlever::CommonConfig::noPatterns_)
      .property("onlyPsoAndPos", &qlever::CommonConfig::onlyPsoAndPos_)
      .property("addHasWordTriples", &qlever::CommonConfig::addHasWordTriples_)
      .function("setMemoryLimitMB",
                optional_override([](qlever::CommonConfig& self, double mb) {
                  self.memoryLimit_ = memorySizeFromMegabytes(mb);
                }));

  class_<qlever::IndexBuilderConfig, base<qlever::CommonConfig>>(
      "IndexBuilderConfig")
      .constructor<>()
      .function("validate", &qlever::IndexBuilderConfig::validate)
      .property("inputFiles", &qlever::IndexBuilderConfig::inputFiles_)
      .property("settingsFile", &qlever::IndexBuilderConfig::settingsFile_)
      .property("keepTemporaryFiles",
                &qlever::IndexBuilderConfig::keepTemporaryFiles_)
      .property("prefixesForIdEncodedIris",
                &qlever::IndexBuilderConfig::prefixesForIdEncodedIris_)
      .property("addWordsFromLiterals",
                &qlever::IndexBuilderConfig::addWordsFromLiterals_)
      .property("vocabType",
                optional_override([](const qlever::IndexBuilderConfig& self) {
                  return self.vocabType_.value();
                }),
                optional_override([](qlever::IndexBuilderConfig& self,
                                     ad_utility::VocabularyType::Enum value) {
                  self.vocabType_ = ad_utility::VocabularyType{value};
                }))
      .function(
          "setParserBufferSizeMB",
          optional_override([](qlever::IndexBuilderConfig& self, double mb) {
            self.parserBufferSize_ = memorySizeFromMegabytes(mb);
          }));

  class_<qlever::EngineConfig, base<qlever::CommonConfig>>("EngineConfig")
      .constructor<>()
      .constructor<const qlever::IndexBuilderConfig&>()
      .property("loadTextIndex", &qlever::EngineConfig::loadTextIndex_)
      .property("persistUpdates", &qlever::EngineConfig::persistUpdates_)
      .property("doNotLoadPermutations",
                &qlever::EngineConfig::doNotLoadPermutations_);

  class_<qlever::Qlever>("Qlever")
      .constructor<const qlever::EngineConfig&>()
      .class_function("buildIndex", &qlever::Qlever::buildIndex)
      .function("query",
                select_overload<std::string(std::string, ad_utility::MediaType)
                                    const>(&qlever::Qlever::query))
      // Overload with the default media type (`sparqlJson`), as embind does
      // not support default arguments.
      .function("query", optional_override(
                             [](const qlever::Qlever& self, std::string query) {
                               return self.query(std::move(query));
                             }))
      .function("queryAndPinResultWithName",
                select_overload<void(std::string, std::string)>(
                    &qlever::Qlever::queryAndPinResultWithName))
      .function("eraseResultWithName", &qlever::Qlever::eraseResultWithName)
      .function("clearNamedResultCache",
                &qlever::Qlever::clearNamedResultCache);
}
