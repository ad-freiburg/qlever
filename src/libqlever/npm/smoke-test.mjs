// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// Smoke test for the packed `@ad-freiburg/qlever` npm module. The CI (see
// `native-build-with-conan-and-emscripten.yml`) runs it against the tarball
// that would be published: it builds a tiny index inside the module's virtual
// filesystem and runs one query against it. This validates the ES module
// packaging, the loading of `qlever.wasm`, and the embind bindings, which the
// C++ unit tests cannot see. The file is not part of the published package
// (see the `files` whitelist in `package.json`).
//
// Index building and querying are blocking calls, so all the work happens in
// a Node.js worker thread (the counterpart of the Web Worker that the README
// recommends for the browser); the main thread only enforces a timeout.
import { Worker, isMainThread, parentPort } from "node:worker_threads";

if (isMainThread) {
  const worker = new Worker(new URL(import.meta.url));
  const timeout = setTimeout(() => {
    console.error("FAIL: smoke test timed out after 120 s");
    process.exit(1);
  }, 120_000);
  worker.on("message", (message) => {
    clearTimeout(timeout);
    if (message === "ok") {
      console.log("Smoke test passed");
      process.exit(0);
    }
    console.error(`FAIL: ${message}`);
    process.exit(1);
  });
  worker.on("error", (error) => {
    clearTimeout(timeout);
    console.error("FAIL:", error);
    process.exit(1);
  });
} else {
  const { default: createQleverModule } = await import("@ad-freiburg/qlever");
  const qlever = await createQleverModule();

  qlever.FS.writeFile(
    "/input.ttl",
    '<http://example.org/a> <http://example.org/p> <http://example.org/b> .\n' +
    '<http://example.org/a> <http://example.org/p> "some literal" .\n' +
    '<http://example.org/b> <http://example.org/q> <http://example.org/c> .\n',
  );

  const config = new qlever.IndexBuilderConfig();
  config.baseName = "/index";
  const file = new qlever.InputFileSpecification();
  file.filename = "/input.ttl";
  file.filetype = qlever.Filetype.Turtle;
  const files = new qlever.InputFileSpecificationVector();
  files.push_back(file);
  config.inputFiles = files;
  qlever.Qlever.buildIndex(config);

  const engine = new qlever.Qlever(new qlever.EngineConfig(config));
  const result = JSON.parse(
    engine.query("SELECT * WHERE { ?s ?p ?o }", qlever.MediaType.sparqlJson),
  );
  const numRows = result.results.bindings.length;
  parentPort.postMessage(
    numRows === 3 ? "ok" : `expected 3 result rows, got ${numRows}`,
  );
}
