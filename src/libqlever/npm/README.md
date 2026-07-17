# QLever.js

[QLever](https://github.com/ad-freiburg/qlever) is a fast SPARQL engine for
knowledge graphs. This package contains QLever compiled to WebAssembly
(via Emscripten), so you can build RDF indexes and run SPARQL queries
directly in the browser or in Node.js, no server required.

## Usage

QLever uses threads, so index building and querying are blocking calls —
run them inside a Web Worker (browser) or a worker thread (Node.js).

```js
const factory = require("@ad-freiburg/qlever"); // or importScripts("qlever.js") in a classic worker

const m = await factory();

// Put the input data into the in-memory filesystem.
m.FS.writeFile("/input.ttl", turtleData);

// Build an index.
const config = new m.IndexBuilderConfig();
config.baseName = "/index";
const file = new m.InputFileSpecification();
file.filename = "/input.ttl";
file.filetype = m.Filetype.Turtle;
const files = new m.InputFileSpecificationVector();
files.push_back(file);
config.inputFiles = files;
m.Qlever.buildIndex(config);

// Load the index and query it.
const engine = new m.Qlever(new m.EngineConfig(config));
const result = engine.query(
    "SELECT * WHERE { ?s ?p ?o }", m.MediaType.sparqlJson);
console.log(JSON.parse(result));
```

## Browser deployment notes

- QLever uses pthreads and therefore `SharedArrayBuffer`. The page that
  loads the module must
  be [cross-origin isolated](https://developer.mozilla.org/en-US/docs/Web/API/Window/crossOriginIsolated),
  i.e. served with the headers `Cross-Origin-Opener-Policy: same-origin` and
  `Cross-Origin-Embedder-Policy: require-corp`.
- Serve `qlever.wasm` next to `qlever.js` (or point the `locateFile` module
  option at its location) so the loader can fetch it.
- The module is built for wasm64, which requires a recent browser (or Node.js ≥ 24).
