# QLever.js

[QLever](https://github.com/ad-freiburg/qlever) is a fast SPARQL engine for knowledge graphs. This package contains
QLever compiled to WebAssembly (via Emscripten), so you can build RDF indexes and run SPARQL queries directly in the
browser or in Node.js, no server required. QLever for WebAssembly is currently still experimental and is not as
efficient as native builds.

## Usage

QLever uses threads, so index building and querying are blocking calls. Run them inside a Web Worker (browser) or a
worker thread (Node.js).

The package is an ES module; its default export is the module factory.

Objects created with `new` own memory in the WebAssembly heap, which JavaScript's garbage collector does not free, so
they have to be released explicitly: either with a `using` declaration as below, which releases them at the end of the
enclosing block (Chrome/Edge ≥ 134, Firefox ≥ 141, Node.js ≥ 24; TypeScript and esbuild compile it down for older
targets, Safari included), or with `.delete()` in a `finally` block.

```js
import createQleverModule from "@ad-freiburg/qlever";

const qlever = await createQleverModule();

// Put the input data into the in-memory filesystem.
qlever.FS.writeFile("/input.ttl", turtleData);

// Build an index.
using config = new qlever.IndexBuilderConfig();
config.baseName = "/index";
using file = new qlever.InputFileSpecification();
file.filename = "/input.ttl";
file.filetype = qlever.Filetype.Turtle;
using files = new qlever.InputFileSpecificationVector();
files.push_back(file);
config.inputFiles = files;
qlever.Qlever.buildIndex(config);

// Load the index and query it.
using engineConfig = new qlever.EngineConfig(config);
using engine = new qlever.Qlever(engineConfig);
const result = engine.query(
    "SELECT * WHERE { ?s ?p ?o }", qlever.MediaType.sparqlJson);
console.log(JSON.parse(result));
```

## Browser deployment notes

- QLever uses pthreads and therefore `SharedArrayBuffer`. The page that loads the module must
  be [cross-origin isolated](https://developer.mozilla.org/en-US/docs/Web/API/Window/crossOriginIsolated), i.e. served
  with the headers `Cross-Origin-Opener-Policy: same-origin` and
  `Cross-Origin-Embedder-Policy: require-corp`.
- Ship `qlever.wasm` alongside `qlever.mjs`. The loader resolves it relative to the module's own URL
  (`import.meta.url`), which bundlers like Vite and webpack handle automatically; point the `locateFile` module option
  at its location only if you deviate from that layout.
- The module is built for wasm64, which requires a recent browser (or Node.js ≥ 24).
