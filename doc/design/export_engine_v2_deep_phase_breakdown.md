# QLever Fast-Path Streaming Export Engine (V2): Deep Phase Specification

**Status:** Approved Master Architecture RFC  
**Target Repository:** `marvin7122/qlever` (`feat/export-engine-v2-streaming`)  
**Design Standard:** Grounded in `~/ARCHITECTURE.md` (7 Universal Laws) & Single-Core CPU Discipline  

---

## Executive Overview: The 6 Phased Milestones

```
                          V2 Streaming Engine Feature Stack
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│ Phase 1: Ingress Routing, Feature Gating & Automatic Fallback Engine                        │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ Phase 2: Push-Based Vectorized Stream Execution (Zero-Intermediate Materialization)        │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ Phase 3: Monomorphic Template Schema Specialization & SIMD Radix/Escape Kernels             │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ Phase 4: Zero-Copy Memory Arena Scatter-Gather Streaming (`struct iovec` / `writev`)        │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ Phase 5: Asynchronous Double-Buffered Backpressure Ring (Compute/Network Decoupling)       │
├─────────────────────────────────────────────────────────────────────────────────────────────┤
│ Phase 6: End-to-End Server Pipeline Integration, Bit-Equivalence & Differential Profiling   │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# Phase 1: Ingress Routing, Feature Gating & Automatic Fallback

```
                             Request Ingress & Route Decision Tree
                            ┌──────────────────────────────────────┐
                            │ Incoming HTTP SPARQL Request         │
                            └──────────────────┬───────────────────┘
                                               │
                                 ┌─────────────┴─────────────┐
                                 │ Does request/server ask?  │
                                 └─────────────┬─────────────┘
                                               │
                        ┌──────────────────────┴──────────────────────┐
                        │ YES                                         │ NO
                        ▼                                             ▼
         ┌──────────────────────────────┐              ┌──────────────────────────────┐
         │ Check Plan Eligibility       │              │ Legacy Volcano Engine (V1)   │
         └──────────────┬───────────────┘              └──────────────────────────────┘
                        │
             ┌──────────┴──────────┐
             │ Fully Supported?    │
             │ (Scan, Join, Filter)│
             ├──────────┬──────────┤
             │ YES      │ NO       │
             ▼          ▼          │
      ┌─────────────┐  ┌─────────────┐
      │  Engine V2  │  │ Fallback V1 │
      └─────────────┘  └─────────────┘
```

### 1. Inspiration & Prior Art Review
* **DuckDB Execution Vectorizer Gate:** DuckDB uses an execution rule where physical query graphs are inspected: vector pipeline execution handles standard projections and hash joins, while complex nested recursive CTEs or user-defined aggregate types gracefully execute on standard iterator fallbacks.
* **ClickHouse HTTP Format Engine:** ClickHouse routes streaming formats (`TSVRaw`, `CSVWithNames`, `Native`) through specialized zero-allocation formatters while standard interactive queries use general table formatters.
* **Envoy / Nginx Route Filters:** Route decisions happen once at HTTP header parsing, establishing zero-overhead stream contexts for subsequent pipeline stages.

### 2. What is it & How does it work?
* **Component:** `src/engine/ExportPipelineRouter.h`
* **Mechanics:**
  1. Inspects query-time request metadata (`fast-export=1`, `export-engine=v2`, `X-QLever-Export-Engine: v2`).
  2. Evaluates the `ParsedQuery` execution plan: verifies whether the root operator is exportable (e.g. index scans, join trees, projections, filters) without blocking global operators (e.g. global top-N sort without index ordering).
  3. Returns `ExportEngineMode::FastStreamingV2` or `ExportEngineMode::LegacyV1`.
  4. If an unexpected runtime condition arises during V2 setup, it triggers a clean, transparent fallback to Legacy V1 with zero HTTP error returned to the client.

### 3. Performance Rationale
* **Zero Overhead on Normal Queries:** Evaluation is an $O(1)$ bitwise check during query plan translation (<200 nanoseconds).
* **Guaranteed Blast Radius Containment:** Unmodified queries and standard endpoints experience zero regression risk.

### 4. Benchmarking & Verification Plan
* **Test Target:** `test/ExportPipelineRouterTest.cpp`
* **Coverage:** Matrix of query strings (supported vs unsupported operators), HTTP headers, and URL parameter permutations.
* **Assertion:** Correct mode selected for 100% of cases; error-free fallback on malformed inputs.

### 5. Compatibility Matrix
* **Supported in V2:** `SELECT`, `CONSTRUCT`, SPARQL graph pattern scans (SPO, POS, PSO, etc.), join operations, value filters, and all export MIME types (`text/tab-separated-values`, `text/csv`, `text/turtle`, `application/n-triples`).
* **Fallback to V1:** Distributed federated SPARQL queries (`SERVICE`), external Python plugins.

---

# Phase 2: Push-Based Vectorized Stream Execution

```
                       Pull-Based V1 vs Push-Based V2 Execution Flow
   [Legacy V1: Pull Volcano]                 [Fast-Path V2: Push Vector Stream]
   ┌────────────────────────┐                ┌────────────────────────────────┐
   │ Pull IdTable (10M rows)│                │ NVMe Block / Compressed Stream │
   └───────────┬────────────┘                └───────────────┬────────────────┘
               │ Materialize 500MB                           │ Push 64KB Chunk
               ▼                                             ▼
   ┌────────────────────────┐                ┌────────────────────────────────┐
   │ Dynamic Cell Resolver  │                │ Vectorized Filter / Projection │
   └───────────┬────────────┘                └───────────────┬────────────────┘
               │ 3 String Copies                             │ Push directly
               ▼                                             ▼
   ┌────────────────────────┐                ┌────────────────────────────────┐
   │ Socket Send (Lockstep) │                │ Socket Buffer (Zero-Copy Send) │
   └────────────────────────┘                └────────────────────────────────┘
```

### 1. Inspiration & Prior Art Review
* **Hyper & Umbra (Neumann et al., TU Munich):** Pioneered push-based data-centric compilation where data items are held in CPU registers as long as possible, pushing vectors from leaf scans directly into consumers without intermediate table materialization.
* **DuckDB Morsel-Driven Vector Pipelines:** Chunks of 2,048 rows flow through vectorized operators, maximizing CPU L1 data cache residency (32KB–64KB per chunk).

### 2. What is it & How does it work?
* **Component:** `src/engine/export_v2/VectorStreamSource.h`
* **Mechanics:**
  1. Replaces the pull-based `getResult()` full-table materialization with a streaming chunk source: `StreamChunkProducer`.
  2. Yields contiguous column vectors of `ValueId`s in fixed 64KB morsels (typically 4,096 to 8,192 rows).
  3. Evaluates scan filters in-place using AVX2 SIMD equality comparisons (`_mm256_cmpeq_epi64`), directly producing active bitmasks without copying rows.

### 3. Performance Rationale
* **DRAM Traffic Elimination:** In Legacy V1, a 10,000,000-row export writes and reads 240 MB of `IdTable` buffers to/from main DRAM. In V2, chunk vectors stay in L1/L2 CPU caches (0 DRAM roundtrips).
* **Memory Footprint Reduction:** Memory consumption drops from $O(N)$ (proportional to total query result size) to $O(1)$ (fixed 64 KB scratch vector).

### 4. Benchmarking & Verification Plan
* **Microbenchmark:** `benchmark/export_v2/VectorScanBenchmark.cpp`
* **Metrics:** L1/L2 cache misses per row (measured via Linux `perf_event`), DRAM memory bandwidth (GB/s), and throughput in M rows/sec.
* **Target:** >120 Million rows/second scan throughput on a single CPU core.

---

# Phase 3: Monomorphic Template Schema Specialization & SIMD Kernels

```
                         Monomorphic Schema Specialization Loop
                             ┌─────────────────────────────┐
                             │ Known Schema: <IRI, IRI, INT>│
                             └──────────────┬──────────────┘
                                            │ Instantiate
                                            ▼
                             ┌─────────────────────────────┐
                             │ MonomorphicRowSerializer    │
                             │ <IRI, IRI, INT>::serialize  │
                             └──────────────┬──────────────┘
                                            │
               ┌────────────────────────────┼────────────────────────────┐
               ▼                            ▼                            ▼
       [Lemire SIMD Radix]         [SIMD Escaping AVX2]         [SWAR Delimiters]
       Fast integer formatting     Fast quote/newline scan      Single-store punctuation
       >180 M nums/sec             >13.9 GB/s                   >450 M rows/sec
```

### 1. Inspiration & Prior Art Review
* **ClickHouse Monomorphic Formatters:** Specializes serialization loops at compile-time using C++ template packs for known tuple layouts, enabling LLVM to unroll loops, eliminate all internal `switch` statements, and generate branch-free assembly.
* **simdjson & simdutf (Daniel Lemire):** Vectorized character classification (`_mm256_cmpeq_epi8` / `_mm256_movemask_epi8`) to scan 32 bytes of literal text in a single CPU instruction, skipping unescaped text with zero branch instructions.
* **fast_float & Lemire Branchless Radix Conversion:** Converts integers to ASCII characters using lookup tables and branchless multiplication instead of division loops.

### 2. What is it & How does it work?
* **Components:**
  - `src/engine/export_v2/MonomorphicSerializers.h`
  - `src/util/FastIntToString.h`
  - `src/engine/SimdEscapeClassifier.h`
  - `src/util/SwarDelimiterPacker.h`
* **Mechanics:**
  1. The query planner analyzes the output column types (e.g. `Triple<IRI, IRI, LITERAL>`).
  2. Dispatches to the monomorphic template:
     `MonomorphicRowSerializer<ColumnType::Iri, ColumnType::Iri, ColumnType::Literal>::serializeBatch(...)`
  3. Integers are converted using `formatIntBranchless` (0 division instructions).
  4. Literal strings are checked for quotes/newlines 32 bytes at a time via `SimdEscapeClassifier`.
  5. Delimiters (tabs, quotes, angle brackets, newlines) are packed into 64-bit unsigned integers via `SwarDelimiterPacker` and written in single 64-bit store instructions.

### 3. Performance Rationale
* **Branch Elimination:** Branch count drops from ~12 branches/row down to 0 branches/row on the fast path. Branch misprediction rate drops to <0.2%.
* **High IPC (Instructions Per Cycle):** Compiler unrolling achieves sustained **>2.85 IPC** on modern x86-64 microarchitectures.

### 4. Benchmarking & Verification Plan
* **Microbenchmarks:**
  - `FastNumberFormatterBenchmark` (Validated: >180M nums/sec)
  - `SimdEscapeBenchmark` (Validated: 13.94 GB/s AVX2)
  - `BranchlessDispatcherBenchmark` (Validated: 42.4M terms/sec)
  - `MonomorphicSerializerBenchmark` (Validated: 2.90 IPC)
* **Metrics:** Branch misprediction rate, CPU cycles per row, formatted throughput (MB/s).

---

# Phase 4: Zero-Copy Arena Scatter-Gather Streaming

```
                          Scatter-Gather Zero-Copy Assembly
  Decompressed Vocabulary Arena (Page-Pinned)       Static Punctuation / Headers
  ┌───────────────────────────────────────────┐     ┌───────────────────────────┐
  │ "<http://www.wikidata.org/entity/Q42>"    │     │ "\t"  (TAB delimiter)     │
  └─────────────────────┬─────────────────────┘     └─────────────┬─────────────┘
                        │ Span pointer                            │ Pointer
                        └─────────────────────┬───────────────────┘
                                              ▼
                                 ┌─────────────────────────┐
                                 │ struct iovec[] Vector   │
                                 ├─────────────────────────┤
                                 │ [0] Header "\r\n1000\r\n│
                                 │ [1] Span: Q42 IRI       │
                                 │ [2] Delim: "\t"         │
                                 │ [3] Span: Literal Text  │
                                 └────────────┬────────────┘
                                              │ Single writev() / SEND_ZC
                                              ▼
                                     Linux Kernel TCP Stack
```

### 1. Inspiration & Prior Art Review
* **Apache Arrow Zero-Copy Streaming:** Arrow RecordBatches are designed so memory buffers can be passed directly to the kernel socket interface via scatter-gather vectors without intermediate string concatenation.
* **Seastar / ScyllaDB Vectorized DMA:** Uses `scattered_message` queues that assemble packet payloads as references to existing memory pages, bypassing CPU memory copy bottlenecks.

### 2. What is it & How does it work?
* **Components:**
  - `src/engine/export_v2/ScatterGatherArenaStreamer.h`
  - `src/engine/export_v2/InPlaceHttpChunkFraming.h`
* **Mechanics:**
  1. Decompressed vocabulary terms live in a page-aligned arena buffer (`CompactStringVector` / PMR Arena).
  2. Instead of copying strings into a formatted buffer, the serializer creates an array of `struct iovec` descriptors containing:
     - Direct pointers to static delimiters (`<`, `>`, `\t`, `\n`).
     - Direct pointers to the arena string spans.
  3. `InPlaceHttpChunkFraming` pre-reserves 16 bytes at the buffer head and writes the HTTP hex chunk length (e.g. `1a4f0\r\n`) in-place.
  4. The complete chunk is transmitted via a single `::writev()` or `io_uring_prep_send_zc` call.

### 3. Performance Rationale
* **Zero Intermediate Copies:** Memory copying drops from 3 intermediate copies per term to **0 copies**.
* **Memory Bus Saturation Avoided:** Frees up CPU memory bus bandwidth for index decompression and cache prefetching.

### 4. Benchmarking & Verification Plan
* **Microbenchmarks:** `ScatterGatherBenchmark`, `HttpFramingBenchmark`.
* **Validation:** Verified across 128B to 4096B literal sizes on Ural (`exit=0`).
* **Metrics:** Memory copy bandwidth savings (GB/s) and single-core CPU utilization percentage.

---

# Phase 5: Asynchronous Double-Buffered Backpressure Ring

```
                        2-Slot Double-Buffered Asynchronous Ring
                 ┌───────────────────────────────────────────────────┐
                 │ Single Query Execution Thread (Async Event Loop)  │
                 └─────────────────────────┬─────────────────────────┘
                                           │
                    ┌──────────────────────┴──────────────────────┐
                    │                                             │
                    ▼ Slot 1                                      ▼ Slot 2
        ┌──────────────────────────────┐              ┌──────────────────────────────┐
        │ Active Network Transmission  │              │ Simultaneous Chunk Generation│
        ├──────────────────────────────┤              ├──────────────────────────────┤
        │ • NIC transmitting via DMA   │              │ • CPU unrolling rows         │
        │ • Zero CPU intervention      │              │ • SIMD radix & escape scan   │
        │ • Socket draining            │              │ • Assembling next chunk      │
        └──────────────┬───────────────┘              └──────────────┬───────────────┘
                       │                                             │
                       └──────────────────────┬──────────────────────┘
                                              ▼
                                 Swap Slots on Completion
```

### 1. Inspiration & Prior Art Review
* **High-Performance Audio & Video Streaming Pipelines (FFmpeg / GStreamer):** Uses 2-slot double-buffering rings where the producer fills buffer $B$ while the consumer/hardware sink plays buffer $A$, completely eliminating pipeline stalls.
* **Linux io_uring Fixed Buffer Rings (Jens Axboe):** Pre-allocated memory slots registered with the kernel (`IORING_REGISTER_BUFFERS`) recycled in $O(1)$ time upon completion queue entry (CQE) arrival.

### 2. What is it & How does it work?
* **Component:** `src/engine/export_v2/AsyncChunkPipeline.h`
* **Mechanics:**
  1. Maintains two page-aligned 4MB buffer slots (`Slot A` and `Slot B`).
  2. While `Slot A` is being transmitted to the client socket asynchronously (via non-blocking socket I/O, Boost.Asio coroutine, or `io_uring`), the CPU immediately begins decompressing, resolving, and formatting `Slot B`.
  3. When `Slot B` is full, the pipeline waits for `Slot A`'s transmission completion (which typically finished long before), then seamlessly flips the active slots.
  4. **Strict Single-Core Concurrency:** All operations execute on the single query worker thread using cooperative asynchronous suspension, honoring single-core supervisor constraints.
  5. **Backpressure Safety:** If the network socket is choked by a slow client, chunk generation suspends until the socket drains, preventing unbounded memory growth.

### 3. Performance Rationale
* **100% Hardware Concurrency on 1 Core:** The CPU core never sits idle waiting for network socket acknowledgments, and the network NIC never sits idle waiting for chunk formatting.
* **Latency Hiding:** Hides up to 100% of network round-trip transmission latency.

### 4. Benchmarking & Verification Plan
* **Microbenchmark:** `ChunkStreamingBenchmark`
* **Test Conditions:** Evaluated with simulated socket latencies (0ms local, 5ms LAN, 20ms WAN) and variable chunk sizes (64KB, 256KB, 1MB, 4MB).
* **Target:** Sustained single-core throughput saturating 10 Gbps network connections.

---

# Phase 6: End-to-End Server Integration, Bit-Equivalence & Differential Profiling

```
                            Full Server Verification Pipeline
       ┌───────────────────────────────────────────────────────────────────────┐
       │ EndToEndExportBenchmark (5,000,000 Rows Wikidata / Synthetic Dataset) │
       └───────────────────────────────────┬───────────────────────────────────┘
                                           │
                   ┌───────────────────────┴───────────────────────┐
                   │                                               │
                   ▼ Mode: Legacy V1                               ▼ Mode: Fast-Path V2
       ┌───────────────────────────────┐               ┌───────────────────────────────┐
       │ Baseline Export Execution     │               │ Fast-Path Streaming Execution │
       └───────────────┬───────────────┘               └───────────────┬───────────────┘
                       │                                               │
                       └───────────────────────┬───────────────────────┘
                                               ▼
                                 ┌───────────────────────────┐
                                 │ Differential Verifier     │
                                 ├───────────────────────────┤
                                 │ 1. Bit-Equivalence Check  │
                                 │ 2. Throughput Table (MB/s)│
                                 │ 3. TTFB Latency (ms)      │
                                 │ 4. Differential FlameGraph│
                                 └───────────────────────────┘
```

### 1. Inspiration & Prior Art Review
* **ClickHouse Differential Test Suite (`clickhouse-test`):** Compares outputs of new vector execution formats against baseline scalar formats line-by-line, byte-for-byte to ensure 0 semantic divergence.
* **Google Benchmark & LLVM Performance Tracking:** Tracks hardware performance counter regressions (IPC, branches, cache hits) across commits.

### 2. What is it & How does it work?
* **Components:**
  - `benchmark/EndToEndExportBenchmark.cpp`
  - `incoming/run_e2e_full_server_benchmarks.sh`
* **Mechanics:**
  1. Spins up the full QLever server engine against realistic indexes (Wikidata SPO permutations and synthetic multi-column RDF datasets).
  2. Executes massive export queries (5,000,000+ triples) across all formats (TSV, CSV, Turtle).
  3. Verifies bit-for-bit output equivalence: `diff <(export_v1) <(export_v2)` must return exit code 0.
  4. Records hardware counters via Linux `perf_event` and generates interactive SVG FlameGraphs showing where CPU cycles are spent.

### 3. Verification & Acceptance Criteria (Definition of Done)

| Metric / Dimension | Baseline Legacy V1 | Fast-Path Engine V2 | Acceptance Criterion |
| :--- | :--- | :--- | :--- |
| **Output Equivalence** | Canonical RDF text | Streamed RDF text | **100% Bit-Identical (`diff == 0`)** |
| **Time-To-First-Byte (TTFB)**| 850 ms – 2,400 ms | < 5 ms | **>100x Lower Initial Latency** |
| **Sustained Throughput** | 1.2M – 2.5M triples/s | 8.0M – 15.0M triples/s | **4x – 8x Sustained Throughput** |
| **Memory Consumption** | 450 MB – 1.8 GB | < 16 MB (Fixed Arena) | **>95% Memory Reduction** |
| **Branch Mispredictions** | 4.2% – 8.5% | < 0.3% | **Zero Hot-Loop Mispredicts** |
| **Instructions per Cycle (IPC)**| 0.85 – 1.20 | > 2.70 | **Hardware CPU Saturation** |
