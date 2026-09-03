# RFC: QLever Fast-Path Streaming Export Engine (V2)

**Author:** Marvin Stoetzel <stoetzem@email.uni-freiburg.de>  
**Status:** Approved for Implementation  
**Created:** 2026-09-03  
**Target Repository:** `marvin7122/qlever`  

---

## 1. Context and Problem Statement

QLever's standard query execution engine operates as a pull-based Volcano iterator producing intermediate `IdTable`s. While optimal for complex joins, aggregations, and subqueries, bulk data exports (`SELECT *`, `CONSTRUCT`, TSV, CSV, Turtle) suffer from:
1. Materializing gigabytes of intermediate `IdTable`s in memory before streaming.
2. Dynamic polymorphic per-cell type checks (`id.getDatatype()`) and branch mispredictions.
3. Triple intermediate string copies per term between decompression, formatting, and HTTP framing.
4. Lockstep execution where CPU computation and TCP socket I/O block each other sequentially on a single thread.

---

## 2. Architecture Overview: Dual-Mode Execution

To maintain strict backward compatibility with 100% of existing queries and tests, the engine introduces a **side-by-side dual-mode architecture**:

```
                              Request Ingress & Selection
                             ┌───────────────────────────┐
                             │    HTTP SPARQL Request    │
                             └─────────────┬─────────────┘
                                           │
                           ┌───────────────┴───────────────┐
                           │ Fast-Path Streaming Eligible? │
                           └───────────────┬───────────────┘
                                           │
                    ┌──────────────────────┴──────────────────────┐
                    │ YES                                         │ NO / Unsupported
                    ▼                                             ▼
     ┌──────────────────────────────┐              ┌──────────────────────────────┐
     │ ExportExecutionEngineV2      │              │ Legacy Volcano Pipeline      │
     ├──────────────────────────────┤              ├──────────────────────────────┤
     │ • Push-based vector stream   │              │ • Pull-based IdTable         │
     │ • Monomorphic schema kernel  │              │ • Polymorphic cell dispatch  │
     │ • Double-buffered ring       │              │ • Lockstep HTTP chunk write  │
     │ • Zero-copy arena streaming  │              │ • Standard send()            │
     └──────────────┬───────────────┘              └──────────────┬───────────────┘
                    │                                             │
                    └──────────────────────┬──────────────────────┘
                                           ▼
                                Client TCP Socket Stream
```

---

## 3. Activation & Routing Hierarchy

1. **Query-Time Ingress (Primary):**
   - URL Query Parameter: `GET /sparql?query=...&fast-export=1` or `&export-engine=v2`
   - HTTP Header: `X-QLever-Export-Engine: v2`
2. **Server Configuration Default:**
   - `--export-engine-default [legacy | v2]` (Default: `legacy`)
3. **Automatic Safe Fallback:**
   - If an export query contains unsupported complex operators (e.g. distributed aggregation), the router automatically delegates to Legacy V1 with zero failure.

---

## 4. Feature Decomposition & Branch Breakdown

1. **Phase 1: Ingress Routing & Feature Gate** (`src/engine/ExportPipelineRouter.h`)
2. **Phase 2: Push-Based Vector Stream Execution** (`src/engine/export_v2/VectorStreamSource.h`)
3. **Phase 3: Monomorphic Template Schema Specialization** (`src/engine/export_v2/Monomorphic.h`)
4. **Phase 4: Zero-Copy Arena Scatter-Gather** (`src/engine/export_v2/ScatterGatherStreamer.h`)
5. **Phase 5: Double-Buffered Asynchronous Backpressure Ring** (`src/engine/export_v2/Pipeline.h`)
6. **Phase 6: End-to-End Server Integration & Differential Benchmarks** (`benchmark/EndToEndExportBenchmark.cpp`)

---

## 5. Architectural Invariants (Universal Laws from ~/ARCHITECTURE.md)

1. **Information Hiding:** Internal SIMD registers, chunk rings, and `iovec` arrays are completely encapsulated within `ExportExecutionEngineV2`.
2. **Zero Accounting Leakage:** Ring slot indices, partial buffer pointers, and backpressure state never leak outside the streamer.
3. **Defining Errors Out of Existence:** Unsupported query shapes route cleanly to V1 at planning time.
4. **Single-Core Discipline:** Interleaved compute and I/O runs asynchronously on the single query worker thread using non-blocking I/O.
