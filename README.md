# QLever

[![Docker build](https://github.com/ad-freiburg/QLever/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/ad-freiburg/QLever/actions/workflows/docker-publish.yml)
[![Native build](https://github.com/ad-freiburg/qlever/actions/workflows/native-build.yml/badge.svg)](https://github.com/ad-freiburg/qlever/actions/workflows/native-build.yml)
[![Format check](https://github.com/ad-freiburg/qlever/actions/workflows/format-check.yml/badge.svg)](https://github.com/ad-freiburg/qlever/actions/workflows/format-check.yml)
[![Test coverage](https://codecov.io/github/ad-freiburg/qlever/branch/master/graph/badge.svg?token=OHcEh02rW0)](https://codecov.io/github/ad-freiburg/qlever)

QLever (pronounced "Clever") is a graph database implementing the
[RDF](https://www.w3.org/TR/rdf11-concepts/) and
[SPARQL](https://www.w3.org/TR/sparql11-overview/) standards. QLever can
efficiently load and query very large datasets, even with hundreds of billions
of triples, on a single commodity PC or server. QLever outperforms other RDF/SPARQL
databases by [a large margin on most queries](https://qlever.dev/evaluation) in a
[resourceful manner](https://github.com/ad-freiburg/qlever/wiki/QLever-performance-evaluation-and-comparison-to-other-SPARQL-engines).

QLever implements the full SPARQL 1.1 standard, including federated queries,
named graphs, the Graph Store HTTP Protocol, and updates. On top of its
outstanding performance, QLever offers a variety of unique features: advanced
text search capabilities, context-sensitive autocompletion of SPARQL queries,
live query analysis, very efficient spatial queries, and the interactive
visualization of very large numbers of geometric objects on a map.
QLever can also be used as an embedded database, that is, without the standard
client-server setup but running it in-process inside your own C++ program.

[Here are demos of QLever](http://qlever.dev/) on a variety of
large datasets, including the complete Wikidata, Wikimedia Commons,
OpenStreetMap, UniProt, PubChem, and DBLP. Those demos also feature QLever's
context-sensitive autocompletion, which makes SPARQL query construction so much
easier. The datasets are updated regularly. Click on "Index Information" for a
short description (with dates) and basic statistics.

If you use QLever in your research work, please cite one of the following publications:
our [CIKM'17 paper](https://ad-publications.informatik.uni-freiburg.de/CIKM_qlever_BB_2017.pdf) (QLever's beginning, combination of SPARQL and text search),
our [CIKM'22 paper](https://ad-publications.cs.uni-freiburg.de/CIKM_sparql_autocompletion_BKKKS_2022.pdf) (QLever's autocompletion, with an extensive evaluation),
our [2023 book chapter](https://ad-publications.cs.uni-freiburg.de/CHAPTER_knowledge_graphs_BKKK_2023.pdf) (survey of knowledge graphs and basics of QLever, with many example queries),
our [TGDK'24 article](https://drops.dagstuhl.de/entities/document/10.4230/TGDK.2.2.3) (the dblp knowledge graph and SPARQL endpoint),
our [SIGSPATIAL'25
paper](https://ad-publications.cs.uni-freiburg.de/SIGSPATIAL_spatialjoin_BBK_2025.pdf)
(efficient spatial joins, with a performance evaluation against PostgreSQL+PostGIS),
our [ISWC'25 GRASP paper](https://ad-publications.cs.uni-freiburg.de/ISWC_grasp_WB_2025.pdf) (zero-shot question answering on RDF graphs),
and our [ISWC'25 Sparqloscope paper](https://ad-publications.cs.uni-freiburg.de/ISWC_sparqloscope_BKTU_2025.pdf)
(a comprehensive SPARQL benchmark with a performance comparison of QLever and
several other RDF databases).

QLever is open source under the permissive Apache 2.0 license. QLever is in
active and rapid development. If you find a bug or if you are missing a feature
or if there is anything else you want to tell us, please [open an
issue](https://github.com/ad-freiburg/qlever/issues) or [open a
discussion](https://github.com/ad-freiburg/qlever/discussions).

# Quickstart and documentation

To get started with QLever you may use our native packages released for [Debian, Ubuntu](https://docs.qlever.dev/quickstart/#debian-and-ubuntu) and [macOS](https://docs.qlever.dev/quickstart/#macos-apple-silicon). Additionally, a platform-independent version of QLever is available as an [image for Docker and Podman](https://hub.docker.com/r/adfreiburg/qlever), which can be used through our Python-based `qlever` command-line interface (CLI). Please refer to our [Quickstart documentation](https://docs.qlever.dev/quickstart/) for details.

For the official documentation, see [docs.qlever.dev](https://docs.qlever.dev/). Additional
information can also be found in the [QLever Wiki](https://github.com/ad-freiburg/qlever/wiki).
