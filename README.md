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
databases by [a large margin on most queries](https://qlever.dev/evaluation).
QLever implements the full SPARQL 1.1 standard, including federated queries,
named graphs, and updates. On top of its outstanding performance, QLever offers
a variety of unique features: advanced text search capabilities,
context-sensitive autocompletion of SPARQL queries, live query analysis,
extremely efficient spatial queries, and the interactive visualization of very
large numbers of geometric objects on a map.

[Here are demos of QLever](http://qlever.cs.uni-freiburg.de) on a variety of
large datasets, including the complete Wikidata, Wikimedia Commons,
OpenStreetMap, UniProt, PubChem, and DBLP. Those demos also feature QLever's
context-sensitive autocompletion, which makes SPARQL query construction so much
easier. The datasets are updated regularly. Click on "Index Information" for a
short description (with dates) and basic statistics.

If you use QLever in your research work, please cite one of the following publications:
our [CIKM'17 paper](https://ad-publications.informatik.uni-freiburg.de/CIKM_qlever_BB_2017.pdf) (combination of SPARQL and text search, with extensive evaluation),
our [CIKM'22 paper](https://ad-publications.cs.uni-freiburg.de/CIKM_sparql_autocompletion_BKKKS_2022.pdf) (QLever's autocompletion, with extensive evaluation),
our [2023 book chapter](https://ad-publications.cs.uni-freiburg.de/CHAPTER_knowledge_graphs_BKKK_2023.pdf) (survey of knowledge graphs and basics of QLever, with many example queries).

QLever is in active and rapid development. If you find a bug or if you are
missing a feature or if there is anything else you want to tell us, please
[open an issue](https://github.com/ad-freiburg/qlever/issues) or [open a
discussion](https://github.com/ad-freiburg/qlever/discussions).

# Quickstart

Use QLever via the `qlever` command-line interface (CLI),  which can be
installed via `pip install qlever`. It is self-documenting via `qlever --help`
(for an overview of all commands) and `qlever <command> --help` (for details on
any specific command). For more information and example use cases, see
https://github.com/ad-freiburg/qlever-control .

You can control everything `qlever` does via a single configuration file, the
so-called `Qleverfile`. Via `qlever setup-config <config-name>` you can fetch
any of a number of example `Qleverfile`s (in particular, one for each of the
demos mentioned above). To write a `Qleverfile` for your own data, pick one of
these configurations as a starting point and edit the `Qleverfile` as you see
fit. Every option from the `Qleverfile` can also be set (and overridden) via
a command-line option with the same name, see `qlever <command> --help`.


# Using QLever without the `qlever` CLI

This is not recommended but can be useful or necessary in certain (in
particular, non-interactive) environments. QLever's main binaries are called
`IndexBuilderMain` (for loading and indexing data) and `ServerMain` (for
querying the data). Each of these has a `--help` option that describes the
available options.

The easiest way to find out the right command-line is to use the `qlever` CLI,
which for each command prints the exact command-line it is going to execute.
With the option `--show`, it will print the command-line without executing it,
e.g., `qlever start --show`.

# Using QLever as an embedded database

QLever can also be used as an embedded database, that is, without the standard
client-server setup but running it in-process inside your own C++ program.
See https://github.com/ad-freiburg/qlever/pull/2100 for details and a link to a
small example program.

# Wiki and older documentation

For high-level descriptions of how Qlever works, performance evaluations,
and experiences with some concrete datasets, and details about QLever see the
[Qlever Wiki](https://github.com/ad-freiburg/qlever/wiki).

There is quite a bit of additional documentation in the [docs](docs) folder of
this repository. The documents in that folder are not well maintained and may
be outdated. We are currently working on an own `qlever-docs` repository that
will provide extensive documentation and tutorials.
