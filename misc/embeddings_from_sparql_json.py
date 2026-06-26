#!/usr/bin/env python3
"""Compute sentence-transformer embeddings for a SPARQL results JSON and emit
N-Triples for injection into a QLever index.

Input: a SPARQL `application/sparql-results+json` document with (at least) two
selected columns, the first an IRI and the second a string literal, e.g.

    SELECT ?entity ?label WHERE { ?entity rdfs:label ?label } LIMIT 1000

The file is streamed with `ijson`, so arbitrarily large result sets work without
loading everything into memory. For each binding we embed the literal with a
`sentence_transformers` model and write triples following QLever's embedding data
model (see `docs/embedding-redesign.md`):

    <entity>     emb:hasEmbedding  _:embN .
    _:embN       emb:asFp32Vector  "[...]"^^emb:fp32Vector ;
                 emb:type          emb:<model>-fp32-<dim> .

plus, once, the embedding-type metadata:

    emb:<model>-fp32-<dim>  emb:hasMetric     "cosine" ;
                            emb:hasDimension  "<dim>"^^xsd:integer ;
                            emb:hasPrecision  "fp32" .

Example:

    python3 embeddings_from_sparql_json.py labels.json -o embeddings.nt \\
        --model sentence-transformers/all-MiniLM-L6-v2 --metric cosine
"""

import argparse
import re
import sys

import ijson

# Namespace and IRIs, mirroring `src/global/Constants.h`. Recognition of a vector
# is by the `fp32Vector` *datatype*; `hasEmbedding`/`asFp32Vector` are conventions.
EMB = "http://qlever.cs.uni-freiburg.de/embeddings/"
XSD_INTEGER = "http://www.w3.org/2001/XMLSchema#integer"
FP32_DATATYPE = EMB + "fp32Vector"

HAS_EMBEDDING = EMB + "hasEmbedding"
AS_FP32_VECTOR = EMB + "asFp32Vector"
TYPE = EMB + "type"
HAS_METRIC = EMB + "hasMetric"
HAS_DIMENSION = EMB + "hasDimension"
HAS_PRECISION = EMB + "hasPrecision"

# Metric and precision are predefined IRIs in the emb: namespace (not string
# literals); the CLI `--metric` choice maps to its IRI here.
PRECISION_IRI = EMB + "fp32"
METRIC_IRI = {
    "cosine": EMB + "cosine",
    "l2": EMB + "l2",
    "squared-l2": EMB + "squaredL2",
    "dot-product": EMB + "dotProduct",
}
SUPPORTED_METRICS = tuple(METRIC_IRI)


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "input",
        help="SPARQL results JSON file (`-` for stdin).",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="-",
        help="Output N-Triples file (default: stdout).",
    )
    parser.add_argument(
        "--model",
        default="sentence-transformers/all-MiniLM-L6-v2",
        help="sentence-transformers model name (default: %(default)s).",
    )
    parser.add_argument(
        "--metric",
        default="cosine",
        choices=SUPPORTED_METRICS,
        help="emb:hasMetric for the embedding type (default: %(default)s).",
    )
    parser.add_argument(
        "--type-name",
        default=None,
        help="Local name of the embedding-type IRI (default: "
        "`<model>-fp32-<dim>`, with the model name sanitized).",
    )
    parser.add_argument(
        "--iri-var",
        default=None,
        help="Name of the IRI column (default: auto-detect the uri-typed "
        "binding, else the first column).",
    )
    parser.add_argument(
        "--text-var",
        default=None,
        help="Name of the literal column (default: auto-detect the "
        "literal-typed binding, else the second column).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=256,
        help="Number of literals to embed per model call (default: %(default)s).",
    )
    parser.add_argument(
        "--normalize",
        action="store_true",
        help="L2-normalize embeddings before writing (e.g. for cosine).",
    )
    return parser.parse_args()


def sanitize(name):
    """Turn a model name into a safe IRI local name (e.g. `org/model` -> `org_model`)."""
    return re.sub(r"[^A-Za-z0-9.\-]+", "_", name).strip("_")


def pick_vars(binding, iri_var, text_var):
    """Resolve which binding keys hold the IRI and the literal.

    Auto-detection uses the SPARQL `type` field (`uri` vs `literal`); explicit
    `--iri-var`/`--text-var` override it.
    """
    keys = list(binding.keys())
    if iri_var is None:
        iri_candidates = [k for k, v in binding.items() if v.get("type") == "uri"]
        iri_var = iri_candidates[0] if iri_candidates else keys[0]
    if text_var is None:
        text_candidates = [
            k
            for k, v in binding.items()
            if v.get("type") in ("literal", "typed-literal") and k != iri_var
        ]
        text_var = text_candidates[0] if text_candidates else keys[1]
    return iri_var, text_var


def format_vector(vec):
    """Format a float vector as the body of an `fp32Vector` literal: `[a, b, ...]`."""
    # `repr` round-trips float32-derived values without spurious precision loss.
    return "[" + ", ".join(repr(float(x)) for x in vec) + "]"


def write_type_metadata(out, type_iri, metric, dimension):
    out.write(f"<{type_iri}> <{HAS_METRIC}> <{METRIC_IRI[metric]}> .\n")
    out.write(
        f'<{type_iri}> <{HAS_DIMENSION}> "{dimension}"^^<{XSD_INTEGER}> .\n'
    )
    out.write(f"<{type_iri}> <{HAS_PRECISION}> <{PRECISION_IRI}> .\n")


def write_embedding(out, node_id, entity_iri, type_iri, vec):
    node = f"_:emb{node_id}"
    literal = f'"{format_vector(vec)}"^^<{FP32_DATATYPE}>'
    out.write(f"<{entity_iri}> <{HAS_EMBEDDING}> {node} .\n")
    out.write(f"{node} <{AS_FP32_VECTOR}> {literal} .\n")
    out.write(f"{node} <{TYPE}> <{type_iri}> .\n")


def main():
    args = parse_args()

    # Import lazily so `--help` works without the (heavy) dependency installed.
    from sentence_transformers import SentenceTransformer

    print(f"Loading model '{args.model}' ...", file=sys.stderr)
    model = SentenceTransformer(args.model)

    in_file = sys.stdin if args.input == "-" else open(args.input, "r")
    out = sys.stdout if args.output == "-" else open(args.output, "w")

    iri_var = args.iri_var
    text_var = args.text_var
    type_iri = None
    node_id = 0
    written = 0

    def flush(batch):
        """Embed a batch of (entity, text) pairs and write their triples."""
        nonlocal node_id, type_iri, written
        if not batch:
            return
        texts = [text for _, text in batch]
        vectors = model.encode(
            texts,
            batch_size=args.batch_size,
            normalize_embeddings=args.normalize,
            show_progress_bar=False,
        )
        if type_iri is None:
            dimension = len(vectors[0])
            local = args.type_name or f"{sanitize(args.model)}-fp32-{dimension}"
            type_iri = EMB + local
            write_type_metadata(out, type_iri, args.metric, dimension)
            print(f"Embedding type: <{type_iri}> (dim {dimension})", file=sys.stderr)
        for (entity_iri, _), vec in zip(batch, vectors):
            write_embedding(out, node_id, entity_iri, type_iri, vec)
            node_id += 1
            written += 1

    try:
        batch = []
        for binding in ijson.items(in_file, "results.bindings.item"):
            if iri_var is None or text_var is None:
                iri_var, text_var = pick_vars(binding, iri_var, text_var)
            if iri_var not in binding or text_var not in binding:
                # A SELECT may leave a variable unbound for some rows; skip those.
                continue
            entity_iri = binding[iri_var]["value"]
            text = binding[text_var]["value"]
            batch.append((entity_iri, text))
            if len(batch) >= args.batch_size:
                flush(batch)
                batch = []
        flush(batch)
    finally:
        if in_file is not sys.stdin:
            in_file.close()
        if out is not sys.stdout:
            out.close()

    print(f"Wrote {written} embeddings.", file=sys.stderr)


if __name__ == "__main__":
    main()
