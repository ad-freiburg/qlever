#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK
"""
qlproxy endpoint that computes q-grams for input words.

This server receives SPARQL Results JSON with a "word" variable and returns
all q-grams for each word (one q-gram per row).

Example usage:
    python3 qlproxy_qgrams.py --port 8081

Example SPARQL query:
    PREFIX qlproxy: <https://qlever.cs.uni-freiburg.de/qlproxy/>
    SELECT ?word ?qgram WHERE {
      VALUES ?word { "hello" "world" "hi" }
      SERVICE qlproxy: {
        _:config qlproxy:endpoint <http://localhost:8081/qgrams> ;
                 qlproxy:input-word ?word ;
                 qlproxy:output-qgram ?qgram ;
                 qlproxy:output-row ?row ;
                 qlproxy:param-q "3" ;
                 qlproxy:param-padding "left" .
      }
    }

URL parameters:
  - q: q-gram size (default: 3)
  - padding: "left", "both", or "none" (default: "left")
    - left: pad only the beginning with $ (e.g., "$$h", "$he", "hel", "ell", "llo")
    - both: pad both ends with $ (e.g., "$$h", "$he", "hel", "ell", "llo", "lo$", "o$$")
    - none: no padding (e.g., "hel", "ell", "llo")

The server expects a "row" variable in each input binding (specifying the
1-based row index) and echoes it back in each output binding for joining with
the input. This enables 1:N mapping (one input word produces multiple q-grams).
"""

import argparse
import json
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

try:
    import argcomplete
    HAS_ARGCOMPLETE = True
except ImportError:
    HAS_ARGCOMPLETE = False


class QGramHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the q-gram endpoint."""

    verbose = False

    def do_POST(self):
        """Handle POST requests with SPARQL Results JSON payload."""
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        # Get the q value (default 3), padding mode, and row variable name
        q = int(query_params.get("q", ["3"])[0])
        row_var = query_params.get("rowvar", ["row"])[0]
        padding_mode = query_params.get("padding", ["left"])[0]
        if padding_mode not in ("left", "both", "none"):
            self.send_error(400, f"Invalid padding mode: {padding_mode}. Use 'left', 'both', or 'none'")
            return

        # Read and parse the request body
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8")

        try:
            payload = json.loads(body)
        except json.JSONDecodeError as e:
            self.send_error(400, f"Invalid JSON: {e}")
            return

        if self.verbose:
            print(f"[qlproxy-qgrams] Received SPARQL Results JSON:")
            print(json.dumps(payload, indent=2))

        if "results" not in payload or "bindings" not in payload["results"]:
            self.send_error(400, "Missing results.bindings in payload")
            return

        # Process each binding and generate q-grams
        result_bindings = []
        for binding in payload["results"]["bindings"]:
            row_binding = binding.get(row_var)
            word_binding = binding.get("word")

            if row_binding is None:
                if self.verbose:
                    print(f"[qlproxy-qgrams] Skipping: missing row variable")
                continue

            if word_binding is None:
                if self.verbose:
                    print(f"[qlproxy-qgrams] Skipping: missing 'word' variable")
                continue

            word = word_binding.get("value", "")
            if self.verbose:
                print(f"[qlproxy-qgrams] Processing word: '{word}' with q={q}, padding={padding_mode}")

            # Generate q-grams with specified padding mode
            qgrams = self.compute_qgrams(word, q, padding_mode)

            if self.verbose:
                print(f"[qlproxy-qgrams] Generated {len(qgrams)} q-grams: {qgrams}")

            # Create one result row per q-gram, all with the same row index
            for qgram in qgrams:
                result_bindings.append({
                    row_var: row_binding,
                    "qgram": {
                        "type": "literal",
                        "value": qgram
                    }
                })

        # Build the response
        response = {
            "head": {"vars": [row_var, "qgram"]},
            "results": {"bindings": result_bindings}
        }

        if self.verbose:
            print(f"[qlproxy-qgrams] Produced SPARQL Results JSON:")
            print(json.dumps(response, indent=2))

        response_body = json.dumps(response).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/sparql-results+json")
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        self.wfile.write(response_body)

    def compute_qgrams(self, word, q, padding_mode="left"):
        """Compute q-grams for a word with specified padding mode.

        Args:
            word: The input word
            q: The q-gram size
            padding_mode: "left", "both", or "none"
                - left: pad only the beginning (prefix q-grams)
                - both: pad both ends (prefix and suffix q-grams)
                - none: no padding (only complete q-grams from the word)
        """
        if q <= 0 or len(word) == 0:
            return []

        pad_char = "$"

        if padding_mode == "none":
            # No padding - only q-grams fully contained in the word
            if len(word) < q:
                return []
            return [word[i:i + q] for i in range(len(word) - q + 1)]
        elif padding_mode == "left":
            # Pad only the beginning
            padded = (pad_char * (q - 1)) + word
            return [padded[i:i + q] for i in range(len(word))]
        else:  # padding_mode == "both"
            # Pad both ends
            padded = (pad_char * (q - 1)) + word + (pad_char * (q - 1))
            return [padded[i:i + q] for i in range(len(padded) - q + 1)]

    def log_message(self, format, *args):
        """Log HTTP requests."""
        print(f"[qlproxy-qgrams] {self.address_string()} - {format % args}")


def main():
    parser = argparse.ArgumentParser(
        description="qlproxy endpoint for computing q-grams"
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=8081,
        help="Port to listen on (default: 8081)"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host to bind to (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )

    if HAS_ARGCOMPLETE:
        argcomplete.autocomplete(parser)

    args = parser.parse_args()
    QGramHandler.verbose = args.verbose

    server_address = (args.host, args.port)
    httpd = HTTPServer(server_address, QGramHandler)

    print(f"Starting qlproxy-qgrams server on http://{args.host}:{args.port}")
    print("URL parameters: q (q-gram size, default 3), padding (left/both/none, default left)")
    print("Press Ctrl+C to stop")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        httpd.shutdown()


if __name__ == "__main__":
    main()
