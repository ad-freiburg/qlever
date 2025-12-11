#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK
"""
Simple qlproxy endpoint server for testing the qlproxy MagicServiceQuery.

This server receives SPARQL Results JSON with payload variables, performs
arithmetic operations, and returns results in SPARQL Results JSON format.

Example usage:
    python3 qlproxy_server.py --port 8080

Example SPARQL query that uses this endpoint:
    SELECT ?sum WHERE {
      VALUES (?num1 ?num2) { (1 2) (3 4) }
      SERVICE qlproxy: {
        _:config qlproxy:endpoint <http://localhost:8080/compute> ;
                 qlproxy:payload_first ?num1 ;
                 qlproxy:payload_second ?num2 ;
                 qlproxy:result_result ?sum ;
                 qlproxy:param_op "add" .
      }
    }
"""

import argparse
import json
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

import argcomplete


class QlProxyHandler(BaseHTTPRequestHandler):
    """HTTP request handler for the qlproxy endpoint."""

    # Class variable to control verbose logging (set from main)
    verbose = False

    def do_POST(self):
        """Handle POST requests with SPARQL Results JSON payload."""
        # Parse URL and query parameters
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        # Get the operation from query parameters
        op = query_params.get("op", ["add"])[0]

        # Read and parse the request body
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8")

        try:
            payload = json.loads(body)
        except json.JSONDecodeError as e:
            self.send_error(400, f"Invalid JSON: {e}")
            return

        # Log received payload in verbose mode
        if self.verbose:
            print(f"[qlproxy] Received SPARQL Results JSON:")
            print(json.dumps(payload, indent=2))

        # Validate the payload structure
        if "results" not in payload or "bindings" not in payload["results"]:
            self.send_error(400, "Missing results.bindings in payload")
            return

        # Process each binding
        result_bindings = []
        for binding in payload["results"]["bindings"]:
            try:
                result_binding = self.process_binding(binding, op)
                if result_binding is not None:
                    result_bindings.append(result_binding)
            except Exception as e:
                print(f"Error processing binding {binding}: {e}", file=sys.stderr)
                continue

        # Build the response
        response = {
            "head": {"vars": ["res"]},
            "results": {"bindings": result_bindings}
        }

        # Log produced response in verbose mode
        if self.verbose:
            print(f"[qlproxy] Produced SPARQL Results JSON:")
            print(json.dumps(response, indent=2))

        # Send the response
        response_body = json.dumps(response).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/sparql-results+json")
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        self.wfile.write(response_body)

    def process_binding(self, binding, op):
        """Process a single binding and return the result binding."""
        # Extract the values from the binding
        first_binding = binding.get("first")
        second_binding = binding.get("second")

        if self.verbose:
            print(f"[qlproxy] Processing binding: first={first_binding}, second={second_binding}")

        first_val = self.extract_number(first_binding)
        second_val = self.extract_number(second_binding)

        if self.verbose:
            print(f"[qlproxy] Extracted values: first_val={first_val}, second_val={second_val}")

        if first_val is None or second_val is None:
            if self.verbose:
                print(f"[qlproxy] Skipping binding due to None value")
            return None

        # Perform the operation
        if op == "add":
            result = first_val + second_val
        elif op == "subtract" or op == "sub":
            result = first_val - second_val
        elif op == "multiply" or op == "mul":
            result = first_val * second_val
        elif op == "divide" or op == "div":
            if second_val == 0:
                return None
            result = first_val / second_val
        elif op == "mod":
            if second_val == 0:
                return None
            result = first_val % second_val
        elif op == "power" or op == "pow":
            result = first_val ** second_val
        else:
            print(f"Unknown operation: {op}", file=sys.stderr)
            return None

        # Return the result as a literal
        if isinstance(result, float) and result.is_integer():
            result = int(result)

        return {
            "res": {
                "type": "literal",
                "value": str(result),
                "datatype": "http://www.w3.org/2001/XMLSchema#decimal"
            }
        }

    def extract_number(self, binding):
        """Extract a number from a SPARQL JSON binding."""
        if binding is None:
            return None

        value = binding.get("value")
        if value is None:
            return None

        try:
            # Try to parse as integer first, then as float
            if "." in str(value):
                return float(value)
            else:
                return int(value)
        except ValueError:
            return None

    def log_message(self, format, *args):
        """Log HTTP requests."""
        print(f"[qlproxy] {self.address_string()} - {format % args}")


def main():
    parser = argparse.ArgumentParser(
        description="Simple qlproxy endpoint server for arithmetic operations"
    )
    parser.add_argument(
        "--port", "-p",
        type=int,
        default=8080,
        help="Port to listen on (default: 8080)"
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
        help="Enable verbose logging of received and produced JSON"
    )
    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    # Set verbose mode on the handler class
    QlProxyHandler.verbose = args.verbose

    server_address = (args.host, args.port)
    httpd = HTTPServer(server_address, QlProxyHandler)

    print(f"Starting qlproxy server on http://{args.host}:{args.port}")
    print("Supported operations: add, subtract/sub, multiply/mul, divide/div, mod, power/pow")
    print("Press Ctrl+C to stop")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        httpd.shutdown()


if __name__ == "__main__":
    main()
