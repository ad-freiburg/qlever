#!/usr/bin/env python3
"""
QLever Query Tool for End2End Testing
"""

import sys
import urllib.request
from typing import Dict, Any
import json
import yaml

def exec_query(endpoint_url: str, sparql: str) -> Dict[str, Any]:
    """
    Execute a single SPARQL query against the given endpoint
    """
    params = urllib.parse.urlencode({'query': sparql})
    url_suffix = '/?'+params
    request = urllib.request.Request(endpoint_url+url_suffix)
    conn = urllib.request.urlopen(request)
    if conn.status != 200:
        print("Error executing SPARQL Query: ", sparql, file=sys.stderr)
        return None
    return json.load(conn)

def is_result_sane(result: Dict[str, Any]) -> bool:
    """
    Checks a QLever Result object for sanity
    """
    required_fields = ['query', 'status', 'resultsize', 'selected', 'res']
    for field in required_fields:
        if field not in result:
            print('QLever Result is missing "%s" field' % field,
                  file=sys.stderr)
            return False
    return True

def print_qlever_result(result: Dict[str, Any]) -> None:
    """
    Prints a QLever Result to stdout
    """
    print(json.dumps(result), file=sys.stderr)


def main() -> None:
    """
    Run QLever queries stored in a YAML file against a QLever instance
    """
    if len(sys.argv) != 3:
        print("Usage: ", sys.argv[0], "<yaml_in> <qlever_endpoint_url>")
        sys.exit(1)

    inpath = sys.argv[1]
    endpoint_url = sys.argv[2]
    error_detected = False
    with open(inpath, 'rb') if inpath != '-' else sys.stdin as infile:
        yaml_tree = yaml.load(infile)
        queries = yaml_tree['queries']
        for query in queries:
            query_name = query['query']
            solutions = query['solutions']
            for solution in solutions:
                solution_type = solution['type']
                solution_sparql = solution['sparql']
                print('Trying: ', query_name, '(%s)' % solution_type)
                print('SPARQL:')
                print(solution_sparql)
                result = exec_query(endpoint_url, solution_sparql)
                if not result:
                    # A print was already done in exec_query()
                    error_detected = True
                    print_qlever_result(result)
                    continue

                if not is_result_sane(result):
                    error_detected = True
                    print_qlever_result(result)
                    continue

                if result['status'] != 'OK':
                    print('QLever Result "status" is not "OK"',
                          file=sys.stderr)
                    error_detected = True
                    print_qlever_result(result)
                    continue

    if error_detected:
        sys.exit(2)



if __name__ == '__main__':
    main()
