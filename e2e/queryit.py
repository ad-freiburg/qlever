#!/usr/bin/env python3
"""
QLever Query Tool for End2End Testing
"""

import sys
import urllib.parse
import urllib.request
from typing import Dict, Any, List
import json
import yaml

def eprint(*args, **kwargs):
    """
    Like print but to stderr
    """
    print(*args, file=sys.stderr, **kwargs)

def exec_query(endpoint_url: str, sparql: str,
               max_send: int = 4096) -> Dict[str, Any]:
    """
    Execute a single SPARQL query against the given endpoint
    """
    params = urllib.parse.urlencode({'query': sparql, 'send': max_send})
    url_suffix = '/?'+params
    request = urllib.request.Request(endpoint_url+url_suffix)
    conn = urllib.request.urlopen(request)
    if conn.status != 200:
        eprint("Error executing SPARQL Query: ", sparql)
        return None
    return json.load(conn)

def is_result_sane(result: Dict[str, Any]) -> bool:
    """
    Checks a QLever Result object for sanity
    """
    required_fields = ['query', 'status', 'resultsize', 'selected', 'res']
    for field in required_fields:
        if field not in result:
            eprint('QLever Result is missing "%s" field' % field)
            return False
    return True

def test_row(gold_row: List[Any],
             actual_row: List[Any], epsilon=0.1) -> bool:
    """
    Test if gold_row and actual_row match. For floats we allow an epsilon
    difference. If a gold_row cell is None it is ignored.
    Returns True if they match
    """
    for i, gold in enumerate(gold_row):
        if gold is None:
            continue
        actual = actual_row[i]
        matches = False
        if isinstance(gold, int):
            matches = int(actual) == gold
        elif isinstance(gold, float):
            matches = abs(gold - float(actual)) <= epsilon
        else:
            matches = gold == actual

        if not matches:
            return False
    return True

def test_check(check_dict: Dict[str, Any], result: Dict[str, Any]) -> bool:
    """
    Test if the named result check holds. Returns True if it does
    """
    res = result['res']
    for check, value in check_dict.items():
        if check == 'num_rows':
            if len(res) != int(value):
                eprint("num_rows check failed:\n" +
                       "\texpected %r, got %r" %
                       (value, len(res)))
                return False
        elif check == 'num_cols':
            for row in res:
                if len(row) != int(value):
                    eprint("num_cols check failed:\n" +
                           "\texpected %r, got %r, row: %s" %
                           (value, len(row), json.dumps(row)))
                    return False
        elif check == 'selected':
            if value != result['selected']:
                eprint("selected check failed:\n" +
                       "\texpected %r, got %r" %
                       (value, result['selected']))
                return False
        elif check == 'res':
            gold_res = value
            for i, gold_row in enumerate(gold_res):
                actual_row = res[i]
                if not test_row(gold_row, actual_row):
                    eprint("res check failed:\n" +
                           "\tat row %r" % i +
                           "\texpected %r, got %r" %
                           (gold_row, actual_row))
                    return False
        elif check == 'contains_row':
            found = False
            gold_row = value
            for actual_row in res:
                if test_row(gold_row, actual_row):
                    found = True
                    break
            if not found:
                eprint("contains_row check failed:\n" +
                       "\tdid not find %r" % gold_row)
                return False


    return True



def solution_checks(solution: Dict[str, Any],
                    result: Dict[str, Any]) -> bool:
    """
    Tests the checks specified in the solution
    """
    if not 'checks' in solution:
        return True
    passed = True
    checks = solution['checks']
    for check in checks:
        if not test_check(check, result):
            passed = False
    return passed

def print_qlever_result(result: Dict[str, Any]) -> None:
    """
    Prints a QLever Result to stdout
    """
    eprint(json.dumps(result))


def main() -> None:
    """
    Run QLever queries stored in a YAML file against a QLever instance
    """
    if len(sys.argv) != 3:
        eprint("Usage: ", sys.argv[0], "<yaml_in> <qlever_endpoint_url>")
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
                    eprint('QLever Result "status" is not "OK"')
                    error_detected = True
                    print_qlever_result(result)
                    continue

                if not solution_checks(solution, result):
                    eprint('Solution checks failed')
                    error_detected = True
                    continue

    if error_detected:
        sys.exit(2)



if __name__ == '__main__':
    main()
