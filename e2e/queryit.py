#!/usr/bin/env python3
"""
QLever Query Tool for End2End Testing
"""

import sys
import urllib.parse
import urllib.request
from typing import Dict, Any, List
from enum import Enum
import json
import yaml

class Color:
    """
    Enum-like class for storing ANSI Color Codes
    """
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def eprint(*args, color=Color.FAIL, **kwargs):
    """
    Like print but to stderr
    """
    sys.stderr.write(color)
    print(*args, file=sys.stderr, **kwargs)
    print(Color.ENDC, file=sys.stderr)

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
        # from literals only take the part in quotes stripping
        # the quotes and any "^^xsd:type hints.
        # This allows us to ignore double quoting trouble in checks
        if actual and actual[0] == '"':
            actual = actual[1:actual.rfind('"')]
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

def quotes_inner(quoted: str) -> str:
    """
    For a string containing a quoted part returns the inner part
    """
    left_quote = quoted.find('"')
    right_quote = quoted.rfind('"')
    if right_quote < 0:
        right_quote = len(quoted)
    return quoted[left_quote+1:right_quote]

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
            if len(gold_res) != len(res):
                eprint("res check failed:\n"+
                       "\texpected number of rows: %r" % len(gold_res) +
                       "\tdoes not match actual: %r" % len(value))
                return False
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
        elif check.startswith('order_'):
            try:
                direction, var = value['dir'], value['var']
                col_type = check.split('_')[1]
                col_idx = result['selected'].index(var)
                for row_idx in range(1, len(res)):
                    previous = res[row_idx - 1][col_idx]
                    current = res[row_idx][col_idx]
                    if col_type == 'numeric':
                        previous_value = float(quotes_inner(previous))
                        current_value = float(quotes_inner(current))
                    elif col_type == 'string':
                        previous_value = previous
                        current_value = current
                    if direction.lower() == 'asc' and previous_value > current_value:
                        eprint('order_numeric check failed:\n\tnot ascending')
                        return False
                    if direction.lower() == 'desc' and previous_value < current_value:
                        eprint('order_numeric check failed:\n\tnot descending')
                        return False
            except ValueError as ex:
                eprint('order_numeric check failed:\n\t' + str(ex))
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
                print(Color.HEADER+'Trying: ', query_name,
                      '(%s)' % solution_type + Color.ENDC)
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
                    error_detected = True
                    continue

    if error_detected:
        print(Color.FAIL+'Query tool found errors!'+Color.ENDC)
        sys.exit(2)

    print(Color.OKGREEN+'Query tool did not find errors, search harder!'+Color.ENDC)



if __name__ == '__main__':
    main()
