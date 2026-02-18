#!/usr/bin/env python3
"""Merge LCOV records for the same source file.

When llvm-cov exports coverage with --format=lcov, template instantiations
produce separate SF blocks for the same file.  This script merges those
blocks so that branch / line / function coverage is the union across all
instantiations.

Usage:
    python3 merge_lcov_branches.py input.lcov -o output.lcov
    python3 merge_lcov_branches.py < input.lcov > output.lcov
"""

import argparse
import sys
from collections import defaultdict


def parse_records(lines):
    """Yield (source_file, raw_lines) for each SF…end_of_record block."""
    current_sf = None
    current_lines = []
    for line in lines:
        line = line.rstrip("\n\r")
        if line.startswith("SF:"):
            current_sf = line[3:]
            current_lines = [line]
        elif line == "end_of_record":
            if current_sf is not None:
                yield current_sf, current_lines
            current_sf = None
            current_lines = []
        else:
            if current_sf is not None:
                current_lines.append(line)


def merge_records(groups):
    """Merge multiple record line-lists for the same file.

    Returns a list of output lines (without trailing newlines).
    """
    # DA:  line,count  -> max count per line
    da = defaultdict(int)
    # BRDA: line,block,branch,count -> max count per key
    brda = defaultdict(int)
    # FN: start_line,name  -> keep earliest start line per name
    fn_start = {}
    # FNDA: count,name -> max count per name
    fnda = defaultdict(int)

    for record_lines in groups:
        for line in record_lines:
            if line.startswith("DA:"):
                parts = line[3:].split(",", 1)
                lineno = parts[0]
                count = int(parts[1])
                da[lineno] = max(da[lineno], count)
            elif line.startswith("BRDA:"):
                parts = line[5:].split(",", 3)
                key = (parts[0], parts[1], parts[2])
                count_str = parts[3]
                count = 0 if count_str == "-" else int(count_str)
                brda[key] = max(brda[key], count)
            elif line.startswith("FN:"):
                # FN:start_line,function_name  (name may contain commas)
                parts = line[3:].split(",", 1)
                start_line = int(parts[0])
                name = parts[1]
                if name not in fn_start or start_line < fn_start[name]:
                    fn_start[name] = start_line
            elif line.startswith("FNDA:"):
                # FNDA:count,function_name  (name may contain commas)
                parts = line[5:].split(",", 1)
                count = int(parts[0])
                name = parts[1]
                fnda[name] = max(fnda[name], count)
            # Skip summary lines (LF, LH, BRF, BRH, FNF, FNH) — recomputed.

    return da, brda, fn_start, fnda


def format_record(sf, da, brda, fn_start, fnda):
    """Format a single merged record as a list of lines."""
    out = [f"SF:{sf}"]

    # Functions
    for name in sorted(fn_start, key=lambda n: fn_start[n]):
        out.append(f"FN:{fn_start[name]},{name}")
    for name in sorted(fn_start, key=lambda n: fn_start[n]):
        count = fnda.get(name, 0)
        out.append(f"FNDA:{count},{name}")
    fnf = len(fn_start)
    fnh = sum(1 for n in fn_start if fnda.get(n, 0) > 0)
    out.append(f"FNF:{fnf}")
    out.append(f"FNH:{fnh}")

    # Branches
    for key in sorted(brda, key=lambda k: (int(k[0]), int(k[1]), int(k[2]))):
        count = brda[key]
        count_str = "-" if count == 0 else str(count)
        out.append(f"BRDA:{key[0]},{key[1]},{key[2]},{count_str}")
    brf = len(brda)
    brh = sum(1 for c in brda.values() if c > 0)
    out.append(f"BRF:{brf}")
    out.append(f"BRH:{brh}")

    # Lines
    for lineno in sorted(da, key=int):
        out.append(f"DA:{lineno},{da[lineno]}")
    lf = len(da)
    lh = sum(1 for c in da.values() if c > 0)
    out.append(f"LF:{lf}")
    out.append(f"LH:{lh}")

    out.append("end_of_record")
    return out


def main():
    parser = argparse.ArgumentParser(
        description="Merge LCOV records for the same source file."
    )
    parser.add_argument(
        "input", nargs="?", default="-",
        help="Input LCOV file (default: stdin)",
    )
    parser.add_argument(
        "-o", "--output", default="-",
        help="Output LCOV file (default: stdout)",
    )
    args = parser.parse_args()

    # Read input
    if args.input == "-":
        lines = sys.stdin.readlines()
    else:
        with open(args.input) as f:
            lines = f.readlines()

    # Group records by source file
    file_records = defaultdict(list)
    file_order = []
    for sf, record_lines in parse_records(lines):
        if sf not in file_records:
            file_order.append(sf)
        file_records[sf].append(record_lines)

    # Merge and output
    output_lines = []
    for sf in file_order:
        da, brda, fn_start, fnda = merge_records(file_records[sf])
        output_lines.extend(format_record(sf, da, brda, fn_start, fnda))

    output_text = "\n".join(output_lines) + "\n"

    if args.output == "-":
        sys.stdout.write(output_text)
    else:
        with open(args.output, "w") as f:
            f.write(output_text)


if __name__ == "__main__":
    main()
