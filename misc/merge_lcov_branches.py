#!/usr/bin/env python3
"""Merge LCOV branch data across template instantiations.

NOTE: This script was written by Claude Opus 4.6. It has thoroughly been tested
and behaves as expected.

When llvm-cov exports coverage with --format=lcov, template instantiations
produce separate "blocks" of branch data (BRDA entries with different block
IDs) for the same source line.  Codecov treats each block's branches
independently, so a branch will only get full coverage, if ALL instantiations
take the true branch as well as the false branch.
This script merges branch data across blocks by positional index: for each
source line, all blocks' branches are aligned by their position within the
block, and the maximum hit count is taken.  The result is a single block
(block 0) per source line with coverage that reflects the union of all
template instantiations.

It also merges duplicate SF records (if any) and recomputes summary lines.

Usage:
    python3 merge_lcov_branches.py input.lcov -o output.lcov
    python3 merge_lcov_branches.py < input.lcov > output.lcov
"""

import argparse
import sys
from collections import defaultdict


def parse_records(lines):
    """Yield (source_file, raw_lines) for each SF...end_of_record block."""
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

    Returns (da, merged_branches, fn_start, fnda).
    """
    # DA:  line,count  -> max count per line
    da = defaultdict(int)
    # BRDA: collect per (line, block) -> list of (branch, count) in order
    # Then merge across blocks by positional index
    brda_raw = defaultdict(lambda: defaultdict(list))
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
                src_line = parts[0]
                block = parts[1]
                branch = int(parts[2])
                count_str = parts[3]
                count = 0 if count_str == "-" else int(count_str)
                brda_raw[src_line][block].append((branch, count))
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

    # Merge branches across template instantiations by positional index.
    #
    # llvm-cov represents instantiations as separate "blocks" at the same
    # source line, each with the same number of branches.  We align branches
    # by their position within each block and take the max hit count.
    #
    # When multiple SF records for the same file are merged, a single block
    # may accumulate entries from several records (duplicate branch numbers).
    # We detect this by checking for repeated branch numbers and split into
    # sub-groups (cycles) of size = number of unique branch numbers.
    merged_branches = {}  # src_line -> list of max counts (one per position)
    for src_line in sorted(brda_raw, key=int):
        blocks = brda_raw[src_line]
        position_counts = defaultdict(int)
        for block_id, entries in blocks.items():
            unique_branches = len(set(b for b, c in entries))
            cycle_len = unique_branches
            # Split into instantiation sub-groups of cycle_len entries each
            for i in range(0, len(entries), cycle_len):
                group = entries[i:i + cycle_len]
                group.sort(key=lambda x: x[0])
                for pos, (branch, count) in enumerate(group):
                    position_counts[pos] = max(position_counts[pos], count)
        merged_branches[src_line] = [
            position_counts[i] for i in range(len(position_counts))
        ]

    return da, merged_branches, fn_start, fnda


def format_record(sf, da, merged_branches, fn_start, fnda):
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

    # Branches — one block (block 0) per source line
    brf = 0
    brh = 0
    for src_line in sorted(merged_branches, key=int):
        counts = merged_branches[src_line]
        for branch, count in enumerate(counts):
            count_str = "-" if count == 0 else str(count)
            out.append(f"BRDA:{src_line},0,{branch},{count_str}")
            brf += 1
            if count > 0:
                brh += 1
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
        description="Merge LCOV branch data across template instantiations."
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
        da, merged_branches, fn_start, fnda = merge_records(file_records[sf])
        output_lines.extend(
            format_record(sf, da, merged_branches, fn_start, fnda)
        )

    output_text = "\n".join(output_lines) + "\n"

    if args.output == "-":
        sys.stdout.write(output_text)
    else:
        with open(args.output, "w") as f:
            f.write(output_text)


if __name__ == "__main__":
    main()
