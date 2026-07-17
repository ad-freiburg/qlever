#!/usr/bin/env python3
"""Convert llvm-cov JSON export to LCOV, merging template instantiations.

NOTE: This script was written by Claude Opus 4.6. It has thoroughly been tested
and behaves as expected.

When llvm-cov exports coverage in JSON format (without --format=lcov), it
provides richer data than LCOV, including precise source coordinates for each
branch and expansion regions for macros.  This script converts that JSON to
LCOV with the following merging:

- Template instantiations: branches are merged by source coordinates (MAX
  counts across instantiations for the same branch condition)
- Macro expansions: branches inside macros are reported at the expansion site
  (the line where the macro was invoked), not the macro definition line
- Functions: grouped by definition line, MAX count across instantiations

Usage:
    python3 json_to_lcov.py coverage.json -o output.lcov
"""

import argparse
import json
import sys
from collections import defaultdict


def extract_line_data(segments):
    """Walk segments to produce {line: count} for DA entries.

    A segment is [line, col, count, hasCount, isRegionEntry, isGapRegion].
    Lines with isRegionEntry=True get the max count of their region entries.
    Lines spanned by a counted region (without their own region entry) get
    the spanning region's count.
    """
    line_data = {}

    # First pass: lines with region entries get the max count.
    for seg in segments:
        line, col, count, hasCount, isRegionEntry, isGap = seg
        if hasCount and isRegionEntry and not isGap:
            if line not in line_data or count > line_data[line]:
                line_data[line] = count

    # Second pass: lines spanned by counted regions.
    for i in range(len(segments) - 1):
        seg = segments[i]
        next_seg = segments[i + 1]
        line, col, count, hasCount, isRegionEntry, isGap = seg
        next_line, next_col = next_seg[0], next_seg[1]

        if not hasCount or isGap:
            continue

        # Lines between this segment's line and the next segment's line.
        start = line + 1
        end = next_line if next_col > 1 else next_line - 1

        for l in range(start, end + 1):
            if l not in line_data:
                line_data[l] = count

    return line_data


def build_expansion_map(func):
    """For one function, map expandedFileID -> source line.

    Scans regions for Kind=1 (ExpansionRegion):
    [line, col, endline, endcol, count, falseCount, expandedFileID, kind=1]
    Returns {expandedFileID: source_line}.
    """
    expansion_map = {}
    for region in func.get("regions", []):
        if len(region) >= 8 and region[7] == 1:  # kind == 1
            expanded_file_id = region[6]
            source_line = region[0]
            expansion_map[expanded_file_id] = source_line
    return expansion_map


def collect_and_merge_branches(functions_for_file):
    """Merge branches across template instantiations.

    Each branch is [line, col, endline, endcol, trueCount, falseCount,
    fileID, ?, kind].  Branches with fileID==0 are direct; branches with
    fileID!=0 are inside macro expansions and get mapped to the expansion
    site via the function's expansion map.

    Returns a sorted list of (report_line, branch_index, true_count,
    false_count) tuples.
    """
    # group_key -> list of (trueCount, falseCount)
    groups = defaultdict(list)

    for func in functions_for_file:
        expansion_map = build_expansion_map(func)
        for branch in func.get("branches", []):
            line, col, endline, endcol = branch[0], branch[1], branch[2], branch[3]
            true_count, false_count = branch[4], branch[5]
            file_id = branch[6]

            if file_id == 0:
                group_key = (line, col, endline, endcol)
            else:
                mapped_line = expansion_map.get(file_id, line)
                group_key = (mapped_line, col, endline, endcol)

            groups[group_key].append((true_count, false_count))

    # Merge: MAX across instantiations, then produce sorted results.
    merged = []
    for key in sorted(groups):
        entries = groups[key]
        max_true = max(tc for tc, fc in entries)
        max_false = max(fc for tc, fc in entries)
        merged.append((key[0], max_true, max_false))

    return merged


def merge_functions(functions_for_file):
    """Group functions by definition line, keep first name, MAX count.

    Returns sorted list of (line, name, count).
    """
    by_line = defaultdict(list)
    for func in functions_for_file:
        regions = func.get("regions", [])
        if not regions:
            continue
        def_line = regions[0][0]
        by_line[def_line].append(func)

    result = []
    for line in sorted(by_line):
        funcs = by_line[line]
        name = funcs[0]["name"]
        count = max(f["count"] for f in funcs)
        result.append((line, name, count))
    return result


def format_lcov_record(filename, line_data, branches, functions):
    """Format a single LCOV record."""
    out = [f"SF:{filename}"]

    # Functions: FN and FNDA
    for line, name, count in functions:
        out.append(f"FN:{line},{name}")
    for line, name, count in functions:
        out.append(f"FNDA:{count},{name}")
    fnf = len(functions)
    fnh = sum(1 for _, _, c in functions if c > 0)
    out.append(f"FNF:{fnf}")
    out.append(f"FNH:{fnh}")

    # Line data: DA
    for lineno in sorted(line_data):
        out.append(f"DA:{lineno},{line_data[lineno]}")
    lf = len(line_data)
    lh = sum(1 for c in line_data.values() if c > 0)
    out.append(f"LF:{lf}")
    out.append(f"LH:{lh}")

    # Branches: BRDA
    brf = 0
    brh = 0
    branch_idx = 0
    for report_line, true_count, false_count in branches:
        true_str = str(true_count) if true_count > 0 else "-"
        false_str = str(false_count) if false_count > 0 else "-"
        out.append(f"BRDA:{report_line},0,{branch_idx},{true_str}")
        brf += 1
        if true_count > 0:
            brh += 1
        branch_idx += 1
        out.append(f"BRDA:{report_line},0,{branch_idx},{false_str}")
        brf += 1
        if false_count > 0:
            brh += 1
        branch_idx += 1
    out.append(f"BRF:{brf}")
    out.append(f"BRH:{brh}")

    out.append("end_of_record")
    return out


def main():
    parser = argparse.ArgumentParser(
        description="Convert llvm-cov JSON export to LCOV with merged "
                    "template instantiations."
    )
    parser.add_argument(
        "input",
        help="Input JSON file from llvm-cov export",
    )
    parser.add_argument(
        "-o", "--output", default="-",
        help="Output LCOV file (default: stdout)",
    )
    args = parser.parse_args()

    with open(args.input) as f:
        data = json.load(f)

    output_lines = []

    for export in data["data"]:
        # Build a map from filename to file data (for segments).
        file_map = {}
        for file_data in export["files"]:
            file_map[file_data["filename"]] = file_data

        # Group functions by their primary file (filenames[0]).
        funcs_by_file = defaultdict(list)
        for func in export["functions"]:
            primary_file = func["filenames"][0]
            funcs_by_file[primary_file].append(func)

        # Process each file.
        for file_data in export["files"]:
            filename = file_data["filename"]
            line_data = extract_line_data(file_data["segments"])
            branches = collect_and_merge_branches(
                funcs_by_file.get(filename, [])
            )
            functions = merge_functions(funcs_by_file.get(filename, []))
            output_lines.extend(
                format_lcov_record(filename, line_data, branches, functions)
            )

    output_text = "\n".join(output_lines) + "\n"

    if args.output == "-":
        sys.stdout.write(output_text)
    else:
        with open(args.output, "w") as f:
            f.write(output_text)


if __name__ == "__main__":
    main()
