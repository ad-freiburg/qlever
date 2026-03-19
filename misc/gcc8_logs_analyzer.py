# Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

import re
import sys

ERROR_PATTERN = re.compile(r'^(.*?):(\d+):(\d+):\s+error:\s+(.*)$')
LANG_SPECIFIC_REGEX = re.compile(
    r'[‘’"]\b(co_await|co_return|co_yield|concept|consteval|constinit|requires)\b[’‘"]',
    re.IGNORECASE
)
STL_FEATURE_REGEX = re.compile(
    r"is not a member of\s+[‘']?std[’']?",
    re.IGNORECASE
)
STL_FEATURE_CAPTURE_REGEX = re.compile(
    r"[‘'](\w+)[’']\s+is not a member of\s+[‘']?std[’']?",
    re.IGNORECASE
)

class ReportBuilder:
    """Builds a formatted report with optional grouping."""
    def __init__(self, grouped=False, annotate=False, working_dir=""):
        self.lines = []
        self.grouped = grouped
        self.group_level = 0
        self.annotate = annotate
        self.working_dir = working_dir

    def add_tab(self):
        return "\t" * self.group_level

    def begin_group(self, title, collapse=False):
        if self.grouped and collapse:
            self.lines.append(f"::group::{self.add_tab()}{title}")
        else:
            self.add_line(title)
        self.group_level += 1

    def end_group(self, collapse=False):
        self.group_level -= 1
        if self.grouped and self.group_level >= 0 and collapse:
            self.add_line("::endgroup::")

    def add_line(self, line):
        self.lines.append(f"{self.add_tab()}{line}")

    def add_annotation(self, file="", line=0, col=0, message=""):
        self.add_line(f"{file}:{line}:{col}: {message}")
        if self.annotate:
            # remove working directory from file path if specified
            if self.working_dir and file.startswith(self.working_dir):
                file = file[len(self.working_dir):].lstrip("/\\")
            self.add_line(f"::error file={file},line={line}, col={col}::{message}")


    def get_report(self):
        while self.group_level > 0:
            self.end_group()
        return "\n".join(self.lines)

def parse_gcc_log(log_path):
    """Parses the GCC log and returns a dict of unique errors."""
    errors = []
    with open(log_path, 'r') as f:
        for line in f:
            line = re.sub(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z\s*', '', line).strip()
            match = ERROR_PATTERN.match(line)
            if match:
                filename, line_no, col, message = match.groups()
                errors.append({
                    'filename': filename,
                    'line': int(line_no),
                    'column': int(col),
                    'message': message,
                    'language_specific': bool(LANG_SPECIFIC_REGEX.search(message)),
                    'stl_specific': bool(STL_FEATURE_REGEX.search(message))
                })
    # Remove duplicates
    return { (e['filename'], e['line'], e['column'], e['message'], e['language_specific'], e['stl_specific']): e for e in errors }

def generate_report(errors, grouped=False, annotate=False, details_in_group=False, test_mode=False, working_dir=""):
    """Generates a formatted error report."""
    consistency_check = True if test_mode else None

    lang_specific_errors = [e for e in errors if e['language_specific']]
    stl_specific_errors = [e for e in errors if e['stl_specific']]
    general_errors = [e for e in errors if not e['language_specific'] and not e['stl_specific']]

    missing_lang_features = sorted({
        f.lower()
        for e in lang_specific_errors
        for f in LANG_SPECIFIC_REGEX.findall(e['message'])
    })

    missing_stl_features = sorted({
        match.group(1).lower()
        for e in stl_specific_errors
        if (match := STL_FEATURE_CAPTURE_REGEX.search(e['message']))
    })

    builder = ReportBuilder(grouped=grouped, annotate=annotate, working_dir=working_dir)
    builder.add_line("GCC Error Log Report")
    builder.add_line("====================")
    builder.begin_group(f"Total Errors: {len(errors)}")

    # Language-specific errors
    if lang_specific_errors:
        accumulated_errors = 0 if test_mode else None
        builder.begin_group(f"Language-Specific Errors: {len(lang_specific_errors)}")
        builder.add_line(" - Missing Language Features -")
        for feature in missing_lang_features:
            feature_regex = re.compile(rf"\b{re.escape(feature)}\b", re.IGNORECASE)
            feature_errors = [e for e in lang_specific_errors if feature_regex.search(e['message'])]
            builder.begin_group(f"{feature}: {len(feature_errors)} errors", collapse=True)
            if details_in_group:
                for err in feature_errors:
                    if test_mode:
                        accumulated_errors += 1
                    builder.add_annotation(file=err['filename'], line=err['line'], col=err['column'], message=err['message'])
            else:
                if test_mode:
                    accumulated_errors += len(feature_errors)
            builder.end_group(collapse=True)
        if test_mode:
            builder.add_line(f"Accumulated errors for language-specific features: {accumulated_errors}")
            if accumulated_errors != len(lang_specific_errors):
                builder.add_line(f"ERROR: Accumulated errors {accumulated_errors} does not match total language-specific errors {len(lang_specific_errors)}")
                consistency_check = False
        builder.end_group()

    # STL-specific errors
    if stl_specific_errors:
        accumulated_errors = 0 if test_mode else None
        builder.begin_group(f"STL-Specific Errors: {len(stl_specific_errors)}")
        builder.add_line(" - Missing STL Features - ")
        for feature in missing_stl_features:
            feature_regex = re.compile(rf"\b{re.escape(feature)}\b", re.IGNORECASE)
            feature_errors = [e for e in stl_specific_errors if feature_regex.search(e['message'])]
            builder.begin_group(f"{feature}: {len(feature_errors)} errors", collapse=True)
            if details_in_group:
                for err in feature_errors:
                    if test_mode:
                        accumulated_errors += 1
                    builder.add_annotation(file=err['filename'], line=err['line'], col=err['column'], message=err['message'])
            else:
                if test_mode:
                    accumulated_errors += len(feature_errors)
            builder.end_group(collapse=True)
        if test_mode:
            builder.add_line(f"Accumulated errors for STL features: {accumulated_errors}")
            if accumulated_errors != len(stl_specific_errors):
                builder.add_line(f"ERROR: Accumulated errors {accumulated_errors} does not match total STL-specific errors {len(stl_specific_errors)}")
                consistency_check = False
        builder.end_group()

    # General errors
    if general_errors:
        builder.begin_group(f"General Errors: {len(general_errors)}", collapse=True)
        if details_in_group:
            for err in general_errors:
                builder.add_annotation(file=err['filename'], line=err['line'], col=err['column'], message=err['message'])
        builder.end_group(collapse=True)

    # Details section
    if not details_in_group:
        if lang_specific_errors:
            builder.begin_group("Language-Specific Errors Details", collapse=True)
            for err in lang_specific_errors:
                builder.add_annotation(file=err['filename'], line=err['line'], col=err['column'], message=err['message'])
            builder.end_group()
        if stl_specific_errors:
            builder.begin_group("STL-Specific Errors Details", collapse=True)
            for err in stl_specific_errors:
                builder.add_annotation(file=err['filename'], line=err['line'], col=err['column'], message=err['message'])
            builder.end_group()
        if general_errors:
            builder.begin_group("General Errors Details", collapse=True)
            for err in general_errors:
                builder.add_annotation(file=err['filename'], line=err['line'], col=err['column'], message=err['message'])
            builder.end_group()

    builder.end_group()
    if test_mode:
        if builder.group_level != 0:
            builder.add_line(f"ERROR: Mismatched group levels: {builder.group_level}")
        if consistency_check:
            builder.add_line("::notice::Consistency check passed: Accumulated error counts match totals.")
        else:
            builder.add_line("::error::Consistency check failed: Accumulated error counts do not match totals.")
    return builder.get_report()

def main():
    grouped = False
    annotate = False
    details_in_group = False
    working_dir = "/home/runner/work/qlever/qlever"
    args = [a for a in sys.argv[1:] if a not in ('--on-github', '--test' ,"--annotate")]
    test_mode = '--test' in sys.argv
    if '--on-github' in sys.argv:
        grouped = True
        details_in_group = True
        if '--annotate' in sys.argv: annotate = True
    if len(args) < 1:
        print("Usage: python gcc8_logs_analyzer.py <gcc_output_log_file> [--on-github] [--test]")
        sys.exit(1)
    log_file = args[0]
    errors = list(parse_gcc_log(log_file).values())
    report = generate_report(errors,
                             grouped=grouped,
                             annotate=annotate,
                             working_dir=working_dir,
                             details_in_group=details_in_group,
                             test_mode=test_mode)
    print(report)

if __name__ == "__main__":
    main()
