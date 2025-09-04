import re
import sys

# Global regex patterns
ERROR_PATTERN = re.compile(r'^(.*?):(\d+):(\d+):\s+error:\s+(.*)$')
# Matches common C++20 keywords (case-insensitive)
LANG_SPECIFIC_REGEX = re.compile(
    r'\b(concept|requires|co_await|co_yield|co_return|consteval|constinit)\b',
    re.IGNORECASE
)
# Matches the phrase "is not a member of std" (allowing for variations in quotes)
STL_FEATURE_REGEX = re.compile(
    r"is not a member of\s+[‘']?std[’']?",
    re.IGNORECASE
)
# Captures the STL feature name that is reported missing (expects the feature to be in quotes)
STL_FEATURE_CAPTURE_REGEX = re.compile(
    r"[‘'](\w+)[’']\s+is not a member of\s+[‘']?std[’']?",
    re.IGNORECASE
)

def parse_gcc_log(log_path):
    errors = []
    with open(log_path, 'r') as f:
        for line in f:
            line = line.strip()
            match = ERROR_PATTERN.match(line)
            if match:
                filename, line_no, col, message = match.groups()
                # Check for language-specific keywords.
                is_lang_specific = bool(LANG_SPECIFIC_REGEX.search(message))
                # Check if the error message indicates a missing member of std.
                is_stl_specific = bool(STL_FEATURE_REGEX.search(message))
                errors.append({
                    'filename': filename,
                    'line': int(line_no),
                    'column': int(col),
                    'message': message,
                    'language_specific': is_lang_specific,
                    'stl_specific': is_stl_specific
                })
    return errors

def generate_report(errors):
    # Categorize errors.
    lang_specific_errors = [e for e in errors if e['language_specific']]
    stl_specific_errors = [e for e in errors if e['stl_specific']]
    general_errors = [e for e in errors if not e['language_specific'] and not e['stl_specific']]
    
    # Build sets of missing features from language-specific errors and STL-specific errors.
    missing_lang_features = set()
    for e in lang_specific_errors:
        # Extract all language-specific keywords mentioned in the error message.
        features = LANG_SPECIFIC_REGEX.findall(e['message'])
        for f in features:
            missing_lang_features.add(f.lower())
    
    missing_stl_features = set()
    for e in stl_specific_errors:
        # Capture the STL feature name from the error message.
        match = STL_FEATURE_CAPTURE_REGEX.search(e['message'])
        if match:
            missing_stl_features.add(match.group(1).lower())
    
    report_lines = []
    report_lines.append("GCC Error Log Report")
    report_lines.append("====================")
    report_lines.append(f"Total Errors: {len(errors)}")
    report_lines.append(f"Language-Specific Errors: {len(lang_specific_errors)}")
    report_lines.append(f"STL-Specific Errors: {len(stl_specific_errors)}")
    report_lines.append(f"Other Errors: {len(general_errors)}")
    report_lines.append("")
    
    # Short summary of missing features.
    report_lines.append("Missing Features Summary:")
    if missing_lang_features:
        report_lines.append("  Missing Language Features: " + ", ".join(sorted(missing_lang_features)))
    else:
        report_lines.append("  Missing Language Features: None")
    if missing_stl_features:
        report_lines.append("  Missing STL Features: " + ", ".join(sorted(missing_stl_features)))
    else:
        report_lines.append("  Missing STL Features: None")
    report_lines.append("")
    
    if lang_specific_errors:
        report_lines.append("Language-Specific Errors Details:")
        for err in lang_specific_errors:
            report_lines.append(f"{err['filename']}:{err['line']}:{err['column']}: {err['message']}")
        report_lines.append("")
    
    if stl_specific_errors:
        report_lines.append("STL-Specific Errors Details:")
        for err in stl_specific_errors:
            report_lines.append(f"{err['filename']}:{err['line']}:{err['column']}: {err['message']}")
        report_lines.append("")
    
    if general_errors:
        report_lines.append("General Errors Details:")
        for err in general_errors:
            report_lines.append(f"{err['filename']}:{err['line']}:{err['column']}: {err['message']}")
    
    return "\n".join(report_lines)

def main():
    if len(sys.argv) < 2:
        print("Usage: python gcc8_logs_analyzer.py <gcc_output_log_file>")
        sys.exit(1)
    
    log_file = sys.argv[1]
    errors = parse_gcc_log(log_file)
    report = generate_report(errors)
    print(report)

if __name__ == "__main__":
    main()
