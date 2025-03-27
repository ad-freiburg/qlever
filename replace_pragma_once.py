import os

def add_include_guard(file_path):
    # Get the relative path of the file to the current directory
    relative_path = os.path.relpath(file_path, start=os.getcwd())

    # Replace directory separators with underscores and convert to uppercase
    relative_path = relative_path.replace(os.sep, '_').upper()

    # Add _H at the end of the file name to create a unique macro
    guard_macro = f"QLEVER_{relative_path.replace(".", "_")}"

    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Check if the file contains #pragma once
    pragma_once_index = -1
    for i, line in enumerate(lines):
        if line.strip() == '#pragma once':
            pragma_once_index = i
            break

    if pragma_once_index != -1:
        insert_empty_line_before = pragma_once_index > 0 and len(lines[pragma_once_index - 1].strip()) > 0
        insert_empty_line_after = pragma_once_index < len(lines) - 1 and len(lines[pragma_once_index + 1]) > 0
        lines[pragma_once_index] = f"#ifndef {guard_macro}\n"
        lines.insert(pragma_once_index + 1, f"#define {guard_macro}\n")
        if insert_empty_line_after:
            lines.insert(pragma_once_index + 2, "\n")
        if insert_empty_line_before:
            lines.insert(pragma_once_index, "\n")
        lines.append(f"\n#endif // {guard_macro}\n")

        # Write the changes back to the file
        with open(file_path, 'w') as file:
            file.writelines(lines)

        print(f"Refactored {file_path} to use include guards.")
    else:
        print(f"{file_path} does not use #pragma once.")

def process_directory(directory):
    print("processing ", directory)
    for root, dirs, files in os.walk(directory):
        # Only process files in src and test directories
        if 'src' in root or 'test' in root:
            for filename in files:
                if filename.endswith(".h"):
                    file_path = os.path.join(root, filename)
                    add_include_guard(file_path)

# Start the process from the current directory
if __name__ == "__main__":
    cwd = os.getcwd()

    # List all directories in the current working directory
    subdirectories = [d for d in os.listdir(cwd) if os.path.isdir(os.path.join(cwd, d))]

    # Filter only 'src' and 'test' directories
    filtered_dirs = [d for d in subdirectories if d in ['src', 'test']]
    for d in filtered_dirs :
        process_directory(d)
