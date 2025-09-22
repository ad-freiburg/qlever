#!/usr/bin/env python3
"""
Script to upload files to an index builder server.
Sends file contents as HTTP POST requests with exponential backoff for rate limiting.
"""

import sys
import time
import argparse
import requests
from pathlib import Path
from typing import List


def read_file_list(file_list_path: str) -> List[str]:
    """Read list of filenames from a file."""
    with open(file_list_path, 'r') as f:
        return [line.strip() for line in f if line.strip()]


def upload_file_with_retry(file_path: str, hostname: str, port: int, max_retries: int = 10, graph: str = None) -> bool:
    """
    Upload a single file with exponential backoff on 429 errors.
    Returns True if successful, False if max retries exceeded.
    """
    url = f"http://{hostname}:{port}"
    
    try:
        with open(file_path, 'rb') as f:
            file_content = f.read()
    except FileNotFoundError:
        print(f"Error: File {file_path} not found", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Error reading file {file_path}: {e}", file=sys.stderr)
        return False
    
    retry_count = 0
    backoff_time = 1  # Start with 1 second
    
    while retry_count < max_retries:
        try:
            headers = {}
            if graph:
                headers["graph"] = graph
            response = requests.post(url, data=file_content, headers=headers, timeout=60)
            
            if response.status_code == 200:
                print(f"Successfully uploaded: {file_path}")
                return True
            elif response.status_code == 429:  # Too Many Requests
                retry_count += 1
                print(f"Rate limited. Retrying in {backoff_time}s (attempt {retry_count}/{max_retries})")
                time.sleep(backoff_time)
                backoff_time = min(backoff_time * 2, 60)  # Cap at 60 seconds
            else:
                print(f"Error uploading {file_path}: HTTP {response.status_code}", file=sys.stderr)
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"Network error uploading {file_path}: {e}", file=sys.stderr)
            return False
    
    print(f"Max retries exceeded for {file_path}", file=sys.stderr)
    return False


def send_finish_signal(hostname: str, port: int) -> bool:
    """Send the final request with Finish-Index-Building header."""
    url = f"http://{hostname}:{port}"
    headers = {"Finish-Index-Building": "true"}
    
    try:
        response = requests.post(url, headers=headers, timeout=60)
        if response.status_code == 200:
            print("Successfully sent finish signal")
            return True
        else:
            print(f"Error sending finish signal: HTTP {response.status_code}", file=sys.stderr)
            return False
    except requests.exceptions.RequestException as e:
        print(f"Network error sending finish signal: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(description="Upload files to index builder server")
    parser.add_argument("file_list", help="File containing list of filenames to upload")
    parser.add_argument("hostname", help="Server hostname")
    parser.add_argument("port", type=int, help="Server port")
    parser.add_argument("--max-retries", type=int, default=10, 
                       help="Maximum retry attempts for rate limited requests")
    
    args = parser.parse_args()
    
    # Read the list of files
    try:
        file_list = read_file_list(args.file_list)
    except Exception as e:
        print(f"Error reading file list {args.file_list}: {e}", file=sys.stderr)
        sys.exit(1)
    
    if not file_list:
        print("No files to upload", file=sys.stderr)
        sys.exit(1)
    
    print(f"Uploading {len(file_list)} files to {args.hostname}:{args.port}")
    
    # Upload each file
    failed_files = []
    for file_path in file_list:
        if not upload_file_with_retry(file_path, args.hostname, args.port, args.max_retries, file_path):
            failed_files.append(file_path)
    
    # Send finish signal if all files uploaded successfully
    if not failed_files:
        if send_finish_signal(args.hostname, args.port):
            print("All files uploaded successfully and finish signal sent")
            sys.exit(0)
        else:
            print("Files uploaded but failed to send finish signal", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"Failed to upload {len(failed_files)} files:", file=sys.stderr)
        for file_path in failed_files:
            print(f"  {file_path}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()