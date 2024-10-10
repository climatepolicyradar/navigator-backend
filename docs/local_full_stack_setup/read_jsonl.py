import re
import sys

PATTERN = r'"document_import_id":"([A-Za-z0-9._-]+)"'

def read_jsonl_file(file_path: str, pattern: str = PATTERN) -> list:
    pattern_matches = []
    with open(file_path, 'r') as file:
        for line in file:
            match = re.findall(pattern, line)
            if match:
                pattern_matches += match
    return pattern_matches

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script_name.py <file_path>")
        sys.exit(1)

    file_path = sys.argv[1]
    json_data = read_jsonl_file(file_path)
    for d in json_data:
        print(d)
