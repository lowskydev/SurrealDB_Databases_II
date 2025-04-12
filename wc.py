import sys

def count_lines(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            line_count = sum(1 for _ in f)
        print(f"{filename} has {line_count} lines.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python wc.py <filename>")
    else:
        count_lines(sys.argv[1])
