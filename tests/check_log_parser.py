import re

from streaming.log_parser import LINE_PATTERN, BLOCK_ID_PATTERN


line_pattern = re.compile(LINE_PATTERN)
block_pattern = re.compile(BLOCK_ID_PATTERN)

total = 0
no_match = 0
no_block = 0

no_match_samples = []
no_block_samples = []

with open("data/HDFS_v1/HDFS.log", errors="replace") as f:
    for line in f:
        total += 1
        clean_line = line.rstrip("\n")

        match = line_pattern.match(clean_line)

        if not match:
            no_match += 1

            if len(no_match_samples) < 10:
                no_match_samples.append(clean_line)

            continue

        message = match.group(6)

        if not block_pattern.search(message):
            no_block += 1

            if len(no_block_samples) < 10:
                no_block_samples.append(clean_line)

print("===== HDFS LOG PARSER QUALITY =====")
print(f"Total lines: {total:,}")
print(f"No line match: {no_match:,}")
print(f"No block ID: {no_block:,}")

if total:
    print(f"No-match rate: {(no_match / total) * 100:.6f}%")
    print(f"No-block-ID rate: {(no_block / total) * 100:.6f}%")

print("\n===== SAMPLE UNPARSEABLE LINES =====")

if no_match_samples:
    for line in no_match_samples:
        print(line)
else:
    print("None")

print("\n===== SAMPLE LINES WITHOUT BLOCK ID =====")

if no_block_samples:
    for line in no_block_samples:
        print(line)
else:
    print("None")