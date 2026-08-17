from ingestion.replay_producer import parse_line_timestamp

total = 0
bad = 0
bad_samples = []

with open("data/HDFS_v1/HDFS.log", errors="replace") as f:
    for line in f:
        total += 1
        line = line.rstrip("\n")

        if parse_line_timestamp(line) is None:
            bad += 1

            if len(bad_samples) < 10:
                bad_samples.append(line)

print("===== TIMESTAMP PARSER QUALITY =====")
print(f"Total lines: {total:,}")
print(f"Failed timestamps: {bad:,}")

if total:
    print(f"Failure rate: {(bad / total) * 100:.6f}%")

print("\n===== SAMPLE FAILURES =====")

if bad_samples:
    for line in bad_samples:
        print(line)
else:
    print("None")