#!/usr/bin/env bash
# Download and normalize the Loghub HDFS_v1 dataset from Zenodo.
set -euo pipefail

DATA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ARCHIVE="$DATA_DIR/HDFS_v1.zip"
TARGET_DIR="$DATA_DIR/HDFS_v1"
TEMP_DIR="$DATA_DIR/.hdfs_v1_extract"
URL="https://zenodo.org/records/8196385/files/HDFS_v1.zip?download=1"

for command_name in curl unzip find; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Error: '$command_name' is required but was not found." >&2
    exit 1
  fi
done

mkdir -p "$TARGET_DIR"

if [[ ! -s "$ARCHIVE" ]]; then
  echo "Downloading HDFS_v1 from Zenodo..."
  curl --fail --location --retry 5 --retry-delay 5 \
  --retry-all-errors \
  --continue-at - \
  --output "$ARCHIVE.part" "$URL"
  mv "$ARCHIVE.part" "$ARCHIVE"
else
  echo "Archive already present: $ARCHIVE"
fi

echo "Validating archive..."
unzip -tq "$ARCHIVE" >/dev/null

rm -rf "$TEMP_DIR"
mkdir -p "$TEMP_DIR"
echo "Extracting dataset..."
unzip -oq "$ARCHIVE" -d "$TEMP_DIR"

copy_required_file() {
  local filename="$1"
  local source_path
  source_path="$(find "$TEMP_DIR" -type f -name "$filename" -print -quit)"
  if [[ -z "$source_path" ]]; then
    echo "Error: '$filename' was not found in the downloaded archive." >&2
    exit 1
  fi
  cp "$source_path" "$TARGET_DIR/$filename"
}

copy_required_file "HDFS.log"
copy_required_file "anomaly_label.csv"

# Optional reference file. Some archive versions may omit it.
template_path="$(find "$TEMP_DIR" -type f -name 'HDFS_templates.csv' -print -quit)"
if [[ -n "$template_path" ]]; then
  cp "$template_path" "$TARGET_DIR/HDFS_templates.csv"
fi

rm -rf "$TEMP_DIR"

echo "Dataset ready:"
ls -lh "$TARGET_DIR/HDFS.log" "$TARGET_DIR/anomaly_label.csv"
echo
echo "Suggested checks:"
echo "  wc -l '$TARGET_DIR/HDFS.log'"
echo "  wc -l '$TARGET_DIR/anomaly_label.csv'"
echo "  head -n 5 '$TARGET_DIR/HDFS.log'"
echo "  head -n 5 '$TARGET_DIR/anomaly_label.csv'"
