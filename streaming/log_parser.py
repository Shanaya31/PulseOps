"""
Regex definitions for HDFS_v1 raw log lines, shared reference for the Spark
job's regexp_extract calls.

Example raw line:
  081109 203615 148 INFO dfs.DataNode$PacketResponder: PacketResponder 1 for
  block blk_38865049064139660 terminating

Fields: date(6) time(6) pid level component: message
"""

# Full-line pattern, used with regexp_extract(col, PATTERN, group_index)
LINE_PATTERN = r"^(\d{6})\s+(\d{6})\s+(\d+)\s+(\w+)\s+([^:]+):\s+(.*)$"

GROUP_DATE = 1
GROUP_TIME = 2
GROUP_PID = 3
GROUP_LEVEL = 4
GROUP_COMPONENT = 5
GROUP_MESSAGE = 6

# Block ID appears inside the message, e.g. "blk_38865049064139660" or
# "blk_-1608999687919862906" (can be negative)
BLOCK_ID_PATTERN = r"(blk_-?\d+)"

# Spark timestamp format string matching the 6-digit date/time fields once
# concatenated, e.g. "081109203615" -> yyMMddHHmmss
TIMESTAMP_FORMAT = "yyMMddHHmmss"
