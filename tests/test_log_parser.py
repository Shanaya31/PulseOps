# tests/test_log_parser.py

import re

from streaming.log_parser import LINE_PATTERN, BLOCK_ID_PATTERN


def test_valid_hdfs_log_line_matches():
    line = (
        "081109 203518 143 INFO dfs.DataNode$DataXceiver: "
        "Receiving block blk_-1608999687919862906 "
        "src: /10.250.19.102:54106 dest: /10.250.19.102:50010"
    )

    match = re.match(LINE_PATTERN, line)

    assert match is not None
    assert match.group(1) == "081109"
    assert match.group(2) == "203518"
    assert match.group(3) == "143"
    assert match.group(4) == "INFO"
    assert match.group(5) == "dfs.DataNode$DataXceiver"


def test_block_id_is_extracted():
    line = (
        "081109 203518 143 INFO dfs.DataNode$DataXceiver: "
        "Receiving block blk_-1608999687919862906 "
        "src: /10.250.19.102:54106 dest: /10.250.19.102:50010"
    )

    block_match = re.search(BLOCK_ID_PATTERN, line)

    assert block_match is not None
    assert block_match.group(1) == "blk_-1608999687919862906"


def test_line_without_block_id_does_not_match_block_pattern():
    line = (
        "081109 203518 143 INFO dfs.DataNode$DataXceiver: "
        "DataNode service started"
    )

    assert re.search(BLOCK_ID_PATTERN, line) is None