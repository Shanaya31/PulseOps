# tests/test_timestamp_parsing.py

from datetime import datetime

from ingestion.replay_producer import parse_line_timestamp


def test_valid_timestamp_is_parsed():
    line = (
        "081109 203518 143 INFO dfs.DataNode$DataXceiver: "
        "Receiving block blk_-1608999687919862906 "
        "src: /10.250.19.102:54106 dest: /10.250.19.102:50010"
    )

    result = parse_line_timestamp(line)

    assert result is not None
    assert isinstance(result, datetime)
    assert result.year == 2008
    assert result.month == 11
    assert result.day == 9
    assert result.hour == 20
    assert result.minute == 35
    assert result.second == 18


def test_invalid_line_returns_none():
    line = "this is not a valid HDFS log line"

    assert parse_line_timestamp(line) is None