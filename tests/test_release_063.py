"""0.6.3 units: the shadow root reaches the JVM as a Hadoop-style file: URI."""

from pathlib import Path

from local_spark_mcp.engine import SparkEngine, _hadoop_file_uri


def test_hadoop_file_uri_keeps_spaces_raw(tmp_path):
    d = tmp_path / "App Data" / "state"
    d.mkdir(parents=True)
    uri = _hadoop_file_uri(d)
    assert uri.startswith("file:/") and "%20" not in uri and uri.endswith("/App Data/state")
    assert uri != d.resolve().as_uri()  # as_uri() would have encoded the space


def test_mount_table_entry_forms():
    assert SparkEngine._qualified("test", "dbo/publicholidays") == ("test__dbo", "publicholidays")
    assert SparkEngine._qualified("test", "sales_import") == ("test", "sales_import")
