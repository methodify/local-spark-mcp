"""Deletion-vector handling that needs no Spark: SQL write-target detection,
the strategy switch, the error annotation, and the list_tables rendering."""

from local_spark_mcp import engine as engine_mod
from local_spark_mcp.engine import _dv_strategy, _sql_write_target
from local_spark_mcp.server import format_shadow, format_table_features


def test_sql_write_target():
    assert _sql_write_target("MERGE INTO custtable t USING s ON 1=1 WHEN MATCHED THEN UPDATE SET *") == "custtable"
    assert _sql_write_target("insert overwrite table a.b select 1") == "a.b"
    assert _sql_write_target("UPDATE `a`.`b` SET x = 1") == "`a`.`b`"
    assert _sql_write_target("  DELETE FROM a.b WHERE 1=0") == "a.b"
    assert _sql_write_target("SELECT * FROM a.b") is None


def test_dv_strategy_follows_delta_version(monkeypatch):
    monkeypatch.delenv("LOCAL_SPARK_DV_STRATEGY", raising=False)
    import importlib.metadata as md

    monkeypatch.setattr(md, "version", lambda name: "3.2.0")
    assert _dv_strategy() == "view"
    monkeypatch.setattr(md, "version", lambda name: "3.3.3")
    assert _dv_strategy() == "clone"
    monkeypatch.setenv("LOCAL_SPARK_DV_STRATEGY", "view")
    assert _dv_strategy() == "view"


class _Eng(engine_mod.SparkEngine):
    """annotate_error / dv_refusal without a Spark session."""

    def __init__(self):
        self.write_mode = "sandbox"
        self._dv = [{"lakehouse": "dataverse_l2f", "table": "custtable", "source": "abfss://x"}]

    def _dv_tables(self):
        return self._dv


def test_annotate_error_only_for_dv_view_writes():
    eng = _Eng()
    err = "AnalysisException: [DELTA_UNSUPPORTED_SOURCE] UPDATE destination only supports Delta sources.\n  UPDATE dataverse_l2f.custtable SET x = 1"
    out = eng.annotate_error(err)
    assert "local-spark: dataverse_l2f.custtable carries Delta deletion vectors" in out and "read-only" in out
    assert eng.annotate_error("AnalysisException: column nope not found in custtable") == "AnalysisException: column nope not found in custtable"
    assert eng.annotate_error("Saving data into a view is not allowed. salestable") == "Saving data into a view is not allowed. salestable"
    assert eng.annotate_error(None) is None


def test_format_table_features_and_shadow_dv():
    out = format_table_features("dataverse_l2f", ["custtable", "plain", "broken"], {
        "custtable": {"features": ["deletionVectors"], "deletion_vectors": True, "error": None},
        "plain": {"features": [], "deletion_vectors": False, "error": None},
        "broken": {"features": [], "deletion_vectors": None, "error": "X: y"},
    })
    assert "1 with deletion vectors" in out and "custtable  [deletionVectors: read-only here]" in out
    assert "broken  (protocol unreadable: X: y)" in out and "\n  plain\n" in out + "\n"
    out = format_shadow({"write_mode": "sandbox", "shadow_root": "/s", "persistent": False, "tables": [],
                         "deletion_vector_tables": [{"lakehouse": "dataverse_l2f", "table": "custtable"}]})
    assert "deletion-vector tables (1;" in out and "dataverse_l2f.custtable" in out
