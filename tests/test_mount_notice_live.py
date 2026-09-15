"""First touch of a lakehouse table reports "mounted <lh>.<table> in N s" with
the cell result; a second touch does not (REQUEST-007, ADO #298)."""

import os

import pytest

from tests.test_onelake_catalog_live import LH, TABLE, _engine

pytestmark = pytest.mark.skipif(os.environ.get("LOCAL_SPARK_LIVE") != "1", reason="set LOCAL_SPARK_LIVE=1")


def test_first_touch_is_reported(tmp_path):
    eng, srv = _engine(tmp_path, "sandbox")
    try:
        res = eng.run_code(f"print(spark.table('{LH}.{TABLE}').count())")
        assert res.ok and len(res.notices) == 1, res.notices
        assert res.notices[0].startswith(f"mounted {LH}.{TABLE} in ") and " s (shallow clone)" in res.notices[0]
        res = eng.run_code(f"print(spark.table('{LH}.{TABLE}').count())")
        assert res.notices == []
        # the SQL path carries notices too
        sql = eng.run_sql("SELECT COUNT(*) AS c FROM dataverse.GlobalOptionsetMetadata", limit=1)
        assert len(sql.notices) == 1 and sql.notices[0].startswith("mounted dataverse.GlobalOptionsetMetadata in "), sql.notices
    finally:
        eng.stop()
        srv.stop()
