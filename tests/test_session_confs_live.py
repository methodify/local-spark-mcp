"""The profile's session confs take effect in a real Fabric-mode session."""

import os

import pytest

from local_spark_mcp.profiles import current_profile
from tests.test_onelake_catalog_live import LH, _engine, _run

pytestmark = pytest.mark.skipif(os.environ.get("LOCAL_SPARK_LIVE") != "1", reason="set LOCAL_SPARK_LIVE=1")


def test_profile_confs_apply(tmp_path):
    eng, srv = _engine(tmp_path, "sandbox")
    try:
        prof = current_profile()
        for key, want in prof.session_confs.items():
            if key == "spark.serializer":
                got = eng.spark.sparkContext.getConf().get(key)
            else:
                got = eng.spark.conf.get(key)
            assert str(got).lower() == want.lower(), (key, got, want)
        out = _run(eng, "df = spark.sql(\"SELECT timestamp'2026-01-01 00:00:00' AS t, CAST(1 AS DECIMAL(10,2)) AS d\"); pdf = df.toPandas(); print('PANDAS', pdf.dtypes['t'], pdf['t'][0]); print('TZ', spark.conf.get('spark.sql.session.timeZone'))")
        assert "TZ UTC" in out and "PANDAS datetime64" in out, out
        # overwrite into a NEW table must keep working (Spark 3.5 / Delta 3.2 break it under sources.default=delta)
        out = _run(eng, f"spark.range(2).write.mode('overwrite').saveAsTable('{LH}.lsm_conf_probe'); spark.range(3).write.mode('overwrite').saveAsTable('{LH}.lsm_conf_probe'); print('N', spark.table('{LH}.lsm_conf_probe').count())")
        assert "N 3" in out, out
        # sources.default=delta on both profiles: an untyped CREATE TABLE is Delta
        out = _run(eng, f"spark.sql('CREATE TABLE {LH}.lsm_conf_plain (id INT)'); print('FMT', spark.sql('DESCRIBE DETAIL {LH}.lsm_conf_plain').select('format').first()[0])")
        assert "FMT delta" in out, out
    finally:
        eng.stop()
        srv.stop()
