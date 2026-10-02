"""Repeated partitioned overwrites of a shadow (new table and a cloned OneLake
table) keep working: Delta's catalog-update hook vs Spark's in-memory catalog
(ADO #300). Also the write path dwlib >= 0.62 uses (format('delta').save(location))."""

import os

import pytest

from tests.test_onelake_catalog_live import LH, TABLE, _engine, _run

pytestmark = pytest.mark.skipif(os.environ.get("LOCAL_SPARK_LIVE") != "1", reason="set LOCAL_SPARK_LIVE=1")


def test_repeated_partitioned_overwrites(tmp_path):
    eng, srv = _engine(tmp_path, "sandbox")
    try:
        # a brand-new partitioned table under a lakehouse, overwritten four times with schema changes
        for i in range(1, 5):
            _run(eng, f"spark.range(30).withColumn('p', F.col('id') % 3).withColumn('v{i}', F.lit({i})).write.mode('overwrite').option('overwriteSchema','true').partitionBy('p').saveAsTable('{LH}.lsm_300_new'); print('N', spark.table('{LH}.lsm_300_new').count())")
        out = _run(eng, f"print(spark.sql('DESCRIBE DETAIL {LH}.lsm_300_new').select('partitionColumns').first()[0])")
        assert "['p']" in out
        # a cloned OneLake table: overwrite partitioned twice, then unpartitioned, then partitioned
        _run(eng, f"print(spark.table('{LH}.{TABLE}').count())")
        for i, part in enumerate(("('p',)", "('p',)", "()", "('p',)"), 1):
            _run(eng, f"df = spark.table('{LH}.{TABLE}').limit(20).withColumn('p', F.lit({i} % 2)).withColumn('w{i}', F.lit({i})); w = df.write.mode('overwrite').option('overwriteSchema','true'); w = w.partitionBy(*{part}) if {part} else w; w.saveAsTable('{LH}.{TABLE}'); print('W', spark.table('{LH}.{TABLE}').count())")
        # the location path dwlib >= 0.62 uses
        _run(eng, f"loc = spark.sql('DESCRIBE DETAIL {LH}.{TABLE}').select('location').first()[0]; spark.table('{LH}.{TABLE}').limit(5).write.mode('overwrite').option('overwriteSchema','true').partitionBy('p').format('delta').save(loc); print('L', spark.table('{LH}.{TABLE}').count())")
    finally:
        eng.stop()
        srv.stop()
