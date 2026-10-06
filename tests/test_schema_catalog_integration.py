"""`lakehouse.schema.table` through ch.fs.OneLakeSchemaCatalog, without OneLake:
the delegating catalog maps `lh.dbo.t` onto the session database `lh__dbo`.

    LOCAL_SPARK_RUN_INTEGRATION=1 .venv/bin/python -m pytest tests/test_schema_catalog_integration.py -v
"""

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("LOCAL_SPARK_RUN_INTEGRATION") != "1",
    reason="set LOCAL_SPARK_RUN_INTEGRATION=1 to run (starts a real Spark session)",
)


def test_schema_catalog_delegates_to_session_databases():
    from local_spark_mcp.engine import SparkEngine
    from local_spark_mcp.fabric import default_jar_path

    eng = SparkEngine(
        driver_memory="2g",
        extra_jars=[default_jar_path()],
        extra_configs={"spark.sql.catalog.lh": "ch.fs.OneLakeSchemaCatalog", "spark.sql.catalog.lh.lakehouse": "lh",
                       "spark.sql.catalog.lh.default_schema": "dbo"},
    )
    try:
        spark = eng.spark
        # once `lh` is a catalog, a bare `lh` means that catalog: set up through spark_catalog, as the engine
        # does before it registers the catalog
        for db in ("lh", "lh__dbo", "lh__sales"):
            spark.sql(f"CREATE DATABASE spark_catalog.{db}")
        spark.range(3).write.saveAsTable("spark_catalog.lh__dbo.holidays")   # what a first touch of Tables/dbo/holidays produces
        spark.range(5).write.saveAsTable("spark_catalog.lh.toplevel")         # Tables/toplevel

        # the Fabric spelling, three-part
        assert spark.table("lh.dbo.holidays").count() == 3
        assert spark.sql("SELECT COUNT(*) AS c FROM lh.dbo.holidays").first()[0] == 3
        # two-part still reaches the lakehouse's top-level tables
        assert spark.table("lh.toplevel").count() == 5
        # schemas are namespaces
        assert sorted(r[0] for r in spark.sql("SHOW NAMESPACES IN lh").collect()) == ["dbo", "sales"]
        assert [r.tableName for r in spark.sql("SHOW TABLES IN lh.dbo").collect()] == ["holidays"]
        # USE lh -> default schema dbo, as on Fabric
        spark.sql("USE lh")
        assert spark.table("holidays").count() == 3
        assert spark.sql("SELECT current_catalog(), current_schema()").first()[:2] == ("lh", "dbo")
        # writes through the catalog land in the schema database (staging path)
        spark.range(7).write.saveAsTable("lh.sales.orders")
        assert spark.table("spark_catalog.lh__sales.orders").count() == 7
        spark.range(2).write.mode("overwrite").saveAsTable("lh.sales.orders")
        assert spark.table("lh.sales.orders").count() == 2
        spark.sql("CREATE TABLE lh.dbo.made (id INT) USING delta")
        assert spark.catalog.tableExists("spark_catalog.lh__dbo.made")
        spark.sql("DROP TABLE lh.dbo.made")
        assert not spark.catalog.tableExists("spark_catalog.lh__dbo.made")
        # a missing table raises the normal not-found
        with pytest.raises(Exception, match="TABLE_OR_VIEW_NOT_FOUND|cannot be found"):
            spark.table("lh.dbo.nope").count()
        spark.sql("USE spark_catalog.default")
    finally:
        eng.stop()
