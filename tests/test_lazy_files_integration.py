"""ch.fs.LakehouseFileSystem without OneLake: `fs.lakehouse.inner` points the
wrapper at a local directory standing in for the workspace, so a session whose
default filesystem is `lakehouse://<ws>@<lh>.host` resolves `Files/x` there,
refuses writes under sandbox, and passes them through under writethrough.

    LOCAL_SPARK_RUN_INTEGRATION=1 .venv/bin/python -m pytest tests/test_lazy_files_integration.py -v
"""

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("LOCAL_SPARK_RUN_INTEGRATION") != "1",
    reason="set LOCAL_SPARK_RUN_INTEGRATION=1 to run (starts a real Spark session)",
)

WS, LH = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"


def test_lakehouse_filesystem(tmp_path):
    from local_spark_mcp.engine import SparkEngine
    from local_spark_mcp.fabric import default_jar_path

    fake = tmp_path / "onelake"
    (fake / LH / "Files" / "data").mkdir(parents=True)
    (fake / LH / "Files" / "data" / "a.csv").write_text("id,name\n1,x\n2,y\n", encoding="utf-8")
    (fake / LH / "Files" / "data" / "b.csv").write_text("id,name\n3,z\n", encoding="utf-8")
    eng = SparkEngine(
        driver_memory="2g",
        extra_jars=[default_jar_path()],
        extra_configs={"spark.hadoop.fs.lakehouse.impl": "ch.fs.LakehouseFileSystem",
                       "spark.delta.logStore.lakehouse.impl": "io.delta.storage.AzureLogStore",
                       "spark.hadoop.fs.lakehouse.inner": fake.resolve().as_uri(),
                       "spark.localspark.write_mode": "sandbox"},
    )
    try:
        spark = eng.spark
        uri = f"lakehouse://{WS}@{LH}.onelake.dfs.fabric.microsoft.com"
        sa = spark.newSession()
        sa.conf.set("fs.defaultFS", uri)
        # relative Files/ resolves to the lakehouse (and only in this session)
        df = sa.read.option("header", True).csv("Files/data")
        assert df.count() == 3 and set(df.columns) == {"id", "name"}
        paths = [r[0] for r in sa.read.format("binaryFile").load("Files/data").select("path").collect()]
        assert all(p.startswith(uri + "/Files/data/") for p in paths), paths
        with pytest.raises(Exception, match="PATH_NOT_FOUND|does not exist"):
            spark.read.csv("Files/data").count()  # the root session still resolves locally
        # listing + exists through the JVM filesystem API
        jvm = spark._jvm
        hconf = sa._jsparkSession.sessionState().newHadoopConf()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
        assert str(fs.getUri()) == uri and fs.exists(jvm.org.apache.hadoop.fs.Path("Files/data/a.csv"))
        assert sorted(str(s.getPath().getName()) for s in fs.listStatus(jvm.org.apache.hadoop.fs.Path("Files/data"))) == ["a.csv", "b.csv"]
        # sandbox: writes under Files/ are refused with the policy message
        with pytest.raises(Exception, match="write_mode=sandbox") as ei:
            sa.range(3).write.mode("overwrite").csv("Files/out")
        assert "writethrough" in str(ei.value) and not (fake / LH / "Files" / "out").exists()
        with pytest.raises(Exception, match="write_mode=sandbox"):
            sa.range(3).write.format("delta").save("Files/delta_out")
        # a Delta table under Files/ reads fine (written through the inner root directly)
        spark.range(4).write.format("delta").save((fake / LH / "Files" / "dt").resolve().as_uri())
        assert sa.read.format("delta").load("Files/dt").count() == 4
        # (delta.`Files/dt` in SQL is not a thing: Delta's path identifiers must be absolute, on Fabric too)
        assert sa.sql(f"SELECT COUNT(*) FROM delta.`{uri}/Files/dt`").first()[0] == 4
        # writethrough: a fresh lakehouse (new filesystem instance) passes writes through
        LH2 = "33333333-3333-3333-3333-333333333333"
        (fake / LH2 / "Files").mkdir(parents=True)
        sb = spark.newSession()
        sb.conf.set("spark.localspark.write_mode", "writethrough")
        sb.conf.set("fs.defaultFS", f"lakehouse://{WS}@{LH2}.onelake.dfs.fabric.microsoft.com")
        sb.range(5).write.mode("overwrite").format("delta").save("Files/out_delta")
        assert sb.read.format("delta").load("Files/out_delta").count() == 5
        assert (fake / LH2 / "Files" / "out_delta" / "_delta_log").is_dir()
        sb.range(2).write.mode("overwrite").csv("Files/out_csv")
        assert any(p.suffix == ".csv" for p in (fake / LH2 / "Files" / "out_csv").iterdir())
    finally:
        eng.stop()


def test_files_mode_validation():
    from local_spark_mcp.config import Config, ConfigError

    cfg = Config()
    cfg.files.mode = "sometimes"
    with pytest.raises(ConfigError, match="files.mode"):
        cfg.validate()
