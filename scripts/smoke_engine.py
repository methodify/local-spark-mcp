"""Manual smoke test for SparkEngine against local Delta. Run with the venv:

    .venv/bin/python scripts/smoke_engine.py
"""

import tempfile
from pathlib import Path

from local_spark_mcp.engine import SparkEngine


def show(label, result):
    print(f"\n===== {label} =====")
    if hasattr(result, "to_dict"):
        import json

        print(json.dumps(result.to_dict(), indent=2, default=str))
    else:
        print(result)


def main():
    tmp = Path(tempfile.mkdtemp(prefix="spark-smoke-"))
    print(f"workdir: {tmp}")

    engine = SparkEngine(driver_memory="2g", default_sql_limit=5)

    # 1. Persistent state across cells + write a Delta table.
    show(
        "cell 1: create + write delta",
        engine.run_code(
            f"""
people = [(1, "Alice", "2024-01-01"), (2, "Bob", "2024-02-01"), (3, "Cy", "2024-03-01")]
df = spark.createDataFrame(people, ["id", "name", "joined"])
df.write.format("delta").mode("overwrite").save("{tmp / 'people'}")
print("wrote", df.count(), "rows")
"""
        ),
    )

    # 2. Register + last-expression echo (DataFrame repr).
    show(
        "cell 2: register table, last-expr echo",
        engine.run_code(
            f"""
spark.sql("CREATE TABLE IF NOT EXISTS people USING DELTA LOCATION '{tmp / 'people'}'")
df2 = spark.table("people")
df2
"""
        ),
    )

    # 3. print + show()
    show("cell 3: df.show()", engine.run_code("df2.orderBy('id').show()"))

    # 4. persistent var reuse
    show("cell 4: reuse earlier var", engine.run_code("print('people var still here:', people)"))

    # 5. error / traceback capture
    show("cell 5: error", engine.run_code("1/0"))

    # 6. run_sql with truncation (table has 3 rows, limit 2)
    show("sql: select with limit 2", engine.run_sql("SELECT * FROM people ORDER BY id", limit=2))

    # 7. info
    show("info", type("R", (), {"to_dict": lambda self: engine.info()})())

    engine.stop()
    print("\nOK")


if __name__ == "__main__":
    main()
