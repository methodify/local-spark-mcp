"""run_notebook + the notebookutils shim, end to end on a local Spark session
(no Fabric). Gated on LOCAL_SPARK_RUN_INTEGRATION=1."""

import json
import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    os.environ.get("LOCAL_SPARK_RUN_INTEGRATION") != "1",
    reason="set LOCAL_SPARK_RUN_INTEGRATION=1 to run (starts a real Spark session)",
)

HDR = "# Fabric notebook source\n\n# METADATA ********************\n\n# META {\n# META   \"kernel_info\": {\"name\": \"synapse_pyspark\"},\n# META   \"dependencies\": {}\n# META }\n"
PYM = "\n# METADATA ********************\n\n# META {\n# META   \"language\": \"python\",\n# META   \"language_group\": \"synapse_pyspark\"\n# META }\n"
SQLM = PYM.replace("python", "sparksql")


def cell(src, kind="CELL", meta=PYM):
    return f"\n# {kind} ********************\n\n{src}\n{meta}"


def write_nb(root: Path, folder: str, display: str, body: str) -> Path:
    d = root / f"{folder}.Notebook"
    d.mkdir(parents=True, exist_ok=True)
    (d / ".platform").write_text(json.dumps({"metadata": {"type": "Notebook", "displayName": display}}))
    (d / "notebook-content.py").write_text(HDR + body)
    return d / "notebook-content.py"


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    from local_spark_mcp.engine import SparkEngine

    root = tmp_path_factory.mktemp("nbs")
    delta = (root / "nb_t").as_posix()
    main = write_nb(root, "Main folder name", "Main", (
        "\n# MARKDOWN ********************\n\n# # Title\n"
        + cell("p = 1", kind="PARAMETERS CELL")
        + cell(f"spark.range(3).write.format('delta').mode('overwrite').save('{delta}')\n"
               f"spark.sql(\"CREATE TABLE IF NOT EXISTS nb_t USING DELTA LOCATION '{delta}'\")")
        + cell("%pip install nothing\ny = p * 10\nprint('y', y)")
        + cell("# MAGIC %%sql\n# MAGIC select count(*) as c from nb_t", meta=SQLM)
        + cell("import notebookutils as nu\nimport mssparkutils as ms\n"
               "print('ctx', nu.runtime.context['defaultLakehouseName'])\nms.session.stop()\nnu.notebook.exit('bye')")
        + cell("print('never')")
    ))
    write_nb(root, "callee-folder", "Callee", cell("q = 0", kind="PARAMETERS CELL") + cell("notebookutils.notebook.exit(q * 2)"))
    write_nb(root, "broken", "Broken", cell("raise ValueError('boom')") + cell("print('after')"))
    eng = SparkEngine(driver_memory="2g", notebooks_root=str(root), state_root=str(root / "state"))
    yield eng, main
    eng.stop()


def test_full_run_with_parameters_exit_and_magics(env):
    eng, main = env
    res = eng.run_notebook(str(main), parameters={"p": 4})
    by = {c["index"]: c for c in res["cells"]}
    assert res["status"] == "ok", res
    assert res["exit_value"] == "bye"
    assert by[0]["status"] == "skipped"                      # markdown
    assert by[1]["status"] == "ok"                           # parameters cell ran...
    assert "y 40" in by[3]["stdout"]                         # ...then the override won
    assert by[3]["unsupported"] == ["%pip install nothing"]  # reported, not run
    assert by[4]["language"] == "sparksql" and by[4]["status"] == "ok"
    assert "3" in by[4]["stdout"]                            # count(*) = 3
    assert by[5]["status"] == "exited" and "ctx None" in by[5]["stdout"]
    assert 6 not in by                                       # nothing after exit runs
    assert res["default_lakehouse"] is None


def test_cell_selection_reruns_subset(env):
    eng, main = env
    res = eng.run_notebook(str(main), cells="3")
    assert [c["index"] for c in res["cells"]] == [3]
    assert "y 40" in res["cells"][0]["stdout"]  # p persisted in the namespace


def test_stop_on_error_and_traceback(env):
    eng, _ = env
    res = eng.run_notebook("Broken")                     # resolved by display name
    assert res["status"] == "error"
    assert [c["status"] for c in res["cells"]] == ["error"]
    assert "ValueError: boom" in res["first_error"] and "boom" in res["first_traceback"]
    res2 = eng.run_notebook("Broken", stop_on_error=False)
    assert [c["status"] for c in res2["cells"]] == ["error", "ok"]


def test_notebook_run_and_run_multiple_from_a_cell(env):
    eng, _ = env
    r = eng.run_code("print('RUN', notebookutils.notebook.run('Callee', arguments={'q': 7}))")
    assert r.ok and "RUN 14" in r.stdout
    r = eng.run_code(
        "res = mssparkutils.notebook.runMultiple({'activities': ["
        "{'name': 'a', 'path': 'Callee', 'args': {'q': 3}},"
        "{'name': 'b', 'path': 'Callee', 'args': {'q': 5}, 'dependencies': ['a']}]})\n"
        "print('DAG', res['a']['exitVal'], res['b']['exitVal'], res['b']['exception'])"
    )
    assert r.ok and "DAG 6 10 None" in r.stdout
    r = eng.run_code(
        "try:\n    mssparkutils.notebook.runMultiple({'activities': [{'name': 'x', 'path': 'Broken'}, {'name': 'y', 'path': 'Callee', 'dependencies': ['x']}]})\n"
        "except Exception as e:\n    print('FAIL', sorted(e.result), e.result['y']['exception'])"
    )
    assert r.ok and "FAIL ['x', 'y'] skipped" in r.stdout


def test_unknown_member_and_missing_notebook(env):
    eng, _ = env
    r = eng.run_code("notebookutils.lakehouse.get('x')")
    assert not r.ok and "not available locally" in r.error
    with pytest.raises(FileNotFoundError):
        eng.run_notebook("No Such Notebook")
