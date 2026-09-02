"""Parser tests: synthetic samples plus the real Fabric Git corpus when present."""

import json
from pathlib import Path

import pytest

from local_spark_mcp.notebook import (
    HEADER,
    index_notebooks,
    load_notebook,
    parse_notebook_source,
    select_cells,
    strip_line_magics,
)

FILE_META = """# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "11111111-1111-1111-1111-111111111111",
# META       "default_lakehouse_name": "dataverse",
# META       "default_lakehouse_workspace_id": "22222222-2222-2222-2222-222222222222"
# META     }
# META   }
# META }
"""
PY_META = """# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
"""
SQL_META = PY_META.replace('"python"', '"sparksql"')

SAMPLE = f"""{HEADER}

{FILE_META}
# MARKDOWN ********************

# # Title
# 
# - bullet

# PARAMETERS CELL ********************

p = 1

{PY_META}
# CELL ********************

%pip install something
!pip install other
x = p + 1
print(x)

{PY_META}
# CELL ********************

# MAGIC %%sql
# MAGIC select 1 as a
# MAGIC 
# MAGIC from t

{SQL_META}"""


def test_parses_all_cell_kinds():
    nb = parse_notebook_source(SAMPLE)
    assert nb.warnings == []
    assert nb.default_lakehouse_name == "dataverse"
    assert nb.workspace_id == "22222222-2222-2222-2222-222222222222"
    assert [c.kind for c in nb.cells] == ["markdown", "parameters", "code", "code"]
    assert nb.has_parameters_cell
    md, params, py, sql = nb.cells
    assert md.source == "# Title\n\n- bullet"
    assert params.source == "p = 1" and params.meta["language"] == "python"
    assert py.magics == ["%pip install something", "!pip install other"]
    assert strip_line_magics(py) == ("x = p + 1\nprint(x)", py.magics)
    assert sql.language == "sparksql" and sql.cell_magic == "sql"
    assert sql.source == "select 1 as a\n\nfrom t"  # %%sql line removed; blank MAGIC line kept
    assert sql.meta["language"] == "sparksql"
    assert [c.index for c in nb.cells] == [0, 1, 2, 3]


def test_warnings_not_refusals():
    bad = SAMPLE.replace(HEADER, "# not a header", 1).replace(
        "# CELL ********************\n\n# MAGIC %%sql", "# CELL *******************\n\n# MAGIC %%sql", 1
    )
    nb = parse_notebook_source(bad)
    assert len(nb.cells) == 4  # still parsed
    assert any("header" in w for w in nb.warnings)
    assert any("19 asterisks" in w for w in nb.warnings)


def test_meta_language_mismatch_is_warned():
    mismatched = SAMPLE.replace(SQL_META, PY_META)
    nb = parse_notebook_source(mismatched)
    assert nb.cells[3].language == "sparksql"  # the source wins for execution
    assert any("%%sql cell but META language" in w for w in nb.warnings)


def test_select_cells():
    assert select_cells(None, 4) == {0, 1, 2, 3}
    assert select_cells("1-2", 4) == {1, 2}
    assert select_cells("0,3", 4) == {0, 3}
    assert select_cells([2], 4) == {2}
    assert select_cells("2", 4) == {2}
    with pytest.raises(IndexError):
        select_cells("5", 4)


def test_index_notebooks_uses_platform_display_name(tmp_path):
    folder = tmp_path / "Silver" / "Processing - silver - warehouse.Notebook"
    folder.mkdir(parents=True)
    (folder / ".platform").write_text(json.dumps({"metadata": {"type": "Notebook", "displayName": "_silver - warehouse"}}))
    (folder / "notebook-content.py").write_text(SAMPLE)
    (tmp_path / "Other.Lakehouse").mkdir()
    (tmp_path / "Other.Lakehouse" / ".platform").write_text(json.dumps({"metadata": {"type": "Lakehouse", "displayName": "Other"}}))
    idx = index_notebooks(tmp_path)
    assert list(idx) == ["_silver - warehouse"]
    assert idx["_silver - warehouse"] == folder / "notebook-content.py"
    assert load_notebook(folder).default_lakehouse_name == "dataverse"  # folder form


CORPUS = Path.home() / "src/claude-fabric/fabric-git-repo/data-warehouse"


@pytest.mark.skipif(not CORPUS.is_dir(), reason="Fabric Git export not present on this machine")
def test_real_corpus_parses():
    files = sorted(CORPUS.rglob("notebook-content.py"))
    assert len(files) >= 100
    clean, sql_files = 0, 0
    for f in files:
        nb = load_notebook(f)  # never raises
        assert nb.cells, f
        clean += not nb.warnings
        sql_files += any(c.language == "sparksql" for c in nb.cells)
    assert clean >= int(len(files) * 0.85), f"only {clean}/{len(files)} warning-free"
    assert sql_files >= 20  # the corpus has 28 notebooks with %%sql cells


@pytest.mark.skipif(not CORPUS.is_dir(), reason="Fabric Git export not present on this machine")
def test_real_silver_notebook():
    nb = load_notebook(CORPUS / "Silver" / "Processing - silver - warehouse.Notebook")
    assert nb.warnings == []
    assert nb.default_lakehouse_name == "dataverse"
    assert nb.workspace_id
    first_code = nb.code_cells()[0]
    assert "#!pip install" in first_code.source and first_code.magics == []  # commented magic is plain source
    assert nb.cells[0].kind == "markdown" and nb.cells[0].source.startswith("# Processing - silver - warehouse")
    idx = index_notebooks(CORPUS)
    assert idx["_silver - warehouse"].parent.name == "Processing - silver - warehouse.Notebook"
