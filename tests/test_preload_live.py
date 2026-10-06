"""Eager catalog population: preload runs in the background right after init,
in parallel, is observable, consumes its own mount notices, and leaves every
table resolving without a first-touch mount. Also discard_shadow(table=)."""

import os
import time

import pytest

from tests.test_onelake_catalog_live import LH, TABLE, _engine, _run

pytestmark = pytest.mark.skipif(os.environ.get("LOCAL_SPARK_LIVE") != "1", reason="set LOCAL_SPARK_LIVE=1")

PRELOAD_LH = os.environ.get("LOCAL_SPARK_LIVE_PRELOAD_LAKEHOUSE", "customer")


def test_preload_at_startup_and_on_demand(tmp_path):
    t0 = time.time()
    eng, srv = _engine(tmp_path, "sandbox", preload=[PRELOAD_LH], preload_workers=16)
    init_s = time.time() - t0
    try:
        st = eng.preload_status()
        assert st["state"] in ("running", "done"), st  # started in the background, init returned
        # a user cell during preload: its own notices still belong to it, preload's do not leak
        res = eng.run_code("print(spark.table('dataverse.GlobalOptionsetMetadata').count())")
        assert res.ok
        mine = [n for n in res.notices if n.startswith("mounted dataverse.GlobalOptionsetMetadata")]
        leaked = [n for n in res.notices if n.startswith(f"mounted {PRELOAD_LH}.")]
        assert len(mine) == 1 and not leaked, res.notices
        st = eng.wait_preload(timeout=1200)
        assert st["state"] == "done", st
        lh = st["lakehouses"][PRELOAD_LH]
        assert lh["total"] >= 50 and lh["done"] == lh["total"] and lh["failed"] <= 2, lh
        print(f"PRELOAD {lh['total']} tables in {lh['seconds']}s with {st['workers']} workers (init took {init_s:.0f}s)")
        # the completion notice arrives with the next result, then nothing is pending
        res = eng.run_code(f"print(spark.table('{LH}.{TABLE}').count())")
        assert any(n.startswith("preloaded ") for n in res.notices), res.notices
        assert not any(n.startswith(f"mounted {LH}.") for n in res.notices), res.notices  # already preloaded: no first-touch
        res = eng.run_code("print(1)")
        assert res.notices == []
        # discard one shadow by name
        d = eng.discard_shadow(table=f"{LH}.{TABLE}")
        assert d["discarded"] == 1 and d["tables"][0]["table"] == TABLE
        assert not any(t["table"] == TABLE for t in eng.shadow_status()["tables"])
        # the rest are intact: shadows, plus (fabric-1.3) deletion-vector tables that are views, not clones
        status = eng.shadow_status()
        assert len(status["tables"]) + len(status["deletion_vector_tables"]) >= lh["total"] - 3, (len(status["tables"]), len(status["deletion_vector_tables"]), lh["total"])
        with pytest.raises(LookupError):
            eng.discard_shadow(table=f"{LH}.{TABLE}")
        # a second on-demand preload of the same lakehouse is a no-op-ish fast pass
        st2 = eng.start_preload([PRELOAD_LH]); st2 = eng.wait_preload(600)
        assert st2["state"] == "done" and st2["lakehouses"][PRELOAD_LH]["done"] == st2["lakehouses"][PRELOAD_LH]["total"]
    finally:
        eng.stop()
        srv.stop()
