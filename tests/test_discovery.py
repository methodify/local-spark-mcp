import json
from collections import namedtuple

import httpx
import pytest

from local_spark_mcp.discovery import (
    FabricAPIClient,
    FabricAPIError,
    LakehouseInfo,
)

AccessToken = namedtuple("AccessToken", "token expires_on")


class FakeCred:
    def get_token(self, *a, **k):
        return AccessToken("fake-fabric-token", 0)


def make_client(handler) -> FabricAPIClient:
    transport = httpx.MockTransport(handler)
    return FabricAPIClient(credential=FakeCred(), client=httpx.Client(transport=transport))


def json_resp(payload):
    return httpx.Response(200, json=payload)


# ---- LakehouseInfo path building ----

def test_lakehouse_paths_use_guids():
    lh = LakehouseInfo(name="customer", id="LH-GUID", workspace_id="WS-GUID")
    assert lh.abfss_tables == "abfss://WS-GUID@onelake.dfs.fabric.microsoft.com/LH-GUID/Tables"
    assert lh.table_path("t1") == "abfss://WS-GUID@onelake.dfs.fabric.microsoft.com/LH-GUID/Tables/t1"


# ---- workspace resolution ----

def test_resolve_workspace_by_id_no_http():
    def handler(req):
        raise AssertionError("should not hit the network when id is given")

    assert make_client(handler).resolve_workspace(id="WS-1") == "WS-1"


def test_resolve_workspace_by_name():
    def handler(req):
        assert req.url.path == "/v1/workspaces"
        return json_resp({"value": [
            {"id": "a", "displayName": "Other"},
            {"id": "b", "displayName": "Data Warehouse"},
        ]})

    assert make_client(handler).resolve_workspace(name="data warehouse") == "b"


def test_resolve_workspace_name_not_found():
    handler = lambda req: json_resp({"value": [{"id": "a", "displayName": "X"}]})
    with pytest.raises(FabricAPIError, match="No workspace"):
        make_client(handler).resolve_workspace(name="Nope")


def test_resolve_workspace_ambiguous():
    handler = lambda req: json_resp({"value": [
        {"id": "a", "displayName": "Dup"}, {"id": "b", "displayName": "Dup"}
    ]})
    with pytest.raises(FabricAPIError, match="ambiguous"):
        make_client(handler).resolve_workspace(name="Dup")


# ---- lakehouses ----

def test_list_lakehouses():
    def handler(req):
        assert req.url.path == "/v1/workspaces/WS/lakehouses"
        return json_resp({"value": [
            {"id": "lh1", "displayName": "customer"},
            {"id": "lh2", "displayName": "silver"},
        ]})

    lhs = make_client(handler).list_lakehouses("WS")
    assert [(l.name, l.id) for l in lhs] == [("customer", "lh1"), ("silver", "lh2")]
    assert all(l.workspace_id == "WS" for l in lhs)


# ---- tables (data key) ----

def test_list_tables_uses_data_key():
    def handler(req):
        assert req.url.path == "/v1/workspaces/WS/lakehouses/LH/tables"
        return json_resp({"data": [{"name": "t1"}, {"name": "t2"}]})

    assert make_client(handler).list_tables("WS", "LH") == ["t1", "t2"]


# ---- paging ----

def test_continuation_paging():
    calls = []

    def handler(req):
        calls.append(req.url.params.get("continuationToken"))
        if "continuationToken" not in req.url.params:
            return json_resp({"value": [{"id": "1", "displayName": "a"}], "continuationToken": "TOK"})
        return json_resp({"value": [{"id": "2", "displayName": "b"}]})

    lhs = make_client(handler).list_lakehouses("WS")
    assert [l.id for l in lhs] == ["1", "2"]
    assert calls == [None, "TOK"]


# ---- errors ----

def test_non_200_raises():
    handler = lambda req: httpx.Response(403, text="forbidden")
    with pytest.raises(FabricAPIError, match="403"):
        make_client(handler).list_lakehouses("WS")
