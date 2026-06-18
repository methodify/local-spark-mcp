"""Fabric REST discovery: resolve a workspace, list its lakehouses, list a
lakehouse's tables. Auth is via DefaultAzureCredential (Fabric API scope) — the
same ambient `az login` identity used for OneLake storage.

Runs in the MCP server (parent) process, which then hands the resolved
workspace id + lakehouse metadata (GUIDs) to the worker. OneLake is always
addressed by GUID (name-based paths 400); see LakehouseInfo.table_path.
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx

FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com"


class FabricAPIError(Exception):
    """A Fabric REST call failed or returned an unexpected result."""


@dataclass(frozen=True)
class LakehouseInfo:
    name: str
    id: str
    workspace_id: str

    @property
    def abfss_tables(self) -> str:
        return f"abfss://{self.workspace_id}@{ONELAKE_HOST}/{self.id}/Tables"

    def table_path(self, table: str) -> str:
        return f"{self.abfss_tables}/{table}"


class FabricAPIClient:
    def __init__(self, credential=None, *, base_url: str = FABRIC_API_BASE, client: httpx.Client | None = None):
        self._cred = credential
        self.base_url = base_url.rstrip("/")
        self._client = client  # injectable for tests
        self._token: str | None = None

    @property
    def credential(self):
        if self._cred is None:
            from azure.identity import DefaultAzureCredential

            self._cred = DefaultAzureCredential()
        return self._cred

    def _token_value(self) -> str:
        if self._token is None:
            self._token = self.credential.get_token(FABRIC_SCOPE).token
        return self._token

    def _http(self) -> httpx.Client:
        return self._client or httpx.Client(timeout=30.0)

    def _get_pages(self, path: str, items_key: str) -> list[dict]:
        """GET with Fabric continuation-token paging; returns concatenated items."""
        client = self._http()
        owns = self._client is None
        items: list[dict] = []
        params: dict = {}
        try:
            url = f"{self.base_url}{path}"
            while True:
                resp = client.get(
                    url, headers={"Authorization": f"Bearer {self._token_value()}"}, params=params
                )
                if resp.status_code != 200:
                    raise FabricAPIError(
                        f"GET {path} -> {resp.status_code}: {resp.text[:300]}"
                    )
                body = resp.json()
                items.extend(body.get(items_key, []))
                token = body.get("continuationToken")
                if not token:
                    return items
                params = {"continuationToken": token}
        finally:
            if owns:
                client.close()

    def resolve_workspace(self, *, name: str | None = None, id: str | None = None) -> str:
        """Return a workspace GUID from an explicit id or a (unique) display name."""
        if id:
            return id
        if not name:
            raise FabricAPIError("resolve_workspace requires a name or id")
        matches = [
            ws for ws in self._get_pages("/workspaces", "value")
            if ws.get("displayName", "").lower() == name.lower()
        ]
        if not matches:
            raise FabricAPIError(f"No workspace named {name!r} found")
        if len(matches) > 1:
            raise FabricAPIError(
                f"Workspace name {name!r} is ambiguous ({len(matches)} matches); use its id"
            )
        return matches[0]["id"]

    def list_lakehouses(self, workspace_id: str) -> list[LakehouseInfo]:
        items = self._get_pages(f"/workspaces/{workspace_id}/lakehouses", "value")
        return [
            LakehouseInfo(name=it["displayName"], id=it["id"], workspace_id=workspace_id)
            for it in items
        ]

    def list_tables(self, workspace_id: str, lakehouse_id: str) -> list[str]:
        items = self._get_pages(
            f"/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables", "data"
        )
        return [it["name"] for it in items]
