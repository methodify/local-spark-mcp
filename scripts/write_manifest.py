#!/usr/bin/env python3
"""Write profiles.json (the runtime-profile manifest) at the repo root from
local_spark_mcp.profiles. Run after editing profiles.py; a test checks they match."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from local_spark_mcp.profiles import manifest  # noqa: E402

out = Path(__file__).resolve().parents[1] / "profiles.json"
out.write_text(json.dumps(manifest(), indent=2) + "\n", encoding="utf-8")
print(f"wrote {out}")
