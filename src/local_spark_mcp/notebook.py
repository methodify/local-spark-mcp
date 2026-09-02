"""Parse the Fabric notebook Git source format (``notebook-content.py``).

The format has no published spec; this follows the empirically derived one in
the claude-fabric project (byte-level analysis of the workspace corpus). A file
is a flat sequence of blocks introduced by marker lines:

    # METADATA ********************   a "# META "-prefixed JSON block (file-level
                                      first; then one FOLLOWING each code cell)
    # CELL ********************       a code cell (source verbatim)
    # PARAMETERS CELL ****...         a code cell tagged as the parameters cell
    # MARKDOWN ********************   markdown, every line prefixed "# "

Cell magics that switch language (only ``%%sql`` is attested) prefix every line
of the cell with "# MAGIC ". Line magics (``%pip``, ``!pip``) are stored raw in a
python cell. Format problems are reported as warnings; parsing never refuses a
file (the runner runs it anyway and reports).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

HEADER = "# Fabric notebook source"
MARKER_RE = re.compile(r"^# (METADATA|CELL|MARKDOWN|PARAMETERS CELL) (\*+)$")
META_PREFIX = "# META "
MAGIC_PREFIX = "# MAGIC "
# %pip / !pip / %run at column 0 — but not %%cell magics
LINE_MAGIC_RE = re.compile(r"^[%!](?!%)\S")
CELL_MAGIC_RE = re.compile(r"^%%(\w+)")


@dataclass
class Cell:
    index: int  # position among ALL cells (markdown included), 0-based
    kind: str  # "code" | "parameters" | "markdown"
    language: str  # "python" | "sparksql" | "markdown" | other cell-magic name
    source: str  # decoded: MAGIC / markdown prefixes stripped, %%magic line removed
    line: int  # 1-based line number of the marker in the file
    meta: dict = field(default_factory=dict)
    magics: list[str] = field(default_factory=list)  # raw line magics found at col 0
    cell_magic: str | None = None  # "sql" for %%sql, "configure", ...

    @property
    def runnable(self) -> bool:
        return self.kind != "markdown"


@dataclass
class Notebook:
    path: Path | None
    meta: dict
    cells: list[Cell]
    warnings: list[str]

    @property
    def lakehouse_meta(self) -> dict:
        return (self.meta.get("dependencies") or {}).get("lakehouse") or {}

    @property
    def default_lakehouse_name(self) -> str | None:
        return self.lakehouse_meta.get("default_lakehouse_name")

    @property
    def default_lakehouse_id(self) -> str | None:
        return self.lakehouse_meta.get("default_lakehouse")

    @property
    def workspace_id(self) -> str | None:
        return self.lakehouse_meta.get("default_lakehouse_workspace_id")

    @property
    def has_parameters_cell(self) -> bool:
        return any(c.kind == "parameters" for c in self.cells)

    def code_cells(self) -> list[Cell]:
        return [c for c in self.cells if c.runnable]


def _parse_meta(lines: list[str], line_no: int, warnings: list[str]) -> dict:
    payload = []
    for raw in lines:
        if raw.startswith(META_PREFIX):
            payload.append(raw[len(META_PREFIX):])
        elif raw.strip() == "# META":
            payload.append("")
        elif raw.strip() == "":
            continue
        else:
            warnings.append(f"line {line_no}: non-META line inside a METADATA block: {raw[:60]!r}")
    try:
        return json.loads("\n".join(payload)) if payload else {}
    except json.JSONDecodeError as exc:
        warnings.append(f"line {line_no}: METADATA JSON does not parse ({exc.msg})")
        return {}


def _decode_code(content: list[str]) -> tuple[str, str, str | None, list[str]]:
    """Return (source, language, cell_magic, line_magics) for a code block."""
    nonblank = [l for l in content if l.strip()]
    if nonblank and all(l.startswith("# MAGIC") for l in nonblank):
        decoded = [
            l[len(MAGIC_PREFIX):] if l.startswith(MAGIC_PREFIX) else ("" if l.strip() == "# MAGIC" else l)
            for l in content
        ]
    else:
        decoded = list(content)
    language, cell_magic = "python", None
    # first non-blank line may be a %%magic
    for i, l in enumerate(decoded):
        if not l.strip():
            continue
        m = CELL_MAGIC_RE.match(l.strip())
        if m:
            cell_magic = m.group(1).lower()
            language = "sparksql" if cell_magic == "sql" else cell_magic
            decoded = decoded[:i] + decoded[i + 1:]
        break
    magics = [l for l in decoded if LINE_MAGIC_RE.match(l)] if language == "python" else []
    return "\n".join(decoded), language, cell_magic, magics


def _decode_markdown(content: list[str]) -> str:
    out = []
    for l in content:
        if l.startswith("# "):
            out.append(l[2:])
        elif l == "#":
            out.append("")
        else:
            out.append(l)  # trailing bare blanks and anything odd, verbatim
    return "\n".join(out)


def parse_notebook_source(text: str, path: Path | None = None) -> Notebook:
    lines = text.replace("\r\n", "\n").split("\n")
    warnings: list[str] = []
    if not lines or lines[0] != HEADER:
        warnings.append(f"line 1: expected header {HEADER!r}")

    markers = []
    for i, l in enumerate(lines):
        m = MARKER_RE.match(l)
        if m:
            markers.append((i, m.group(1), len(m.group(2))))
            if len(m.group(2)) != 20:
                warnings.append(f"line {i + 1}: marker has {len(m.group(2))} asterisks (expected 20)")

    blocks: list[tuple[str, int, list[str]]] = []
    for n, (i, kind, _) in enumerate(markers):
        last = n + 1 == len(markers)
        end = len(lines) if last else markers[n + 1][0]
        content = lines[i + 1:end]
        if content and content[0] == "":  # separator after marker
            content = content[1:]
        if content and content[-1] == "":  # separator before next marker / EOF newline
            content = content[:-1]
        blocks.append((kind, i + 1, content))

    meta: dict = {}
    cells: list[Cell] = []
    pending: Cell | None = None  # code cell awaiting its METADATA block
    for bi, (kind, line_no, content) in enumerate(blocks):
        if kind == "METADATA":
            obj = _parse_meta(content, line_no, warnings)
            if bi == 0:
                meta = obj
            elif pending is not None:
                pending.meta = obj
                lang = obj.get("language")
                if lang == "sparksql" and pending.cell_magic != "sql":
                    warnings.append(f"line {pending.line}: META says sparksql but the cell has no %%sql magic")
                elif pending.cell_magic == "sql" and lang not in (None, "sparksql"):
                    warnings.append(f"line {pending.line}: %%sql cell but META language is {lang!r}")
                pending = None
            else:
                warnings.append(f"line {line_no}: METADATA block with no preceding code cell")
            continue
        if pending is not None:
            warnings.append(f"line {pending.line}: code cell has no METADATA block")
            pending = None
        if kind == "MARKDOWN":
            cells.append(Cell(len(cells), "markdown", "markdown", _decode_markdown(content), line_no))
            continue
        source, language, cell_magic, magics = _decode_code(content)
        cell = Cell(
            len(cells),
            "parameters" if kind == "PARAMETERS CELL" else "code",
            language,
            source,
            line_no,
            magics=magics,
            cell_magic=cell_magic,
        )
        cells.append(cell)
        pending = cell
    if pending is not None:
        warnings.append(f"line {pending.line}: code cell has no METADATA block")
    if not blocks or blocks[0][0] != "METADATA":
        warnings.append("file-level METADATA block is missing")
    return Notebook(path=path, meta=meta, cells=cells, warnings=warnings)


def load_notebook(path: str | Path) -> Notebook:
    p = Path(path)
    if p.is_dir():  # a `<name>.Notebook/` folder
        p = p / "notebook-content.py"
    return parse_notebook_source(p.read_text(encoding="utf-8"), path=p)


def select_cells(spec, count: int) -> set[int]:
    """Cell selection: None = all; ints; or strings like "3", "2-5", "0,4-6"."""
    if spec is None:
        return set(range(count))
    items = spec if isinstance(spec, (list, tuple)) else str(spec).split(",")
    chosen: set[int] = set()
    for item in items:
        if isinstance(item, int):
            chosen.add(item)
            continue
        token = str(item).strip()
        if not token:
            continue
        if "-" in token:
            a, b = token.split("-", 1)
            chosen.update(range(int(a), int(b) + 1))
        else:
            chosen.add(int(token))
    bad = sorted(i for i in chosen if i < 0 or i >= count)
    if bad:
        raise IndexError(f"cell index out of range: {bad} (notebook has {count} cells)")
    return chosen


def strip_line_magics(cell: Cell) -> tuple[str, list[str]]:
    """Source with %pip / !pip / %run lines removed, plus those lines."""
    if not cell.magics:
        return cell.source, []
    kept = [l for l in cell.source.split("\n") if not LINE_MAGIC_RE.match(l)]
    return "\n".join(kept), list(cell.magics)


def index_notebooks(root: str | Path) -> dict[str, Path]:
    """displayName -> notebook-content.py for every ``*.Notebook/.platform``
    under root. Folder names often differ from display names, so resolution
    goes through .platform."""
    root = Path(root).expanduser()
    found: dict[str, Path] = {}
    if not root.is_dir():
        return found
    for platform in root.rglob(".platform"):
        try:
            md = json.loads(platform.read_text(encoding="utf-8")).get("metadata", {})
        except (OSError, json.JSONDecodeError):
            continue
        if md.get("type") != "Notebook":
            continue
        content = platform.parent / "notebook-content.py"
        if content.is_file() and md.get("displayName"):
            found[md["displayName"]] = content
    return found
