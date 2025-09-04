#!/usr/bin/env python3
# v4.6: Flink SQL generator from STTM with static validation + split YAMLs
# - Combined SQL file
# - Validation CSV (result first, then flattened SQL with comments removed)
# - views.yaml, sinks.yaml, inserts.yaml (each one has a single "SQL queries: |" block, with comments removed)
# - Static validation via sqlglot with dialect fallback
# - Robust handling of WITH(...) properties and EXECUTE STATEMENT SET unwrapping

import argparse
import re
from pathlib import Path
from typing import Dict, List, Tuple
import pandas as pd

# Optional static SQL validation via sqlglot
try:
    import sqlglot
    _HAS_SQLGLOT = True
except Exception:
    _HAS_SQLGLOT = False

_VALIDATION_DIALECT = None  # resolved at runtime

# -------------- DataFrame helpers --------------
def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    def n(s: str) -> str:
        return (s or "").strip().lower().replace(" ", "_").replace("-", "_").replace("/", "_")
    df = df.copy()
    df.columns = [n(c) for c in df.columns]
    return df

def safe(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    return "" if s.lower() == "nan" else s

def cfg_get(cfg: pd.DataFrame, key: str, default: str = "") -> str:
    if "key" not in cfg.columns or "value" not in cfg.columns:
        return default
    m = cfg[cfg["key"] == key]
    if m.empty:
        return default
    v = m["value"].iloc[0]
    return default if (isinstance(v, float) and pd.isna(v)) else str(v)

def cfg_bool(cfg: pd.DataFrame, key: str, default: bool = True) -> bool:
    raw = cfg_get(cfg, key, str(default)).strip().lower()
    return raw in {"1", "y", "yes", "true", "on"}

def normalize_ws(s: str) -> str:
    return " ".join(s.split()) if s else ""

def view_name(prefix: str, table: str, suffix: str) -> str:
    return f"{prefix}{table}{suffix}"

def _resolve_sqlglot_dialect() -> str:
    if not _HAS_SQLGLOT:
        return ""
    for name in ["ansi", "hive", "spark", "presto", "postgres"]:
        try:
            sqlglot.parse_one("SELECT 1", read=name)
            return name
        except Exception:
            continue
    return ""

def extract_view_refs(cond: str) -> List[str]:
    if not cond:
        return []
    candidates = re.findall(r"([A-Za-z0-9_]+_vw)\.", cond)
    seen = []
    for c in candidates:
        if c not in seen:
            seen.append(c)
    return seen

# -------------- Section builders --------------
def build_views_section(cfg: pd.DataFrame, sttm: pd.DataFrame) -> str:
    raw_table = cfg_get(cfg, "raw_table_name", "hm_db")
    raw_col   = cfg_get(cfg, "raw_value_column", "val")
    table_field = cfg_get(cfg, "table_identifier_field", "$.tbl")
    vprefix = cfg_get(cfg, "view_prefix", "hm_")
    vsuffix = cfg_get(cfg, "view_suffix", "_vw")

    bysrc: Dict[str, List[str]] = {}
    order: List[str] = []
    for _, r in sttm.iterrows():
        st = safe(r.get("source_table"))
        sc = safe(r.get("source_column"))
        if not st or not sc:
            continue
        if st not in bysrc:
            bysrc[st] = []
            order.append(st)
        if sc not in bysrc[st]:
            bysrc[st].append(sc)

    parts: List[str] = ["-- ===== VIEWS ====="]
    for st in order:
        cols = bysrc[st]
        alias_cols = ", ".join(cols)
        selects = ",\n  ".join([f"json_value(cast({raw_col} as string), '$.{c}')" for c in cols])
        parts.append(
            "create view " + view_name(vprefix, st, vsuffix) + f" ({alias_cols}) as\n"
            + "select\n  " + selects + "\n"
            + f"from {raw_table}\n"
            + "where\n  " + f"json_value(cast({raw_col} as string), '{table_field}') = '{st}';"
        )
    return "\n\n".join(parts) + "\n"

def collect_targets(sttm: pd.DataFrame) -> List[str]:
    vals = [safe(x) for x in sttm.get("target_table", []) if safe(x)]
    out = []
    for v in vals:
        if v not in out:
            out.append(v)
    return out

def sink_with_options(cfg: pd.DataFrame, target: str) -> List[str]:
    opts = []
    # global target_with.*
    for _, row in cfg.iterrows():
        k = safe(row.get("key")); v = safe(row.get("value"))
        if not k or not v: continue
        if k.startswith("target_with."):
            key = k[len("target_with."):]
            opts.append((key, v))
    # per-target with.<table>.*
    for _, row in cfg.iterrows():
        k = safe(row.get("key")); v = safe(row.get("value"))
        if not k or not v: continue
        prefix = f"with.{target}."
        if k.startswith(prefix):
            key = k[len(prefix):]
            opts.append((key, v))
    # default value.format
    seen = {k for k, _ in opts}
    if "value.format" not in seen:
        opts.append(("value.format", cfg_get(cfg, "sink_value_format", "avro-registry")))
    return [f"'{k}'='{v}'" for k, v in opts]

def build_sinks_section(cfg: pd.DataFrame, sttm: pd.DataFrame) -> str:
    bytarget: Dict[str, List[Tuple[str, str]]] = {}
    order = collect_targets(sttm)
    for _, r in sttm.iterrows():
        tt = safe(r.get("target_table"))
        if not tt: continue
        tc = safe(r.get("target_column")) or safe(r.get("source_column"))
        ttyp = safe(r.get("target_data_type")) or safe(r.get("data_type")) or "string"
        if not tc: continue
        bytarget.setdefault(tt, [])
        if tc.lower() not in [c.lower() for c, _ in bytarget[tt]]:
            bytarget[tt].append((tc, ttyp.lower()))

    parts = ["-- ===== SINK TABLES ====="]
    for tt in order:
        cols = bytarget.get(tt, [])
        if not cols: continue
        withs = ", ".join(sink_with_options(cfg, tt))
        col_lines = ",\n  ".join([f"{c} {t}" for c, t in cols])
        parts.append(
            f"drop table if exists {tt};\n"
            + f"create table {tt} (\n  {col_lines}\n)\nwith ({withs});"
        )
    return "\n\n".join(parts) + "\n"

def rows_by_target(sttm: pd.DataFrame, target: str) -> List[dict]:
    rows = []
    for _, r in sttm.iterrows():
        if safe(r.get("target_table")) != target: continue
        rows.append({
            "source_table": safe(r.get("source_table")),
            "source_column": safe(r.get("source_column")),
            "data_type": safe(r.get("data_type")),
            "target_table": target,
            "target_column": safe(r.get("target_column")) or safe(r.get("source_column")),
            "target_type": safe(r.get("target_data_type")) or safe(r.get("data_type")) or "string",
            "expression": safe(r.get("expression")),
            "filter": normalize_ws(safe(r.get("filter"))),
            "join_order": safe(r.get("join_order")),
            "join_type": safe(r.get("join_type")),
            "join_condition": normalize_ws(safe(r.get("join_condition"))),
        })
    return rows

def arm_key(row: dict) -> tuple:
    return (row["filter"], row["join_condition"] or "NOJOIN")

def normalize_join_type(jt: str) -> str:
    s = (jt or "").strip().upper()
    if s in {"", "LEFT"}: return "LEFT OUTER"
    if s in {"LEFT OUTER", "INNER", "RIGHT OUTER", "RIGHT", "FULL OUTER", "FULL"}:
        return {"RIGHT": "RIGHT OUTER", "FULL": "FULL OUTER"}.get(s, s)
    return "LEFT OUTER"

def select_expr_for(row: dict, multi_source: bool, vprefix: str, vsuffix: str) -> str:
    if row["expression"]:
        return row["expression"]
    st, sc = row["source_table"], row["source_column"]
    if multi_source:
        return f"{view_name(vprefix, st, vsuffix)}.{sc}"
    else:
        return f"{sc}"

def driving_view_for(rows: List[dict], vprefix: str, vsuffix: str) -> str:
    def to_int(v: str) -> int:
        try: return int(v)
        except: return 1_000_000
    if not rows: return ""
    rows_sorted = sorted(rows, key=lambda r: (to_int(r.get("join_order", "")), rows.index(r)))
    st = rows_sorted[0]["source_table"]
    return view_name(vprefix, st, vsuffix)

def build_join_block(rows: List[dict], driving_view: str) -> str:
    conds = {}
    def to_int(v: str) -> int:
        try: return int(v)
        except: return 1_000_000
    for r in rows:
        jc = r["join_condition"]
        if not jc: continue
        key = normalize_ws(jc)
        if key not in conds:
            refs = extract_view_refs(jc)
            conds[key] = {
                "join_order": to_int(r.get("join_order", "")),
                "join_type": normalize_join_type(r.get("join_type", "")),
                "join_condition": jc,
                "refs": refs
            }
        else:
            conds[key]["join_order"] = min(conds[key]["join_order"], to_int(r.get("join_order", "")))
    items = sorted(conds.values(), key=lambda x: (x["join_order"], x["join_condition"]))
    used = {driving_view}
    lines = []
    for it in items:
        candidates = [v for v in it["refs"] if v not in used]
        right = candidates[0] if candidates else (it["refs"][0] if it["refs"] else None)
        if not right or right in used:
            continue
        used.add(right)
        lines.append(f"{it['join_type']} JOIN {right} ON {it['join_condition']}")
    return ("\n" + "\n".join(lines)) if lines else ""

def build_insert_for_target(cfg: pd.DataFrame, sttm: pd.DataFrame, target: str) -> str:
    vprefix = cfg_get(cfg, "view_prefix", "hm_")
    vsuffix = cfg_get(cfg, "view_suffix", "_vw")

    rows = rows_by_target(sttm, target)
    if not rows: return ""
    arms: Dict[tuple, List[dict]] = {}
    for r in rows:
        k = arm_key(r)
        arms.setdefault(k, []).append(r)

    tgt_order = []
    for r in rows:
        tc = r["target_column"]
        if tc and tc not in tgt_order: tgt_order.append(tc)

    arm_sqls = []
    for (filt, _), grp in arms.items():
        srcs = []
        for r in grp:
            st = r["source_table"]
            if st and st not in srcs: srcs.append(st)
        multi = len(srcs) > 1
        driving = driving_view_for(grp, vprefix, vsuffix)
        join_block = build_join_block(grp, driving)

        expr_by_tgt = {}
        for r in grp:
            tc = r["target_column"]
            if not tc: continue
            if tc in expr_by_tgt: continue
            expr_by_tgt[tc] = select_expr_for(r, multi, vprefix, vsuffix)

        select_lines = []
        for col in tgt_order:
            expr = expr_by_tgt.get(col, "NULL")
            select_lines.append(f"  {expr} AS {col}")
        select_sql = ",\n".join(select_lines)

        where_clause = f"\nWHERE {filt}" if filt else ""
        arm_sqls.append(f"SELECT\n{select_sql}\nFROM {driving}{join_block}{where_clause}")

    cols_csv = ", ".join(tgt_order)
    if len(arm_sqls) == 1:
        return f"INSERT INTO {target} ({cols_csv})\n{arm_sqls[0]};"
    else:
        return f"INSERT INTO {target} ({cols_csv})\n" + "\nUNION ALL\n".join(arm_sqls) + ";"

def build_inserts_section(cfg: pd.DataFrame, sttm: pd.DataFrame) -> str:
    wrap = cfg_bool(cfg, "wrap_in_statement_set", True)
    targets = collect_targets(sttm)
    inserts = []
    for t in targets:
        ins = build_insert_for_target(cfg, sttm, t)
        if ins: inserts.append(ins)
    if not inserts:
        return "-- ===== INSERTS =====\n-- (none)\n"
    if wrap:
        body = "\n\n".join(inserts)
        return f"-- ===== INSERTS =====\nexecute statement set\nbegin\n{body}\nend;\n"
    else:
        return "-- ===== INSERTS =====\n" + "\n\n".join(inserts) + "\n"

# -------------- Validation helpers --------------
def _strip_with_block_original(s: str) -> str:
    """Strip WITH (...) in CREATE TABLE for parsing only, respecting quotes and parentheses."""
    m = re.search(r"\bWITH\s*\(", s, flags=re.IGNORECASE)
    if not m:
        return s
    start = m.start()
    i = m.end() - 1  # position of '('
    depth = 0
    in_s = False
    in_d = False
    j = i
    while j < len(s):
        ch = s[j]
        if ch == "'" and not in_d:
            in_s = not in_s
        elif ch == '"' and not in_s:
            in_d = not in_d
        if not in_s and not in_d:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    j += 1  # include closing ')'
                    break
        j += 1
    if depth != 0:
        return s  # malformed props; let parser handle
    return s[:start] + s[j:]

def _unwrap_statement_set(sql_text: str) -> str:
    pattern = re.compile(r"execute\s+statement\s+set\s+begin(.*?)end\s*;", flags=re.IGNORECASE | re.DOTALL)
    return pattern.sub(lambda m: m.group(1), sql_text)

def _split_sql_statements(sql_text: str) -> List[str]:
    stmts = []
    buf = []
    in_s = False
    in_d = False
    for ch in sql_text:
        if ch == "'" and not in_d:
            in_s = not in_s
        elif ch == '"' and not in_s:
            in_d = not in_d
        if ch == ";" and not in_s and not in_d:
            stmts.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return [s for s in stmts if s]

def _normalize_literal_escapes(s: str) -> str:
    # Convert literal escapes to real control chars for parsing
    return s.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\t", "\t")

def _strip_sql_comments(s: str) -> str:
    # remove /* ... */ blocks and -- line comments
    s2 = re.sub(r"/\*.*?\*/", "", s, flags=re.DOTALL)
    s2 = re.sub(r"(?m)^\s*--.*$", "", s2)
    return s2

def _is_nonempty_after_comment_strip(s: str) -> bool:
    return bool(_strip_sql_comments(s).strip())

def _flatten_for_csv(stmt: str) -> str:
    # Replace newlines with a single space, collapse multiple spaces/tabs to one.
    s = stmt.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

def validate_statements(sql_text: str) -> List[Tuple[str, str]]:
    """
    Returns list of (original_statement, result_text).
    - Comment lines are ignored for emptiness test
    - Parsing uses comment-stripped text to avoid false errors
    - CSV write uses comment-stripped + flattened text
    """
    results: List[Tuple[str, str]] = []
    unwrapped = _unwrap_statement_set(sql_text)
    statements = _split_sql_statements(unwrapped)
    statements = [s for s in statements if _is_nonempty_after_comment_strip(s)]

    if not _HAS_SQLGLOT:
        return [(s, "SKIPPED: sqlglot not installed") for s in statements]

    global _VALIDATION_DIALECT
    if not _VALIDATION_DIALECT:
        _VALIDATION_DIALECT = _resolve_sqlglot_dialect()
        if not _VALIDATION_DIALECT:
            return [(s, "SKIPPED: no supported sqlglot dialect found") for s in statements]

    for stmt in statements:
        stmt_clean = _strip_sql_comments(_normalize_literal_escapes(stmt))
        if re.search(r"^\s*create\s+table\b", stmt_clean, flags=re.IGNORECASE):
            stmt_clean = _strip_with_block_original(stmt_clean)
        try:
            sqlglot.parse_one(stmt_clean, read=_VALIDATION_DIALECT or None)
            results.append((stmt, "OK"))
        except Exception as e:
            results.append((stmt, f"ERROR: {type(e).__name__}: {e}"))
    return results

# -------------- YAML writer --------------
def write_yaml_without_comments(path: Path, sql_block: str):
    # drop lines that are comments (start with --)
    cleaned_lines = [ln for ln in (sql_block or "").splitlines() if not ln.strip().startswith("--")]
    data = "SQL queries: |\n"
    for line in cleaned_lines:
        data += f"  {line}\n"
    path.write_text(data, encoding="utf-8")

# -------------- Main --------------
def generate(sql_xlsx: Path, out_file: Path):
    xl = pd.ExcelFile(sql_xlsx)
    cfg = norm_cols(pd.read_excel(xl, sheet_name="Config"))
    sttm = norm_cols(pd.read_excel(xl, sheet_name="STTM"))

    views_sql   = build_views_section(cfg, sttm).strip() + "\n"
    sinks_sql   = build_sinks_section(cfg, sttm).strip() + "\n"
    inserts_sql = build_inserts_section(cfg, sttm).strip() + "\n"

    combined = "\n\n".join([views_sql, sinks_sql, inserts_sql])
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(combined, encoding="utf-8")

    # Validation CSV: result first, then SQL; comments removed and whitespace flattened
    results = validate_statements(combined)
    csv_path = Path(str(out_file) + "_validation.csv")
    with csv_path.open("w", encoding="utf-8") as f:
        f.write("result,sql\n")
        for stmt, res in results:
            stmt_clean_for_csv = _strip_sql_comments(stmt)
            s = _flatten_for_csv(stmt_clean_for_csv).replace('"', '""')
            r = res.replace('"', '""')
            f.write(f"\"{r}\",\"{s}\"\n")

    # YAML outputs without comments
    base_dir = out_file.parent
    write_yaml_without_comments(base_dir / "views.yaml",   views_sql.strip())
    write_yaml_without_comments(base_dir / "sinks.yaml",   sinks_sql.strip())
    write_yaml_without_comments(base_dir / "inserts.yaml", inserts_sql.strip())

    print("[done] wrote", out_file)
    print("[done] validation ->", csv_path)
    print("[done] yaml ->", base_dir / "views.yaml", ",", base_dir / "sinks.yaml", ",", base_dir / "inserts.yaml")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", required=True, help="Path to STTM workbook")
    ap.add_argument("--out-file", required=True, help="Path to single Flink SQL output file")
    args = ap.parse_args()
    generate(Path(args.excel), Path(args.out_file))

if __name__ == "__main__":
    main()
