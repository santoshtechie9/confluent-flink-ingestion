
#!/usr/bin/env python3
"""
sttm_codegen_from_template_v2.py
--------------------------------
Quality-updated generator for Flink SQL from the updated STTM template.

Improvements vs v1:
- Deterministic column ordering in SELECT & DDL based on first appearance in STTM.
- Stricter validation of required columns; helpful error messages.
- Cleaner JOIN builder:
  * Sorts by Join Order (int), then stable by condition text
  * Uses driving table; picks the "right table" from condition that isn't yet in the FROM clause
  * Skips malformed/empty join conditions gracefully
- Safer expression handling (whitespace & duplication guards).
- DDL emitter:
  * Orders columns by STTM first appearance
  * Optional PRIMARY KEY NOT ENFORCED if Is PK = Y on any Target Column
- Config passthrough for optional target connector (commented placeholders left if unspecified).
- More readable output structure & logging hints.

Usage:
  python sttm_codegen_from_template_v2.py --excel STTM.xlsx --out-dir out

"""

import argparse
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set, Any

import pandas as pd


# ---------------- Utilities ----------------

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    def norm(s: str) -> str:
        s = (s or "").strip().lower().replace(" ", "_").replace("-", "_").replace("/", "_")
        return s
    df = df.copy()
    df.columns = [norm(c) for c in df.columns]
    return df


def safe_str(v) -> str:
    import pandas as pd
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    s = str(v).strip()
    return "" if s.lower() == "nan" else s

def load_sheets(xls_path: Path, sttm_sheet: Optional[str], config_sheet: str = "Config") -> Tuple[pd.DataFrame, pd.DataFrame]:
    xl = pd.ExcelFile(xls_path)
    df_sttm = pd.read_excel(xl, sheet_name=sttm_sheet or "STTM")
    df_cfg  = pd.read_excel(xl, sheet_name=config_sheet)
    return normalize_headers(df_sttm), normalize_headers(df_cfg)

def cfg_get(df_cfg: pd.DataFrame, key: str, default: Optional[str] = None) -> Optional[str]:
    if "key" not in df_cfg.columns or "value" not in df_cfg.columns:
        return default
    m = df_cfg[df_cfg["key"] == key]
    if m.empty: return default
    v = m["value"].iloc[0]
    if isinstance(v, float) and pd.isna(v): return default
    return str(v)

def flink_type(typ: Any) -> str:
    if typ is None or (isinstance(typ, float) and pd.isna(typ)): return "STRING"
    s = str(typ).strip().upper()
    m = re.match(r"NUMBER\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)", s)
    if m:
        p = int(m.group(1)); sc = int(m.group(2)) if m.group(2) else 0
        if sc == 0 and p <= 18: return "BIGINT"
        return f"DECIMAL({p},{sc})"
    if s in {"INTEGER","INT","SMALLINT","TINYINT","BIGINT"}:
        # Flink supports all; but normalize INT-ish to BIGINT unless explicit
        return "BIGINT" if s != "BIGINT" else "BIGINT"
    if s in {"FLOAT","DOUBLE","BINARY_DOUBLE"}: return "DOUBLE"
    m = re.match(r"(VAR)?CHAR2?\s*\(\s*(\d+)\s*\)", s)
    if m: return f"VARCHAR({m.group(2)})"
    if "TIMESTAMP" in s: return "TIMESTAMP(3)"
    if s == "DATE": return "DATE"
    if s in {"STRING","BOOLEAN","BYTES","VARBINARY"}: return s
    # Fallback
    return "STRING"

def vs_alias(table_name: str) -> str:
    return f"{table_name}_VS"

def detect_columns(df: pd.DataFrame) -> Dict[str, str]:
    want = {
        "sourcedb": ["sourcedb","source_db","schema"],
        "source_table": ["source_table","table_name","table"],
        "source_column": ["source_column","column_name","column"],
        "data_type": ["data_type","datatype","type"],
        "is_pk": ["is_pk","pk"],
        "is_fk": ["is_fk","fk"],
        "is_not_null": ["is_not_null","not_null","required"],
        "default_value": ["default_value","default"],
        "target_table": ["target_table"],
        "target_column": ["target_column","target_col"],
        "target_type": ["target_data_type","target_type"],
        "expression": ["expression","transformation_logic","expr"],
        "filter": ["filter","where"],
        "join_order": ["join_order","join_priority","order"],
        "join_type": ["join_type"],
        "join_condition": ["join_condition","join"],
    }
    colmap = {}
    for k, cand in want.items():
        for c in cand:
            if c in df.columns:
                colmap[k] = c
                break
    # Validation
    missing = [k for k in ["source_table","source_column","data_type"] if k not in colmap]
    if missing:
        raise SystemExit(f"STTM is missing required column(s): {missing}. "
                         f"Found columns: {list(df.columns)}")
    return colmap

TRUE_SET = {"y","yes","true","1",1,True}
def as_bool(v) -> bool:
    if v is None or (isinstance(v, float) and pd.isna(v)): return False
    return str(v).strip().lower() in TRUE_SET


# ---------------- SQL emitters ----------------

def raw_events_sql(topic: str, brokers: str, table_path: str, payload_path: str) -> str:
    return f"""
-- RAW Kafka events (string value). Envelope via JSON_VALUE/JSON_QUERY
CREATE TABLE RAW_EVENTS (
  `value` STRING,
  `table_name` AS JSON_VALUE(value, '{table_path}'),
  `payload`    AS JSON_QUERY(value, '{payload_path}')
) WITH (
  'connector' = 'kafka',
  'topic'     = '{topic}',
  'properties.bootstrap.servers' = '{brokers}',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'raw'
);
""".strip()+"\n"

def json_cast_expr(payload_col: str, field: str, flink_typ: str) -> str:
    return f"CAST(JSON_VALUE({payload_col}, '$.{field}') AS {flink_typ}) AS {field}"

def build_per_table_view(table: str, cols_in_order: List[Tuple[str,str]]) -> str:
    
    inner = [ json_cast_expr("payload", c, t) for (c,t) in cols_in_order ]
    outer = ",\n  ".join([ c for (c,_) in cols_in_order ])
    inner_joined = ",\n    ".join(inner)
    return f"""
-- Staged view for {table} (typed casts only)
CREATE OR REPLACE TEMPORARY VIEW {vs_alias(table)} AS
SELECT
  {outer}
FROM (
  SELECT
    {inner_joined}
  FROM RAW_EVENTS
  WHERE table_name = '{table}'
) t;
""".strip()+"\n"


def parse_tables_in_condition(cond: str) -> List[str]:
    # Return order-preserving list of aliases mentioned with _VS
    found = re.findall(r"([A-Za-z_][A-Za-z0-9_]*)_VS", cond or "")
    seen = []
    for f in found:
        a = f + "_VS"
        if a not in seen:
            seen.append(a)
    return seen

def choose_driving_table(all_tables: List[str], join_rows_sorted: List[Tuple[int,str,str,List[str]]]) -> str:
    # Prefer *_ADR if present
    for t in all_tables:
        if str(t).upper().endswith("_ADR"):
            return t
    # Else the first table appearing in the earliest join condition
    if join_rows_sorted:
        aliases = parse_tables_in_condition(join_rows_sorted[0][2])
        # Strip _VS back to base name if possible
        if aliases:
            a0 = aliases[0]
            if a0.endswith("_VS"):
                return a0[:-3]
    # Fallback: first listed table alphabetically for stability
    return sorted(all_tables)[0]

def build_join_block(join_rows_sorted: List[Tuple[int,str,str,List[str]]], driving_vs: str) -> str:
    lines = []
    used = {driving_vs}
    for order, jtype, cond, aliases in join_rows_sorted:
        # Determine a "right" table that's not yet used; else pick the first alias not the driving
        right = None
        for a in aliases:
            if a not in used and a != driving_vs:
                right = a
                break
        if right is None:
            for a in aliases:
                if a != driving_vs:
                    right = a
                    break
        if right is None:
            # malformed; skip
            continue
        if right in used:
            continue
        used.add(right)
        lines.append(f"{jtype} JOIN {right} ON {cond}")
    return ("\n" + "\n".join(lines)) if lines else ""


def _row_weight_for_target(r, colmap, driving_vs: str) -> int:
    src_tbl = safe_str(r.get(colmap["source_table"]))
    expr = safe_str(r.get(colmap.get("expression","")))
    if vs_alias(src_tbl) == driving_vs:
        return 2
    if expr:
        return 1
    return 0


def build_select_list(df: pd.DataFrame, colmap: Dict[str,str], driving_vs: str) -> List[str]:
    # Group rows by target column and choose the best mapping
    groups: Dict[str, List[pd.Series]] = {}
    for _, r in df.iterrows():
        src_tbl  = safe_str(r.get(colmap["source_table"]))
        src_col  = safe_str(r.get(colmap["source_column"]))
        tgt_col  = safe_str(r.get(colmap.get("target_column",""))) or src_col
        if not src_tbl or not src_col or not tgt_col:
            continue
        groups.setdefault(tgt_col.lower(), []).append(r)

    # Determine deterministic order by first appearance in the sheet
    order = []
    for _, r in df.iterrows():
        tgt = safe_str(r.get(colmap.get("target_column",""))) or safe_str(r.get(colmap["source_column"]))
        if not tgt:
            continue
        k = tgt.lower()
        if k not in order and k in groups:
            order.append(k)

    out = []
    for k in order:
        rows = groups[k]
        # pick best row by weight
        best = sorted(rows, key=lambda r: _row_weight_for_target(r, colmap, driving_vs), reverse=True)[0]
        src_tbl  = safe_str(best.get(colmap["source_table"]))
        src_col  = safe_str(best.get(colmap["source_column"]))
        tgt_col  = safe_str(best.get(colmap.get("target_column",""))) or src_col
        expr_raw = safe_str(best.get(colmap.get("expression","")))
        default  = best.get(colmap.get("default_value",""), "")
        is_nn    = as_bool(best.get(colmap.get("is_not_null",""), ""))
        alias = vs_alias(src_tbl)
        base  = f"{alias}.{src_col}"
        sel_expr = expr_raw if expr_raw else base
        if is_nn and not (default is None or (isinstance(default, float) and pd.isna(default)) or str(default).strip()==""):
            if not sel_expr.strip().upper().startswith("COALESCE("):
                sel_expr = f"COALESCE({sel_expr}, {str(default).strip()})"
        out.append(f"{sel_expr} AS {tgt_col}")
    return out


def build_where(df: pd.DataFrame, colmap: Dict[str,str]) -> str:
    if "filter" not in colmap: return ""
    vals = [str(x).strip() for x in df[colmap["filter"]].dropna().tolist() if str(x).strip()]
    if not vals: return ""
    uniq = list(dict.fromkeys(vals))
    return " AND ".join(f"({v})" for v in uniq)

def ordered_unique_targets(df: pd.DataFrame, colmap: Dict[str,str]) -> List[Tuple[str,str]]:
    """Return list of (target_col, target_type) in first-appearance order."""
    out = []
    seen = set()
    for _, r in df.iterrows():
        tc = r.get(colmap.get("target_column",""))
        tt = r.get(colmap.get("target_type",""))
        if tc is None or (isinstance(tc, float) and pd.isna(tc)) or not str(tc).strip():
            continue
        name = str(tc).strip()
        if name.lower() in seen: 
            continue
        seen.add(name.lower())
        out.append((name, flink_type(tt)))
    return out

def build_target_ddl(df_sttm: pd.DataFrame, colmap: Dict[str,str], target_table: str, cfg: pd.DataFrame) -> str:
    cols = ordered_unique_targets(df_sttm, colmap)
    if not cols:
        return ""

    # Primary key columns from STTM where Is PK = Y for the corresponding target column
    pk_cols = []
    for _, r in df_sttm.iterrows():
        tc = r.get(colmap.get("target_column",""))
        if tc is None or (isinstance(tc, float) and pd.isna(tc)): 
            continue
        if as_bool(r.get(colmap.get("is_pk",""))):
            name = str(tc).strip()
            if name and name not in pk_cols:
                pk_cols.append(name)

    col_lines = [f"  {name} {typ}" for (name, typ) in cols]
    pk_clause = (",\n  PRIMARY KEY (" + ", ".join(pk_cols) + ") NOT ENFORCED") if pk_cols else ""

    # Optional connector hints from Config
    target_connector = cfg_get(cfg, "target_connector", None)  # e.g., 'jdbc' or 'iceberg'
    with_lines = []
    if target_connector:
        with_lines.append(f"'connector' = '{target_connector}'")
        # Allow arbitrary passthrough of options like target_with.option_name via Config
        for _, row in cfg.iterrows():
            k = str(row.get("key",""))
            v = row.get("value", "")
            if k.startswith("target_with.") and v not in (None, "", float("nan")):
                opt = k[len("target_with."):]
                with_lines.append(f"'{opt}' = '{v}'")
    else:
        # Leave placeholders
        with_lines.append("-- TODO: connector options here (e.g., 'connector'='jdbc', 'url'='...', 'table-name'='...')")

    ddl = "CREATE TABLE {t} (\n{cols}{pk}\n) WITH (\n  {withs}\n);\n".format(
        t=target_table,
        cols=",\n".join(col_lines),
        pk=pk_clause,
        withs=",\n  ".join(with_lines)
    )
    return ddl


# ---------------- Main ----------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", required=True, help="Path to updated STTM Excel")
    ap.add_argument("--sttm-sheet", default="STTM")
    ap.add_argument("--config-sheet", default="Config")
    ap.add_argument("--out-dir", default="out")
    ap.add_argument("--target-table", help="Override target table")
    ap.add_argument("--topic")
    ap.add_argument("--brokers")
    ap.add_argument("--table-path")
    ap.add_argument("--payload-path")
    args = ap.parse_args()

    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)

    df_sttm, df_cfg = load_sheets(Path(args.excel), args.sttm_sheet, args.config_sheet)
    colmap = detect_columns(df_sttm)

    # Resolve config
    topic = args.topic or cfg_get(df_cfg, "kafka_topic", "my.schema.topic")
    brokers = args.brokers or cfg_get(df_cfg, "bootstrap_servers", "localhost:9092")
    table_path = args.table_path or cfg_get(df_cfg, "table_identifier_jsonpath", "$.table")
    payload_path = args.payload_path or cfg_get(df_cfg, "payload_jsonpath", "$.payload")

    # Target table
    tgt = args.target_table
    if not tgt and "target_table" in colmap:
        vals = [str(x).strip() for x in df_sttm[colmap["target_table"]].dropna().unique().tolist() if str(x).strip()]
        if vals: tgt = vals[0]
    if not tgt:
        raise SystemExit("Target table not provided. Use --target-table or fill 'Target Table' in STTM.")

    # Emit RAW_EVENTS
    (out_dir / "00_raw_events.sql").write_text(raw_events_sql(topic, brokers, table_path, payload_path), encoding="utf-8")

    # Build per-table views in a stable table order
    # Order tables by first appearance
    table_order = []
    table_cols: Dict[str, List[Tuple[str,str]]] = {}
    for _, r in df_sttm.iterrows():
        t = str(r.get(colmap["source_table"])).strip()
        c = str(r.get(colmap["source_column"])).strip()
        if not t or not c: 
            continue
        if t not in table_order:
            table_order.append(t)
        ty = flink_type(r.get(colmap["data_type"]))
        table_cols.setdefault(t, [])
        if c not in [x[0] for x in table_cols[t]]:
            table_cols[t].append((c, ty))

    views_sql = []
    for t in table_order:
        views_sql.append(build_per_table_view(t, table_cols[t]))
    (out_dir / "01_per_table_views.sql").write_text("\n\n".join(views_sql), encoding="utf-8")

    # JOINs
    join_rows = []
    seen_conds = set()
    if "join_condition" in colmap:
        for _, r in df_sttm.iterrows():
            cond = str(r.get(colmap["join_condition"]) or "").strip()
            if not cond:
                continue
            cond_norm = " ".join(cond.split())
            if cond_norm in seen_conds:
                continue
            seen_conds.add(cond_norm)
            try:
                jo = int(r.get(colmap.get("join_order",""), 999999) or 999999)
            except Exception:
                jo = 999999
            jt = str(r.get(colmap.get("join_type",""), "") or "LEFT").strip().upper()
            aliases = parse_tables_in_condition(cond_norm)
            if not aliases:
                continue
            join_rows.append((jo, jt, cond_norm, aliases))
    join_rows_sorted = sorted(join_rows, key=lambda x: (x[0], x[2]))

    driving_table = choose_driving_table(table_order, join_rows_sorted)
    driving_vs = vs_alias(driving_table)

    # SELECT list in first-appearance order of Target Column
    select_list = build_select_list(df_sttm, colmap, driving_vs)
    if not select_list:
        raise SystemExit("No columns produced for SELECT. Ensure STTM has Source/Target columns populated.")

    # WHERE clause (combined filters)
    where_clause = build_where(df_sttm, colmap)

    # DDL for target (ordered by first-appearance of Target Column)
    ddl_sql = build_target_ddl(df_sttm, colmap, tgt, df_cfg)
    if ddl_sql:
        (out_dir / f"05_create_{tgt}.sql").write_text(ddl_sql, encoding="utf-8")

    # Final INSERT…SELECT
    join_block = build_join_block(join_rows_sorted, driving_vs)
    final_sql = "INSERT INTO {tgt}\nSELECT\n  {sel}\nFROM {driving}{joins}{where};\n".format(
        tgt=tgt,
        sel=",\n  ".join(select_list),
        driving=driving_vs,
        joins=join_block,
        where=(f"\nWHERE {where_clause}" if where_clause else "")
    )
    (out_dir / f"10_insert_{tgt}.sql").write_text(final_sql, encoding="utf-8")

    print("[done] Wrote SQL files to", out_dir)


if __name__ == "__main__":
    main()
