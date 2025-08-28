
#!/usr/bin/env python3
"""
sttm_to_flink_sql.py
--------------------
Convert a Source-to-Target Mapping (STTM) Excel document into Flink SQL.

Supported layouts
-----------------
A) Current layout (minimal)
   Columns (case-insensitive, space-insensitive):
     - SourceDB
     - View Name           (recommended to use as short alias like CI, ADR, XREF)
     - Table name          (source table)
     - Column name
     - Data type
     - Primary/Foreign key (string; e.g., PK, FK, blank)
     - Not Null            (Y/N/True/False/1/0)
     - Null                (ignored if Not Null present)
     - Join                (free-text; ex: "LEFT JOIN XREF.CI_ID = ADR.CI_ID")
     - Transformation Logic (SQL expression; ex: "TRIM(ADR.ADDR_LINE1)")

   Assumptions:
     - Target table is provided via CLI (--target-table).
     - We'll drive the INSERT .. SELECT from a "driving" table, defaulting to the child table if present
       or else the first table found. You can override with --driving-alias.

B) Recommended richer layout (auto-detected if columns are present)
   Additional columns (optional):
     - Target Schema       (or TargetDB)
     - Target Table
     - Target Column
     - Target Data Type
     - Expression          (preferred over "Transformation Logic" if present)
     - Join Type, Left Alias, Right Alias, Join Condition
     - Filter              (WHERE clause fragment)
     - Load Mode           (append/merge/upsert)
     - Grain / Natural Key (comma-separated columns for uniqueness)

Usage
-----
python sttm_to_flink_sql.py --excel path/to/sttm.xlsx \
       --sheet STTM --target-table CBA_CI_ADR_FGAC \
       --catalog mycat --database mydb --out-dir ./out

Output
------
- out/staging_ddls.sql     : CREATE VIEW/TABLE stubs for each source table (placeholders for connectors)
- out/insert_CBA_CI_ADR_FGAC.sql : INSERT..SELECT based on your mapping

Notes
-----
- Without connector options, the DDLs are emitted as CREATE VIEW statements that reference the 
  catalog.db.table (or just table if not provided). Replace with actual connectors as needed.
- Data type mapping is best-effort; adjust TYPE_MAP inside this script for your environment.
"""

import argparse
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd


# ------------------------- Utilities -------------------------

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    def norm(s: str) -> str:
        s = (s or "").strip().lower()
        s = s.replace(" ", "_")
        s = s.replace("-", "_")
        s = s.replace("/", "_")
        return s
    df = df.copy()
    df.columns = [norm(c) for c in df.columns]
    return df


TRUE_SET = {"y", "yes", "true", "1", 1, True}
FALSE_SET = {"n", "no", "false", "0", 0, False}


def as_bool(v) -> Optional[bool]:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip().lower()
    if s in TRUE_SET:
        return True
    if s in FALSE_SET:
        return False
    return None


# Heuristic type map to Flink SQL
def to_flink_type(raw: str) -> str:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return "STRING"
    s = str(raw).strip().upper()

    # NUMBER(p[,s])
    m = re.match(r"NUMBER\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)", s)
    if m:
        p = int(m.group(1))
        s_ = int(m.group(2)) if m.group(2) else 0
        if s_ == 0 and p <= 18:
            return "BIGINT"
        return f"DECIMAL({p},{s_})"

    # INTEGER / INT
    if s in {"INTEGER", "INT"}:
        return "BIGINT"

    # FLOAT/DOUBLE
    if s in {"FLOAT", "DOUBLE", "BINARY_DOUBLE"}:
        return "DOUBLE"

    # VARCHAR2(n) / VARCHAR(n)
    m = re.match(r"(VAR)?CHAR2?\s*\(\s*(\d+)\s*\)", s)
    if m:
        n = m.group(2)
        return f"VARCHAR({n})"

    # DATE/TIMESTAMP
    if "TIMESTAMP" in s:
        return "TIMESTAMP(3)"
    if s == "DATE":
        return "DATE"

    # Default
    return s  # STRING, BOOLEAN, etc. fall through


def prefer(a, b):
    return a if a not in (None, "", float("nan")) else b


# ------------------------- Parsing -------------------------

def load_sttm(path: Path, sheet: Optional[str]) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet) if sheet else pd.read_excel(path)
    return normalize_headers(df)


def detect_columns(df: pd.DataFrame) -> Dict[str, str]:
    # Map canonical keys to actual dataframe columns (best-effort)
    want = {
        "sourcedb": ["sourcedb", "source_db", "source_database", "schema"],
        "view_name": ["view_name", "alias", "src_alias"],
        "table_name": ["table_name", "source_table", "src_table", "table"],
        "column_name": ["column_name", "source_column", "src_column", "column"],
        "data_type": ["data_type", "datatype", "type"],
        "pkfk": ["primary_foreign_key", "primary_forigen_key", "primary_foreign", "primary_key", "pk_fk", "primary_forigen_key"],
        "not_null": ["not_null", "required"],
        "null": ["null"],
        "join": ["join", "join_condition", "join_clause"],
        "xform": ["transformation_logic", "expression", "expr"],
        # Target-side (optional)
        "target_schema": ["target_schema", "targetdb", "target_db"],
        "target_table": ["target_table"],
        "target_column": ["target_column", "target_col"],
        "target_type": ["target_data_type", "target_type"],
        "filter": ["filter", "where"],
        "grain": ["grain", "natural_key", "unique_key"],
        "load_mode": ["load_mode", "mode"],
    }
    colmap = {}
    for canon, candidates in want.items():
        for c in candidates:
            if c in df.columns:
                colmap[canon] = c
                break
    return colmap


# ------------------------- Generation -------------------------

def build_alias(row, colmap) -> str:
    # Prefer explicit view_name; else derive alias from table_name suffix
    alias = None
    if "view_name" in colmap:
        alias = row.get(colmap["view_name"])
    alias = str(alias).strip() if alias is not None and not pd.isna(alias) else None
    if alias:
        return alias

    t = str(row.get(colmap.get("table_name", ""), "")).strip()
    if not t:
        return "T"
    # Heuristic: take last token after underscores as short alias
    return t.split("_")[-1][:6].upper()


def collect_tables(df: pd.DataFrame, colmap: Dict[str, str]) -> Dict[str, Dict]:
    """
    Returns {alias: {"db":..., "table":..., "full":...}} mapping.
    """
    tables = {}
    for _, r in df.iterrows():
        tname = str(r.get(colmap.get("table_name", ""), "")).strip()
        if not tname:
            continue
        alias = build_alias(r, colmap)
        db = str(r.get(colmap.get("sourcedb", ""), "")).strip()
        key = alias
        if key not in tables:
            tables[key] = {
                "db": db,
                "table": tname,
                "full": ".".join([x for x in [db, tname] if x]),
                "alias": alias
            }
    return tables


def pick_driving_alias(tables: Dict[str, Dict]) -> str:
    # Prefer ADR/child if present
    for cand in ["ADR", "CBA_CI_ADR", "CI_ADR"]:
        for alias in tables:
            if alias.upper() == cand or tables[alias]["table"].upper().endswith("_ADR"):
                return alias
    # else arbitrary stable pick
    return sorted(tables.keys())[0]


def gather_joins(df: pd.DataFrame, colmap: Dict[str, str]) -> List[str]:
    if "join" not in colmap:
        return []
    raw = df[colmap["join"]].dropna().astype(str).str.strip()
    out = []
    seen = set()
    for j in raw:
        j1 = " ".join(j.split())
        if not j1 or j1 in seen:
            continue
        seen.add(j1)
        # Ensure we start with a JOIN keyword
        if not re.match(r"(?i)^\s*(left|right|inner|full)\s+join\s+", j1):
            j1 = "LEFT JOIN " + j1
        out.append(j1)
    return out


def map_columns(df: pd.DataFrame, colmap: Dict[str, str], driving_alias: str, prefer_target_cols=True) -> List[str]:
    """
    Build select list expressions: <expr> AS <target_column>
    """
    out = []
    for _, r in df.iterrows():
        src_col = str(r.get(colmap.get("column_name", ""), "")).strip()
        if not src_col:
            continue

        alias = build_alias(r, colmap)
        base_ref = f"{alias}.{src_col}"

        expr = str(r.get(colmap.get("xform", ""), "")).strip()
        expr = expr if expr else base_ref

        tgt_col = None
        if prefer_target_cols and "target_column" in colmap:
            tgt_col = str(r.get(colmap["target_column"])).strip() if r.get(colmap["target_column"]) is not None else None
        if not tgt_col:
            tgt_col = src_col  # fallback

        out.append(f"{expr} AS {tgt_col}")
    # de-duplicate while preserving order
    seen = set()
    dedup = []
    for e in out:
        key = e.split(" AS ")[-1].strip().lower()
        if key in seen:
            continue
        seen.add(key)
        dedup.append(e)
    return dedup


def build_where(df: pd.DataFrame, colmap: Dict[str, str]) -> str:
    if "filter" in colmap:
        filt = df[colmap["filter"]].dropna().astype(str).str.strip()
        uniq = [f for f in filt if f]
        uniq = list(dict.fromkeys(uniq))
        if uniq:
            return " AND ".join(f"({f})" for f in uniq)
    # Also derive NOT NULL guards if provided
    parts = []
    if "not_null" in colmap:
        for _, r in df.iterrows():
            if as_bool(r.get(colmap["not_null"])):
                src_col = str(r.get(colmap.get("column_name", ""), "")).strip()
                alias = build_alias(r, colmap)
                if src_col:
                    parts.append(f"{alias}.{src_col} IS NOT NULL")
    if parts:
        return " AND ".join(parts)
    return ""


def emit_staging_ddls(tables: Dict[str, Dict], catalog: str, database: str) -> str:
    lines = ["-- Staging views for source tables (replace with connectors if needed)"]
    for alias, meta in tables.items():
        full = ".".join([x for x in [catalog, database, meta["full"]] if x])
        # In many orgs you'll reference catalog.db.schema.table; adjust accordingly
        lines.append(dedent(f"""
        -- Source: {meta["full"]} as {alias}
        CREATE OR REPLACE TEMPORARY VIEW {alias} AS
        SELECT * FROM {full};
        """).strip())
    return "\n\n".join(lines) + "\n"


def assemble_insert_sql(target_table: str,
                        select_list: List[str],
                        driving_alias: str,
                        joins: List[str],
                        where_clause: str) -> str:
    sel = ",\n  ".join(select_list) if select_list else "*"
    join_block = ""
    if joins:
        join_block = "\n" + "\n".join(joins)
    where_block = f"\nWHERE {where_clause}" if where_clause else ""

    return dedent(f"""
    INSERT INTO {target_table}
    SELECT
      {sel}
    FROM {driving_alias} {join_block}{where_block};
    """).strip() + "\n"


def maybe_target_from_sheet(df: pd.DataFrame, colmap: Dict[str, str], cli_target: str) -> str:
    if "target_table" in colmap:
        vals = [str(x).strip() for x in df[colmap["target_table"]].dropna().unique().tolist() if str(x).strip()]
        if vals:
            # If multiple, we pick the first and warn the user via print
            if len(vals) > 1:
                print(f"[warn] Multiple target tables detected in sheet: {vals}. Using '{vals[0]}'")
            return vals[0]
    return cli_target


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--excel", required=True, help="Path to the STTM Excel (.xlsx)")
    ap.add_argument("--sheet", help="Worksheet name (defaults to first sheet)")
    ap.add_argument("--target-table", help="Target table name (used if not present in sheet)")
    ap.add_argument("--driving-alias", help="Alias to drive the FROM clause (e.g., ADR)")
    ap.add_argument("--catalog", default="", help="Optional Flink catalog to prefix sources")
    ap.add_argument("--database", default="", help="Optional Flink database to prefix sources")
    ap.add_argument("--out-dir", default="out", help="Output directory for SQL files")
    args = ap.parse_args()

    xls = Path(args.excel)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_sttm(xls, args.sheet)
    colmap = detect_columns(df)

    tables = collect_tables(df, colmap)
    if not tables:
        raise SystemExit("No source tables found. Check 'Table name' column.")

    driving_alias = args.driving_alias or pick_driving_alias(tables)
    print(f"[info] Driving alias: {driving_alias}")

    # Target table resolve
    target = maybe_target_from_sheet(df, colmap, args.target_table)
    if not target:
        raise SystemExit("Target table not provided. Use --target-table or include 'Target Table' column.")

    # Emit staging DDLs
    ddls_sql = emit_staging_ddls(tables, args.catalog, args.database)
    (out_dir / "staging_ddls.sql").write_text(ddls_sql, encoding="utf-8")

    # Build SELECT list
    select_list = map_columns(df, colmap, driving_alias, prefer_target_cols=True)

    # Join and WHERE
    joins = gather_joins(df, colmap)
    where_clause = build_where(df, colmap)

    insert_sql = assemble_insert_sql(target, select_list, driving_alias, joins, where_clause)
    (out_dir / f"insert_{target}.sql").write_text(insert_sql, encoding="utf-8")

    # Optional: emit a best-effort target DDL if target types present
    if "target_column" in colmap and "target_type" in colmap:
        cols = []
        seen = set()
        for _, r in df.iterrows():
            tc = r.get(colmap["target_column"])
            if tc is None or (isinstance(tc, float) and pd.isna(tc)):
                continue
            tctype = to_flink_type(str(r.get(colmap["target_type"])))
            key = str(tc).strip().lower()
            if key in seen:
                continue
            seen.add(key)
            cols.append(f"  {tc} {tctype}")
        if cols:
            ddl = "CREATE TABLE IF NOT EXISTS {t} (\n{cols}\n) WITH (\n  -- TODO: connector options here\n);\n".format(
                t=target, cols=",\n".join(cols)
            )
            (out_dir / f"ddl_{target}.sql").write_text(ddl, encoding="utf-8")

    print(f"[done] Wrote:\n - {out_dir/'staging_ddls.sql'}\n - {out_dir/f'insert_{target}.sql'}\n"
          f"{' - ' + str(out_dir/f'ddl_{target}.sql') if (out_dir/f'ddl_{target}.sql').exists() else ''}")


if __name__ == "__main__":
    main()
