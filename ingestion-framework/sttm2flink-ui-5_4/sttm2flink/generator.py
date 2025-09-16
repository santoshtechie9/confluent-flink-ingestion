from __future__ import annotations
import io, re
from typing import Dict, List, Tuple, Optional
import pandas as pd
import chardet
from pydantic import BaseModel
from .models import GeneratorOptions
from .utils import remove_sql_comments, normalize_sql_whitespace

class GeneratedSQL(BaseModel):
    schema: str
    table: str
    op: str
    sql: str

def _norm(s: str) -> str:
    return re.sub(r'[^a-z0-9]+', '_', str(s).strip().lower())

def load_sttm_dataframe(sttm_bytes: bytes, filename: str, sheet: Optional[str] = None) -> pd.DataFrame:
    if filename.lower().endswith('.xlsx'):
        xls = pd.ExcelFile(io.BytesIO(sttm_bytes))
        use = None
        if sheet and sheet in xls.sheet_names:
            use = sheet
        elif "STTM" in xls.sheet_names:
            use = "STTM"
        else:
            use = xls.sheet_names[0]
        return xls.parse(use)
    enc = chardet.detect(sttm_bytes).get('encoding') or 'utf-8'
    return pd.read_csv(io.StringIO(sttm_bytes.decode(enc)))

def _detect_format(df: pd.DataFrame) -> str:
    cols = { _norm(c) for c in df.columns }
    if {'target_table','target_column'}.issubset(cols):
        return 'column-spec'
    if {'schema','table'}.issubset(cols):
        return 'row-per-table'
    return 'column-spec'

def _normalize_sql_type(t: str) -> str:
    t = str(t).strip().lower()
    if re.match(r'decimal\s*\(\s*\d+\s*,\s*\d+\s*\)', t):
        return re.sub(r'\s+', '', t).upper()
    if t in {'string','varchar','text','char'}: return 'STRING'
    if t in {'int','integer'}: return 'INT'
    if t in {'bigint','long'}: return 'BIGINT'
    if t in {'double','float','real'}: return 'DOUBLE'
    if t in {'date'}: return 'DATE'
    if t in {'timestamp','datetime'}: return 'TIMESTAMP(3)'
    if t in {'boolean','bool'}: return 'BOOLEAN'
    return t.upper()

# --- Config helpers ---
def load_config_from_excel(sttm_bytes: bytes) -> Dict[str, str]:
    cfg: Dict[str, str] = {}
    try:
        xls = pd.ExcelFile(io.BytesIO(sttm_bytes))
        if "Config" not in xls.sheet_names:
            return cfg
        df = xls.parse("Config")
        if df.empty:
            return cfg
        if df.shape[1] >= 2:
            key_col, val_col = df.columns[:2]
            for _, row in df.iterrows():
                k = str(row.get(key_col, "")).strip()
                v = "" if pd.isna(row.get(val_col, None)) else str(row.get(val_col)).strip()
                if k:
                    cfg[k.lower()] = v
        return cfg
    except Exception:
        return cfg

def _parse_with_from_config(cfg: Dict[str, str], base_table_name: str) -> Dict[str, str]:
    props: Dict[str,str] = {}
    if not cfg: return props
    tkey = str(base_table_name).lower()
    for k, v in cfg.items():
        lk = k.lower().strip()
        if lk.startswith("with."):
            props[lk[5:]] = v
        elif lk.startswith("table.") and ".with." in lk:
            try:
                _, rest = lk.split("table.", 1)
                t, rest2 = rest.split(".with.", 1)
                if t.strip() == tkey:
                    props[rest2.strip()] = v
            except Exception:
                continue
    return props

def _build_with_clause(props: Dict[str,str]) -> str:
    if not props: return ""
    lines = []
    for k in sorted(props.keys()):
        v = props[k].replace("'", "''")
        lines.append(f"  '{k}' = '{v}'")
    inner = ",\n".join(lines)
    return f"\nWITH (\n{inner}\n)"

def _quote_qualified(ident: str) -> str:
    ident = str(ident).strip()
    if not ident:
        return ident
    if '.' in ident:
        parts = [p.strip('` ') for p in ident.split('.', 1)]
        if len(parts) == 2:
            return f'`{parts[0]}`.`{parts[1]}`'
    if ident.startswith('`') and ident.endswith('`'):
        return ident
    return f'`{ident}`'

def generate_sql_from_sttm(sttm_bytes: bytes, filename: str, options: GeneratorOptions):
    df = load_sttm_dataframe(sttm_bytes, filename, options.excel_sheet)
    cfg = load_config_from_excel(sttm_bytes) if filename.lower().endswith('.xlsx') else {}
    fmt = options.sttm_format if options.sttm_format != 'auto' else _detect_format(df)
    if fmt != 'column-spec':
        items: List[GeneratedSQL] = []
        for i, row in df.iterrows():
            schema = str(row.get('schema','public'))
            table = str(row.get('table', f'table_{i}'))
            table_pref = (options.name_prefix or '') + table
            if options.emit_create:
                cols = [c for c in df.columns if c not in ('schema','table')]
                cols_sql = ',\n  '.join([f'`{c}` STRING' for c in cols]) or '`id` STRING'
                ddl = f'CREATE TABLE `{schema}`.`{table_pref}` (\n  {cols_sql}\n);'
                items.append(GeneratedSQL(schema=schema, table=table_pref, op='CREATE', sql=ddl))
        return items, {'rows':len(df),'columns':list(df.columns)}

    # column-spec path
    cols = { _norm(c): c for c in df.columns }
    t_table = cols['target_table']
    t_col = cols['target_column']
    t_type = cols.get('target_data_type')
    s_table = cols.get('source_table')
    s_col = cols.get('source_column')
    expr_col = cols.get('expression')
    filter_col = cols.get('filter')
    join_order_col = cols.get('join_order')
    join_type_col = cols.get('join_type')
    join_cond_col = cols.get('join_condition')

    items: List[GeneratedSQL] = []
    insert_sqls: List[str] = []
    validation = {'tables':{}, 'warnings':[], 'errors':[], 'warnings_by_table':{}, 'errors_by_table':{}, 'source_tables_by_target':{}}

    # --- DDL + INSERT per target table (unchanged) ---
    for table_name, grp in df.groupby(t_table):
        schema = 'public'
        base_name = str(table_name).strip()
        table_name_pref = (options.name_prefix or '') + base_name

        # DDL
        if options.emit_create:
            defs = []
            for _, row in grp.iterrows():
                colname = str(row[t_col]).strip()
                dtype = _normalize_sql_type(row[t_type]) if t_type else 'STRING'
                if not colname:
                    validation['warnings'].append(f'Empty target column for table {base_name}')
                    continue
                defs.append(f'`{colname}` {dtype}')
            body = ',\n  '.join(defs) if defs else '`id` STRING'
            ddl_sql = f'CREATE TABLE `{schema}`.`{table_name_pref}` (\n  {body}\n);'
            items.append(GeneratedSQL(schema=schema, table=table_name_pref, op='CREATE', sql=ddl_sql))
            validation['tables'][table_name_pref] = len(defs)

        # Build SELECT plan
        joins = []
        from_clause = None
        if s_table is not None:
            join_df = grp.copy()
            if join_order_col and join_order_col in join_df.columns:
                join_df['_order'] = pd.to_numeric(join_df[join_order_col], errors='coerce').fillna(0)
            else:
                join_df['_order'] = range(len(join_df))
            distinct_sources = join_df.dropna(subset=[s_table]).drop_duplicates(subset=[s_table]).sort_values('_order')[s_table].tolist()
            if distinct_sources:
                base = distinct_sources[0]
                base_alias = f'{_norm(base)}'
                from_clause = f'`{base}` AS `{base_alias}`'
                for src in distinct_sources[1:]:
                    alias = f'{_norm(src)}'
                    row_match = grp[grp[s_table] == src].iloc[0]
                    jtype = 'INNER JOIN'
                    if join_type_col and pd.notna(row_match.get(join_type_col, None)):
                        jtype = str(row_match[join_type_col]).strip().upper()
                        if 'JOIN' not in jtype:
                            jtype += ' JOIN'
                    jcond = None
                    if join_cond_col and pd.notna(row_match.get(join_cond_col, None)):
                        jcond = str(row_match[join_cond_col]).strip()
                    join_sql = f'{jtype} `{src}` AS `{alias}`'
                    if jcond:
                        join_sql += f' ON {jcond}'
                    joins.append(join_sql)

        # select list & where
        dml_targets = []
        for _, row in grp.iterrows():
            colname = str(row[t_col]).strip()
            if expr_col is not None and pd.notna(row.get(expr_col, None)):
                src_expr = str(row[expr_col]).strip()
            elif s_table is not None and s_col is not None and pd.notna(row.get(s_table, None)) and pd.notna(row.get(s_col, None)):
                stbl = str(row[s_table]).strip()
                scol = str(row[s_col]).strip()
                alias = f'{_norm(stbl)}'
                src_expr = f'`{alias}`.`{scol}`'
            else:
                src_expr = 'NULL'
            dml_targets.append((colname, src_expr))

        select_list = ',\n    '.join([f'{expr} AS `{c}`' for c, expr in dml_targets])
        where_parts = []
        if filter_col is not None and filter_col in grp.columns:
            for fval in grp[filter_col].dropna().unique().tolist():
                ftxt = str(fval).strip()
                if ftxt:
                    where_parts.append(f'({ftxt})')
        where_clause = f"\nWHERE {' AND '.join(where_parts)}" if where_parts else ''

        # INSERT (collect always; emit if requested)
        if from_clause:
            join_sql = '\n    '.join(joins) if joins else ''
            dml_sql = f'''INSERT INTO `{schema}`.`{table_name_pref}`
SELECT
    {select_list}
FROM {from_clause}
    {join_sql}{where_clause};'''
        else:
            dml_sql = f'''INSERT INTO `{schema}`.`{table_name_pref}`
SELECT
    {select_list}'''
        insert_sqls.append(dml_sql)
        if options.emit_dml:
            items.append(GeneratedSQL(schema=schema, table=table_name_pref, op='INSERT', sql=dml_sql))

    # --- NEW VIEW LOGIC: build one view per Source Table using JSON_VALUE from a single source topic ---
    if options.emit_view:
        src_from = cfg.get('source.view.from', 'public.events')
        json_col = cfg.get('source.json.column', 'value')
        tbl_path = cfg.get('source.json.tbl_path', '$.tbl')
        src_map: Dict[str, List[str]] = {}
        s_table_col = cols.get('source_table')
        s_col_col = cols.get('source_column')
        if s_table_col and s_col_col:
            for _, row in df.iterrows():
                s_tbl = str(row.get(s_table_col, '') or '').strip()
                s_c = str(row.get(s_col_col, '') or '').strip()
                if s_tbl and s_c:
                    src_map.setdefault(s_tbl, [])
                    if s_c not in src_map[s_tbl]:
                        src_map[s_tbl].append(s_c)

        for s_tbl, s_cols in src_map.items():
            if not s_cols:
                continue
            view_base = s_tbl
            view_name = f"{(options.name_prefix or '')}{view_base}{options.view_suffix or ''}"
            select_lines = []
            for c in s_cols:
                path = f"$.{c}"
                select_lines.append(f"JSON_VALUE({json_col}, '{path}') AS `{c}`")
            select_sql = ",\n  ".join(select_lines)
            from_sql = _quote_qualified(src_from) + " AS `e`"
            view_sql = f"""CREATE VIEW `public`.`{view_name}` AS
SELECT
  {select_sql}
FROM {from_sql}
WHERE JSON_VALUE({json_col}, '{tbl_path}') = '{s_tbl}';"""
            items.append(GeneratedSQL(schema='public', table=view_name, op='VIEW', sql=view_sql))

    # Apply config to CREATE TABLEs
    if options.apply_config_to == 'tables':
        patched = []
        for it in items:
            if it.op == 'CREATE':
                base = it.table
                pref = options.name_prefix or ''
                base_no_pref = base[len(pref):] if pref and base.lower().startswith(pref.lower()) else base
                props = _parse_with_from_config(cfg, base_no_pref)
                with_clause = _build_with_clause(props)
                if with_clause:
                    if it.sql.strip().endswith(';'):
                        it.sql = it.sql.strip()[:-1] + with_clause + ';'
                    else:
                        it.sql = it.sql.strip() + with_clause + ';'
            patched.append(it)
        items = patched

    # Apply config to VIEWS as a comment
    if options.apply_config_to == 'views':
        for it in items:
            if it.op == 'VIEW':
                base = it.table
                pref = options.name_prefix or ''
                base_no_pref = base[len(pref):] if pref and base.lower().startswith(pref.lower()) else base
                vsuf = options.view_suffix or ''
                if vsuf and base_no_pref.endswith(vsuf):
                    base_no_pref = base_no_pref[:-len(vsuf)]
                props = _parse_with_from_config(cfg, base_no_pref)
                if props:
                    comment = ' /* VIEW CONFIG: ' + '; '.join([f"{k}={v}" for k,v in sorted(props.items())]) + ' */'
                    it.sql = it.sql.rstrip(';') + comment + ';'

    # Statement set at the end (always uses all collected INSERTs)
    if options.emit_statement_set and insert_sqls:
        inserts = [s if s.strip().endswith(';') else s.strip()+';' for s in insert_sqls]
        body = "\n  ".join(inserts)
        stmt = f"EXECUTE STATEMENT SET\nBEGIN\n  {body}\nEND;"
        items.append(GeneratedSQL(schema='public', table='__all__', op='STATEMENT_SET', sql=stmt))

    validation = {'rows': len(df), 'columns': list(df.columns)}
    return items, validation