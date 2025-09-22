from __future__ import annotations
import streamlit as st, pandas as pd
from sttm2flink import GeneratorOptions, generate_sql_from_sttm, bundle_outputs_zip, validate_sql_with_sqlglot

st.set_page_config(page_title="STTM to Flink SQL", page_icon="🛠️", layout="wide")
st.title("STTM to Flink SQL")

# --- Sidebar ---
with st.sidebar:
    st.header("Formatting")
    format_sql = st.toggle("Format SQL", value=True)
    normalize_ws = st.toggle("Normalize whitespace", value=True)
    remove_com = st.toggle("Remove comments", value=True)
    dialect = st.selectbox("SQL dialect", ["flinksql","ansi","hive","spark","trino","presto"], index=0)

    st.header("Outputs")
    emit_report = st.toggle("Emit validation report", value=True)
    emit_create = st.toggle("Generate CREATE TABLE (DDL)", value=True)
    emit_dml = st.toggle("Generate INSERT/SELECT (DML)", value=True)
    emit_view = st.toggle("Emit CREATE VIEW (from SELECT)", value=False)
    emit_stmt = st.toggle("Emit EXECUTE STATEMENT SET (wrap all INSERTs)", value=False)

    st.header("Advanced")
    prefix_mode = st.selectbox("Prefix override", ["Use workbook (Config)", "None", "Custom..."], index=0)
    custom_prefix = ""
    if prefix_mode == "Custom...":
        custom_prefix = st.text_input("Custom prefix", value="cust_")
    apply_cfg_to = st.selectbox("Apply table configuration", ["none","tables","views"], index=0, help="Choose where to apply Config sheet WITH(...) properties")

tabs = st.tabs(['Generate SQL', 'Validation'])

# --- Session ---
if "results" not in st.session_state:
    st.session_state.results = None
    st.session_state.validation = None
    st.session_state.filename = None
if "picker_idx" not in st.session_state:
    st.session_state.picker_idx = 0             # our internal selected index
if "picker_idx_widget" not in st.session_state:
    st.session_state.picker_idx_widget = 0      # the widget's key (separate from our state)
if "uploader_key" not in st.session_state:
    st.session_state.uploader_key = 0           # for resetting file_uploader safely
if "sql_zip" not in st.session_state:
    st.session_state.sql_zip = None

def _sync_picker_from_widget():
    # Copy widget value into our internal state only via callback
    st.session_state.picker_idx = st.session_state.get("picker_idx_widget", 0)

with tabs[0]:
    c1, c2, c3 = st.columns([2,1,1])
    with c1:
        st.subheader("Upload STTM")
        cur = st.file_uploader("CSV or XLSX", type=["csv","xlsx"], key=f"curfile_{st.session_state.uploader_key}")
    with c2:
        st.subheader("Actions")
        run = st.button("Generate SQL", type="primary")
        clear = st.button("Clear", type="secondary", help="Reset the app and clear results")
    with c3:
        st.subheader("Download")
        if st.session_state.results:
            if prefix_mode == "Use workbook (Config)":
                npref = ""
            elif prefix_mode == "None":
                npref = ""
            else:
                npref = custom_prefix
            opts = GeneratorOptions(
                format_sql=format_sql, normalize_whitespace=normalize_ws, remove_comments=remove_com,
                dialect=dialect, emit_validation_report=emit_report, emit_create=emit_create, emit_dml=emit_dml,
                emit_view=emit_view, emit_statement_set=emit_stmt, name_prefix=npref or "", apply_config_to=apply_cfg_to
            )
            data = bundle_outputs_zip(st.session_state.results, opts, st.session_state.validation if emit_report else None)
            st.download_button("SQL bundle (.zip)", data=data, file_name="sttm_sql_outputs.zip", mime="application/zip", use_container_width=False)

    if clear:
        st.session_state.results = None
        st.session_state.validation = None
        st.session_state.filename = None
        st.session_state.picker_idx = 0
        st.session_state.picker_idx_widget = 0
        st.session_state.uploader_key += 1
        st.rerun()

    if run:
        if not (emit_create or emit_dml or emit_view or emit_stmt):
            st.error("Select at least one output: CREATE, DML, VIEW, or STATEMENT SET.")
        elif cur is None:
            st.error("Please upload an STTM file.")
        else:
            if prefix_mode == "Use workbook (Config)":
                name_prefix = ""
            elif prefix_mode == "None":
                name_prefix = ""
            else:
                name_prefix = custom_prefix

            opts = GeneratorOptions(
                format_sql=format_sql, normalize_whitespace=normalize_ws, remove_comments=remove_com,
                dialect=dialect, emit_validation_report=emit_report, emit_create=emit_create, emit_dml=emit_dml,
                emit_view=emit_view, emit_statement_set=emit_stmt, name_prefix=name_prefix, apply_config_to=apply_cfg_to
            )
            items, val = generate_sql_from_sttm(cur.read(), cur.name, opts)
            st.session_state.results = items
            st.session_state.validation = val
            st.session_state.filename = cur.name
            # Reset selection (both internal and widget) BEFORE rendering the widget
            st.session_state.picker_idx = 0
            st.session_state.picker_idx_widget = 0
            
            # prepare the ZIP now so the button can enable immediately
            data = bundle_outputs_zip(items, opts, val)
            st.session_state.sql_zip = data.getvalue() if hasattr(data, "getvalue") else data
            st.success(f"Generated {len(items)} statements from {cur.name}.")


    st.set_page_config(menu_items={"Get Help": None, "Report a bug": None, "About": None})
    st.markdown("""
        <style>
        #MainMenu {visibility: hidden;}
        header {visibility: hidden;}   /* removes the entire top bar, including Deploy */
        footer {visibility: hidden;}
        </style>
    """, unsafe_allow_html=True)

    results = st.session_state.results
    if not results:
        st.info("Upload and click **Generate SQL** to see statements.")
    else:
        index_rows = [{"Schema": i.schema, "Table": i.table, "Op": i.op} for i in results]
        df_index = pd.DataFrame(index_rows)
        st.dataframe(df_index, use_container_width=True, hide_index=True)

        labels = [f"{r['Schema']}.{r['Table']} ({r['Op']})" for r in index_rows]
        # Clamp both indices to range BEFORE instantiating widget
        max_idx = max(len(labels) - 1, 0)
        st.session_state.picker_idx = min(st.session_state.picker_idx, max_idx)
        st.session_state.picker_idx_widget = min(st.session_state.picker_idx_widget, max_idx)

        options = list(range(len(labels)))
        def _fmt(i: int) -> str:
            return labels[i] if 0 <= i < len(labels) else "(invalid)"

        # Use a different key for the widget and update our state only via callback
        st.selectbox(
            "Choose an item",
            options=options,
            index=st.session_state.picker_idx_widget,
            format_func=_fmt,
            key="picker_idx_widget",
            on_change=_sync_picker_from_widget
        )

        # Read the chosen index from our internal state and render the SQL
        chosen = results[st.session_state.picker_idx]
        st.code(chosen.sql, language="sql")


with tabs[1]:
    st.subheader("Validation report (sqlglot: Result, SQL)")
    if st.session_state.results and emit_report:
        report = validate_sql_with_sqlglot(st.session_state.results, read_dialect="hive")
        df = pd.DataFrame(report, columns=["Result","SQL"])
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.caption("ZIP includes consolidated files (create.sql, views.sql, inserts_statement_set.sql) and validation/sqlglot_report.csv.")
    else:
        st.info("Generate SQL with **Emit validation report** enabled to view this tab.")
