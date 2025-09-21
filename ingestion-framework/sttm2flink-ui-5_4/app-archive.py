from __future__ import annotations
import streamlit as st, pandas as pd
from sttm2flink import GeneratorOptions, generate_sql_from_sttm, bundle_outputs_zip, validate_sql_with_sqlglot

st.set_page_config(page_title="STTM → Flink SQL Generator (v5.3.3)", page_icon="🛠️", layout="wide")
st.title("STTM → Flink SQL Generator (v5.3.3)")

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

tabs = st.tabs(["Generate SQL", "Validation", "Connector Config"])

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
            st.download_button("SQL bundle (.zip)", data=data, file_name="sttm_sql_outputs.zip", mime="application/zip", use_container_width=True)

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
            st.success(f"Generated {len(items)} statements from {cur.name}.")

    st.markdown("---")
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

# ==============================
# Connector Config (Confluent Cloud – GCS)
# ==============================
with tabs[2]:
    st.subheader("Connector Config (Confluent Cloud – GCS)")

    from connector_config.models import GcsSourceOptions, GcsSinkOptions
    from connector_config.renderers import build_gcs_source_config, build_gcs_sink_config
    import json

    conn_type = st.selectbox("Connector type", ["GCS Source","GCS Sink"], index=0, key="conn_type")

    st.markdown("### Common")
    c1, c2 = st.columns(2)
    with c1:
        cc_name = st.text_input("Connector name", value="gcs-connector")
        api_key = st.text_input("Kafka API key", value="${secrets:CCLOUD_API_KEY}")
        tasks_max = st.number_input("tasks.max", min_value=1, max_value=32, value=1, step=1)
        errors_tol = st.selectbox("errors.tolerance", ["none","all"], index=0)
    with c2:
        api_secret = st.text_input("Kafka API secret", value="${secrets:CCLOUD_API_SECRET}")
        dlq_topic = st.text_input("DLQ topic (optional)", value="")
        extra_props = st.text_area("Custom properties (key=value per line)", value="", height=100)

    extra = {}
    for line in extra_props.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            extra[k.strip()] = v.strip()

    if conn_type == "GCS Source":
        st.markdown("### GCS Source")
        s1, s2 = st.columns(2)
        with s1:
            bucket = st.text_input("gcs.bucket.name", value="my-bucket")
            prefix = st.text_input("gcs.prefix (optional)", value="")
            credentials = st.text_area("gcs.credentials.config (JSON or ${secrets:...})", value="${secrets:GCS_CREDENTIALS}", height=80)
            fmt = st.selectbox("format", ["json","csv","avro","parquet"], index=0)
        with s2:
            topic = st.text_input("kafka.topic", value="gcs_source_topic")
            file_pattern = st.text_input("input.file.pattern", value=r".*\.(json|csv|avro|parquet)$")
            poll_ms = st.number_input("poll.interval.ms", min_value=1000, value=60000, step=500)
            post = st.selectbox("On finish", ["leave","move","delete"], index=0)
            archive_prefix = st.text_input("archive.prefix (required if move)", value="archive/")

        src_opts = GcsSourceOptions(
            name=cc_name,
            kafka_api_key=api_key or None,
            kafka_api_secret=api_secret or None,
            tasks_max=tasks_max,
            errors_tolerance=errors_tol,
            dlq_topic=dlq_topic or None,
            extra=extra,
            gcs_bucket=bucket,
            gcs_prefix=prefix or None,
            credentials_json=credentials or None,
            input_file_pattern=file_pattern,
            format=fmt,
            kafka_topic=topic,
            poll_interval_ms=int(poll_ms),
            on_finish=post,
            archive_prefix=archive_prefix or None,
        )

        cfg = build_gcs_source_config(src_opts)
        st.markdown("#### Preview JSON")
        st.code(json.dumps({"name": src_opts.name, "config": cfg}, indent=2), language="json")
        st.download_button("Download JSON", data=json.dumps({"name": src_opts.name, "config": cfg}, indent=2), file_name=f"gcs-source-{src_opts.name}.json", mime="application/json")

    else:
        st.markdown("### GCS Sink")
        s1, s2 = st.columns(2)
        with s1:
            bucket = st.text_input("gcs.bucket.name", value="my-bucket")
            prefix = st.text_input("gcs.prefix (optional)", value="")
            credentials = st.text_area("gcs.credentials.config (JSON or ${secrets:...})", value="${secrets:GCS_CREDENTIALS}", height=80)
            fmt_class = st.selectbox("format.class", [
                "io.confluent.connect.gcs.format.json.JsonFormat",
                "io.confluent.connect.gcs.format.csv.CsvFormat",
                "io.confluent.connect.gcs.format.avro.AvroFormat",
                "io.confluent.connect.gcs.format.parquet.ParquetFormat",
                "io.confluent.connect.gcs.format.bytearray.ByteArrayFormat",
            ], index=0)
        with s2:
            topics_csv = st.text_input("topics (comma-separated)", value="topic1,topic2")
            flush_size = st.number_input("flush.size", min_value=1, value=1000, step=100)
            rotate_ms = st.number_input("rotate.interval.ms", min_value=0, value=300000, step=1000)
            partitioner = st.selectbox("partitioner.class", [
                "io.confluent.connect.storage.partitioner.DefaultPartitioner",
                "io.confluent.connect.storage.partitioner.FieldPartitioner",
                "io.confluent.connect.storage.partitioner.TimeBasedPartitioner"
            ], index=0)
            path_fmt = st.text_input("path.format (optional)", value="")

        sink_opts = GcsSinkOptions(
            name=cc_name,
            kafka_api_key=api_key or None,
            kafka_api_secret=api_secret or None,
            tasks_max=tasks_max,
            errors_tolerance=errors_tol,
            dlq_topic=dlq_topic or None,
            extra=extra,
            gcs_bucket=bucket,
            gcs_prefix=prefix or None,
            credentials_json=credentials or None,
            topics=[t.strip() for t in topics_csv.split(",") if t.strip()],
            format_class=fmt_class,
            flush_size=int(flush_size),
            rotate_interval_ms=int(rotate_ms),
            partitioner_class=partitioner,
            path_format=path_fmt or None,
        )

        cfg = build_gcs_sink_config(sink_opts)
        st.markdown("#### Preview JSON")
        st.code(json.dumps({"name": sink_opts.name, "config": cfg}, indent=2), language="json")
        st.download_button("Download JSON", data=json.dumps({"name": sink_opts.name, "config": cfg}, indent=2), file_name=f"gcs-sink-{sink_opts.name}.json", mime="application/json")
