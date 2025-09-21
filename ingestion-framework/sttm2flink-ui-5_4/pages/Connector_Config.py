import streamlit as st
from connector_config.models import GcsSourceOptions, GcsSinkOptions
from connector_config.renderers import build_gcs_source_config, build_gcs_sink_config
import json

st.set_page_config(page_title="Connector Config", layout="wide")
st.title("Connector Config (Confluent Cloud – GCS)")

st.subheader("Connector Config (Confluent Cloud – GCS)")
from connector_config.models import GcsSourceOptions, GcsSinkOptions
from connector_config.renderers import build_gcs_source_config, build_gcs_sink_config
import json

conn_type = st.selectbox("Connector type", ["GCS Source","GCS Sink"], index=0, key="cc_conn_type")

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
