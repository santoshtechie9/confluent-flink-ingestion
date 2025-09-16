
from __future__ import annotations
from typing import Dict
from .models import GcsSourceOptions, GcsSinkOptions

def _base_common(o) -> Dict[str, str]:
    cfg = {
        "name": o.name,
        "tasks.max": str(o.tasks_max),
        "errors.tolerance": o.errors_tolerance,
    }
    if o.kafka_api_key: cfg["kafka.api.key"] = o.kafka_api_key
    if o.kafka_api_secret: cfg["kafka.api.secret"] = o.kafka_api_secret
    if o.dlq_topic: cfg["errors.deadletterqueue.topic.name"] = o.dlq_topic
    for k, v in (o.extra or {}).items():
        cfg[str(k)] = str(v)
    return cfg

def build_gcs_source_config(o: GcsSourceOptions) -> Dict[str, str]:
    cfg = _base_common(o)
    cfg.update({
        "connector.class": "GcsSource",
        "gcs.bucket.name": o.gcs_bucket,
        "kafka.topic": o.kafka_topic,
        "input.file.pattern": o.input_file_pattern,
        "format": o.format,
        "poll.interval.ms": str(o.poll_interval_ms),
    })
    if o.credentials_json: cfg["gcs.credentials.config"] = o.credentials_json
    if o.gcs_prefix: cfg["gcs.prefix"] = o.gcs_prefix
    if o.on_finish == "delete":
        cfg["behavior.on.error"] = "delete"
    elif o.on_finish == "move":
        cfg["archive.prefix"] = o.archive_prefix or ""
    return cfg

def build_gcs_sink_config(o: GcsSinkOptions) -> Dict[str, str]:
    cfg = _base_common(o)
    cfg.update({
        "connector.class": "GcsSink",
        "gcs.bucket.name": o.gcs_bucket,
        "format.class": o.format_class,
        "flush.size": str(o.flush_size),
        "rotate.interval.ms": str(o.rotate_interval_ms),
        "partitioner.class": o.partitioner_class,
        "topics": ",".join(o.topics),
    })
    if o.credentials_json: cfg["gcs.credentials.config"] = o.credentials_json
    if o.gcs_prefix: cfg["gcs.prefix"] = o.gcs_prefix
    if o.path_format: cfg["path.format"] = o.path_format
    return cfg
