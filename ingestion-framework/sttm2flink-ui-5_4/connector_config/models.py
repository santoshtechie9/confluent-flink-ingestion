
from __future__ import annotations
from typing import List, Optional, Literal, Dict
from pydantic import BaseModel, Field, validator

ConnectorType = Literal["GCS Source", "GCS Sink"]

class CommonOptions(BaseModel):
    name: str = Field(..., description="Connector name")
    kafka_api_key: Optional[str] = Field(default=None, description="Confluent Cloud API Key (or ${secrets:...})")
    kafka_api_secret: Optional[str] = Field(default=None, description="Confluent Cloud API Secret (or ${secrets:...})")
    tasks_max: int = Field(default=1, ge=1, le=32)
    errors_tolerance: Literal["none","all"] = "none"
    dlq_topic: Optional[str] = None
    extra: Dict[str, str] = Field(default_factory=dict, description="Custom properties to merge")

class GcsSourceOptions(CommonOptions):
    gcs_bucket: str = Field(..., description="GCS bucket name")
    gcs_prefix: Optional[str] = None
    credentials_json: Optional[str] = Field(default=None, description="GCS Credentials JSON or ${secrets:...}")
    input_file_pattern: str = Field(default=r".*\.(json|csv|avro|parquet)$")
    format: Literal["json","csv","avro","parquet"] = "json"
    kafka_topic: str = Field(..., description="Output Kafka topic")
    poll_interval_ms: int = Field(default=60000, ge=1000)
    on_finish: Literal["leave","move","delete"] = "leave"
    archive_prefix: Optional[str] = None

    @validator("archive_prefix")
    def require_prefix_if_move(cls, v, values):
        if values.get("on_finish") == "move" and not v:
            raise ValueError("archive_prefix is required when on_finish=move")
        return v

class GcsSinkOptions(CommonOptions):
    gcs_bucket: str = Field(..., description="GCS bucket name")
    gcs_prefix: Optional[str] = None
    credentials_json: Optional[str] = Field(default=None, description="GCS Credentials JSON or ${secrets:...}")
    topics: List[str] = Field(default_factory=list, description="Input Kafka topics")
    format_class: Literal[
        "io.confluent.connect.gcs.format.json.JsonFormat",
        "io.confluent.connect.gcs.format.avro.AvroFormat",
        "io.confluent.connect.gcs.format.parquet.ParquetFormat",
        "io.confluent.connect.gcs.format.bytearray.ByteArrayFormat",
        "io.confluent.connect.gcs.format.csv.CsvFormat"
    ] = "io.confluent.connect.gcs.format.json.JsonFormat"
    flush_size: int = Field(default=1000, ge=1)
    rotate_interval_ms: int = Field(default=300000, ge=0)
    partitioner_class: Literal[
        "io.confluent.connect.storage.partitioner.DefaultPartitioner",
        "io.confluent.connect.storage.partitioner.FieldPartitioner",
        "io.confluent.connect.storage.partitioner.TimeBasedPartitioner"
    ] = "io.confluent.connect.storage.partitioner.DefaultPartitioner"
    path_format: Optional[str] = None

    @validator("topics")
    def non_empty_topics(cls, v):
        if not v:
            raise ValueError("At least one topic is required")
        return v
