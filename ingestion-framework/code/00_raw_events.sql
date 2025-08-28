-- RAW Kafka events (string value). Envelope via JSON_VALUE/JSON_QUERY
CREATE TABLE RAW_EVENTS (
  `value` STRING,
  `table_name` AS JSON_VALUE(value, '$.table'),
  `payload`    AS JSON_QUERY(value, '$.payload')
) WITH (
  'connector' = 'kafka',
  'topic'     = 'my.schema.topic',
  'properties.bootstrap.servers' = 'localhost:9092',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'raw'
);
