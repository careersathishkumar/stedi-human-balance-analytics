CREATE EXTERNAL TABLE IF NOT EXISTS stedi.accelerometer_landing (
  user STRING,
  timestamp BIGINT,
  x DOUBLE,
  y DOUBLE,
  z DOUBLE
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
LOCATION 's3://stedi-project-sathish/accelerometer/landing/'
TBLPROPERTIES ('has_encrypted_data'='false');