CREATE DATABASE BTC;

CREATE SCHEMA BTC_SCHEMA;

CREATE OR REPLACE STAGE BTC.BTC_SCHEMA.BTC_STAGE
URL='s3://aws-public-blockchain/v1.0/btc/'
FILE_FORMAT=(TYPE=PARQUET);

LIST @BTC.BTC_SCHEMA.BTC_STAGE;


CREATE OR REPLACE WAREHOUSE LARGE_WH
    WAREHOUSE_SIZE = 'LARGE';

SELECT
t.$1:hash AS hashkey,
t.$1:block_hash,
t.$1:block_number,
t.$1:block_timestamp,
t.$1:fee,
t.$1:input_value,
t.$1:output_value AS output_btc,
ROUND(t.$1:fee / t.$1:size, 12) AS fee_per_byte,
t.$1:is_coinbase,
t.$1:outputs

from @BTC.BTC_SCHEMA.BTC_STAGE/transactions/date=2026-02-06 t;


CREATE OR REPLACE TABLE BTC.BTC_SCHEMA.BTC (
  HASH_KEY VARCHAR,
  BLOCK_HASH VARCHAR,
  BLOCK_NUMBER INT,
  BLOCK_TIMESTAMP TIMESTAMP,
  FEE FLOAT,
  INPUT_VALUE FLOAT,
  OUTPUT_VALUE FLOAT,
  FEE_PER_BYTE FLOAT,
  IS_COINBASE BOOLEAN,
  OUTPUTS VARIANT
);


LIST @BTC.BTC_SCHEMA.BTC_STAGE;


CREATE OR REPLACE TASK BTC.BTC_SCHEMA.BTC_LOAD_TASK
WAREHOUSE = LARGE_WH
SCHEDULE = '2 HOUR'
AS

COPY INTO BTC.BTC_SCHEMA.BTC
FROM (
SELECT
t.$1:hash AS hashkey,
t.$1:block_hash,
t.$1:block_number,
t.$1:block_timestamp,
t.$1:fee,
t.$1:input_value,
t.$1:output_value AS output_btc,
ROUND(t.$1:fee / t.$1:size, 12) AS fee_per_byte,
t.$1:is_coinbase,
t.$1:outputs
FROM @BTC.BTC_SCHEMA.BTC_STAGE/transactions t
)
PATTERN = '.*/[0-9]{6,7}[.]snappy[.]parquet';



ALTER TASK BTC.BTC_SCHEMA.BTC_LOAD_TASK SUSPEND;