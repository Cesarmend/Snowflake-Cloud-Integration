SELECT
*
FROM BTC.BTC_SCHEMA.BTC
limit 10;

select
      max(BLOCK_TIMESTAMP) as max_loaded_at,
      convert_timezone('UTC', current_timestamp()) as snapshotted_at
    from btc.btc_schema.btc;


EXECUTE task  BTC.BTC_SCHEMA.BTC_LOAD_TASK;