{{ config(
    materialized = 'incremental',
    unique_key = 'calendar_period',
    on_schema_change = 'sync_all_columns'
) }}

{{ growth_accounting(
    activity_table = var('activity_table'),
    activity_customer_id = var('activity_customer_id'),
    activity_timestamp = var('activity_timestamp'),
    time_grain = var('time_grain', 'MONTH'),
    first_activation_table = var('first_activation_table', none),
    first_activation_customer_id = var('first_activation_customer_id', none),
    first_activation_timestamp = var('first_activation_timestamp', none),
    date_spine_table = var('date_spine_table', none),
    date_spine_column = var('date_spine_column', none),
    is_incremental_run = is_incremental()
) }}
