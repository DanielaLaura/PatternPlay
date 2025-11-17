{{ config(
    materialized = 'table'
) }}

{{ cumulative_snapshot(
    snapshot_table      = var('snapshot_table'),
    fact_table          = var('fact_table'),
    key_column          = var('key_column'),
    period_column       = var('period_column'),
    prev_period         = var('prev_period'),
    curr_period         = var('curr_period'),
    metric_type         = var('metric_type'),
    metric_column       = var('metric_column'),
    today_col_name      = var('today_col_name'),
    cumulative_col_name = var('cumulative_col_name')
) }}