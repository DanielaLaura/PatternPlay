{{ config(
    materialized='incremental',
    unique_key='event_date'  -- retention macro outputs cohort_date + event_date
) }}

{{ retention(
    source_dataset=var('source_dataset'),
    entity_id=var('entity_id'),
    event_timestamp=var('event_timestamp'),
    event_type=var('event_type')
) }}