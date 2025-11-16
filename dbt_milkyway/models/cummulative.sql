{{ config(
    materialized='incremental',
    unique_key='dt'
) }}

{{ cumulative(
    source_dataset=var('source_dataset'),
    entity_id=var('entity_id'),
    event_timestamp=var('event_timestamp'),
    event_type=var('event_type')
) }}