{% macro retention(source_dataset, entity_id, event_timestamp, event_type=None) %}
WITH first_event AS (
    SELECT
        {{ entity_id }} AS user_id,
        MIN(DATE({{ event_timestamp }})) AS cohort_date
    FROM {{ source_dataset }}
    {% if is_incremental() %}
      WHERE DATE({{ event_timestamp }}) > (SELECT MAX(event_date) FROM {{ this }})
    {% endif %}
    {% if event_type %}
      AND {{ event_type }} IN ({{ event_type | join(', ') }})
    {% endif %}
    GROUP BY {{ entity_id }}
),
events AS (
    SELECT
        {{ entity_id }} AS user_id,
        DATE({{ event_timestamp }}) AS event_date
    FROM {{ source_dataset }}
    {% if is_incremental() %}
      WHERE DATE({{ event_timestamp }}) > (SELECT MAX(event_date) FROM {{ this }})
    {% endif %}
    {% if event_type %}
      AND {{ event_type }} IN ({{ event_type | join(', ') }})
    {% endif %}
)
SELECT
    f.cohort_date,
    e.event_date,
    COUNT(DISTINCT e.user_id) AS active_users,
    COUNT(DISTINCT e.user_id) * 1.0 / COUNT(DISTINCT f.user_id) AS retention_rate
FROM first_event f
JOIN events e
    ON f.user_id = e.user_id
GROUP BY f.cohort_date, e.event_date
ORDER BY f.cohort_date, e.event_date;
{% endmacro %}
