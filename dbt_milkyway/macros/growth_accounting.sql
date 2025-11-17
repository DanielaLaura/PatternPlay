{% macro growth_accounting(source_dataset, entity_id, event_timestamp, event_type=None) %}
WITH user_dates AS (
    SELECT
        {{ entity_id }} AS user_id,
        DATE({{ event_timestamp }}) AS dt,
        1 AS is_active
    FROM {{ source_dataset }}
    {% if is_incremental() %}
      WHERE DATE({{ event_timestamp }}) > (SELECT MAX(dt) FROM {{ this }})
    {% endif %}
    {% if event_type %}
      AND {{ event_type }} IN ({{ event_type | join(', ') }})
    {% endif %}
    GROUP BY {{ entity_id }}, DATE({{ event_timestamp }})
),
user_status AS (
    SELECT
        user_id,
        dt,
        is_active,
        LAG(is_active) OVER (PARTITION BY user_id ORDER BY dt) AS prev_active
    FROM user_dates
)
SELECT
    dt,
    SUM(CASE WHEN is_active = 1 AND prev_active IS NULL THEN 1 ELSE 0 END) AS new_users,
    SUM(CASE WHEN is_active = 1 AND prev_active = 1 THEN 1 ELSE 0 END) AS retained_users,
    SUM(CASE WHEN is_active = 1 AND prev_active = 0 THEN 1 ELSE 0 END) AS resurrected_users,
    SUM(CASE WHEN is_active = 0 AND prev_active = 1 THEN 1 ELSE 0 END) AS churned_users
FROM user_status
GROUP BY dt
ORDER BY dt;
{% endmacro %}
