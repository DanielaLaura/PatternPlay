{% macro cumulative_snapshot(
    snapshot_table,
    fact_table,
    key_column,
    period_column,
    prev_period,
    curr_period,
    metric_type='count',
    metric_column=None,
    today_col_name='today_value',
    cumulative_col_name='cumulative_value'
) %}

{# 
  Cumulative snapshot pattern using outer join
  - snapshot_table: e.g. ref('user_snapshot') or a table name string
  - fact_table: e.g. ref('user_events') or a table name string
  - key_column: e.g. 'user_id'
  - period_column: e.g. 'dt' or 'year'
  - prev_period: e.g. "'1969-01-01'" or 1969
  - curr_period: e.g. "'1970-01-01'" or 1970
  - metric_type: 'count' or 'sum'
  - metric_column: used only if metric_type='sum'
#}

{# metric expression: COUNT(*) or SUM(column) #}
{% if metric_type == 'sum' and metric_column %}
    {% set metric_expr = "SUM(" ~ metric_column ~ ")" %}
{% else %}
    {% set metric_expr = "COUNT(*)" %}
{% endif %}

WITH yesterday AS (
    SELECT
        {{ key_column }}        AS key_col,
        {{ period_column }}     AS period,
        {{ cumulative_col_name }} AS {{ cumulative_col_name }}
    FROM {{ snapshot_table }}
    WHERE {{ period_column }} = {{ prev_period }}
),

today AS (
    SELECT
        {{ key_column }}        AS key_col,
        {{ curr_period }}       AS period,
        {{ metric_expr }}       AS {{ today_col_name }}
    FROM {{ fact_table }}
    WHERE {{ period_column }} = {{ curr_period }}
    GROUP BY {{ key_column }}
)

SELECT
    COALESCE(t.key_col, y.key_col)         AS {{ key_column }},
    COALESCE(t.period,  {{ curr_period }}) AS {{ period_column }},

    COALESCE(t.{{ today_col_name }}, 0)    AS {{ today_col_name }},

    CASE
        WHEN y.{{ cumulative_col_name }} IS NULL
            THEN COALESCE(t.{{ today_col_name }}, 0)
        ELSE y.{{ cumulative_col_name }} + COALESCE(t.{{ today_col_name }}, 0)
    END AS {{ cumulative_col_name }}

FROM today t
FULL OUTER JOIN yesterday y
    ON t.key_col = y.key_col

{% endmacro %}
