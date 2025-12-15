{% macro growth_accounting(
    activity_table,
    activity_customer_id,
    activity_timestamp,
    time_grain='MONTH',
    first_activation_table=none,
    first_activation_customer_id=none,
    first_activation_timestamp=none,
    date_spine_table=none,
    date_spine_column=none,
    is_incremental_run=false
) %}

{# 
  Growth Accounting Macro - Fully Configurable
  
  Required Parameters:
    - activity_table: table with customer activity events
    - activity_customer_id: customer ID column in activity table
    - activity_timestamp: timestamp column in activity table
    
  Time Grain:
    - time_grain: DAY, WEEK, MONTH, QUARTER, YEAR (default: MONTH)
    
  Optional Parameters:
    - first_activation_table: separate table for first activations (defaults to activity_table)
    - first_activation_customer_id: customer ID in activation table
    - first_activation_timestamp: timestamp in activation table
    - date_spine_table: external date spine table (auto-generated if not provided)
    - date_spine_column: date column in spine table
    - is_incremental_run: if true, only processes recent periods
#}

{# Set defaults for optional parameters #}
{% set activation_table = first_activation_table if first_activation_table else activity_table %}
{% set activation_customer_id = first_activation_customer_id if first_activation_customer_id else activity_customer_id %}
{% set activation_timestamp = first_activation_timestamp if first_activation_timestamp else activity_timestamp %}

{# Set interval for incremental lookback based on time grain #}
{% set lookback_interval = {
    'DAY': 'INTERVAL 7 DAY',
    'WEEK': 'INTERVAL 4 WEEK', 
    'MONTH': 'INTERVAL 2 MONTH',
    'QUARTER': 'INTERVAL 2 QUARTER',
    'YEAR': 'INTERVAL 2 YEAR'
}[time_grain] %}

{# Set interval for date spine generation #}
{% set spine_interval = {
    'DAY': 'INTERVAL 1 DAY',
    'WEEK': 'INTERVAL 1 WEEK',
    'MONTH': 'INTERVAL 1 MONTH',
    'QUARTER': 'INTERVAL 1 QUARTER',
    'YEAR': 'INTERVAL 1 YEAR'
}[time_grain] %}

WITH 

source_data AS (
    SELECT * FROM {{ activity_table }}
    {% if is_incremental_run %}
    WHERE DATE({{ activity_timestamp }}) >= DATE_SUB(CURRENT_DATE(), {{ lookback_interval }})
    {% endif %}
),

-- Get first activation period for each customer
customer_first_activation AS (
    SELECT
        {{ activation_customer_id }} AS customer_id,
        MIN(DATE_TRUNC(DATE({{ activation_timestamp }}), {{ time_grain }})) AS first_activation_period
    FROM {{ activation_table }}
    GROUP BY 1
),

-- Generate date spine
{% if date_spine_table and date_spine_column %}
-- Using provided date spine table
date_spine AS (
    SELECT DISTINCT DATE_TRUNC(DATE({{ date_spine_column }}), {{ time_grain }}) AS period_start
    FROM {{ date_spine_table }}
),
{% elif is_incremental_run %}
-- Incremental: only recent periods
date_spine AS (
    SELECT period_start
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
            DATE_SUB(DATE_TRUNC(CURRENT_DATE(), {{ time_grain }}), {{ lookback_interval }}),
            DATE_TRUNC(CURRENT_DATE(), {{ time_grain }}),
            {{ spine_interval }}
        )
    ) AS period_start
),
{% else %}
-- Full refresh: generate all periods from first activity to now
date_spine AS (
    SELECT period_start
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
            (SELECT MIN(DATE_TRUNC(DATE({{ activation_timestamp }}), {{ time_grain }})) FROM {{ activation_table }}),
            DATE_TRUNC(CURRENT_DATE(), {{ time_grain }}),
            {{ spine_interval }}
        )
    ) AS period_start
),
{% endif %}

-- Create customer-period combinations
all_customer_periods AS (
    SELECT
        cfa.customer_id,
        DATE(ds.period_start) AS calendar_period,
        cfa.first_activation_period
    FROM customer_first_activation AS cfa
    CROSS JOIN date_spine AS ds
    WHERE DATE(ds.period_start) >= cfa.first_activation_period
),

-- Get period activity status
period_activity_status AS (
    SELECT
        acp.customer_id,
        acp.calendar_period,
        acp.first_activation_period,
        MAX(IF(DATE_TRUNC(DATE(act.{{ activity_timestamp }}), {{ time_grain }}) = acp.calendar_period, 1, 0)) AS is_active_current_period
    FROM all_customer_periods AS acp
    LEFT JOIN source_data AS act
        ON acp.customer_id = act.{{ activity_customer_id }}
        AND DATE_TRUNC(DATE(act.{{ activity_timestamp }}), {{ time_grain }}) = acp.calendar_period
    GROUP BY 1, 2, 3
),

-- Add lagged activity status
lagged_activity_status AS (
    SELECT
        customer_id,
        calendar_period,
        first_activation_period,
        is_active_current_period,
        LAG(is_active_current_period, 1, 0) OVER (PARTITION BY customer_id ORDER BY calendar_period) AS is_active_prev_period,
        LAG(is_active_current_period, 2, 0) OVER (PARTITION BY customer_id ORDER BY calendar_period) AS is_active_two_periods_ago
    FROM period_activity_status
),

-- Categorize users
categorized_users AS (
    SELECT
        calendar_period,
        customer_id,
        first_activation_period,
        is_active_current_period,
        is_active_prev_period,
        is_active_two_periods_ago,
        CASE
            WHEN is_active_current_period = 1 AND calendar_period = first_activation_period THEN 'New'
            WHEN is_active_current_period = 1 AND is_active_prev_period = 1 THEN 'Retained'
            WHEN is_active_current_period = 1 AND is_active_prev_period = 0 AND calendar_period > first_activation_period THEN 'Resurrected'
            WHEN is_active_current_period = 0 AND is_active_prev_period = 1 THEN 'Lost'
            WHEN is_active_current_period = 0 AND is_active_prev_period = 0 AND is_active_two_periods_ago = 1 THEN 'Churned'
            ELSE 'Other'
        END AS user_category
    FROM lagged_activity_status
)

-- Final aggregation
SELECT
    calendar_period,
    '{{ time_grain }}' AS time_grain,
    COUNT(DISTINCT CASE WHEN user_category = 'New' THEN customer_id END) AS new_users,
    COUNT(DISTINCT CASE WHEN user_category = 'Retained' THEN customer_id END) AS retained_users,
    COUNT(DISTINCT CASE WHEN user_category = 'Resurrected' THEN customer_id END) AS resurrected_users,
    COUNT(DISTINCT CASE WHEN user_category = 'Lost' THEN customer_id END) AS lost_users,
    COUNT(DISTINCT CASE WHEN user_category = 'Churned' THEN customer_id END) AS churned_users,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM categorized_users
{% if is_incremental_run %}
WHERE calendar_period >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), {{ time_grain }}), {{ lookback_interval }})
{% endif %}
GROUP BY 1
ORDER BY 1 DESC

{% endmacro %}
