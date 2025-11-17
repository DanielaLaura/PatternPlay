
import streamlit as st
from core import SchemaAnalytics
import pandas as pd
import sqlalchemy

st.set_page_config(page_title="MilkyWay Analytics", layout="wide")
st.title("MilkyWay Analytics Framework")

# --- 1. Choose analytical pattern ---

st.header("Select analytical pattern")
pattern = st.selectbox(
    "Pattern",
    [
        "cumulative",           # existing simple time-series macro
        "growth_accounting",
        "retention",
        "cumulative_snapshot",  # new outer-join snapshot macro
    ],
)

params = {}

# --- 2. Pattern-specific parameter inputs ---

if pattern in ("cumulative", "growth_accounting", "retention"):
    st.subheader("Inputs for time-series patterns (cumulative / growth / retention)")

    dataset_name = st.text_input("Source dataset / table", "raw.user_activity")
    entity_id = st.text_input("Entity ID column", "user_id")
    event_timestamp = st.text_input("Event timestamp column", "event_time")
    event_type_input = st.text_input(
        "Event type(s) (comma separated, optional)", "login,purchase"
    )
    event_type = (
        [x.strip() for x in event_type_input.split(",")]
        if event_type_input.strip()
        else None
    )

    params = {
        "dataset_name": dataset_name,
        "entity_id": entity_id,
        "event_timestamp": event_timestamp,
        "event_type": event_type,
    }

elif pattern == "cumulative_snapshot":
    st.subheader("Inputs for outer-join cumulative snapshot pattern")

    snapshot_table = st.text_input(
        "Snapshot table (yesterday state; dbt var snapshot_table)",
        "user_snapshot",
    )
    fact_table = st.text_input(
        "Fact table (today events; dbt var fact_table)",
        "user_events",
    )

    key_column = st.text_input("Key column", "user_id")
    period_column = st.text_input("Period column (date / year)", "dt")

    prev_period = st.text_input(
        "Previous period literal (as used in SQL)",
        "'2025-11-15'",
    )
    curr_period = st.text_input(
        "Current period literal (as used in SQL)",
        "'2025-11-16'",
    )

    metric_type = st.selectbox("Metric type", ["count", "sum"])
    metric_column = ""
    if metric_type == "sum":
        metric_column = st.text_input("Metric column for SUM()", "amount")

    today_col_name = st.text_input("Output column: today value", "today_value")
    cumulative_col_name = st.text_input(
        "Output column: cumulative value", "cumulative_value"
    )

    params = {
        "snapshot_table": snapshot_table,
        "fact_table": fact_table,
        "key_column": key_column,
        "period_column": period_column,
        "prev_period": prev_period,
        "curr_period": curr_period,
        "metric_type": metric_type,
        "metric_column": metric_column or None,
        "today_col_name": today_col_name,
        "cumulative_col_name": cumulative_col_name,
    }

# --- 3. Optional: Warehouse connection for preview ---

st.header("Warehouse Connection (Optional)")
db_url = st.text_input("SQLAlchemy connection URL", "")


def build_sa() -> SchemaAnalytics:
    if not params:
        raise ValueError("Pattern parameters not set")
    return SchemaAnalytics(pattern=pattern, params=params)


# --- 4. Generate SQL (dbt compile) ---

if st.button("Generate SQL"):
    try:
        sa = build_sa()
        sql = sa.generate_sql()
        st.subheader("Compiled SQL from dbt")
        st.code(sql, language="sql")
    except Exception as e:
        st.error(str(e))


# --- 5. Preview results (if db_url provided) ---

if st.button("Preview Results") and db_url:
    try:
        sa = build_sa()
        sql = sa.generate_sql()

        engine = sqlalchemy.create_engine(db_url)
        with engine.connect() as conn:
            df = pd.read_sql_query(f"SELECT * FROM ({sql}) LIMIT 100", conn)

        st.subheader("Preview")
        st.dataframe(df)
    except Exception as e:
        st.error(str(e))


# --- 6. Run dbt model ---

if st.button("Run dbt"):
    try:
        sa = build_sa()
        result = sa.run_dbt()
        if result.returncode == 0:
            st.success(f"dbt run for '{pattern}' completed successfully.")
            st.text(result.stdout)
        else:
            st.error(f"dbt run failed for '{pattern}':\n{result.stderr}")
    except Exception as e:
        st.error(str(e))
