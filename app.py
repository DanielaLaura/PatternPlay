import streamlit as st
from core import SchemaAnalytics
import subprocess
import pandas as pd
import sqlalchemy

st.set_page_config(page_title="MilkyWay Analytics", layout="wide")
st.title("MilkyWay Analytics Framework")

# --- 1. Input schema details ---
st.header("Define your schema")
dataset_name = st.text_input("Dataset / Table Name", "raw.user_activity")
entity_id = st.text_input("Entity ID Column", "user_id")
event_timestamp = st.text_input("Event Timestamp Column", "event_time")
event_type_input = st.text_input("Event Type(s) (comma separated)", "login,purchase")
event_type = [x.strip() for x in event_type_input.split(",")] if event_type_input else None

# --- 2. Choose analytical pattern ---
st.header("Select analytical pattern")
pattern = st.selectbox("Pattern", ["cumulative", "growth_accounting", "retention"])

# --- 3. Optional date range for incremental execution ---
st.header("Optional: Incremental window")
start_date = st.date_input("Start Date", value=None)
end_date = st.date_input("End Date", value=None)

# --- 4. Optional: Warehouse connection info for preview ---
st.header("Warehouse Connection (Optional)")
db_url = st.text_input("SQLAlchemy Connection URL", "")

# --- 5. Generate SQL ---
if st.button("Generate SQL"):
    schema_desc = {
        "dataset_name": dataset_name,
        "entity_id": entity_id,
        "event_timestamp": event_timestamp,
        "event_type": event_type
    }
    sa = SchemaAnalytics(schema_desc, target_table=f"analytics.{pattern}_daily")
    sql = sa.generate_sql(pattern, start_date=start_date, end_date=end_date)
    st.subheader("Generated SQL")
    st.code(sql, language="sql")

# --- 6. Preview results from warehouse ---
if st.button("Preview Results") and db_url:
    engine = sqlalchemy.create_engine(db_url)
    with engine.connect() as conn:
        df = pd.read_sql_query(f"SELECT * FROM ({sql}) LIMIT 100", conn)
    st.subheader("Preview")
    st.dataframe(df)

# --- 7. Trigger dbt incremental run ---
if st.button("Run dbt Incremental"):
    dbt_vars = f"""
{{
    "source_dataset": "{dataset_name}",
    "entity_id": "{entity_id}",
    "event_timestamp": "{event_timestamp}",
    "event_type": {event_type}
}}
"""
    result = subprocess.run([
        "dbt", "run",
        "--models", pattern,
        "--vars", dbt_vars
    ], capture_output=True, text=True)
    if result.returncode == 0:
        st.success(f"dbt run for {pattern} completed successfully!")
    else:
        st.error(f"dbt run failed:\n{result.stderr}")
