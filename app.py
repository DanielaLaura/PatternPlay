
import streamlit as st
from core import SchemaAnalytics
from agent import get_agent
import pandas as pd
from google.cloud import bigquery

st.set_page_config(page_title="MilkyWay Analytics", layout="wide")

# --- Agent Sidebar ---
with st.sidebar:
    st.header("🤖 Analytics Agent")
    
    # API Key configuration
    with st.expander("⚙️ Settings", expanded=False):
        api_key = st.text_input(
            "Anthropic API Key", 
            type="password", 
            value=st.session_state.get("anthropic_api_key", ""),
            help="Get your key at console.anthropic.com"
        )
        if api_key:
            st.session_state["anthropic_api_key"] = api_key
        
        if st.button("🗑️ Clear Chat"):
            st.session_state.agent_messages = []
            agent = get_agent(st.session_state.get("anthropic_api_key"))
            agent.clear_history()
            st.rerun()
    
    # Show agent status
    agent = get_agent(st.session_state.get("anthropic_api_key"))
    if agent.is_llm_enabled:
        st.success("✅ LLM Mode (Claude)")
    else:
        st.warning("⚡ Basic Mode (no API key)")
    
    st.caption("Ask me anything about your data or patterns")
    
    # Initialize chat history
    if "agent_messages" not in st.session_state:
        st.session_state.agent_messages = []
    if "suggested_config" not in st.session_state:
        st.session_state.suggested_config = None
    
    # Chat container with scroll
    chat_container = st.container(height=300)
    with chat_container:
        for msg in st.session_state.agent_messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask the agent...", key="agent_input"):
        # Add user message
        st.session_state.agent_messages.append({"role": "user", "content": prompt})
        
        # Get agent response
        agent = get_agent(st.session_state.get("anthropic_api_key"))
        with st.spinner("Thinking..."):
            response = agent.process_message(prompt)
        
        # Store suggested config if any
        if response.get("suggested_config"):
            st.session_state.suggested_config = response["suggested_config"]
        
        # Add agent response
        st.session_state.agent_messages.append({"role": "assistant", "content": response["text"]})
        
        st.rerun()
    
    # Apply configuration button
    if st.session_state.suggested_config:
        st.divider()
        st.markdown("**📋 Suggested Configuration:**")
        config = st.session_state.suggested_config
        
        # Nice display
        for key, value in config.items():
            if value:
                st.text(f"  {key}: {value}")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Apply", use_container_width=True):
                # Store in session state for form to pick up
                if "pattern" in config:
                    st.session_state["selected_pattern"] = config["pattern"]
                if "activity_table" in config:
                    st.session_state["applied_activity_table"] = config["activity_table"]
                if "activity_customer_id" in config:
                    st.session_state["applied_customer_id"] = config["activity_customer_id"]
                if "activity_timestamp" in config:
                    st.session_state["applied_timestamp"] = config["activity_timestamp"]
                if "time_grain" in config:
                    st.session_state["applied_time_grain"] = config["time_grain"]
                st.session_state.suggested_config = None
                st.success("Applied! Check the form.")
                st.rerun()
        with col2:
            if st.button("❌ Dismiss", use_container_width=True):
                st.session_state.suggested_config = None
                st.rerun()
    
    # Quick actions
    st.divider()
    st.markdown("**Quick Actions:**")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("📊 Explore", use_container_width=True):
            agent = get_agent(st.session_state.get("anthropic_api_key"))
            response = agent.process_message("What tables are available?")
            st.session_state.agent_messages.append({"role": "user", "content": "What tables are available?"})
            st.session_state.agent_messages.append({"role": "assistant", "content": response["text"]})
            st.rerun()
    with col2:
        if st.button("❓ Help", use_container_width=True):
            agent = get_agent(st.session_state.get("anthropic_api_key"))
            response = agent.process_message("help")
            st.session_state.agent_messages.append({"role": "user", "content": "Help"})
            st.session_state.agent_messages.append({"role": "assistant", "content": response["text"]})
            st.rerun()
    
    # Memory info
    if agent.memory.memory.get("recent_tables"):
        st.divider()
        st.markdown("**🧠 Memory:**")
        st.caption(f"Recent tables: {', '.join(agent.memory.memory['recent_tables'][:3])}")

st.title("MilkyWay Analytics Framework")

# BigQuery client for fetching table schemas
@st.cache_resource
def get_bq_client():
    return bigquery.Client(project="repeatable-analyses")

@st.cache_data(ttl=300)
def get_table_columns(table_name: str) -> list:
    """Fetch column names from a BigQuery table."""
    try:
        client = get_bq_client()
        # Handle dataset.table format
        if "." in table_name:
            full_table = f"repeatable-analyses.{table_name}"
        else:
            full_table = f"repeatable-analyses.sessions.{table_name}"
        
        table = client.get_table(full_table)
        return [field.name for field in table.schema]
    except Exception:
        return []

@st.cache_data(ttl=300)
def get_tables_in_dataset(dataset: str = "sessions") -> list:
    """Fetch table names from a BigQuery dataset."""
    try:
        client = get_bq_client()
        tables = client.list_tables(f"repeatable-analyses.{dataset}")
        return [f"{dataset}.{t.table_id}" for t in tables]
    except Exception:
        return []

# --- 1. Choose analytical pattern ---

st.header("Select analytical pattern")
pattern = st.selectbox(
    "Pattern",
    [
        "growth_accounting",
        "retention",
        "cumulative_snapshot",  # outer-join snapshot macro
    ],
)

params = {}

# --- 2. Pattern-specific parameter inputs ---

if pattern == "growth_accounting":
    st.subheader("Inputs for Growth Accounting")
    
    st.markdown("**Activity Data (Required)**")
    # Use applied config from agent if available
    default_table = st.session_state.get("applied_activity_table", "sessions.user_activity")
    activity_table = st.text_input("Activity table (dataset.table)", default_table, key="ga_activity_table")
    
    # Fetch columns from activity table
    activity_columns = get_table_columns(activity_table) if activity_table else []
    if activity_columns:
        activity_customer_id = st.selectbox("Customer ID column", activity_columns, key="ga_customer_id")
        activity_timestamp = st.selectbox("Timestamp column", activity_columns, key="ga_timestamp")
    else:
        st.warning(f"Could not fetch columns from '{activity_table}'. Enter column names manually.")
        activity_customer_id = st.text_input("Customer ID column", "user_id", key="ga_customer_id_manual")
        activity_timestamp = st.text_input("Timestamp column", "event_time", key="ga_timestamp_manual")
    
    st.markdown("**Time Grain**")
    time_grain_options = ["DAY", "WEEK", "MONTH", "QUARTER", "YEAR"]
    default_grain = st.session_state.get("applied_time_grain", "MONTH")
    default_index = time_grain_options.index(default_grain) if default_grain in time_grain_options else 2
    time_grain = st.selectbox("Time grain", time_grain_options, index=default_index, key="ga_time_grain")
    
    st.markdown("**First Activation Data (Optional)**")
    use_separate_activation = st.checkbox("Use separate first activation table", key="ga_use_activation")
    first_activation_table = None
    first_activation_customer_id = None
    first_activation_timestamp = None
    if use_separate_activation:
        first_activation_table = st.text_input("First activation table (dataset.table)", "sessions.customer_activations", key="ga_activation_table")
        activation_columns = get_table_columns(first_activation_table) if first_activation_table else []
        if activation_columns:
            first_activation_customer_id = st.selectbox("Activation - Customer ID column", activation_columns, key="ga_act_customer_id")
            first_activation_timestamp = st.selectbox("Activation - Timestamp column", activation_columns, key="ga_act_timestamp")
        else:
            st.warning(f"Could not fetch columns from '{first_activation_table}'. Enter column names manually.")
            first_activation_customer_id = st.text_input("Activation - Customer ID column", "customer_id", key="ga_act_customer_id_manual")
            first_activation_timestamp = st.text_input("Activation - Timestamp column", "activation_timestamp", key="ga_act_timestamp_manual")
    
    st.markdown("**Date Spine (Optional)**")
    use_date_spine = st.checkbox("Use external date spine table", key="ga_use_spine")
    date_spine_table = None
    date_spine_column = None
    if use_date_spine:
        date_spine_table = st.text_input("Date spine table (dataset.table)", "sessions.date_spine", key="ga_spine_table")
        spine_columns = get_table_columns(date_spine_table) if date_spine_table else []
        if spine_columns:
            date_spine_column = st.selectbox("Date column", spine_columns, key="ga_spine_column")
        else:
            st.warning(f"Could not fetch columns from '{date_spine_table}'. Enter column name manually.")
            date_spine_column = st.text_input("Date column", "date_day", key="ga_spine_column_manual")

    params = {
        "activity_table": activity_table,
        "activity_customer_id": activity_customer_id,
        "activity_timestamp": activity_timestamp,
        "time_grain": time_grain,
        "first_activation_table": first_activation_table,
        "first_activation_customer_id": first_activation_customer_id,
        "first_activation_timestamp": first_activation_timestamp,
        "date_spine_table": date_spine_table,
        "date_spine_column": date_spine_column,
    }

elif pattern == "retention":
    st.subheader("Inputs for Retention")

    source_table = st.text_input("Source table", "sessions.user_activity")
    customer_id = st.text_input("Customer ID column", "user_id")
    activity_timestamp = st.text_input("Activity timestamp column", "event_time")

    params = {
        "source_table": source_table,
        "customer_id": customer_id,
        "activity_timestamp": activity_timestamp,
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

# --- 3. BigQuery connection for preview ---

st.header("BigQuery Preview (Optional)")
st.info("Uses your authenticated Google Cloud credentials to preview results.")


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


# --- 5. Preview results using BigQuery ---

if st.button("Preview Results"):
    try:
        sa = build_sa()
        sql = sa.generate_sql()

        client = bigquery.Client(project="repeatable-analyses")
        query = f"SELECT * FROM ({sql}) LIMIT 100"
        df = client.query(query).to_dataframe()

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
