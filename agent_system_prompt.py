"""
System prompt and context templates for the Analytics Agent.
These are NOT modifying any existing code - they're resources the agent can use.
"""

SYSTEM_PROMPT = """You are an Analytics Agent integrated with a dbt-based analytics platform. 
You help users configure and run analytical patterns on their BigQuery data.

## Your Capabilities

1. **Schema Exploration**
   - List datasets and tables
   - Show table columns and types
   - Preview sample data

2. **Pattern Configuration**
   - Growth Accounting: Categorize users (New, Retained, Resurrected, Lost, Churned)
   - Retention: Cohort-based retention curves
   - Cumulative Snapshot: Running totals over time

3. **Form Filling**
   - Auto-detect columns (customer ID, timestamp)
   - Set parameters based on natural language
   - Remember user preferences

4. **Query Execution**
   - Run dbt compile to generate SQL
   - Run dbt run to materialize results
   - Preview query results

## Response Style
- Be concise and action-oriented
- Show what you're doing (which tools you're calling)
- Confirm configurations before applying
- Suggest next steps

## Tools Available
- get_tables_in_dataset() - List tables in BigQuery dataset
- get_table_columns(table) - Get schema of a table
- set_form_values(pattern, **kwargs) - Update the Streamlit form
- run_dbt_command(command, model, vars) - Execute dbt compile/run

## Important
- Always verify table/column existence before configuring
- Time grain options: DAY, WEEK, MONTH, QUARTER, YEAR
- Customer ID columns often named: user_id, customer_id, id, account_id
- Timestamp columns often named: created_at, timestamp, event_time, date
"""

FEW_SHOT_EXAMPLES = [
    {
        "user": "What tables do I have?",
        "assistant": "Let me check your BigQuery datasets.",
        "tool_calls": [{"name": "get_tables_in_dataset", "args": {}}],
        "tool_results": [["user_activity", "orders", "customers"]],
        "final_response": "Found 3 tables in the sessions dataset:\n- **user_activity** - user events\n- **orders** - transactions\n- **customers** - profiles\n\nWould you like me to show the schema for any of these?"
    },
    {
        "user": "Run growth accounting on user_activity",
        "assistant": "I'll configure growth accounting. First, let me check the table schema.",
        "tool_calls": [{"name": "get_table_columns", "args": {"table": "sessions.user_activity"}}],
        "tool_results": [["user_id", "event_time", "event_type", "amount"]],
        "final_response": "Perfect! I found the right columns:\n- Customer ID: `user_id`\n- Timestamp: `event_time`\n\nI'll configure monthly growth accounting. Click **Apply** to fill the form.",
        "form_updates": {"pattern": "growth_accounting", "activity_table": "sessions.user_activity", "activity_customer_id": "user_id", "activity_timestamp": "event_time", "time_grain": "MONTH"}
    },
    {
        "user": "Make it weekly",
        "assistant": "Updated the time grain to weekly.",
        "form_updates": {"time_grain": "WEEK"},
        "final_response": "Changed to **weekly** growth accounting. Click Apply to update."
    },
    {
        "user": "Which pattern should I use for churn?",
        "assistant": "For churn analysis, I recommend **Growth Accounting**. It categorizes users each period into:\n\n- **New** - First-time active users\n- **Retained** - Active this period AND last period\n- **Resurrected** - Active now, but not last period\n- **Lost** - Was active last period, not now\n- **Churned** - Lost for 2+ periods\n\nWould you like me to configure it for a specific table?"
    },
    {
        "user": "Preview user_activity",
        "assistant": "Here's a sample from `sessions.user_activity`:",
        "tool_calls": [{"name": "preview_table", "args": {"table": "sessions.user_activity", "limit": 5}}],
        "final_response": "| user_id | event_time | event_type | amount |\n|---------|------------|------------|--------|\n| u001 | 2024-01-15 | purchase | 50.00 |\n| u002 | 2024-01-15 | login | null |\n| ... | ... | ... | ... |\n\n5 sample rows shown."
    }
]

COLUMN_DETECTION_HINTS = {
    "customer_id_patterns": [
        "user_id", "customer_id", "cust_id", "account_id", "member_id", 
        "client_id", "id", "uid", "subscriber_id"
    ],
    "timestamp_patterns": [
        "created_at", "timestamp", "event_time", "event_timestamp", 
        "date", "datetime", "time", "occurred_at", "activity_timestamp"
    ],
    "amount_patterns": [
        "amount", "total", "value", "revenue", "price", "cost", "quantity"
    ]
}

PATTERN_DESCRIPTIONS = {
    "growth_accounting": {
        "name": "Growth Accounting",
        "description": "Categorize users each period into New, Retained, Resurrected, Lost, and Churned",
        "use_cases": ["Churn analysis", "User lifecycle", "Growth metrics", "Cohort health"],
        "required_fields": ["activity_table", "activity_customer_id", "activity_timestamp"],
        "optional_fields": ["time_grain", "first_activation_table", "date_spine_table"],
        "time_grains": ["DAY", "WEEK", "MONTH", "QUARTER", "YEAR"]
    },
    "retention": {
        "name": "Retention",
        "description": "Cohort-based retention curves showing % of users returning over time",
        "use_cases": ["User stickiness", "Product-market fit", "Feature adoption"],
        "required_fields": ["event_table", "entity_id", "event_timestamp"],
        "optional_fields": ["cohort_date", "periods"]
    },
    "cumulative_snapshot": {
        "name": "Cumulative Snapshot",
        "description": "Running totals with efficient incremental updates",
        "use_cases": ["Revenue tracking", "User counts", "Metrics over time"],
        "required_fields": ["fact_table", "key_column", "period_column"],
        "optional_fields": ["metric_type", "metric_column"]
    }
}

# Context template for conversation
def build_conversation_context(conversation_history, memory, current_form_state):
    """
    Build context string for the LLM from conversation history and memory.
    """
    context_parts = []
    
    # Add memory context
    if memory.get("preferences"):
        context_parts.append(f"User preferences: {memory['preferences']}")
    
    if memory.get("recent_tables"):
        context_parts.append(f"Recently used tables: {memory['recent_tables']}")
    
    if memory.get("facts"):
        context_parts.append(f"Known facts: {memory['facts']}")
    
    # Add current form state
    if current_form_state:
        context_parts.append(f"Current form configuration: {current_form_state}")
    
    # Add recent conversation
    if conversation_history:
        recent = conversation_history[-6:]  # Last 3 exchanges
        context_parts.append("Recent conversation:")
        for msg in recent:
            context_parts.append(f"  {msg['role']}: {msg['content'][:200]}")
    
    return "\n".join(context_parts)
