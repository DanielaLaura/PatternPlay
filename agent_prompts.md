# Agent Prompts & Examples

## System Context

The agent understands these analytics patterns:

### 1. Growth Accounting
Categorizes users into: **New**, **Retained**, **Resurrected**, **Lost**, **Churned**

**Use when:** Understanding user lifecycle, churn analysis, growth metrics

### 2. Retention
Cohort-based retention over time periods

**Use when:** Measuring how many users return after N days/weeks/months

### 3. Cumulative Snapshot
Running totals with efficient incremental updates

**Use when:** Daily/monthly aggregates that build on previous values

---

## Sample Prompts

### Exploration

```
"What tables are available?"
"Show me the schema of sessions.user_activity"
"What columns does the orders table have?"
"List all datasets"
"Preview sessions.user_activity"
```

### Configuration

```
"Run growth accounting on sessions.user_activity"
"Analyze weekly churn for sessions.orders using customer_id and created_at"
"Set up monthly retention analysis on the events table"
"Configure growth accounting with daily granularity"
```

### Recommendations

```
"Which pattern should I use for churn analysis?"
"How do I measure user retention?"
"What's the best way to track new vs returning users?"
"I want to understand why users leave"
```

### Memory & Preferences

```
"Remember that I usually use weekly time grain"
"My main table is sessions.orders"
"Remember that customer_id is called user_id in our tables"
```

### Follow-ups (requires conversation context)

```
"Make it weekly instead"
"Use a different timestamp column"
"Add the orders table too"
"Show me the results"
```

---

## Expected Agent Behavior

### Example 1: Basic Configuration

**User:** "Run growth accounting on sessions.user_activity"

**Agent Actions:**
1. Call `get_table_schema("sessions", "user_activity")`
2. Auto-detect customer ID column (e.g., `user_id`)
3. Auto-detect timestamp column (e.g., `event_time`)
4. Call `configure_growth_accounting(...)` with detected values

**Agent Response:**
```
I've configured growth accounting for you:
- Table: sessions.user_activity
- Customer ID: user_id
- Timestamp: event_time  
- Time grain: MONTH

Click Apply to fill the form.
```

---

### Example 2: Exploration

**User:** "What tables do I have?"

**Agent Actions:**
1. Call `list_datasets()`
2. Call `list_tables("sessions")` for each dataset

**Agent Response:**
```
Here are your available tables:

**sessions dataset:**
- user_activity (user events)
- orders (transactions)
- customers (user profiles)

Would you like me to show the schema for any of these?
```

---

### Example 3: With Memory

**User:** "Use my usual table"

**Agent Actions:**
1. Check memory for recent tables or preferences
2. Find `sessions.user_activity` was used recently
3. Configure with that table

**Agent Response:**
```
Using your recent table: sessions.user_activity

Configured growth accounting with:
- Customer ID: user_id
- Timestamp: event_time
- Time grain: MONTH (your default)
```

---

## Few-Shot Examples for System Prompt

These examples help the LLM understand expected behavior:

```
Example 1:
User: "analyze churn on orders"
Assistant: I'll set up growth accounting on the orders table. Let me check its schema first.
[Calls get_table_schema]
Found columns: order_id, customer_id, order_date, amount
[Calls configure_growth_accounting with customer_id and order_date]
Done! I've configured growth accounting with customer_id and order_date.

Example 2:
User: "make it weekly"
Assistant: [Calls configure_growth_accounting with time_grain="WEEK"]
Updated to weekly time grain. Click Apply to update the form.

Example 3:
User: "what patterns can I use?"
Assistant: There are 3 patterns available:
1. **Growth Accounting** - Track New, Retained, Resurrected, Lost, Churned users
2. **Retention** - Cohort retention curves
3. **Cumulative Snapshot** - Running totals over time

For churn analysis, I recommend Growth Accounting. What would you like to analyze?
```

---

## Context Variables

The agent has access to:

| Context | Source | Example |
|---------|--------|---------|
| Recent tables | Memory | `["sessions.user_activity", "sessions.orders"]` |
| Preferences | Memory | `{"default_time_grain": "WEEK"}` |
| Facts | Memory | `["Customer ID is always user_id", "Data starts from 2024"]` |
| Conversation | Chat history | Previous messages in session |
| Schema | BigQuery | Column names, types |

---

## Error Handling Examples

**Table not found:**
```
User: "analyze foo.bar"
Agent: I couldn't find the table foo.bar. Here are available tables:
- sessions.user_activity
- sessions.orders
Which one would you like to use?
```

**Column detection failed:**
```
User: "run growth on sessions.logs"
Agent: I found the table but couldn't automatically detect the customer ID and timestamp columns.
Schema: [log_id, message, created, level]
Which column should I use for customer ID?
```

**API error:**
```
Agent: I encountered an error accessing BigQuery: [error message]
Please check your credentials or try again.
```
