"""
Analytics Agent - Natural language interface for pattern configuration
"""
import json
import re
from typing import Optional, Dict, Any
from google.cloud import bigquery


class AnalyticsAgent:
    """Agent that helps users configure analytics patterns via natural language."""
    
    def __init__(self, project_id: str = "repeatable-analyses"):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        self.context = {}
    
    # --- BigQuery Tools ---
    
    def list_datasets(self) -> list:
        """List all datasets in the project."""
        datasets = list(self.client.list_datasets())
        return [d.dataset_id for d in datasets]
    
    def list_tables(self, dataset: str) -> list:
        """List all tables in a dataset."""
        try:
            tables = list(self.client.list_tables(f"{self.project_id}.{dataset}"))
            return [t.table_id for t in tables]
        except Exception as e:
            return []
    
    def get_table_schema(self, dataset: str, table: str) -> list:
        """Get column names and types for a table."""
        try:
            full_table = f"{self.project_id}.{dataset}.{table}"
            table_ref = self.client.get_table(full_table)
            return [{"name": f.name, "type": f.field_type} for f in table_ref.schema]
        except Exception:
            return []
    
    def preview_table(self, dataset: str, table: str, limit: int = 5) -> list:
        """Preview rows from a table."""
        try:
            query = f"SELECT * FROM `{self.project_id}.{dataset}.{table}` LIMIT {limit}"
            result = self.client.query(query).to_dataframe()
            return result.to_dict(orient='records')
        except Exception:
            return []
    
    # --- Pattern Recommendation ---
    
    def recommend_pattern(self, user_goal: str) -> Dict[str, Any]:
        """Recommend a pattern based on user's goal."""
        goal_lower = user_goal.lower()
        
        if any(word in goal_lower for word in ["churn", "retain", "growth", "new user", "lost", "resurrect"]):
            return {
                "pattern": "growth_accounting",
                "reason": "Growth Accounting categorizes users into New, Retained, Resurrected, Lost, and Churned - perfect for understanding user lifecycle and churn."
            }
        elif any(word in goal_lower for word in ["retention", "cohort", "come back", "return"]):
            return {
                "pattern": "retention",
                "reason": "Retention analysis shows what percentage of users come back over time, grouped by cohort."
            }
        elif any(word in goal_lower for word in ["cumulative", "running total", "snapshot", "daily total"]):
            return {
                "pattern": "cumulative_snapshot",
                "reason": "Cumulative Snapshot tracks running totals over time using an efficient incremental pattern."
            }
        else:
            return {
                "pattern": "growth_accounting",
                "reason": "Growth Accounting is the most versatile pattern for understanding user behavior over time."
            }
    
    # --- Column Detection ---
    
    def detect_columns(self, schema: list) -> Dict[str, Optional[str]]:
        """Auto-detect likely customer ID and timestamp columns from schema."""
        customer_id = None
        timestamp = None
        
        # Common customer ID patterns
        id_patterns = ["user_id", "customer_id", "account_id", "member_id", "id", "uid", "cust_id"]
        # Common timestamp patterns
        ts_patterns = ["event_time", "created_at", "timestamp", "event_timestamp", "activity_timestamp", 
                       "created", "date", "event_date", "activity_date", "time"]
        
        column_names = [col["name"].lower() for col in schema]
        
        for pattern in id_patterns:
            for col in schema:
                if pattern in col["name"].lower():
                    customer_id = col["name"]
                    break
            if customer_id:
                break
        
        for pattern in ts_patterns:
            for col in schema:
                if pattern in col["name"].lower() and col["type"] in ["TIMESTAMP", "DATETIME", "DATE"]:
                    timestamp = col["name"]
                    break
            if timestamp:
                break
        
        # Fallback: any timestamp/date column
        if not timestamp:
            for col in schema:
                if col["type"] in ["TIMESTAMP", "DATETIME", "DATE"]:
                    timestamp = col["name"]
                    break
        
        return {"customer_id": customer_id, "timestamp": timestamp}
    
    # --- Natural Language Processing ---
    
    def parse_user_request(self, message: str) -> Dict[str, Any]:
        """Parse user's natural language request into structured config."""
        message_lower = message.lower()
        result = {
            "action": None,
            "params": {}
        }
        
        # Detect action
        if any(word in message_lower for word in ["show", "run", "analyze", "calculate", "get"]):
            result["action"] = "run_pattern"
        elif any(word in message_lower for word in ["list", "what tables", "what datasets", "explore"]):
            result["action"] = "explore_schema"
        elif any(word in message_lower for word in ["help", "how", "what can", "explain"]):
            result["action"] = "help"
        elif any(word in message_lower for word in ["recommend", "suggest", "which pattern"]):
            result["action"] = "recommend"
        
        # Extract time grain
        if "daily" in message_lower or "day" in message_lower:
            result["params"]["time_grain"] = "DAY"
        elif "weekly" in message_lower or "week" in message_lower:
            result["params"]["time_grain"] = "WEEK"
        elif "monthly" in message_lower or "month" in message_lower:
            result["params"]["time_grain"] = "MONTH"
        elif "quarterly" in message_lower or "quarter" in message_lower:
            result["params"]["time_grain"] = "QUARTER"
        elif "yearly" in message_lower or "year" in message_lower:
            result["params"]["time_grain"] = "YEAR"
        
        # Extract table reference (dataset.table pattern)
        table_match = re.search(r'(\w+)\.(\w+)', message)
        if table_match:
            result["params"]["dataset"] = table_match.group(1)
            result["params"]["table"] = table_match.group(2)
        
        # Extract pattern type
        if "growth" in message_lower or "churn" in message_lower:
            result["params"]["pattern"] = "growth_accounting"
        elif "retention" in message_lower:
            result["params"]["pattern"] = "retention"
        elif "cumulative" in message_lower or "snapshot" in message_lower:
            result["params"]["pattern"] = "cumulative_snapshot"
        
        return result
    
    # --- Response Generation ---
    
    def process_message(self, message: str) -> Dict[str, Any]:
        """Process user message and return structured response."""
        parsed = self.parse_user_request(message)
        
        response = {
            "text": "",
            "suggested_config": None,
            "action": parsed["action"]
        }
        
        if parsed["action"] == "explore_schema":
            datasets = self.list_datasets()
            response["text"] = f"**Available datasets:** {', '.join(datasets)}\n\n"
            
            # Show tables in sessions dataset by default
            if "sessions" in datasets:
                tables = self.list_tables("sessions")
                response["text"] += f"**Tables in 'sessions':** {', '.join(tables)}"
        
        elif parsed["action"] == "recommend":
            rec = self.recommend_pattern(message)
            response["text"] = f"**Recommended pattern:** `{rec['pattern']}`\n\n{rec['reason']}"
            response["suggested_config"] = {"pattern": rec["pattern"]}
        
        elif parsed["action"] == "run_pattern" or parsed["action"] is None:
            # Try to build a configuration
            config = {}
            
            # Get pattern
            if "pattern" in parsed["params"]:
                config["pattern"] = parsed["params"]["pattern"]
            else:
                rec = self.recommend_pattern(message)
                config["pattern"] = rec["pattern"]
            
            # Get table
            if "dataset" in parsed["params"] and "table" in parsed["params"]:
                dataset = parsed["params"]["dataset"]
                table = parsed["params"]["table"]
                config["activity_table"] = f"{dataset}.{table}"
                
                # Auto-detect columns
                schema = self.get_table_schema(dataset, table)
                if schema:
                    detected = self.detect_columns(schema)
                    config["activity_customer_id"] = detected["customer_id"]
                    config["activity_timestamp"] = detected["timestamp"]
                    config["schema"] = schema
            
            # Get time grain
            config["time_grain"] = parsed["params"].get("time_grain", "MONTH")
            
            response["suggested_config"] = config
            
            # Build response text
            if config.get("activity_table"):
                response["text"] = f"""**Got it!** Here's what I configured:

- **Pattern:** `{config['pattern']}`
- **Table:** `{config['activity_table']}`
- **Customer ID:** `{config.get('activity_customer_id', 'not detected')}`
- **Timestamp:** `{config.get('activity_timestamp', 'not detected')}`
- **Time Grain:** `{config['time_grain']}`

Click **Apply Configuration** to fill in the form, then **Generate SQL** to see the query."""
            else:
                response["text"] = f"""**I'll help you set up {config['pattern']}.**

Please specify which table to analyze. For example:
- "Analyze sessions.user_activity"
- "Run growth accounting on sessions.orders"

Or type "explore" to see available tables."""
        
        elif parsed["action"] == "help":
            response["text"] = """**I can help you with:**

1. **Explore data:** "What tables are available?" or "Show me the schema of sessions.user_activity"

2. **Run patterns:**
   - "Show me monthly growth accounting for sessions.user_activity"
   - "Analyze weekly retention for sessions.orders"
   - "Run cumulative snapshot on sessions.events"

3. **Get recommendations:** "Which pattern should I use to analyze churn?"

**Available patterns:**
- `growth_accounting` - New, Retained, Resurrected, Lost, Churned users
- `retention` - Cohort retention over time
- `cumulative_snapshot` - Running totals with incremental updates"""
        
        return response


# Singleton instance
_agent = None

def get_agent() -> AnalyticsAgent:
    global _agent
    if _agent is None:
        _agent = AnalyticsAgent()
    return _agent

