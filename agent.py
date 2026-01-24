"""
Analytics Agent - LLM-powered assistant with memory and tool execution
"""
import json
import os
from datetime import datetime
from typing import Optional, Dict, Any, List
from pathlib import Path

# Try to import anthropic, fall back to rule-based if not available
try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

from google.cloud import bigquery


# --- Memory Storage ---

class AgentMemory:
    """Persistent memory for the agent."""
    
    def __init__(self, memory_file: str = ".agent_memory.json"):
        self.memory_file = Path(memory_file)
        self.memory = self._load()
    
    def _load(self) -> Dict:
        if self.memory_file.exists():
            try:
                with open(self.memory_file, "r") as f:
                    return json.load(f)
            except:
                pass
        return {
            "preferences": {},
            "recent_tables": [],
            "recent_queries": [],
            "facts": []
        }
    
    def save(self):
        with open(self.memory_file, "w") as f:
            json.dump(self.memory, f, indent=2, default=str)
    
    def add_preference(self, key: str, value: Any):
        self.memory["preferences"][key] = value
        self.save()
    
    def get_preference(self, key: str, default=None):
        return self.memory["preferences"].get(key, default)
    
    def add_recent_table(self, table: str):
        if table not in self.memory["recent_tables"]:
            self.memory["recent_tables"].insert(0, table)
            self.memory["recent_tables"] = self.memory["recent_tables"][:10]
            self.save()
    
    def add_fact(self, fact: str):
        self.memory["facts"].append({"fact": fact, "timestamp": datetime.now().isoformat()})
        self.memory["facts"] = self.memory["facts"][-20:]  # Keep last 20
        self.save()
    
    def get_context_summary(self) -> str:
        """Get a summary of memory for LLM context."""
        parts = []
        if self.memory["preferences"]:
            parts.append(f"User preferences: {json.dumps(self.memory['preferences'])}")
        if self.memory["recent_tables"]:
            parts.append(f"Recently used tables: {', '.join(self.memory['recent_tables'][:5])}")
        if self.memory["facts"]:
            recent_facts = [f["fact"] for f in self.memory["facts"][-5:]]
            parts.append(f"Known facts: {'; '.join(recent_facts)}")
        return "\n".join(parts) if parts else "No prior context."


# --- Tools for the Agent ---

class AgentTools:
    """Tools the agent can use to interact with BigQuery and the app."""
    
    def __init__(self, project_id: str = "repeatable-analyses"):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        self.memory = AgentMemory()
    
    def list_datasets(self) -> Dict:
        """List all datasets in the project."""
        try:
            datasets = list(self.client.list_datasets())
            return {"success": True, "datasets": [d.dataset_id for d in datasets]}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def list_tables(self, dataset: str) -> Dict:
        """List all tables in a dataset."""
        try:
            tables = list(self.client.list_tables(f"{self.project_id}.{dataset}"))
            return {"success": True, "tables": [t.table_id for t in tables]}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def get_table_schema(self, dataset: str, table: str) -> Dict:
        """Get schema for a table."""
        try:
            full_table = f"{self.project_id}.{dataset}.{table}"
            table_ref = self.client.get_table(full_table)
            schema = [{"name": f.name, "type": f.field_type} for f in table_ref.schema]
            self.memory.add_recent_table(f"{dataset}.{table}")
            return {"success": True, "schema": schema}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def preview_table(self, dataset: str, table: str, limit: int = 5) -> Dict:
        """Preview rows from a table."""
        try:
            query = f"SELECT * FROM `{self.project_id}.{dataset}.{table}` LIMIT {limit}"
            result = self.client.query(query).to_dataframe()
            return {"success": True, "rows": result.to_dict(orient='records')}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def run_query(self, sql: str, limit: int = 100) -> Dict:
        """Run a SQL query."""
        try:
            query = f"SELECT * FROM ({sql}) LIMIT {limit}"
            result = self.client.query(query).to_dataframe()
            return {"success": True, "rows": result.to_dict(orient='records'), "row_count": len(result)}
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def configure_growth_accounting(
        self,
        activity_table: str,
        customer_id_column: str,
        timestamp_column: str,
        time_grain: str = "MONTH"
    ) -> Dict:
        """Configure growth accounting pattern."""
        self.memory.add_recent_table(activity_table)
        return {
            "success": True,
            "config": {
                "pattern": "growth_accounting",
                "activity_table": activity_table,
                "activity_customer_id": customer_id_column,
                "activity_timestamp": timestamp_column,
                "time_grain": time_grain
            }
        }
    
    def remember_preference(self, key: str, value: str) -> Dict:
        """Remember a user preference."""
        self.memory.add_preference(key, value)
        return {"success": True, "message": f"Remembered: {key} = {value}"}
    
    def remember_fact(self, fact: str) -> Dict:
        """Remember a fact about the user or their data."""
        self.memory.add_fact(fact)
        return {"success": True, "message": f"Remembered: {fact}"}


# --- Tool Definitions for Claude ---

TOOL_DEFINITIONS = [
    {
        "name": "list_datasets",
        "description": "List all BigQuery datasets available in the project",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
    {
        "name": "list_tables",
        "description": "List all tables in a BigQuery dataset",
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset": {"type": "string", "description": "Dataset name (e.g., 'sessions')"}
            },
            "required": ["dataset"]
        }
    },
    {
        "name": "get_table_schema",
        "description": "Get the column names and types for a BigQuery table",
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset": {"type": "string", "description": "Dataset name"},
                "table": {"type": "string", "description": "Table name"}
            },
            "required": ["dataset", "table"]
        }
    },
    {
        "name": "preview_table",
        "description": "Preview sample rows from a table to understand its data",
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset": {"type": "string"},
                "table": {"type": "string"},
                "limit": {"type": "integer", "default": 5}
            },
            "required": ["dataset", "table"]
        }
    },
    {
        "name": "configure_growth_accounting",
        "description": "Configure the growth accounting pattern with specified parameters. Use this when the user wants to analyze user growth, churn, retention categories (New, Retained, Resurrected, Lost, Churned).",
        "input_schema": {
            "type": "object",
            "properties": {
                "activity_table": {"type": "string", "description": "Full table name as dataset.table (e.g., 'sessions.user_activity')"},
                "customer_id_column": {"type": "string", "description": "Column containing customer/user ID"},
                "timestamp_column": {"type": "string", "description": "Column containing event timestamp"},
                "time_grain": {"type": "string", "enum": ["DAY", "WEEK", "MONTH", "QUARTER", "YEAR"], "default": "MONTH"}
            },
            "required": ["activity_table", "customer_id_column", "timestamp_column"]
        }
    },
    {
        "name": "remember_preference",
        "description": "Remember a user preference for future sessions",
        "input_schema": {
            "type": "object",
            "properties": {
                "key": {"type": "string", "description": "Preference name (e.g., 'default_time_grain', 'favorite_table')"},
                "value": {"type": "string", "description": "Preference value"}
            },
            "required": ["key", "value"]
        }
    },
    {
        "name": "remember_fact",
        "description": "Remember an important fact about the user or their data for future reference",
        "input_schema": {
            "type": "object",
            "properties": {
                "fact": {"type": "string", "description": "The fact to remember"}
            },
            "required": ["fact"]
        }
    }
]


# --- Main Agent Class ---

class AnalyticsAgent:
    """LLM-powered analytics agent with memory and tools."""
    
    def __init__(self, api_key: Optional[str] = None):
        self.tools = AgentTools()
        self.memory = self.tools.memory
        self.conversation_history: List[Dict] = []
        
        # Initialize Anthropic client if available
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        self.client = None
        if HAS_ANTHROPIC and self.api_key:
            self.client = anthropic.Anthropic(api_key=self.api_key)
    
    @property
    def is_llm_enabled(self) -> bool:
        return self.client is not None
    
    def _get_system_prompt(self) -> str:
        memory_context = self.memory.get_context_summary()
        return f"""You are an Analytics Agent helping users configure and run analytics patterns on their BigQuery data.

Available patterns:
1. **growth_accounting** - Categorizes users as New, Retained, Resurrected, Lost, Churned over time
2. **retention** - Cohort-based retention analysis
3. **cumulative_snapshot** - Running totals with incremental updates

Your capabilities:
- Explore BigQuery schemas (list datasets, tables, columns)
- Auto-detect appropriate columns for customer ID and timestamps
- Configure analytics patterns based on user requests
- Remember user preferences and facts for future sessions

User Context:
{memory_context}

Guidelines:
- Be concise and helpful
- When configuring patterns, first check the table schema to find appropriate columns
- Proactively suggest improvements or alternatives
- Remember important details the user mentions
- If something fails, explain why and suggest fixes"""
    
    def _execute_tool(self, tool_name: str, tool_input: Dict) -> Any:
        """Execute a tool and return the result."""
        tool_map = {
            "list_datasets": lambda: self.tools.list_datasets(),
            "list_tables": lambda: self.tools.list_tables(**tool_input),
            "get_table_schema": lambda: self.tools.get_table_schema(**tool_input),
            "preview_table": lambda: self.tools.preview_table(**tool_input),
            "configure_growth_accounting": lambda: self.tools.configure_growth_accounting(**tool_input),
            "remember_preference": lambda: self.tools.remember_preference(**tool_input),
            "remember_fact": lambda: self.tools.remember_fact(**tool_input),
        }
        
        if tool_name in tool_map:
            return tool_map[tool_name]()
        return {"error": f"Unknown tool: {tool_name}"}
    
    def process_message(self, user_message: str) -> Dict[str, Any]:
        """Process a user message and return response with optional config."""
        
        # Add to conversation history
        self.conversation_history.append({"role": "user", "content": user_message})
        
        # If no LLM, fall back to rule-based
        if not self.is_llm_enabled:
            return self._rule_based_response(user_message)
        
        # Build messages for Claude
        messages = self.conversation_history.copy()
        
        response = {
            "text": "",
            "suggested_config": None,
            "tool_results": []
        }
        
        try:
            # Call Claude with tools
            result = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1024,
                system=self._get_system_prompt(),
                tools=TOOL_DEFINITIONS,
                messages=messages
            )
            
            # Process response - handle tool calls
            while result.stop_reason == "tool_use":
                # Find tool use blocks
                tool_uses = [block for block in result.content if block.type == "tool_use"]
                
                # Execute tools and collect results
                tool_results = []
                for tool_use in tool_uses:
                    tool_result = self._execute_tool(tool_use.name, tool_use.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tool_use.id,
                        "content": json.dumps(tool_result)
                    })
                    response["tool_results"].append({
                        "tool": tool_use.name,
                        "input": tool_use.input,
                        "output": tool_result
                    })
                    
                    # Check if this is a config
                    if tool_use.name == "configure_growth_accounting" and tool_result.get("success"):
                        response["suggested_config"] = tool_result["config"]
                
                # Add assistant response and tool results to messages
                messages.append({"role": "assistant", "content": result.content})
                messages.append({"role": "user", "content": tool_results})
                
                # Continue conversation
                result = self.client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=1024,
                    system=self._get_system_prompt(),
                    tools=TOOL_DEFINITIONS,
                    messages=messages
                )
            
            # Extract text response
            text_blocks = [block.text for block in result.content if hasattr(block, 'text')]
            response["text"] = "\n".join(text_blocks)
            
            # Add to conversation history
            self.conversation_history.append({"role": "assistant", "content": response["text"]})
            
        except Exception as e:
            response["text"] = f"Error communicating with LLM: {str(e)}\n\nFalling back to basic mode."
            fallback = self._rule_based_response(user_message)
            response["text"] += "\n\n" + fallback["text"]
            response["suggested_config"] = fallback.get("suggested_config")
        
        return response
    
    def _rule_based_response(self, message: str) -> Dict[str, Any]:
        """Fallback rule-based response when LLM is not available."""
        import re
        message_lower = message.lower()
        response = {"text": "", "suggested_config": None}
        
        # Explore
        if any(w in message_lower for w in ["list", "explore", "what tables", "datasets"]):
            result = self.tools.list_datasets()
            if result["success"]:
                response["text"] = f"**Datasets:** {', '.join(result['datasets'])}\n\n"
                tables = self.tools.list_tables("sessions")
                if tables["success"]:
                    response["text"] += f"**Tables in 'sessions':** {', '.join(tables['tables'])}"
            return response
        
        # Help
        if any(w in message_lower for w in ["help", "how", "what can"]):
            response["text"] = """**I can help you with:**

🔍 **Explore:** "What tables are available?"
📊 **Configure:** "Run growth accounting on sessions.user_activity"
💡 **Recommend:** "Which pattern for churn analysis?"

⚠️ **Note:** Running in basic mode (no API key). Add ANTHROPIC_API_KEY for full capabilities."""
            return response
        
        # Configure pattern
        table_match = re.search(r'(\w+)\.(\w+)', message)
        if table_match:
            dataset, table = table_match.groups()
            schema_result = self.tools.get_table_schema(dataset, table)
            
            if schema_result["success"]:
                schema = schema_result["schema"]
                # Auto-detect columns
                customer_id = None
                timestamp = None
                
                for col in schema:
                    name_lower = col["name"].lower()
                    if not customer_id and any(x in name_lower for x in ["user_id", "customer_id", "id"]):
                        customer_id = col["name"]
                    if not timestamp and col["type"] in ["TIMESTAMP", "DATETIME", "DATE"]:
                        timestamp = col["name"]
                
                if customer_id and timestamp:
                    config = self.tools.configure_growth_accounting(
                        f"{dataset}.{table}", customer_id, timestamp, "MONTH"
                    )
                    response["suggested_config"] = config["config"]
                    response["text"] = f"""**Configured growth_accounting:**
- Table: `{dataset}.{table}`
- Customer ID: `{customer_id}`
- Timestamp: `{timestamp}`
- Time grain: MONTH

Click **Apply Configuration** to fill the form."""
                else:
                    response["text"] = f"Found table but couldn't auto-detect columns.\nSchema: {schema}"
            else:
                response["text"] = f"Couldn't access table: {schema_result.get('error', 'Unknown error')}"
        else:
            response["text"] = "Please specify a table (e.g., 'sessions.user_activity') or ask for help."
        
        return response
    
    def clear_history(self):
        """Clear conversation history."""
        self.conversation_history = []


# --- Singleton ---
_agent_instance = None

def get_agent(api_key: Optional[str] = None) -> AnalyticsAgent:
    global _agent_instance
    if _agent_instance is None:
        _agent_instance = AnalyticsAgent(api_key)
    elif api_key and not _agent_instance.is_llm_enabled:
        # Reinitialize with API key
        _agent_instance = AnalyticsAgent(api_key)
    return _agent_instance
