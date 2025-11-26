
import json
import os
import subprocess
from typing import Any, Dict


class SchemaAnalytics:
    """
    Bridge between the Streamlit UI and dbt.

    Supported patterns (dbt models):
      - 'growth_accounting'
      - 'retention'
      - 'cumulative_snapshot'  (the outer-join snapshot macro)
    """

    def __init__(self, pattern: str, params: Dict[str, Any], dbt_project_dir: str = "dbt_milkyway"):
        self.pattern = pattern
        self.params = params
        self.dbt_project_dir = dbt_project_dir

    # ---------- build dbt --vars ----------

    def _build_vars(self) -> str:
        """
        Build a JSON string for dbt --vars based on the selected pattern.
        """
        if self.pattern in ("growth_accounting", "retention"):
            # Patterns using the simple schema interface
            payload = {
                "source_dataset": self.params["dataset_name"],
                "entity_id": self.params["entity_id"],
                "event_timestamp": self.params["event_timestamp"],
                "event_type": self.params.get("event_type"),
            }

        elif self.pattern == "cumulative_snapshot":
            # Outer-join cumulative snapshot pattern
            payload = {
                "snapshot_table": self.params["snapshot_table"],
                "fact_table": self.params["fact_table"],
                "key_column": self.params["key_column"],
                "period_column": self.params["period_column"],
                "prev_period": self.params["prev_period"],
                "curr_period": self.params["curr_period"],
                "metric_type": self.params.get("metric_type", "count"),
                "metric_column": self.params.get("metric_column"),
                "today_col_name": self.params.get("today_col_name", "today_value"),
                "cumulative_col_name": self.params.get("cumulative_col_name", "cumulative_value"),
            }

        else:
            raise ValueError(f"Unknown pattern: {self.pattern}")

        return json.dumps(payload)

    # ---------- compile & return SQL ----------

    def generate_sql(self) -> str:
        """
        Run `dbt compile` for the selected model and return the compiled SQL.
        """
        vars_str = self._build_vars()

        result = subprocess.run(
            [
                "dbt",
                "compile",
                "--project-dir",
                self.dbt_project_dir,
                "--models",
                self.pattern,
                "--vars",
                vars_str,
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)

        compiled_path = os.path.join(
            self.dbt_project_dir,
            "target",
            "compiled",
            "dbt_milkyway",
            "models",
            f"{self.pattern}.sql",
        )
        with open(compiled_path, "r") as f:
            return f.read()

    # ---------- run dbt model ----------

    def run_dbt(self) -> subprocess.CompletedProcess:
        """
        Run `dbt run` for the selected model and return the CompletedProcess.
        """
        vars_str = self._build_vars()

        result = subprocess.run(
            [
                "dbt",
                "run",
                "--project-dir",
                self.dbt_project_dir,
                "--models",
                self.pattern,
                "--vars",
                vars_str,
            ],
            capture_output=True,
            text=True,
        )
        return result
