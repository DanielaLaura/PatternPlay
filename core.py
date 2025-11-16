import json
import os
import subprocess


class SchemaAnalytics:
    def __init__(self, schema_desc, dbt_project_dir="dbt_milkyway"):
        """
        schema_desc example (what you pass from app.py):

        {
            "dataset_name": "raw.user_activity",   # used only for display if you want
            "entity_id": "user_id",
            "event_timestamp": "event_time",
            "event_type": ["login", "purchase"]
        }

        NOTE: dbt macros expect vars named:
          - source_dataset
          - entity_id
          - event_timestamp
          - event_type (optional)
        We'll map dataset_name -> source_dataset when building vars.
        """
        self.schema = schema_desc
        self.dbt_project_dir = dbt_project_dir

    # --- build vars for dbt from the Streamlit schema ---
    def _dbt_vars(self):
        vars_payload = {
            "source_dataset": self.schema["dataset_name"],
            "entity_id": self.schema["entity_id"],
            "event_timestamp": self.schema["event_timestamp"],
        }
        if self.schema.get("event_type") is not None:
            vars_payload["event_type"] = self.schema["event_type"]
        return json.dumps(vars_payload)

    # --- compile model + return compiled SQL (for "Generate SQL" button) ---
    def generate_sql(self, pattern, **_ignored):
        # compile the specific model
        subprocess.run(
            [
                "dbt", "compile",
                "--project-dir", self.dbt_project_dir,
                "--models", pattern,
                "--vars", self._dbt_vars(),
            ],
            check=True,
            capture_output=True,
            text=True,
        )

        # read compiled SQL that dbt wrote
        compiled_path = os.path.join(
            self.dbt_project_dir,
            "target",
            "compiled",
            "dbt_milkyway",
            "models",
            f"{pattern}.sql",
        )
        with open(compiled_path, "r") as f:
            return f.read()

    # --- run the dbt model incrementally (for your "Run dbt Incremental" button) ---
    def run_dbt(self, pattern):
        result = subprocess.run(
            [
                "dbt", "run",
                "--project-dir", self.dbt_project_dir,
                "--models", pattern,
                "--vars", self._dbt_vars(),
            ],
            capture_output=True,
            text=True,
        )
        return result