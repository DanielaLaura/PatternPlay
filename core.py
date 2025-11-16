# core.py

import json
import subprocess


class SchemaAnalytics:
    """
    A thin wrapper for:
    - validating schema inputs
    - mapping to dbt model names
    - building vars
    - running dbt
    - optionally previewing SQL by reading the dbt model file
    """

    PATTERNS = {
        "cumulative": {
            "dbt_model": "cumulative",
        },
        "growth_accounting": {
            "dbt_model": "growth_accounting",
        },
        "retention": {
            "dbt_model": "retention",
        },
    }

    def __init__(self, schema_desc, target_table=None, dbt_project_dir="dbt_milkyway"):
        self.schema = schema_desc
        self.target_table = target_table
        self.dbt_project_dir = dbt_project_dir

    # --- SQL preview (by reading the actual dbt model file) ---
    def read_model_sql(self, pattern):
        model_file = f"{self.dbt_project_dir}/models/{pattern}.sql"
        with open(model_file, "r") as f:
            return f.read()

    # --- Return dbt vars for this schema ---
    def build_dbt_vars(self):
        return json.dumps(self.schema)

    # --- Run dbt model ---
    def run_dbt(self, pattern):
        model_name = self.PATTERNS[pattern]["dbt_model"]
        vars_str = self.build_dbt_vars()

        result = subprocess.run(
            [
                "dbt", "run",
                "--project-dir", self.dbt_project_dir,
                "--models", model_name,
                "--vars", vars_str,
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        return result.stdout

    # --- For backward compatibility: generate_sql() returns dbt model SQL ---
    def generate_sql(self, pattern, **_):
        return self.read_model_sql(pattern)
