# PatternPlay

PatternPlay is a Streamlit web app that lets you run common analytics queries (user growth, churn, retention) on BigQuery without writing SQL—you pick a pattern, select your table and columns from dropdowns, and dbt generates and executes the SQL using reusable macros. An optional AI agent can auto-configure everything from natural language like "analyze weekly churn on my orders table."

<img width="1905" height="876" alt="image" src="https://github.com/user-attachments/assets/7449eca7-00de-4741-a8dd-8b5d5fed25c8" />
<img width="1910" height="871" alt="image" src="https://github.com/user-attachments/assets/061bf694-06d9-4314-a61d-0b7954128929" />

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/DanielaLaura/PatternPlay.git
cd PatternPlay
```

### 2. Create and activate virtual environment

```bash
python3 -m venv patternenv
source patternenv/bin/activate  # On Windows: patternenv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Set up Google Cloud authentication

```bash
# Install Google Cloud CLI (if not installed)
brew install google-cloud-sdk  # On macOS

# Authenticate
gcloud auth login
gcloud auth application-default login
```

### 5. Configure dbt profile

Edit `dbt_milkyway/profiles.yml` with your BigQuery project and dataset:

```yaml
dbt_milkyway:
  outputs:
    dev:
      dataset: YOUR_DATASET        # e.g., sessions
      project: YOUR_GCP_PROJECT    # e.g., my-project-123
      method: oauth
      location: US
      type: bigquery
      threads: 4
  target: dev
```

### 6. Test dbt connection

```bash
cd dbt_milkyway
dbt debug --profiles-dir .
```

### 7. Run the app

```bash
cd ..  # Back to PatternPlay root
streamlit run app.py
```

Open http://localhost:8501 in your browser.

## Optional: AI Agent

To enable the Claude-powered agent:

1. Get an API key from [console.anthropic.com](https://console.anthropic.com)
2. In the app sidebar, click **Settings** and paste your API key

Without an API key, the agent runs in basic rule-based mode.

## Project Structure

```
PatternPlay/
├── app.py                 # Streamlit UI
├── core.py                # Bridge between UI and dbt
├── agent.py               # AI agent with LLM + tools
├── requirements.txt       # Python dependencies
├── dbt_milkyway/          # dbt project
│   ├── profiles.yml       # BigQuery connection config
│   ├── dbt_project.yml    # dbt project config
│   ├── models/            # dbt models
│   │   ├── growth_accounting.sql
│   │   ├── retention.sql
│   │   └── cumulative_snapshot.sql
│   └── macros/            # Reusable SQL templates
│       ├── growth_accounting.sql
│       ├── retention.sql
│       └── cumulative_snapshot.sql
└── create_sample_data.py  # Script to create test data
```

## Available Patterns

| Pattern | Description |
|---------|-------------|
| **Growth Accounting** | Categorize users as New, Retained, Resurrected, Lost, Churned |
| **Retention** | Cohort-based retention analysis |
| **Cumulative Snapshot** | Running totals with incremental updates |

## Create Sample Data (Optional)

To create test data in BigQuery:

```bash
python create_sample_data.py
```

This creates a `user_activity` table with ~14k sample events.

