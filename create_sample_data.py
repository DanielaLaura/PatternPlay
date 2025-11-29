"""
Create sample user activity data and upload to BigQuery
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from google.cloud import bigquery

# Configuration
PROJECT_ID = "repeatable-analyses"
DATASET_ID = "sessions"
TABLE_ID = "user_activity"

# Generate sample data
np.random.seed(42)

# 500 users
user_ids = [f"user_{i:04d}" for i in range(1, 501)]

# Generate events over the last 90 days
end_date = datetime.now()
start_date = end_date - timedelta(days=90)

records = []
event_types = ["login", "purchase", "view", "signup", "logout"]

for user_id in user_ids:
    # Each user has 5-50 events
    num_events = np.random.randint(5, 51)
    
    # Random signup date for this user
    signup_offset = np.random.randint(0, 60)
    user_start = start_date + timedelta(days=signup_offset)
    
    for _ in range(num_events):
        # Random event time after signup
        event_offset = np.random.randint(0, (end_date - user_start).days + 1)
        event_time = user_start + timedelta(
            days=event_offset,
            hours=np.random.randint(0, 24),
            minutes=np.random.randint(0, 60)
        )
        
        # Weighted event types (more views than purchases)
        event_type = np.random.choice(
            event_types, 
            p=[0.25, 0.10, 0.40, 0.05, 0.20]
        )
        
        # Add amount for purchases
        amount = round(np.random.uniform(10, 500), 2) if event_type == "purchase" else None
        
        records.append({
            "user_id": user_id,
            "event_time": event_time,
            "event_type": event_type,
            "amount": amount
        })

# Create DataFrame
df = pd.DataFrame(records)
df = df.sort_values("event_time").reset_index(drop=True)

print(f"Generated {len(df)} events for {len(user_ids)} users")
print(f"\nEvent type distribution:")
print(df["event_type"].value_counts())
print(f"\nDate range: {df['event_time'].min()} to {df['event_time'].max()}")
print(f"\nSample data:")
print(df.head(10))

# Upload to BigQuery
print(f"\nUploading to {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}...")

client = bigquery.Client(project=PROJECT_ID)
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",  # Overwrite if exists
)

job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
job.result()  # Wait for completion

print(f"✓ Uploaded {job.output_rows} rows to {table_ref}")

