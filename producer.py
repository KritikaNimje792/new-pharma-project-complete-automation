import boto3
import pandas as pd
import json
import time
import random
 
STREAM_NAME = "retail-stream-kritika"
REGION = "ap-south-1"
 
kinesis = boto3.client("kinesis", region_name=REGION)
 
# Load datasets
inventory_df = pd.read_csv("inventory_events.csv")
sales_df = pd.read_csv("sales_transactions.csv")
temperature_df = pd.read_csv("temperature_events.csv")
 
# Add source type
inventory_df["source_type"] = "inventory"
sales_df["source_type"] = "sales"
temperature_df["source_type"] = "temperature"
 
# Combine all datasets
combined_df = pd.concat([inventory_df, sales_df, temperature_df], ignore_index=True)
 
# Shuffle data (important)
combined_df = combined_df.sample(frac=1).reset_index(drop=True)
 
print("🚀 Starting streaming...")
 
for index, row in combined_df.iterrows():
    
    data = row.to_dict()
    
    # Convert to string for JSON
    data = {k: str(v) for k, v in data.items()}
    
    print(f"Sending record {index} from {data['source_type']}")
    
    kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(data),
        PartitionKey=str(random.randint(1, 100))
    )
    
    # 30 sec delay
    time.sleep(30)
