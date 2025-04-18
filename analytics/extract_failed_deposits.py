import os
import pandas as pd # type: ignore
from sqlalchemy import create_engine # type: ignore
from datetime import datetime
from dotenv import load_dotenv # type: ignore

# Load environment variables
load_dotenv()

# Get database URL from environment
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set")

print(f"Connecting to database...")

# Create database engine
engine = create_engine(DATABASE_URL)

# SQL query to get failed deposits
query = """
    SELECT deposit_id, wallet_address, mon_amount
    FROM transaction_history 
    WHERE status='failed'
"""

print(f"Executing query...")

# Read data into pandas DataFrame
df = pd.read_sql_query(query, engine)

print(f"Retrieved {len(df)} rows of failed deposits")

if len(df) == 0:
    print("No failed deposits found.")
else:
    # Generate filename with current timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"failed_deposits_{timestamp}.csv"
    
    # Save to CSV
    df.to_csv(output_file, index=False)
    print(f"Data has been saved to {output_file}") 