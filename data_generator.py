import pandas as pd
import random
import datetime

# Number of rows to generate
num_rows = 100

# Generate sample data
data = {
    "id": [i for i in range(1, num_rows + 1)],
    "timestamp": [
        (datetime.datetime.now() - datetime.timedelta(seconds=i * 10)).strftime("%Y-%m-%d %H:%M:%S")
        for i in range(num_rows)
    ],
    "value": [round(random.uniform(10.0, 100.0), 2) for _ in range(num_rows)],
}

# Create a DataFrame
df = pd.DataFrame(data)

# Save to CSV
df.to_csv("data/data.csv", index=False)

print("Data generated and saved to data/data.csv")