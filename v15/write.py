# uv: pyarrow==15.0.2
import pyarrow as pa
import pyarrow.parquet as pq
import os
import random
import shutil

total_rows = 50_000_000
output_dir = "../BIG_DATASET"

if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

schema = pa.schema(
    [
        ("unit_id", pa.int64()),
        ("observation", pa.int64()),
        ("start", pa.int16()),
        ("stop", pa.int16()),
    ]
)

unit_id = []
observation = []
start = []
stop = []

for next_unit_id in range(1, total_rows + 1):
    unit_id.append(next_unit_id)
    observation.append(random.randint(0, 1000))
    start.append(random.randint(1, 365))
    stop.append(random.randint(1, 365))

# Convert to pyarrow arrays
table = pa.table(
    {
        "unit_id": pa.array(unit_id, type=pa.int64()),
        "observation": pa.array(observation, type=pa.int64()),
        "start": pa.array(start, type=pa.int16()),
        "stop": pa.array(stop, type=pa.int16()),
    },
    schema=schema,
)

print(f"Generated in-memory table with {table.num_rows:,} rows. Now writing...")

# Write whole table to disk, partitioned by start_year
pq.write_to_dataset(table, root_path=output_dir)

print(f"✅ Done. Data written to: {output_dir}")
