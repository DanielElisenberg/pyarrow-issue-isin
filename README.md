# pyarrow-issue-isin

### Introduction
We use pyarrow at work for statistical analysis of big populations. One of the key features we
use to achieve this is the incredibly fast predicate-pushdown-filters. We use these to only retrieve data for our
current working population.
We discovered a bug/performance-issue that seems to have been introduced in version 18 of pyarrow. This coincides
with the removal of numpy as a dependency. This seems to affect the `isin`-filter which handles arrays, so a link to
the removal of numpy seems feasible

### Reproducible example
I have made a [repository containing a reproducible example here](https://github.com/DanielElisenberg/pyarrow-issue-isin).
It requires you to have python version 3.11.* installed and poetry. Run the example with the shell script at the root of the
repository:
```sh
./run_tests.sh
```

### Explanation
Let's say we have a Parquet file that contains an observation for each unit in a population. It also constains the
start and stop time of this observation:

| unit_id    | observation | start | stop |
| ---------- | ----------- | ----- | ---- |
| 1          | 23          | 123   | 456  |
| 2          | 23          | 123   | 456  |
| ...        | ...         | ...   | ...  |
| 50_000_000 | 23          | 123   | 456  |

The dataset includes 50 million units, each with a single observation. Now let's say I was currently working with
a population of the 10 million first units. I would then use predicate pushdown with the `isin` filter to read only
the relevant population:
```py
from pyarrow import dataset

parquet_path = "big_dataset"
population = [unit_id for unit_id in range(1, 10_000_001)]
population_filter = dataset.field("unit_id").isin(population)
dataset.dateset(parquet_path).to_table(population_filter)
```
In earlier pyarrow versions, this was blazingly fast, but after v18, it has become up to 40 times slower. It is to the point
where we can't upgrade. Previously, our analytics services could handle large volumes of requests per minute, but now they freeze.

### Stats
Running the code in [the repository containing a reproducible example](https://github.com/DanielElisenberg/pyarrow-issue-isin) yields
these results on my local machine:
```
Generated in-memory table with 50,000,000 rows. Now writing...
✅ Done. Data written to: ../BIG_DATASET

=== PYARROW VERSION 15 ===
Retrieved 10,000,000 rows in 1.71 seconds.

=== PYARROW VERSION 16 ===
Retrieved 10,000,000 rows in 1.67 seconds.

=== PYARROW VERSION 17 ===
Retrieved 10,000,000 rows in 1.66 seconds.

=== PYARROW VERSION 18 ===
Retrieved 10,000,000 rows in 61.83 seconds.

=== PYARROW VERSION 19 ===
Retrieved 10,000,000 rows in 61.09 seconds.

=== PYARROW VERSION 20 ===
Retrieved 10,000,000 rows in 61.53 seconds.
```
