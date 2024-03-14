## Simple Pyspark Poject to Perfom Data Preprocessing

#### Created a Master cluster and then Added worker cluster to execute the task

## Data Source
```https://zindi.africa/competitions/fraud-detection-in-electricity-and-gas-consumption-challenge/data```

### Preprocessing
* Removed Null values
* Removed Duplicate values

### Feature Extraction
* Extracted month, year values from datetime column
* Utilized joins and groupby.

### Data Partitioning
* Partitioned the cleaned dataset using repartitions.

