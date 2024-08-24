1. Load your dataset into DBFS using:
```python
dbutils.fs.cp("file:/local/path/testFile.json", "dbfs:/your-folder/testFile.json")
```

2. Flatten the JSON fields:
```python
from pyspark.sql.functions import col, explode
df = spark.read.json("dbfs:/testFolder/testFile.json")
flattened_df = df.select(col("field1"), col("nested_field.subfield1"), col("nested_field.subfield2"), explode(col("array_field")).alias("array_item"))
```

3. Write the flattened file as an external Parquet table:
```python
flattened_df.write.mode("overwrite").parquet("dbfs:/testFolder/flattened_data_parquet")
```

4. Create the external table using SQL:
```sql
CREATE TABLE your_database.flattened_table
USING PARQUET
LOCATION 'dbfs:/testFolder/flattened_data_parquet';
```
