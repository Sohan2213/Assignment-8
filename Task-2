1. Load your dataset into DBFS using:
```python
dbutils.fs.cp("file:/local/path/your_file.json", "dbfs:/your-folder/your_file.json")
```

2. Flatten the JSON fields:
```python
from pyspark.sql.functions import col, explode
df = spark.read.json("dbfs:/your-folder/your_file.json")
flattened_df = df.select(col("field1"), col("nested_field.subfield1"), col("nested_field.subfield2"), explode(col("array_field")).alias("array_item"))
```

3. Write the flattened file as an external Parquet table:
```python
flattened_df.write.mode("overwrite").parquet("dbfs:/your-folder/flattened_data_parquet")
```

4. Create the external table using SQL:
```sql
CREATE TABLE your_database.flattened_table
USING PARQUET
LOCATION 'dbfs:/your-folder/flattened_data_parquet';
```
