# Databricks notebook source
bronze = spark.sql(''' describe external location `bronze` ''').select('url').collect()[0][0]
silver = spark.sql(''' describe external location `silver` ''').select('url').collect()[0][0]

# COMMAND ----------

from pyspark.sql.functions import col,isnan, when, count, mean, lpad, concat, lit, trim, to_timestamp, hour, minute, date_format
from pyspark.sql.types import StringType, IntegerType, FloatType, TimestampType
from delta.tables import DeltaTable

# COMMAND ----------

df = spark.read.format("PARQUET")\
                    .option("inferSchema", "true")\
                    .load(f"{bronze}/dh_transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Nulls

# COMMAND ----------

def check_nulls(df):
    df = df.select([count(when(col(c).isNull(), True)).alias(c) for c in df.columns])
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Nulls

# COMMAND ----------

def null_handling (df):
    df_clean = df.withColumn("dollar_sales", when(col("dollar_sales").isNull(), df.agg(mean("dollar_sales")).first()[0]).otherwise(col("dollar_sales"))) \
                    .withColumn("units", when(col("units").isNull(), df.agg(mean("units")).first()[0]).otherwise(col("units"))) \
                    .withColumn("household", when(col("household").isNull(), -1).otherwise(col("household"))) \
                    .withColumn("store", when(col("store").isNull(), -1).otherwise(col("store"))) \
                    .withColumn("basket", when(col("basket").isNull(), -1).otherwise(col("basket")))
    print("Nulls detected and handled.")
    return df_clean

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove Duplicates

# COMMAND ----------

def remove_duplicates(df):
    print("Initial row count:", df.count())
    df_non_duplicates = df.dropDuplicates()
    print("Row count after removing duplicates:", df_non_duplicates.count())
    return df_non_duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardize Inconsistent Formats

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert time_of_transaction to HH:MM format

# COMMAND ----------


def format_time_of_transaction(df):
    # Convert HHMM (e.g., "1100") to TimestampType
    format_df = df.withColumn("time_of_transaction", 
                              to_timestamp(lpad(col("time_of_transaction").cast("string"), 4, "0"), "HHmm"))
    return format_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check Typos in UPC

# COMMAND ----------

def correct_typos(df):
    df_clean = df.filter(~col("upc").cast("string").cast("double").isNull())
    return df_clean

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Data Validity

# COMMAND ----------

def check_data_validity(df):
    df = df.filter(col("coupon").isin([0, 1]))
    print("After coupon: ", df.count())
    
    df = df.filter(col("geography").isin([1, 2]))
    print("After geography: ", df.count())
    
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Data Types

# COMMAND ----------

def validate_data_types(df):
    df_formated = df.withColumn("upc", col("upc").cast(StringType())) \
             .withColumn("dollar_sales", col("dollar_sales").cast(FloatType())) \
             .withColumn("units", col("units").cast(IntegerType())) \
             .withColumn("time_of_transaction", col("time_of_transaction").cast(TimestampType())) \
             .withColumn("geography", col("geography").cast(IntegerType())) \
             .withColumn("week", col("week").cast(IntegerType())) \
             .withColumn("household", col("household").cast(IntegerType())) \
             .withColumn("store", col("store").cast(IntegerType())) \
             .withColumn("basket", col("basket").cast(IntegerType())) \
             .withColumn("day", col("day").cast(IntegerType())) \
             .withColumn("coupon", col("coupon").cast(IntegerType()))
    return df_formated

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main pipeline function

# COMMAND ----------

def main(df, delta_path = f"{silver}/dh_transactions"):
    # Step 1: Check for nulls
    print("Checking for null values...")
    null_counts = check_nulls(df).collect()[0]  # Collect the null counts
    nulls_present = any(null_counts[c] > 0 for c in null_counts.asDict())  # True if any column has nulls
    
    # Step 2: Handle nulls conditionally
    if nulls_present:
        print("Null values found. Running null_handling...")
        df = null_handling(df)
    else:
        print("No null values found. Skipping null_handling.")

    # Step 3: Remove duplicates
    print("Removing duplicates...")
    df = remove_duplicates(df)

    # Step 4: Format time_of_transaction
    print("Formatting time_of_transaction...")
    df = format_time_of_transaction(df)
    print(f"time_of_transaction formatted. ROW COUNT: {df.count()}")

    # Step 5: Correct typos (filter invalid UPCs)
    print("Correcting typos (filtering invalid UPCs)...")
    df = correct_typos(df)
    print(f"Invalid UPCs filtered. ROW COUNT: {df.count()}")

    # Step 6: Validate data types
    print("Validating data types...")
    df = validate_data_types(df)
    print(f"Data types validated. ROW COUNT: {df.count()}")

    # Step 7: Check data validity
    print("Checking data validity...")
    df = check_data_validity(df)
    print(f"Data validity checked. ROW COUNT: {df.count()}")

    #Final logging
    print(f"Data cleaning complete. Final row count: {df.count()}")

    # Delta Lake Merge
    if DeltaTable.isDeltaTable(spark, delta_path):
        print("\nDelta table exists, performing MERGE...")
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("target")\
            .merge(
                df.alias("source"),
                "target.upc = source.upc"  # Assuming upc is the key; adjust if needed
            )\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        print("\nDelta table does not exist, creating new table...")
        df.write.format("delta")\
            .mode("overwrite")\
            .option("path", delta_path)\
            .saveAsTable("carbo_catalog.silver.dh_transactions")

    # Load and log final state
    final_df = spark.read.format("delta").load(delta_path)
    print("\nCleaning and merge completed!")
    print(f"Final row count in Delta table: {final_df.count()}")

    return final_df

# Apply the pipeline
df_clean = main(df)

# Display sample of cleaned data
print("Displaying sample of cleaned data:")
display(df_clean)
