# Databricks notebook source
bronze = spark.sql(''' describe external location `bronze` ''').select('url').collect()[0][0]
silver = spark.sql(''' describe external location `silver` ''').select('url').collect()[0][0]

# COMMAND ----------

from pyspark.sql.functions import col, when, regexp_replace, regexp_extract, lit, concat_ws, trim, concat, sum as pyspark_sum, length
from delta.tables import DeltaTable

# COMMAND ----------

df = spark.read.format("PARQUET")\
                    .option("inferSchema", "true")\
                    .load(f"{bronze}/dh_product_lookup")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Remove Duplicates

# COMMAND ----------

def remove_duplicates(df):
    df_non_duplicates =  df.dropDuplicates(["upc"])
    return df_non_duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handle Missing Values

# COMMAND ----------

def handle_missing_values(df):
    df_replace_missing = df.na.fill({
        "product_description": "Unknown",
        "commodity": "Unknown",
        "brand": "Unknown",
        "product_size": "N/A"
    })
    return df_replace_missing

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardizing

# COMMAND ----------

# MAGIC %md
# MAGIC #### Standardize product_description

# COMMAND ----------

def standardize_product_description(df):
    df_product_description =  df.withColumn("product_description",
        regexp_replace(regexp_replace(regexp_replace(col("product_description"),
            "SCE", "SAUCE"),
            "CHAMAPNOLA", "CHAMPAGNOLA"),
            "PMODORO", "POMODORO"))
    return df_product_description

# COMMAND ----------

# MAGIC %md
# MAGIC #### Standardize product_size

# COMMAND ----------

def standardize_product_size(df):
    # Step 1: Clean prefixes and initial transformations
    df_cleaned = df.withColumn("product_size_temp",
        when(col("product_size").contains("OUNCE"), regexp_replace(col("product_size"), "OUNCE", "OZ"))
        .when(col("product_size").startswith("N "), regexp_replace(col("product_size"), "N ", ""))
        .when(col("product_size").startswith("P "), regexp_replace(col("product_size"), "P ", ""))
        .when(col("product_size").startswith("CR "), regexp_replace(col("product_size"), "CR ", ""))
        .when(col("product_size").startswith("SO "), regexp_replace(col("product_size"), "SO ", ""))
        .when(col("product_size").rlike(r"KH#|%KH#|CUST REQST|NO TAG"), "N/A")
        .when(col("product_size") == "GAL", "128 OZ")
        .when(col("product_size").contains("FL OZ"), regexp_replace(col("product_size"), "FL OZ", "OZ"))
        .when(col("product_size") == "##########", "N/A")
        .when(col("product_size") == "", "N/A")
        .when(col("product_size") == " ", "N/A")
        .otherwise(col("product_size")))

    # Step 2: Standardize to OZ, converting LB to OZ, all as floats
    df_product_size = df_cleaned.withColumn("product_size_standardized",
        # Simple OZ case
        when(col("product_size_temp").rlike(r"^\s*\d+\.?\d*\s*OZ\s*$"), 
             concat_ws(" ", 
                       trim(regexp_extract(col("product_size_temp"), r"(\d+\.?\d*)", 1)).cast("float").cast("string"), 
                       lit("OZ")))
        # Simple LB case: Convert to OZ
        .when(col("product_size_temp").rlike(r"^\s*\d+\.?\d*\s*LB\s*$"), 
             concat_ws(" ", 
                       (trim(regexp_extract(col("product_size_temp"), r"(\d+\.?\d*)", 1)).cast("float") * 16).cast("string"), 
                       lit("OZ")))
        # Mixed LB OZ case: Convert to total OZ
        .when(col("product_size_temp").rlike(r"^\s*\d+\.?\d*\s*LB\s+\d+\.?\d*\s*OZ\s*$"), 
             concat_ws(" ", 
                       ((trim(regexp_extract(col("product_size_temp"), r"(\d+\.?\d*)\s*LB", 1)).cast("float") * 16) + 
                        trim(regexp_extract(col("product_size_temp"), r"(\d+\.?\d*)\s*OZ", 1)).cast("float")).cast("string"), 
                       lit("OZ")))
        # OZ with trailing text
        .when(col("product_size_temp").rlike(r"^\s*\d+\.?\d*\s*OZ\s+[A-Za-z]+$"), 
             concat_ws(" ", 
                       trim(regexp_extract(col("product_size_temp"), r"(\d+\.?\d*)", 1)).cast("float").cast("string"), 
                       lit("OZ")))
        # Numbers only: Assume OZ
        .when(col("product_size_temp").rlike(r"^\d+$"), 
             concat_ws(" ", 
                       col("product_size_temp").cast("float").cast("string"), 
                       lit("OZ")))
        # Z to OZ
        .when(col("product_size_temp").rlike(r"^\d+\s*Z$"), 
             concat_ws(" ", 
                       trim(regexp_extract(col("product_size_temp"), r"(\d+)", 1)).cast("float").cast("string"), 
                       lit("OZ")))
        # Fractions with OZ
        .when(col("product_size_temp").rlike(r"(\d+)\s+(\d+\/\d+)\s*OZ"), 
             regexp_replace(col("product_size_temp"), r"(\d+)\s+(\d+\/\d+)\s*OZ", r"\1.\2 OZ"))
        # Clean up extra OZ formatting
        .when(col("product_size_temp").rlike(r"(\d+)\s*OZ\."), regexp_replace(col("product_size_temp"), r"OZ\.", "OZ"))
        .when(col("product_size_temp").rlike(r"(\d+)\s+OZ"), regexp_replace(col("product_size_temp"), r"\s+", " "))
        .otherwise(regexp_replace(regexp_replace(col("product_size_temp"), r"\s+", " "), r"OZ\.", "OZ"))) \
        .drop("product_size_temp")

    return df_product_size

df = standardize_product_size(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Standardize commodity

# COMMAND ----------

def standardize_commodity(df):
    valid_commodities = ["pasta", "pasta sauce", "syrups", "pancake mixes"]
    df_valid_com =  df.withColumn("commodity",
        when(col("commodity").isin(valid_commodities), col("commodity"))
        .otherwise("Unknown"))
    return df_valid_com

# COMMAND ----------

# MAGIC %md
# MAGIC ### Address Outliers

# COMMAND ----------

def validate_data_types(df):
    # Ensure all columns are strings as required
    df = df.withColumn("upc", col("upc").cast("string")) \
           .withColumn("product_description", col("product_description").cast("string")) \
           .withColumn("commodity", col("commodity").cast("string")) \
           .withColumn("brand", col("brand").cast("string")) \
           .withColumn("product_size_standardized", col("product_size_standardized").cast("string"))
    
    return df

# COMMAND ----------

def main(df, delta_path=f"{silver}/dh_product_lookup"):
    print("Starting dh_product_lookup cleaning process...")
    print("Initial row count:", df.count())
    display(df.limit(5))

    # Step 1: Handle Missing Values
    df_step1 = handle_missing_values(df)
    print("\nStep 1 - Handle Missing Values")
    null_counts = df_step1.select([col(c).isNull().cast("int").alias(c) for c in df_step1.columns]).agg(
        *[pyspark_sum(col(c)).alias(f"{c}_null_count") for c in df_step1.columns]
    ).collect()[0].asDict()
    print(f"Null counts after handling: {null_counts}")
    display(df_step1.limit(5))

    # Step 2: Remove Duplicates
    df_step2 = remove_duplicates(df_step1)
    print("\nStep 2 - Remove Duplicates")
    initial_count = df_step1.count()
    final_count = df_step2.count()
    print(f"Initial row count: {initial_count}, Final row count: {final_count}, Duplicates removed: {initial_count - final_count}")
    display(df_step2.limit(5))

    # Step 3: Standardize Formats (split into sub-steps)
    df_step3a = standardize_product_description(df_step2)
    df_step3b = standardize_product_size(df_step3a)  # Note: Adds "product_size_standardized"
    df_step3c = standardize_commodity(df_step3b)
    print("\nStep 3 - Standardize Formats")
    print("Distinct commodity values:")
    display(df_step3c.select("commodity").distinct().limit(5))
    print("Sample product_size_standardized values:")
    display(df_step3c.select("product_size_standardized").distinct().limit(5))
    display(df_step3c.limit(5))

    # Step 4: Validate Data Types (no explicit outlier function in your list, skipping outlier step)
    df_cleaned = validate_data_types(df_step3c)
    print("\nStep 4 - Validate Data Types")
    invalid_upc_count = df_cleaned.filter(length(col("upc")) != 10).count()
    print(f"UPCs with incorrect length: {invalid_upc_count}")
    print("Schema after type enforcement:")
    df_cleaned.printSchema()
    display(df_cleaned.limit(5))

    print("\nCleaning process completed!")
    print(f"Final row count: {df_cleaned.count()}")
    display(df_cleaned.limit(5))

    # Delta Lake Merge
    if DeltaTable.isDeltaTable(spark, delta_path):
        print("\nDelta table exists, performing MERGE...")
        delta_table = DeltaTable.forPath(spark, delta_path)
        delta_table.alias("target")\
            .merge(
                df_cleaned.alias("source"),
                "target.upc = source.upc"
            )\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()
    else:
        print("\nDelta table does not exist, creating new table...")
        df_cleaned.write.format("delta")\
                    .mode("overwrite")\
                    .option("path", delta_path)\
                    .saveAsTable("carbo_catalog.silver.dh_product_lookup")

    # Load and log final state
    final_df = spark.read.format("delta").load(delta_path)
    print("\nCleaning and merge completed!")
    print(f"Final row count in Delta table: {final_df.count()}")
    display(final_df.limit(5))

    return final_df

# Execute
cleaned_df = main(df)
display(cleaned_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM carbo_catalog.silver.dh_product_lookup
