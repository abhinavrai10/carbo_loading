# Databricks notebook source
silver = spark.sql(''' describe external location `silver` ''').select('url').collect()[0][0]
gold_path = spark.sql(''' describe external location `gold` ''').select('url').collect()[0][0]

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, when
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prototype Dimension Table Class

# COMMAND ----------

class DimensionTable:
    def __init__(self, dim_name, columns, id_column, silver_path, gold_base_path=gold_path):
        self.dim_name = dim_name.lower()
        self.silver_path = silver_path  # Full Delta table path, e.g., "DELTA.`{silver}/dh_transactions`"
        self.gold_dim_path = f"{gold_base_path}/dim_{self.dim_name}"
        self.table_name = f"`carbo_catalog`.`gold`.dim_{self.dim_name}"
        self.columns = ", ".join(columns)  # String for SQL
        self.column_list = columns  # List for column selection
        self.id_column = id_column  # Natural key for joining
        self.spark = SparkSession.builder.getOrCreate()

    def source_data(self):
        """Extract distinct source data from Silver layer."""
        try:
            df = self.spark.sql(f"SELECT DISTINCT {self.columns} FROM {self.silver_path}").alias("src")
            return df
        except Exception as e:
            print(f"Source read failed for {self.dim_name}: {e}")  # Changed from self.logger.error
            raise

    def is_incremental(self):
        """Check if the dimension table already exists."""
        return self.spark.catalog.tableExists(self.table_name)

    def sink_data(self):
        """Retrieve existing sink data or create an empty DataFrame."""
        if self.is_incremental():
            return self.spark.sql(f"SELECT * FROM {self.table_name}").alias("sink")
        else:
            query = f"SELECT {self.columns} FROM {self.silver_path} WHERE 1=0"
            return self.spark.sql(query).alias("sink")

    def new_data(self):
        """Identify new records not in the sink."""
        src_df = self.source_data()
        sink_df = self.sink_data()
        return src_df.join(
            sink_df.select(self.id_column),
            on=[self.id_column],
            how="left_anti"
        ).select([col(f"src.{c}") for c in self.column_list])

    def final_data(self):
        """Prepare final data (new records only for simplicity)."""
        df_new = self.new_data()
        if df_new.count() == 0:
            print(f"No new data for {self.dim_name}")  
            return None
        return df_new

    def create_surrogate_key(self, df):
        """Add a surrogate key based on the id_column."""
        window_spec = Window.orderBy(self.id_column)
        return df.withColumn(f"{self.dim_name}_key", row_number().over(window_spec))

    def write_to_deltalake(self, df):
        """Write data to Delta Lake with merge or initial load."""
        try:
            if df is None:
                print(f"Skipping write for {self.dim_name} - no data")  
                return

            df_final = self.create_surrogate_key(df)
            if self.is_incremental():
                delta_table = DeltaTable.forPath(self.spark, self.gold_dim_path)
                merge_condition = f"target.{self.id_column} = source.{self.id_column}"
                delta_table.alias("target").merge(
                    df_final.alias("source"),
                    merge_condition
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                print(f"Incremental update for {self.gold_dim_path}")  
            else:
                df_final.write.format("delta").mode("overwrite").save(self.gold_dim_path)
                self.spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {self.table_name}
                    USING DELTA
                    LOCATION '{self.gold_dim_path}'
                """)
                print(f"Initial load for {self.gold_dim_path}")  
            display(df_final)
        except Exception as e:
            print(f"Write failed for {self.dim_name}: {e}") 
            raise

    def run(self):
        """Execute the pipeline."""
        print(f"Starting pipeline for {self.dim_name}")  
        df_final = self.final_data()
        if df_final is None:
            print(f"No new data to process for {self.dim_name}")  
            return
        self.write_to_deltalake(df_final)
        print(f"Pipeline completed for {self.dim_name}")    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Dimension

# COMMAND ----------

def create_time_dim():
    silver_path = f"DELTA.`{silver}dh_transactions`"
    columns = ["day", "week", "time_of_transaction"]
    time_dim = DimensionTable(
        dim_name="time",
        columns=columns,
        id_column="day",  # Natural key
        silver_path=silver_path  # Use the correct table path with underscores
    )
    time_dim.run()

create_time_dim()

spark.sql("SELECT * FROM `carbo_catalog`.`gold`.dim_time")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Dimennsion

# COMMAND ----------

def create_product_dim():
    silver_path = f"DELTA.`{silver}dh_product_lookup`"
    columns = ["upc", "product_description", "commodity", "brand", "product_size"]
    product_dim = DimensionTable(
        dim_name="product",
        columns=columns,
        id_column="upc",
        silver_path=silver_path
    )
    product_dim.run()
create_product_dim()

spark.sql("SELECT * FROM `carbo_catalog`.`gold`.dim_product")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Household Dimension

# COMMAND ----------

def create_household_dim():
    silver_path = f"DELTA.`{silver}dh_transactions`"
    columns = ["household", "geography"]
    household_dim = DimensionTable(
        dim_name="household",
        columns=columns,
        id_column="household",
        silver_path=silver_path
    )
    household_dim.run()

create_household_dim()

spark.sql("SELECT * FROM `carbo_catalog`.`gold`.dim_household")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Store Dimension

# COMMAND ----------

def create_store_dim():
    silver_path = f"DELTA.`{silver}dh_transactions`"
    columns = ["store", "geography"]
    store_dim = DimensionTable(
        dim_name="store",
        columns=columns,
        id_column="store",
        silver_path=silver_path
    )
    store_dim.run()

create_store_dim()

spark.sql("SELECT * FROM `carbo_catalog`.`gold`.dim_store")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trade Dimension

# COMMAND ----------

def create_trade_dim():
    silver_path = f"DELTA.`{silver}dh_causal_lookup`"
    columns = ["upc", "store", "week", "feature_desc", "display_desc", "geography"]
    trade_dim = DimensionTable(
        dim_name="trade",
        columns=columns,
        id_column="upc",  # Using UPC as primary natural key
        silver_path=silver_path
    )
    trade_dim.run()

create_trade_dim()

spark.sql("SELECT * FROM `carbo_catalog`.`gold`.dim_trade")

# COMMAND ----------

df = spark.sql("SELECT * FROM `carbo_catalog`.`gold`.dim_product")
df.display()

# COMMAND ----------

spark.sql("SELECT * FROM `carbo_catalog`.`gold`.dim_product")
