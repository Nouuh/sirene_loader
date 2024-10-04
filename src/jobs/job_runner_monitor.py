from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import os
from pathlib import Path

current_path = os.path.dirname(os.path.abspath(__file__))
project_path = Path(__file__).resolve().parent.parent.parent
spark = SparkSession.builder.appName("sirene_loader") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
                .getOrCreate()

#read the data_src uploaded from opendatasoft
df_source = spark.read.format("delta")\
                .load(f"{project_path}/data/delta")

# Process the data

df_source.groupBy.limit(10).show()
