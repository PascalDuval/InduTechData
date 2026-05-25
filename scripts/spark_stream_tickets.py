from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, when
from pyspark.sql.types import StructType, StringType, IntegerType

import os
os.environ["HADOOP_HOME"] = "C:/spark"
os.environ["spark.hadoop.fs.file.impl"] = "org.apache.hadoop.fs.LocalFileSystem"

# ==========================
# CONFIG COHERENTE PRODUCER
# ==========================
KAFKA_SERVER = "localhost:19092"
TOPIC = "client_tickets"

MYSQL_URL = "jdbc:mysql://localhost:3306/support?serverTimezone=UTC"
MYSQL_USER = "root"
MYSQL_PASSWORD = "admin"

CHECKPOINT = "C:/spark_checkpoint/tickets"

# ==========================
# SPARK SESSION (Windows safe)
# ==========================
spark = (
    SparkSession.builder
    .appName("tickets-stream")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.hadoop.io.native.lib.available", "false")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ==========================
# SCHEMA
# ==========================
schema = StructType() \
    .add("ticket_id", IntegerType()) \
    .add("client_id", IntegerType()) \
    .add("created_at", StringType()) \
    .add("demande", StringType()) \
    .add("type_demande", StringType()) \
    .add("priorite", StringType())

# ==========================
# READ FROM REDPANDA
# ==========================
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).alias("t"))
      .select("t.*")
)

final_df = (
    parsed
    .withColumn("created_ts", to_timestamp("created_at"))
    .withColumn(
        "support_team",
        when(col("type_demande") == "technique", "Equipe Tech")
        .when(col("type_demande") == "facturation", "Equipe Billing")
        .otherwise("Equipe Support")
    )
)

# ==========================
# WRITE TO MYSQL
# ==========================
def write_mysql(batch_df, batch_id):
    count = batch_df.count()
    print(f"Batch {batch_id} -> {count} rows")

    if count == 0:
        return

    batch_df.write \
        .format("jdbc") \
        .option("url", MYSQL_URL) \
        .option("dbtable", "tickets_enriched") \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()

# ==========================
# STREAM
# ==========================
query = (
    final_df.writeStream
    .foreachBatch(write_mysql)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .start()
)

# temporairement pour debug
# final_df.writeStream.format("console").start()

query.awaitTermination()