from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, count, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


spark = (
    SparkSession.builder
    .appName("ClientTicketsStreaming")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config(
        "spark.sql.streaming.checkpointFileManagerClass",
        "org.apache.spark.sql.execution.streaming.FileSystemBasedCheckpointFileManager"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

ticket_schema = StructType([
    StructField("ticket_id", IntegerType(), True),
    StructField("client_id", IntegerType(), True),
    StructField("created_at", StringType(), True),
    StructField("demande", StringType(), True),
    StructField("type_demande", StringType(), True),
    StructField("priorite", StringType(), True),
])

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:19092")
    .option("subscribe", "client_tickets")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_value")

tickets_df = (
    json_df
    .select(from_json(col("json_value"), ticket_schema).alias("data"))
    .select("data.*")
)

tickets_enriched_df = (
    tickets_df
    .withColumn("created_at_ts", to_timestamp(col("created_at")))
    .withColumn(
        "equipe_support",
        when(col("type_demande") == "technique", "Equipe Technique")
        .when(col("type_demande") == "facturation", "Equipe Facturation")
        .when(col("type_demande") == "compte", "Equipe Compte")
        .otherwise("Equipe Generale")
    )
)

tickets_by_type_df = (
    tickets_enriched_df
    .groupBy("type_demande")
    .agg(count("*").alias("nb_tickets"))
)

query_enriched = (
    tickets_enriched_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 20)
    .option("checkpointLocation", "C:/tmp/chk_tickets_enriched")
    .start()
)

query_by_type = (
    tickets_by_type_df.writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate", "false")
    .option("checkpointLocation", "C:/tmp/chk_tickets_by_type")
    .start()
)

query_enriched.awaitTermination()
query_by_type.awaitTermination()