from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, count, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# -----------------------------
# Paramètres MySQL (désactivés pour conteneurisation sans MySQL)
# -----------------------------
# MYSQL_URL = "jdbc:mysql://localhost:3306/support"
# MYSQL_USER = "root"
# MYSQL_PASSWORD = "admin"
# MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"


# -----------------------------
# Session Spark
# -----------------------------
spark = (
    SparkSession.builder
    .appName("POC_ClientTickets_Batch")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# -----------------------------
# Schéma des tickets JSON
# -----------------------------
ticket_schema = StructType([
    StructField("ticket_id", IntegerType(), True),
    StructField("client_id", IntegerType(), True),
    StructField("created_at", StringType(), True),
    StructField("demande", StringType(), True),
    StructField("type_demande", StringType(), True),
    StructField("priorite", StringType(), True),
])

# -----------------------------
# Lecture batch depuis Redpanda/Kafka
# -----------------------------
raw_df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:19092")
    .option("subscribe", "client_tickets")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

# -----------------------------
# Conversion JSON
# -----------------------------
tickets_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) as json_value")
    .select(from_json(col("json_value"), ticket_schema).alias("data"))
    .select("data.*")
)

# -----------------------------
# Nettoyage / enrichissement
# -----------------------------
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

# -----------------------------
# Analyse
# -----------------------------
tickets_by_type_df = (
    tickets_enriched_df
    .groupBy("type_demande")
    .agg(count("*").alias("nb_tickets"))
)

# -----------------------------
# Affichage console
# -----------------------------
print("\n=== TICKETS ENRICHIS ===")
tickets_enriched_df.show(20, truncate=False)

print("\n=== NOMBRE DE TICKETS PAR TYPE ===")
tickets_by_type_df.show(truncate=False)

# -----------------------------
# Ecriture MySQL (désactivée pour conteneurisation sans MySQL)
# -----------------------------
# (
#     tickets_enriched_df.write
#     .format("jdbc")
#     .option("url", MYSQL_URL)
#     .option("dbtable", "tickets_enriched_batch")
#     .option("user", MYSQL_USER)
#     .option("password", MYSQL_PASSWORD)
#     .option("driver", MYSQL_DRIVER)
#     .mode("overwrite")
#     .save()
# )

# (
#     tickets_by_type_df.write
#     .format("jdbc")
#     .option("url", MYSQL_URL)
#     .option("dbtable", "tickets_by_type_batch")
#     .option("user", MYSQL_USER)
#     .option("password", MYSQL_PASSWORD)
#     .option("driver", MYSQL_DRIVER)
#     .mode("overwrite")
#     .save()
# )

# print("\nRésultats écrits dans MySQL :")
# print("- tickets_enriched_batch")
# print("- tickets_by_type_batch")

spark.stop()