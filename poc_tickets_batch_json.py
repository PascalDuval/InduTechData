import os
import re
from pathlib import Path


def _bootstrap_env():
    project_dir = Path(__file__).resolve().parent
    venv_python = project_dir / "venv" / "Scripts" / "python.exe"
    if venv_python.exists():
        os.environ.setdefault("PYSPARK_PYTHON", str(venv_python))
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", str(venv_python))

    # Always prefer PySpark-bundled Spark (avoids mismatch with system Spark)
    try:
        import pyspark  # no JVM start on import

        pyspark_home = Path(pyspark.__file__).resolve().parent
        if (pyspark_home / "jars").exists():
            os.environ["SPARK_HOME"] = str(pyspark_home)
            spark_home = str(pyspark_home)
        else:
            spark_home = None
    except Exception:
        spark_home = None

    if not spark_home:
        spark_home = os.environ.get("SPARK_HOME")
        if spark_home and not Path(spark_home).exists():
            print("WARN: SPARK_HOME is set but does not exist:", spark_home)
            spark_home = None
            os.environ.pop("SPARK_HOME", None)

    if not spark_home:
        candidates = [
            r"C:\spark",
        ]
        for candidate in candidates:
            if Path(candidate).exists():
                os.environ["SPARK_HOME"] = candidate
                spark_home = candidate
                break

    # Java is now handled by Dockerfile, skip local detection
    java_home = os.environ.get("JAVA_HOME")
    if not java_home:
        print("INFO: Using container Java installation")

    if spark_home:
        spark_bin = str(Path(spark_home) / "bin")
        if spark_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = spark_bin + os.pathsep + os.environ.get("PATH", "")
        jars_dir = Path(spark_home) / "jars"
        if not jars_dir.exists():
            print("ERROR: SPARK_HOME exists but jars directory is missing.")
            print("Check your Spark installation at:", spark_home)
            raise SystemExit(1)
    if not spark_home:
        print("ERROR: SPARK_HOME could not be resolved.")
        print("Install Spark or install PySpark in the venv, then retry.")
        raise SystemExit(1)

    # Optional Hadoop native on Windows
    hadoop_home = os.environ.get("HADOOP_HOME")
    if hadoop_home and Path(hadoop_home).exists():
        hadoop_bin = str(Path(hadoop_home) / "bin")
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")


_bootstrap_env()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, count, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# -----------------------------
# Paramètres MySQL (désactivés pour containerisation sans MySQL)
# -----------------------------
# MYSQL_URL = "jdbc:mysql://localhost:3306/support"
# MYSQL_USER = "root"
# MYSQL_PASSWORD = "admin"
# MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"

# -----------------------------
# Session Spark
# -----------------------------
def _detect_scala_binary_version(pyspark_home: Path) -> str:
    jars_dir = pyspark_home / "jars"
    for jar in jars_dir.glob("scala-library-*.jar"):
        m = re.search(r"scala-library-(\d+\.\d+)", jar.name)
        if m:
            return m.group(1)
    return "2.12"


def _detect_spark_version(pyspark_home: Path) -> str | None:
    jars_dir = pyspark_home / "jars"
    for jar in jars_dir.glob("spark-sql_*.jar"):
        m = re.search(r"spark-sql_[\d.]+-(\d+\.\d+\.\d+)", jar.name)
        if m:
            return m.group(1)
    return None


pyspark_home = Path(pyspark.__file__).resolve().parent
spark_version = _detect_spark_version(pyspark_home) or pyspark.__version__
scala_binary = _detect_scala_binary_version(pyspark_home)

if spark_version.startswith("4."):
    print("ERROR: Spark 4.x detected:", spark_version)
    print("Kafka connector for Spark 4.x is not available on Maven Central.")
    print("Fix: install PySpark 3.5.1 in the venv (pip install pyspark==3.5.1)")
    print("or use a Spark 3.5.x distribution.")
    raise SystemExit(1)

kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_{scala_binary}:{spark_version}"
token_provider_pkg = f"org.apache.spark:spark-token-provider-kafka-0-10_{scala_binary}:{spark_version}"
# mysql_pkg = "mysql:mysql-connector-java:8.0.33"  # Désactivé pour conteneurisation sans MySQL

spark = (
    SparkSession.builder
    .appName("POC_ClientTickets_Batch")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.hadoop.io.nativeio.NativeIO.disable", "true")
    .config(
        "spark.jars.packages",
        f"{kafka_pkg},{token_provider_pkg}",  # MySQL package retiré
    )
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
kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19093")

raw_df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
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
tickets_enriched_df.show(20)

print("\n=== NOMBRE DE TICKETS PAR TYPE ===")
tickets_by_type_df.show(truncate=False)

# Export fichiers
output_base = os.getenv("OUTPUT_BASE", "/app/output")
os.makedirs(output_base, exist_ok=True)
parquet_path = f"{output_base}/tickets_enriched_parquet"
json_path = f"{output_base}/tickets_by_type_json"
os.makedirs(parquet_path, exist_ok=True)
os.makedirs(json_path, exist_ok=True)
json_file = os.path.join(json_path, "part-00000.json")
parquet_file = os.path.join(parquet_path, "part-00000.parquet")

def _fallback_json_write(df, target_dir):
    import pandas as pd  # noqa: F401

    os.makedirs(target_dir, exist_ok=True)
    pdf = df.toPandas()
    pdf.to_json(os.path.join(target_dir, "part-00000.json"), orient="records", lines=True, force_ascii=False)


def _fallback_parquet_write(df, target_dir):
    import pandas as pd  # noqa: F401
    import pyarrow  # noqa: F401

    os.makedirs(target_dir, exist_ok=True)
    pdf = df.toPandas()
    pdf.to_parquet(os.path.join(target_dir, "part-00000.parquet"), index=False)


def _fallback_csv_write(df, target_dir):
    import pandas as pd  # noqa: F401

    os.makedirs(target_dir, exist_ok=True)
    pdf = df.toPandas()
    pdf.to_csv(os.path.join(target_dir, "part-00000.csv"), index=False)


def _export_json(df, target_dir):
    os.makedirs(target_dir, exist_ok=True)
    try:
        df.write.mode("overwrite").json(target_dir)
        print("JSON export OK:", target_dir)
    except Exception as exc:
        print("ERROR: JSON export failed:", exc)
        print("Fallback: writing JSON with pandas...")
        _fallback_json_write(df, target_dir)


def _export_parquet(df, target_dir):
    os.makedirs(target_dir, exist_ok=True)
    try:
        df.write.mode("overwrite").parquet(target_dir)
        print("Parquet export OK:", target_dir)
    except Exception as exc:
        print("ERROR: Parquet export failed:", exc)
        print("Fallback: writing Parquet with pandas...")
        try:
            _fallback_parquet_write(df, target_dir)
        except Exception as inner_exc:
            print("ERROR: Parquet fallback failed:", inner_exc)


def _export_csv(df, target_dir):
    os.makedirs(target_dir, exist_ok=True)
    try:
        df.write.mode("overwrite").option("header", "true").csv(target_dir)
        print("CSV export OK:", target_dir)
    except Exception as exc:
        print("ERROR: CSV export failed:", exc)
        print("Fallback: writing CSV with pandas...")
        _fallback_csv_write(df, target_dir)

try:
    _export_json(tickets_by_type_df, json_path)
except Exception as exc:
    print("\nERROR: JSON export failed:", exc)

try:
    _export_parquet(tickets_enriched_df, parquet_path)
except Exception as exc:
    print("ERROR: Parquet export failed:", exc)

print("\nFichiers présents :")
print("- json dir:", os.path.exists(json_path), "file:", os.path.exists(json_file))
print("- parquet dir:", os.path.exists(parquet_path), "file:", os.path.exists(parquet_file))

# Analyses supplementaires
tickets_by_priority_df = tickets_enriched_df.groupBy("priorite").agg(count("*").alias("nb_tickets"))
tickets_by_team_df = tickets_enriched_df.groupBy("equipe_support").agg(count("*").alias("nb_tickets"))
tickets_by_day_df = tickets_enriched_df.groupBy(to_date(col("created_at_ts")).alias("jour")).agg(count("*").alias("nb_tickets"))
top_clients_df = (
    tickets_enriched_df.groupBy("client_id")
    .agg(count("*").alias("nb_tickets"))
    .orderBy(col("nb_tickets").desc())
    .limit(10)
)

export_analysis_base = f"{output_base}/analyses"
by_priority_json = f"{export_analysis_base}/tickets_by_priority_json"
by_team_json = f"{export_analysis_base}/tickets_by_team_json"
by_day_csv = f"{export_analysis_base}/tickets_by_day_csv"
top_clients_csv = f"{export_analysis_base}/top_clients_csv"

_export_json(tickets_by_priority_df, by_priority_json)
_export_json(tickets_by_team_df, by_team_json)
_export_csv(tickets_by_day_df, by_day_csv)
_export_csv(top_clients_df, top_clients_csv)

# Ecriture MySQL (désactivée dans la version conteneurisée)
# try:
#     (
#         tickets_enriched_df.write
#         .format("jdbc")
#         .option("url", MYSQL_URL)
#         .option("dbtable", "tickets_enriched_batch")
#         .option("user", MYSQL_USER)
#         .option("password", MYSQL_PASSWORD)
#         .option("driver", MYSQL_DRIVER)
#         .mode("overwrite")
#         .save()
#     )
#
#     (
#         tickets_by_type_df.write
#         .format("jdbc")
#         .option("url", MYSQL_URL)
#         .option("dbtable", "tickets_by_type_batch")
#         .option("user", MYSQL_USER)
#         .option("password", MYSQL_PASSWORD)
#         .option("driver", MYSQL_DRIVER)
#         .mode("overwrite")
#         .save()
#     )
#
#     print("\nRésultats écrits dans MySQL :")
#     print("- tickets_enriched_batch")
#     print("- tickets_by_type_batch")
# except Exception as exc:
#     print("\nERROR: MySQL write failed:", exc)

spark.stop()

