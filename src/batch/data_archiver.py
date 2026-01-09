import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = 'kafka:9092'
TOPIC_NAME = 'machine_temps'

ARCHIVE_PATH = 'hdfs://namenode:9000/iot/batch/archive'
CHECKPOINT_PATH = 'hdfs://namenode:9000/spark/checkpoint/batch'

sensor_schema = StructType([
    StructField("timestamp", LongType(), False),
    StructField("machine_id", StringType(), False),
    StructField("temperature", FloatType(), False)
])


def create_spark_session():
    spark = SparkSession.builder \
        .appName("IoT-Data-Archiver-BatchLayer") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Session erfolgreich erstellt (Cluster-Modus mit HDFS)")
    return spark


def archive_stream(spark):
    """
    Liest alle Daten von Kafka und speichert sie im Parquet-Format für historische Analysen.
    Die Daten werden partitioniert nach Jahr/Monat/Tag gespeichert.
    
    Args:
        spark (SparkSession): Die Spark Session
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
    
    logger.info("Kafka Stream erfolgreich initialisiert")
    
    # JSON-Daten aus dem Kafka-Value-Feld parsen
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), sensor_schema).alias("data")) \
        .select("data.*")
    
    # Füge Datums-Partitionierungsfelder hinzu (für effiziente Abfragen)
        .withColumn("date", col("timestamp").cast("timestamp")) \
        .withColumn("year", year(col("date"))) \
        .withColumn("month", month(col("date"))) \
        .withColumn("day", dayofmonth(col("date")))
    
    # Schreibe Stream als Parquet-Dateien
    # Partitioniert nach Jahr/Monat/Tag für optimale Performance bei historischen Abfragen
    query = partitioned_stream \
        .format("parquet") \
        .option("path", ARCHIVE_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .partitionBy("year", "month", "day") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    logger.info("Batch Layer Archivierung gestartet")
    logger.info(f"Daten werden im Parquet-Format gespeichert: {ARCHIVE_PATH}")
    logger.info("Partitionierung: Jahr/Monat/Tag für optimierte historische Abfragen")
    logger.info("Trigger: Alle 10 Sekunden werden neue Micro-Batches verarbeitet")
    
    # Warte auf Beendigung
    query.awaitTermination()


def query.awaitTermination()


def main():ept KeyboardInterrupt:
        logger.info("Batch-Archivierung durch Benutzer beendet")
    except Exception as e:
        logger.error(f"Fehler in der Batch-Archivierung: {e}", exc_info=True)
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark Session geschlossen")


if __name__ == "__main__":
    main()
