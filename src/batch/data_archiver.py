"""
Data Archiver - Batch Layer
Liest den kompletten Datenstrom von Kafka und archiviert die Rohdaten im Parquet-Format.
Dies ist Teil der Batch Layer in der Lambda Architecture für historische Analysen.
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType

# Logging-Konfiguration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka-Konfiguration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'machine_temps'

# Archiv-Konfiguration
ARCHIVE_PATH = 'data/archive'
CHECKPOINT_PATH = 'data/checkpoint/batch'

# Schema für die eingehenden Sensordaten
sensor_schema = StructType([
    StructField("timestamp", LongType(), False),
    StructField("machine_id", StringType(), False),
    StructField("temperature", FloatType(), False)
])


def create_spark_session():
    """
    Erstellt eine Spark Session mit Kafka-Integration für Batch Processing.
    
    Returns:
        SparkSession: Konfigurierte Spark Session
    """
    spark = SparkSession.builder \
        .appName("IoT-Data-Archiver-BatchLayer") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Session erfolgreich erstellt")
    return spark


def archive_stream(spark):
    """
    Liest alle Daten von Kafka und speichert sie im Parquet-Format für historische Analysen.
    Die Daten werden partitioniert nach Jahr/Monat/Tag gespeichert.
    
    Args:
        spark (SparkSession): Die Spark Session
    """
    logger.info(f"Verbinde mit Kafka Broker: {KAFKA_BROKER}")
    logger.info(f"Lese von Topic: {TOPIC_NAME}")
    logger.info(f"Archiviere Daten nach: {ARCHIVE_PATH}")
    
    # Kafka Stream lesen
    kafka_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()
    
    logger.info("Kafka Stream erfolgreich initialisiert")
    
    # JSON-Daten aus dem Kafka-Value-Feld parsen
    parsed_stream = kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), sensor_schema).alias("data")) \
        .select("data.*")
    
    # Füge Datums-Partitionierungsfelder hinzu (für effiziente Abfragen)
    # Konvertiere Unix-Timestamp zu Datum
    partitioned_stream = parsed_stream \
        .withColumn("date", col("timestamp").cast("timestamp")) \
        .withColumn("year", year(col("date"))) \
        .withColumn("month", month(col("date"))) \
        .withColumn("day", dayofmonth(col("date")))
    
    # Schreibe Stream als Parquet-Dateien
    # Partitioniert nach Jahr/Monat/Tag für optimale Performance bei historischen Abfragen
    query = partitioned_stream \
        .writeStream \
        .outputMode("append") \
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


def main():
    """
    Hauptfunktion: Initialisiert Spark und startet die Batch-Archivierung.
    """
    try:
        spark = create_spark_session()
        archive_stream(spark)
    except KeyboardInterrupt:
        logger.info("Batch-Archivierung durch Benutzer beendet")
    except Exception as e:
        logger.error(f"Fehler in der Batch-Archivierung: {e}", exc_info=True)
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark Session geschlossen")


if __name__ == "__main__":
    main()
