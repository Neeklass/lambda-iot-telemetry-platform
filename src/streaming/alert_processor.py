"""
Alert Processor - Spark Streaming
Verarbeitet eingehende Sensordaten aus Kafka in Echtzeit und filtert kritische Temperatur-Alerts.
Dies ist Teil der Speed Layer in der Lambda Architecture.
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
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
TEMPERATURE_THRESHOLD = 80.0

# Schema für die eingehenden Sensordaten
sensor_schema = StructType([
    StructField("timestamp", LongType(), False),
    StructField("machine_id", StringType(), False),
    StructField("temperature", FloatType(), False)
])


def create_spark_session():
    """
    Erstellt eine Spark Session mit Kafka-Integration.
    
    Returns:
        SparkSession: Konfigurierte Spark Session
    """
    spark = SparkSession.builder \
        .appName("IoT-Alert-Processor") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Session erfolgreich erstellt")
    return spark


def process_stream(spark):
    """
    Liest Daten von Kafka, filtert kritische Temperaturwerte und gibt Alerts aus.
    
    Args:
        spark (SparkSession): Die Spark Session
    """
    logger.info(f"Verbinde mit Kafka Broker: {KAFKA_BROKER}")
    logger.info(f"Lese von Topic: {TOPIC_NAME}")
    logger.info(f"Temperatur-Schwellwert für Alerts: {TEMPERATURE_THRESHOLD}°C")
    
    # Kafka Stream lesen
    kafka_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()
    
    logger.info("Kafka Stream erfolgreich initialisiert")
    
    # JSON-Daten aus dem Kafka-Value-Feld parsen
    parsed_stream = kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), sensor_schema).alias("data")) \
        .select("data.*")
    
    # Filter: Nur Temperaturen über dem Schwellwert
    alerts = parsed_stream.filter(col("temperature") > TEMPERATURE_THRESHOLD)
    
    # Ausgabe auf der Konsole
    query = alerts \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    logger.info("Stream-Verarbeitung gestartet - Warte auf Daten...")
    logger.info(f"Zeige alle Alerts mit Temperatur > {TEMPERATURE_THRESHOLD}°C")
    
    # Warte auf Beendigung
    query.awaitTermination()


def main():
    """
    Hauptfunktion: Initialisiert Spark und startet die Stream-Verarbeitung.
    """
    try:
        spark = create_spark_session()
        process_stream(spark)
    except KeyboardInterrupt:
        logger.info("Stream-Verarbeitung durch Benutzer beendet")
    except Exception as e:
        logger.error(f"Fehler in der Stream-Verarbeitung: {e}", exc_info=True)
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark Session geschlossen")


if __name__ == "__main__":
    main()
