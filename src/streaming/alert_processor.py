import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = 'kafka:9092'
TOPIC_NAME = 'machine_temps'
TEMPERATURE_THRESHOLD = 80.0

sensor_schema = StructType([
    StructField("timestamp", LongType(), False),
    StructField("machine_id", StringType(), False),
    StructField("temperature", FloatType(), False)
])


def create_spark_session():
    spark = SparkSession.builder \
        .appName("IoT-Alert-Processor") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", "hdfs://namenode:9000/spark/checkpoint/alerts") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Session erfolgreich erstellt (Cluster-Modus)")
    return spark


def process_stream(spark):
    """
    Liest Daten von Kafka, filtert kritische Temperaturwerte und gibt Alerts aus.
    
    Args:
        spark (SparkSession): Die Spark Session
    """
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .load()
    
    logger.info("Kafka Stream erfolgreich initialisiert")
    
    # JSON-Daten aus dem Kafka-Value-Feld parsen
    parsed_stream = kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), sensor_schema).alias("data")) \
        .select("data.*")
    
    # Filter: Nur Temperaturen über dem Schwellwert
    alerts = parsed_stream.filter(col("temperature") > TEMPERATURE_THRESHOLD)
    
        .outputMode("append") \
        .format("parquet") \
        .option("path", "hdfs://namenode:9000/iot/alerts") \
        .option("checkpointLocation", "hdfs://namenode:9000/spark/checkpoint/alerts") \
        .trigger(processingTime="5 seconds") \
        .start()
    
    # Ausgabe auf der Konsole für Monitoring
    console_query = alerts \
        .writeStream \
        .outputMode("append") \
        .option("truncate", "false") \
        .start()
    
    logger.info("Stream-Verarbeitung gestartet - Warte auf Daten...")
    logger.info(f"Zeige alle Alerts mit Temperatur > {TEMPERATURE_THRESHOLD}°C")
    logger.info("Alerts werden auf HDFS gespeichert: hdfs://namenode:9000/iot/alerts")
    
    # Warte auf Beendigung beider Queries
    hdfs_query.awaitTermination()


def hdfs_query.awaitTermination()


def main():ept KeyboardInterrupt:
        logger.info("Stream-Verarbeitung durch Benutzer beendet")
    except Exception as e:
        logger.error(f"Fehler in der Stream-Verarbeitung: {e}", exc_info=True)
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark Session geschlossen")


if __name__ == "__main__":
    main()
