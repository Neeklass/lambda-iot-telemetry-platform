import json
import time
import random
import logging
from datetime import datetime
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'machine_temps'

MACHINE_IDS = [
    'ROTOR_01', 'ROTOR_02', 'ROTOR_03',
    'TURBINE_01', 'TURBINE_02',
    'PUMP_01', 'PUMP_02'
]


def create_sensor_data(machine_id):
    return {
        'timestamp': int(time.time()),
        'machine_id': machine_id,
        'temperature': round(random.uniform(30.0, 90.0), 2)
    }


def main():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        
        logger.info(f"Kafka Producer erfolgreich verbunden mit {KAFKA_BROKER}")
        logger.info(f"Sende Daten an Topic: {TOPIC_NAME}")
        logger.info("Starte Sensor-Simulation...")
        
        while True:
            machine_id = random.choice(MACHINE_IDS)
            sensor_data = create_sensor_data(machine_id)
            future = producer.send(TOPIC_NAME, value=sensor_data)
            
            try:
                record_metadata = future.get(timeout=10)
                logger.info(
                    f"Gesendet: {sensor_data} -> "
                    f"Topic: {record_metadata.topic}, "
                    f"Partition: {record_metadata.partition}, "
                    f"Offset: {record_metadata.offset}"
                )
            except Exception as e:
                logger.error(f"Fehler beim Senden der Nachricht: {e}")
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Sensor-Simulation durch Benutzer beendet")
    except Exception as e:
        logger.error(f"Fehler im Producer: {e}")
    finally:
        if 'producer' in locals():
            producer.close()
            logger.info("Kafka Producer geschlossen")


if __name__ == "__main__":
    main()
