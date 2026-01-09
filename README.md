# lambda-iot-telemetry-platform
A Python-based Big Data platform implementing a Lambda Architecture for real-time industrial sensor monitoring using Apache Kafka, PySpark, and Hadoop HDFS.

## Architecture Components

**Speed Layer (Real-time)**
- Apache Kafka: Message broker for sensor data ingestion
- PySpark Streaming: Real-time alert processing for critical temperatures

**Batch Layer (Historical)**
- Hadoop HDFS: Distributed storage for long-term data retention
- PySpark Batch: Parquet-based archival with year/month/day partitioning

**Serving Layer (Visualization)**
- Grafana: Dashboards for real-time monitoring and historical analysis

## Web Interfaces

**Grafana Dashboard:**
- http://localhost:3000
- Login: admin/admin

**Spark Master Web UI:**
- http://localhost:8080

**Spark Worker Web UI:**
- http://localhost:8081

**HDFS NameNode Web UI:**
- http://localhost:9870

**API Access Only:**
- Kafka (Port 9092)
- Zookeeper (Port 2181)
- HDFS (Port 9000)

## Usage

Start all services:
```bash
docker-compose up -d
```

Run sensor simulator:
```bash
python src/producer/sensor_simulator.py
```

Run alert processor (Speed Layer):
```bash
spark-submit --master spark://localhost:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  src/streaming/alert_processor.py
```

Run data archiver (Batch Layer):
```bash
spark-submit --master spark://localhost:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  src/batch/data_archiver.py
```
