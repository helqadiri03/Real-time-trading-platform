# Architecture Overview

This document outlines the end-to-end architecture and implementation details for the real-time trading platform pipeline. The system was designed to handle high-throughput financial data natively, ensuring both real-time availability and fault-tolerant storage without data duplication.

## Components & Data Flow

1. **Trade Data Ingestion (Python Producer)**: 
   - A Python-based producer service connects to the **Finnhub WebSocket API** to receive live streaming trade data.
   - It parses these trades and immediately publishes them to the `trades` topic in a Kafka cluster.

2. **Message Broker (Apache Kafka + Zookeeper)**: 
   - **Kafka** acts as the central ingestion buffer and single source of truth for raw event data.
   - **Zookeeper** handles the cluster management for Kafka.
   - A **Redpanda Console** instance is attached to the Kafka broker to provide a user-friendly web interface for real-time monitoring of topics and messages.

3. **Stream Processing (Apache Flink / PyFlink)**: 
   - **JobManager & TaskManager**: The core Flink cluster infrastructure for distributed stream processing.
   - **Flink Job Submitter**: A dedicated, automated container service that eliminates manual job submission by programmatically launching the `flink_job.py` PyFlink script on startup.
   - **Processing Logic**: The PyFlink job subscribes to the Kafka `trades` topic, applies 1-minute `TumblingEventTimeWindows`, and calculates OHLC (Open, High, Low, Close) prices and total trading volume per symbol.
   - **Fault Tolerance & Deduplication**: The job uses a JDBC sink equipped with heavily customized Postgres SQL `ON CONFLICT (time, symbol) DO UPDATE` semantic logic to perform reliable upserts. This ensures that job restarts or lagging messages will absolutely never create duplicate candle data.

4. **Time-Series Storage (TimescaleDB / PostgreSQL)**: 
   - Selected for optimized time-series data storage. The standard PostgreSQL table is converted into a hypertable based on the event timestamp.
   - A strict `UNIQUE (time, symbol)` constraint operates at the schema level to reject duplicates and support the Flink upsert mechanism.

5. **Visualization (Grafana)**: 
   - The platform provides a dynamic **Trading Dashboard** populated directly from TimescaleDB.
   - **Automated Provisioning**: The dashboard and its data sources are loaded automatically using Grafana's provisioning configuration (`/etc/grafana/provisioning`). This avoids manual UI configuration while still permitting real-time dashboard editing and deletion modifications via the specific `dashboards.yaml` settings.
   - **Visualizations Included**: 
     - Real-time **Candlestick** charts mapped to Open, High, Low, and Close over time.
     - Stacked **Volume** histograms correlating directly with the candlestick periods.
     - **Current Price** indicators dynamically updating with the latest data points.

## System Resilience & Deployment

The entire architecture is containerized using **Docker Compose**, allowing the comprehensive distributed system to be started consistently on any environment with a single command. Robust health checks specifically gate the Kafka, Zookeeper, and Flink dependencies, ensuring components like the PyFlink producer or job auto-submitter only boot once their prerequisite services are fully operational.
